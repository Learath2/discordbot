use std::env;
use std::net::AddrParseError;
use std::num::ParseIntError;

use std::error::Error as StdError;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use atomic::Atomic;

use futures::stream::StreamExt;

use tokio::select;
use tracing::Instrument;
use tracing::{debug, error, info, info_span, instrument, warn};
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

use twilight_cache_inmemory::{InMemoryCache, ResourceType};
use twilight_gateway::{
    cluster::{Cluster, ShardScheme},
    Event,
};
use twilight_http::request::channel::message::create_message::CreateMessageError;
use twilight_http::Client as TwHttpClient;
use twilight_model::channel::Message;
use twilight_model::gateway::Intents;
use twilight_model::id::MessageId;
use twilight_model::id::{ChannelId, GuildId, RoleId, UserId};

use sqlx::sqlite::SqlitePool;
use sqlx::sqlite::SqlitePoolOptions;

use reqwest::{Client as HttpClient, Url};

use std::time::Duration;

mod ddnet;
use ddnet::Error as DDNetError;

mod ban;
mod mt;
use mt::Error as MTError;
mod lexer;
mod util;

#[derive(Debug)]
pub struct Config {
    pub paste_service: Option<String>,
    pub discord_token: String,
    pub database_url: String,
    pub ddnet_token: String,
    pub ddnet_ban_endpoint: String,
    pub ddnet_regions: Vec<String>,
    pub ddnet_guild: GuildId,
    pub ddnet_moderator_channels: Vec<ChannelId>,
    pub ddnet_admin_role: RoleId,
    pub ddnet_moderator_role: RoleId,

    pub ddnet_mt_tester_role: RoleId,
    pub ddnet_mt_tester_lead_role: RoleId,
    pub ddnet_mt_sub_channel: ChannelId,
    pub ddnet_mt_info_channel: ChannelId,
    pub ddnet_mt_active_cat: ChannelId,
    pub ddnet_mt_waiting_cat: ChannelId,
    pub ddnet_mt_evaluated_cat: ChannelId,
}

#[derive(Debug)]
pub struct Context {
    pub alive: AtomicBool,
    pub discord_http: TwHttpClient,
    pub http_client: HttpClient,
    pub sql_pool: SqlitePool,

    pub bot_id: Atomic<UserId>,
}

fn get_config_from_env() -> Config {
    dotenv::dotenv().expect("Couldn't load .env");
    let mut config = Config {
        paste_service: env::var("PASTE_SERVICE").ok(),
        discord_token: env::var("DISCORD_TOKEN").expect("DISCORD_TOKEN is missing"),
        database_url: env::var("DATABASE_URL").expect("DATABASE_URL is missing"),
        ddnet_token: env::var("DDNET_TOKEN").expect("DDNET_TOKEN is missing"),
        ddnet_ban_endpoint: env::var("DDNET_BAN_ENDPOINT").expect("DDNET_BAN_ENDPOINT is missing"),
        ddnet_regions: vec![],
        ddnet_guild: env::var("DDNET_GUILD")
            .expect("DDNET_GUILD is missing")
            .parse::<u64>()
            .expect("DDNET_GUILD is malformed")
            .into(),
        ddnet_moderator_channels: vec![],
        ddnet_admin_role: env::var("DDNET_ADMIN_ROLE")
            .expect("DDNET_ADMIN_ROLE is missing")
            .parse::<u64>()
            .expect("DDNET_ADMIN_ROLE is malformed")
            .into(),
        ddnet_moderator_role: env::var("DDNET_MODERATOR_ROLE")
            .expect("DDNET_MODERATOR_ROLE is missing")
            .parse::<u64>()
            .expect("DDNET_MODERATOR_ROLE is malformed")
            .into(),
        ddnet_mt_tester_role: env::var("DDNET_MT_ROLE_TESTER")
            .expect("DDNET_MT_ROLE_TESTER is missing")
            .parse::<u64>()
            .expect("DDNET_MT_ROLE_TESTER is malformed")
            .into(),
        ddnet_mt_tester_lead_role: env::var("DDNET_MT_ROLE_TESTER_LEAD")
            .expect("DDNET_MT_ROLE_TESTER_LEAD is missing")
            .parse::<u64>()
            .expect("DDNET_MT_ROLE_TESTER_LEAD is malformed")
            .into(),
        ddnet_mt_sub_channel: env::var("DDNET_MT_CHN_SUB")
            .expect("DDNET_MT_CHN_SUB is missing")
            .parse::<u64>()
            .expect("Malformed channel id in DDNET_MT_CHN_SUB")
            .into(),
        ddnet_mt_info_channel: env::var("DDNET_MT_CHN_INFO")
            .expect("DDNET_MT_CHN_INFO is missing")
            .parse::<u64>()
            .expect("Malformed channel id in DDNET_MT_CHN_INFO")
            .into(),
        ddnet_mt_active_cat: env::var("DDNET_MT_CAT_ACTIVE")
            .expect("DDNET_MT_CAT_ACTIVE is missing")
            .parse::<u64>()
            .expect("Malformed channel id in DDNET_MT_CAT_ACTIVE")
            .into(),
        ddnet_mt_waiting_cat: env::var("DDNET_MT_CAT_WAITING")
            .expect("DDNET_MT_CAT_WAITING is missing")
            .parse::<u64>()
            .expect("Malformed channel id in DDNET_MT_CAT_WAITING")
            .into(),
        ddnet_mt_evaluated_cat: env::var("DDNET_MT_CAT_EVALUATED")
            .expect("DDNET_MT_CAT_EVALUATED is missing")
            .parse::<u64>()
            .expect("Malformed channel id in DDNET_MT_CAT_EVALUATED")
            .into(),
    };

    if let Some(ref u) = config.paste_service {
        match Url::parse(&u) {
            Err(_) => {
                warn!("PASTE_SERVICE malformed, disabling pastes");
                config.paste_service = None;
            }
            Ok(u) => {
                if u.scheme() != "https" && u.scheme() != "http" {
                    warn!(
                        "PASTE_SERVICE has weird scheme {}, disabling pastes",
                        u.scheme()
                    );
                    config.paste_service = None;
                }
            }
        }
    } else {
        warn!("PASTE_SERVICE missing, disabling pastes");
    }

    Url::parse(&config.ddnet_ban_endpoint).expect("DDNET_BAN_ENDPOINT malformed");

    let regions = env::var("DDNET_REGIONS").expect("DDNET_REGIONS is missing");
    config.ddnet_regions = regions.split(',').map(String::from).collect();
    if config.ddnet_regions.iter().any(|r| r.len() != 3) {
        panic!("Invalid region in DDNET_REGIONS");
    }

    let mod_channels =
        env::var("DDNET_MODERATOR_CHANNELS").expect("DDNET_MODERATOR_CHANNELS is missing");
    config.ddnet_moderator_channels = mod_channels
        .split(',')
        .map(|s| {
            s.parse::<u64>()
                .expect("Malformed channel id in DDNET_MODERATOR_CHANNELS")
                .into()
        })
        .collect();

    config
}

#[tokio::main]
async fn main() {
    let env_filter_layer = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("{}=info", env!("CARGO_CRATE_NAME"))));
    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(env_filter_layer)
        .with_thread_ids(true)
        .finish()
        .init();
    info!("Starting");

    let config = Arc::new(get_config_from_env());
    debug!(?config);

    let cluster = Cluster::builder(&config.discord_token, Intents::GUILD_MESSAGES)
        .shard_scheme(ShardScheme::Auto)
        .build()
        .await
        .expect("Couldn't build cluster");

    let cluster_spawn = cluster.clone();
    tokio::spawn(async move {
        cluster_spawn.up().await;
    });

    let discord_http = TwHttpClient::new(&config.discord_token);
    let cache = InMemoryCache::builder()
        .resource_types(ResourceType::MESSAGE)
        .build();

    info!("Connecting to database: {}", config.database_url);
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&config.database_url)
        .await
        .expect("Failed to connect to db");

    info!("Running migrations");
    sqlx::migrate!()
        .run(&pool)
        .await
        .expect("Failed to run migrations");
    info!("Done");

    let context = Arc::new(Context {
        alive: AtomicBool::new(true),
        discord_http,
        http_client: HttpClient::new(),
        sql_pool: pool,
        bot_id: Atomic::<UserId>::new(0.into()),
    });

    tokio::spawn(ban::handle_expiries(context.clone(), config.clone()));

    let context_cc = context.clone();
    tokio::spawn(async move {
        if select! {
            _ = async {
                while context_cc.alive.load(Ordering::Relaxed) { tokio::time::sleep(Duration::from_secs(1)).await; };
            } => { false }
            _ = tokio::signal::ctrl_c() => { true }
        } {
            info!("Received ^C: Cleaning up");
            context_cc.alive.store(false, Ordering::Relaxed);
        }
    }.instrument(info_span!("^C")));

    let mut events = cluster.events();
    loop {
        let context_el = context.clone();
        let m = select! {
            _ = async move { while context_el.alive.load(Ordering::Relaxed) { tokio::time::sleep(Duration::from_secs(1)).await; };} => { Err(()) }
            e = events.next() => { Ok(e)}
        };

        let (_shard_id, event) = match m {
            Ok(e) => match e {
                Some(e) => e,
                None => {
                    continue;
                }
            },
            Err(()) => {
                break;
            }
        };

        if !context.alive.load(Ordering::Relaxed) {
            break;
        }
        cache.update(&event);

        match event {
            Event::MessageCreate(msg) => {
                debug!(?msg);
                tokio::spawn(handle_message(msg.0, context.clone(), config.clone()));
            }
            Event::Ready(r) => {
                info!("Connected and ready with name: {}", r.user.name);
                context.bot_id.store(r.user.id, Ordering::SeqCst);
            }
            _ => {}
        }
    }
}
pub struct Target(ChannelId, MessageId);
impl From<&Message> for Target {
    fn from(m: &Message) -> Self {
        Self(m.channel_id, m.id)
    }
}

pub async fn reply<T: Into<Target>>(
    target: T,
    message: &str,
    context: Arc<Context>,
) -> Result<Message, Box<dyn StdError>> {
    let target: Target = target.into();
    context
        .discord_http
        .create_message(target.0)
        .reply(target.1)
        .content(message)
        .map_err(Box::new)?
        .await
        .map_err(|e| -> Box<dyn StdError> { Box::new(e) })
}

#[instrument(skip(message, config, context), fields(caller, message = &message.content.as_str()))]
async fn handle_message(
    message: Message,
    context: Arc<Context>,
    config: Arc<Config>,
) -> Result<(), Box<dyn StdError + Send + Sync>> {
    let caller = format!("{}#{}", message.author.name, message.author.discriminator);
    tracing::Span::current().record("caller", &caller.as_str());

    if message.guild_id != Some(config.ddnet_guild)
        || message.author.id == context.bot_id.load(Ordering::SeqCst)
    {
        return Ok(());
    }

    if message.channel_id == config.ddnet_mt_sub_channel {
        if let Err(e) = mt::handle_submission(message, &config, &context).await {}
        return Ok(());
    }

    if !message.content.starts_with('!')
        || message.guild_id != Some(config.ddnet_guild)
        || !config
            .ddnet_moderator_channels
            .contains(&message.channel_id)
    {
        return Ok(());
    }

    if let Err(e) = ban::handle_command(&message, &config, &context).await {
        info!("CommandError `{}`", e.0);
        if let Err(e) = reply(&message, &e.0, context).await {
            error!("Error sending error message: {}", e.to_string());
        }
    }

    Ok(())
}

pub struct CommandError(pub String);
impl From<AddrParseError> for CommandError {
    fn from(_: AddrParseError) -> Self {
        CommandError("Invalid ip".to_owned())
    }
}

impl From<ParseIntError> for CommandError {
    fn from(_: ParseIntError) -> Self {
        CommandError("Invalid integer".to_owned())
    }
}

impl From<DDNetError> for CommandError {
    fn from(e: DDNetError) -> Self {
        CommandError(format!("DDNet Error: {}", e.to_string()))
    }
}

impl From<CreateMessageError> for CommandError {
    fn from(_: CreateMessageError) -> Self {
        CommandError("Failed to create message".to_owned())
    }
}

impl From<twilight_http::Error> for CommandError {
    fn from(_: twilight_http::Error) -> Self {
        CommandError("Failed to send message".to_owned())
    }
}

impl From<sqlx::Error> for CommandError {
    fn from(_: sqlx::Error) -> Self {
        CommandError("Database error".to_owned())
    }
}

impl From<lexer::Error> for CommandError {
    fn from(e: lexer::Error) -> Self {
        CommandError(format!("Error parsing argument: {}", e.to_string()))
    }
}
