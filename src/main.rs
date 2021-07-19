use std::env;
use std::fmt;
use std::fmt::Display;
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
use twilight_http::Error as TwError;
use twilight_http::Client as TwHttpClient;
use twilight_model::channel::Message;
use twilight_model::channel::Reaction;
use twilight_model::gateway::payload::MessageDeleteBulk;
use twilight_model::gateway::Intents;
use twilight_model::id::MessageId;
use twilight_model::id::{ChannelId, GuildId, RoleId, UserId};

use sqlx::sqlite::SqlitePool;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::Error as SqlError;

use reqwest::{Client as HttpClient, Url};

use std::time::Duration;

mod ddnet;
use ddnet::Error as DDNetError;

mod ban;
mod lexer;
use lexer::Error as LexerError;

mod mt;
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
    pub config: Arc<Config>,
    pub discord_http: TwHttpClient,
    pub http_client: HttpClient,
    pub sql_pool: SqlitePool,

    pub bot_id: Atomic<UserId>, // This probably should be a OnceCell
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

    let (cluster, mut events) = Cluster::builder(
        &config.discord_token,
        Intents::GUILD_MESSAGES | Intents::GUILD_MESSAGE_REACTIONS,
    )
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
        config,
        discord_http,
        http_client: HttpClient::new(),
        sql_pool: pool,
        bot_id: Atomic::<UserId>::new(0.into()),
    });

    tokio::spawn(ban::handle_expiries(context.clone()));

    let context_cc = context.clone();
    tokio::spawn(async move {
        // Ideally this awful spin would be replaced with a condition variable
        //     However tokio doesn't have condition variables
        // TODO: Look into using a broadcast channel for the entire bot so all parts
        //       can get a shutdown signal.
        if select! {
            _ = async {
                while context_cc.alive.load(Ordering::SeqCst) { tokio::time::sleep(Duration::from_secs(1)).await; };
            } => { false }
            _ = tokio::signal::ctrl_c() => { true }
        } {
            info!("Received ^C: Cleaning up");
            context_cc.alive.store(false, Ordering::SeqCst);
        }
    }.instrument(info_span!("^C")));

    loop {
        let context_el = context.clone();
        let m = select! {
            _ = async move { while context_el.alive.load(Ordering::SeqCst) { tokio::time::sleep(Duration::from_secs(1)).await; };} => { Err(()) }
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

        if !context.alive.load(Ordering::SeqCst) {
            break;
        }
        cache.update(&event);

        match event {
            Event::MessageCreate(msg) => {
                debug!(?msg);
                tokio::spawn(handle_message(msg.0, context.clone()));
            }
            Event::MessageDelete(m) => {
                let mdb = MessageDeleteBulk {
                    channel_id: m.channel_id,
                    guild_id: m.guild_id,
                    ids: vec![m.id],
                };
                tokio::spawn(handle_message_deletion(mdb, context.clone()));
            }
            Event::MessageDeleteBulk(m) => {
                tokio::spawn(handle_message_deletion(m, context.clone()));
            }
            Event::ReactionAdd(r) => {
                tokio::spawn(handle_reaction_add(r.0, context.clone()));
            }
            Event::Ready(r) => {
                info!("Connected and ready with name: {}", r.user.name);
                context.bot_id.store(r.user.id, Ordering::SeqCst);
                let jh = tokio::spawn(mt::init(Arc::new(*r), context.clone()));
                match jh.await {
                    Ok(Ok(_)) => {}
                    Err(e) => {
                        panic!("Error while initializing map testing {:?}", e);
                    }
                    Ok(Err(e)) => {
                        panic!("Error while initializing map testing {:?}", e);
                    }
                }
            }
            _ => {}
        }
    }
}

pub trait Target {
    fn into_tuple(self) -> (ChannelId, MessageId);
}

impl Target for (ChannelId, MessageId) {
    fn into_tuple(self) -> (ChannelId, MessageId) {
        self
    }
}

impl Target for &Message {
    fn into_tuple(self) -> (ChannelId, MessageId) {
        (self.channel_id, self.id)
    }
}

pub async fn reply(
    target: impl Target,
    message: &str,
    context: &Arc<Context>,
) -> Result<Message, Box<dyn StdError + Send + Sync>> {
    let target = target.into_tuple();
    context
        .discord_http
        .create_message(target.0)
        .reply(target.1)
        .content(message)
        .map_err(Box::new)?
        .await
        .map_err(|e| -> Box<dyn StdError + Send + Sync> { Box::new(e) })
}

#[instrument(skip(message, context), fields(caller, message = &message.content.as_str()))]
async fn handle_message(
    message: Message,
    context: Arc<Context>,
) -> Result<(), Box<dyn StdError + Send + Sync>> {
    let caller = format!("{}#{}", message.author.name, message.author.discriminator);
    tracing::Span::current().record("caller", &caller.as_str());

    let config = &context.config;

    if message.guild_id != Some(config.ddnet_guild)
        || message.author.id == context.bot_id.load(Ordering::SeqCst)
    {
        return Ok(());
    }

    let member = context.discord_http.guild_member(config.ddnet_guild, message.author.id).await?;
    let member = match member {
        Some(m) => m,
        None => {
            reply(&message, "Couldn't get member", &context).await?;
            return Ok(());
        }
    };

    if message.channel_id == config.ddnet_mt_sub_channel {
        if let Err(e) = mt::handle_submission(&message, &context).await {
            debug!("MTError `{}`", e.to_string());
        }
    }

    if message.content.starts_with("!") {
        if &message.content[1..] == "die" {
            if !member.roles.contains(&config.ddnet_admin_role) {
                reply(&message, "Access denied", &context).await?;
                return Ok(());
            }

            context.alive.store(false, Ordering::SeqCst);
        }

        if let Err(e) = ban::handle_command(&message, &member, &context).await {
            debug!(?e);
            if !matches!(&e, CommandError::NotFound(_)) {
                info!(%e);
                reply(&message, &format!("{}", e), &context).await?;
            }
        }

        if let Err(e) = mt::handle_command(&message, &member, &context).await {

        }
    }

    Ok(())
}

async fn handle_message_deletion(
    deleted: MessageDeleteBulk,
    context: Arc<Context>,
) -> Result<(), Box<dyn StdError + Send + Sync>> {
    let config = &context.config;

    if deleted.guild_id != Some(config.ddnet_guild) {
        return Ok(());
    }

    if deleted.channel_id == config.ddnet_mt_sub_channel {
        if let Err(e) = mt::handle_message_deletion(&deleted, &context).await {
            debug!(?e);
            error!("Error handling message deletion: {}", e.to_string());
        }
    }

    Ok(())
}

async fn handle_reaction_add(
    reaction: Reaction,
    context: Arc<Context>,
) -> Result<(), Box<dyn StdError + Send + Sync>> {
    let config = &context.config;

    if reaction.guild_id != Some(config.ddnet_guild)
        || reaction.user_id == context.bot_id.load(Ordering::SeqCst)
    {
        return Ok(());
    }

    if reaction.channel_id == config.ddnet_mt_sub_channel {
        if let Err(e) = mt::handle_reaction_add(&reaction, &context).await {
            debug!(?e);
            error!("Error handling reaction_add: {}", e.to_string());
        }
    }

    Ok(())
}

#[derive(Debug)]
pub enum CommandError {
    Failed(String, Option<Box<dyn StdError + Send + Sync>>),
    BadCall(String, Option<Box<dyn StdError + Send + Sync>>),
    AccessDenied,
    CME,
    NotFound(String),
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandError::Failed(str, _) => write!(f, "Failed: {}", str),
            CommandError::BadCall(str, _) => write!(f, "BadCall: {}", str),
            CommandError::AccessDenied => write!(f, "Access denied"),
            CommandError::CME => write!(f, "Create Message Error"),
            CommandError::NotFound(str) => write!(f, "Command not found: `{}`", str),
        }
    }
}

impl StdError for CommandError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match &self {
            CommandError::Failed(_, src) => src.as_ref().map(|err| err.as_ref() as _),
            CommandError::BadCall(_, src) => src.as_ref().map(|err| err.as_ref() as _),
            _ => None,
        }
    }
}

impl From<LexerError> for CommandError {
    fn from(e: LexerError) -> Self {
        CommandError::BadCall(format!("Error parsing arguments: {}", e.to_string()), Some(e.into()))
    }
}

impl From<DDNetError> for CommandError {
    fn from(e: DDNetError) -> Self {
        match e {
            DDNetError::Internal(e) => CommandError::Failed(format!("Configuration error: {}", e.to_string()), Some(e)),
            DDNetError::BackendError { ref endpoint_short, .. } => CommandError::Failed(format!("Backend error from `{}`", endpoint_short), Some(e.into())),
        }
    }
}

impl From<CreateMessageError> for CommandError {
    fn from(_: CreateMessageError) -> Self {
        CommandError::CME
    }
}

impl From<TwError> for CommandError {
    fn from(e: TwError) -> Self {
        CommandError::Failed("Backend error from `discord`".to_owned(), Some(e.into()))
    }
}

impl From<SqlError> for CommandError {
    fn from(e: SqlError) -> Self {
        CommandError::Failed("Database error".to_owned(), Some(e.into()))
    }
}

