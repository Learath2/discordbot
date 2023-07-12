use std::borrow::Cow;
use std::convert::TryInto;
use std::env;
use std::fmt;

use std::error::Error as StdError;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use atomic::Atomic;

use either::Either;
use futures::stream::StreamExt;

use tokio::select;
use tracing::Instrument;
use tracing::{debug, error, info, info_span, instrument, warn};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::prelude::*;

use twilight_cache_inmemory::{InMemoryCache, ResourceType};
use twilight_gateway::{Event, Shard, ShardId};
use twilight_http::Client as TwHttpClient;
use twilight_http::Error as TwError;
use twilight_model::channel::message::Reaction;
use twilight_model::channel::Message;
use twilight_model::gateway::payload::incoming::MessageDeleteBulk;
use twilight_model::gateway::GatewayReaction;
use twilight_model::gateway::Intents;
use twilight_model::guild::Member;
use twilight_model::id::{
    marker::{ChannelMarker, GuildMarker, MessageMarker, RoleMarker, UserMarker, WebhookMarker},
    Id,
};

use sqlx::sqlite::SqlitePool;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::Error as SqlError;

use reqwest::{Client as HttpClient, Url};
use twilight_validate::message::MessageValidationError;

use std::time::Duration;

mod ddnet;
use ddnet::Error as DDNetError;

mod ban;
mod lexer;
use lexer::Error as LexerError;

//mod mt;
//use mt::Error as MTError;
mod util;

#[derive(Debug)]
pub struct Config {
    pub paste_service: Option<String>,
    pub discord_token: String,
    pub database_url: String,
    pub ddnet_token: String,
    pub ddnet_ban_endpoint: String,
    pub ddnet_regions: Vec<String>,
    pub ddnet_guild: Id<GuildMarker>,
    pub ddnet_moderator_channels: Vec<Id<ChannelMarker>>,
    pub ddnet_admin_role: Id<RoleMarker>,
    pub ddnet_moderator_role: Id<RoleMarker>,
    pub qq_webhook_id: Id<WebhookMarker>,

    pub ddnet_mt_tester_role: Id<RoleMarker>,
    pub ddnet_mt_tester_lead_role: Id<RoleMarker>,
    pub ddnet_mt_sub_channel: Id<ChannelMarker>,
    pub ddnet_mt_info_channel: Id<ChannelMarker>,
    pub ddnet_mt_active_cat: Id<ChannelMarker>,
    pub ddnet_mt_waiting_cat: Id<ChannelMarker>,
    pub ddnet_mt_evaluated_cat: Id<ChannelMarker>,
}

#[derive(Debug)]
pub struct Context {
    pub alive: AtomicBool,
    pub config: Arc<Config>,
    pub discord_http: TwHttpClient,
    pub http_client: HttpClient,
    pub sql_pool: SqlitePool,

    pub bot_id: Atomic<Option<Id<UserMarker>>>, // This probably should be a OnceCell
}

macro_rules! get_id_from_env {
    ( $x:literal ) => {
        env::var($x)
            .expect(std::concat!($x, " is missing"))
            .parse::<u64>()
            .expect(std::concat!($x, " is malformed"))
            .try_into()
            .expect(std::concat!($x, " can't be 0"))
    };
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
        ddnet_guild: get_id_from_env!("DDNET_GUILD"),
        ddnet_moderator_channels: vec![],
        ddnet_admin_role: get_id_from_env!("DDNET_ADMIN_ROLE"),
        ddnet_moderator_role: get_id_from_env!("DDNET_MODERATOR_ROLE"),
        qq_webhook_id: get_id_from_env!("QQ_WEBHOOK_ID"),
        ddnet_mt_tester_role: get_id_from_env!("DDNET_MT_ROLE_TESTER"),
        ddnet_mt_tester_lead_role: get_id_from_env!("DDNET_MT_ROLE_TESTER_LEAD"),
        ddnet_mt_sub_channel: get_id_from_env!("DDNET_MT_CHN_SUB"),
        ddnet_mt_info_channel: get_id_from_env!("DDNET_MT_CHN_INFO"),
        ddnet_mt_active_cat: get_id_from_env!("DDNET_MT_CAT_ACTIVE"),
        ddnet_mt_waiting_cat: get_id_from_env!("DDNET_MT_CAT_WAITING"),
        ddnet_mt_evaluated_cat: get_id_from_env!("DDNET_MT_CAT_EVALUATED"),
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
                .try_into()
                .expect("0 channel id in DDNET_MODERATOR_CHANNELS")
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

    let mut shard = Shard::new(
        ShardId::ONE,
        config.discord_token.clone(),
        Intents::GUILD_MESSAGES | Intents::GUILD_MESSAGE_REACTIONS | Intents::MESSAGE_CONTENT,
    );

    let discord_http = TwHttpClient::new(config.discord_token.clone());
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
        bot_id: Atomic::<Option<Id<UserMarker>>>::new(None),
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
            e = shard.next_event() => { Ok(e)}
        };

        let event = match m {
            Ok(e_res) => match e_res {
                Ok(ev) => ev,
                Err(err) => {
                    error!("Got ReceiveMessageError: {} from Shard::next_event()", err);
                    continue;
                }
            },
            Err(()) => {
                //^C
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
                context.bot_id.store(Some(r.user.id), Ordering::SeqCst);
                /*let jh = tokio::spawn(mt::init(Arc::new(*r), context.clone()));
                match jh.await {
                    Ok(Ok(_)) => {}
                    Err(e) => {
                        panic!("Error while initializing map testing {:?}", e);
                    }
                    Ok(Err(e)) => {
                        panic!("Error while initializing map testing {:?}", e);
                    }
                }*/
            }
            _ => {}
        }
    }
}

pub trait Target {
    fn into_tuple(self) -> (Id<ChannelMarker>, Id<MessageMarker>);
}

impl Target for (Id<ChannelMarker>, Id<MessageMarker>) {
    fn into_tuple(self) -> (Id<ChannelMarker>, Id<MessageMarker>) {
        self
    }
}

impl Target for &Message {
    fn into_tuple(self) -> (Id<ChannelMarker>, Id<MessageMarker>) {
        (self.channel_id, self.id)
    }
}

pub async fn reply(
    target: impl Target,
    message: &str,
    context: &Arc<Context>,
) -> Result<Message, Box<dyn StdError + Send + Sync>> {
    let target = target.into_tuple();
    let resp = context
        .discord_http
        .create_message(target.0)
        .reply(target.1)
        .content(message)
        .map_err(Box::new)?
        .await?;
    Ok(resp.model().await?)
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
        || message.author.id == context.bot_id.load(Ordering::SeqCst).unwrap()
    // Yeah, yeah, bad
    {
        return Ok(());
    }

    let member = match message.webhook_id {
        Some(w) => Caller::Webhook(w),
        None => {
            let member = context
                .discord_http
                .guild_member(config.ddnet_guild, message.author.id)
                .await?
                .model()
                .await?;

            Caller::Member(member)
        }
    };


    /*if message.channel_id == config.ddnet_mt_sub_channel {
        if let Err(e) = mt::handle_submission(&message, &context).await {
            debug!("MTError `{}`", e.to_string());
        }
    }*/

    if message.content.starts_with('!') {
        if &message.content[1..] == "die" {
            if member
                .check_access(&[config.ddnet_admin_role], &[])
                .is_err()
            {
                reply(&message, "Access denied", &context).await?;
                return Ok(());
            }

            context.alive.store(false, Ordering::SeqCst);
            return Ok(());
        }

        if let Err(e) = ban::handle_command(&message, &member, &context).await {
            debug!(?e);
            if !matches!(&e, CommandError::NotFound(_)) {
                info!(%e);
                reply(&message, &format!("{}", e), &context).await?;
                return Ok(());
            }
        } else {
            return Ok(());
        }

        /*if let Err(e) = mt::handle_command(&message, &member, &context).await {
            info!(%e);
            reply(&message, &format!("{}", e), &context).await?;
        }*/

        reply(&message, "Command not found!", &context).await?;
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

    /*if deleted.channel_id == config.ddnet_mt_sub_channel {
        if let Err(e) = mt::handle_message_deletion(&deleted, &context).await {
            debug!(?e);
            error!("Error handling message deletion: {}", e.to_string());
        }
    }*/

    Ok(())
}

async fn handle_reaction_add(
    reaction: GatewayReaction,
    context: Arc<Context>,
) -> Result<(), Box<dyn StdError + Send + Sync>> {
    let config = &context.config;

    if reaction.guild_id != Some(config.ddnet_guild)
        || reaction.user_id == context.bot_id.load(Ordering::SeqCst).expect("unreachable")
    {
        return Ok(());
    }

    /*if reaction.channel_id == config.ddnet_mt_sub_channel {
        if let Err(e) = mt::handle_reaction_add(&reaction, &context).await {
            debug!(?e);
            error!("Error handling reaction_add: {}", e.to_string());
        }
    }*/

    Ok(())
}

pub struct AccessDenied;
pub enum Caller {
    Member(Member),
    Webhook(Id<WebhookMarker>),
}

// These probably could take slices of references but I couldn't figure it out
impl Caller {
    pub fn check_access_m(&self, allowed_roles: &[Id<RoleMarker>]) -> Result<(), AccessDenied> {
        match self {
            Caller::Member(m) => m.roles.iter().any(|r| allowed_roles.contains(r)),
            Caller::Webhook(_) => true,
        }
        .then(|| ())
        .ok_or(AccessDenied)
    }

    pub fn check_access_w(&self, allowed_hooks: &[Id<WebhookMarker>]) -> Result<(), AccessDenied> {
        match self {
            Caller::Member(_) => true,
            Caller::Webhook(w) => allowed_hooks.contains(w),
        }
        .then(|| ())
        .ok_or(AccessDenied)
    }

    pub fn check_access(
        &self,
        allowed_roles: &[Id<RoleMarker>],
        allowed_hooks: &[Id<WebhookMarker>],
    ) -> Result<(), AccessDenied> {
        match self {
            Caller::Member(_) => self.check_access_m(allowed_roles),
            Caller::Webhook(_) => self.check_access_w(allowed_hooks),
        }
    }
}

#[allow(clippy::needless_lifetimes)]
pub async fn get_referenced_message<'a>(
    message: &'a Message,
    discord: &TwHttpClient,
) -> Result<Option<Cow<'a, Message>>, TwError> {
    Ok(if let Some(m) = &message.referenced_message {
        Some(Cow::Borrowed(m))
    } else if let Some(mref) = &message.reference {
        let cid = mref.channel_id.unwrap(); // Bad idea, but I'd rather rewrite this using Anyhow then the ugly SimpleError
        let mid = mref.message_id.unwrap();
        let Ok(msg) = discord.message(cid, mid).await?.model().await else {
            return Ok(None);
        };

        Some(Cow::Owned(msg))
    } else {
        None
    })
}

#[derive(Debug)]
pub struct SimpleError(pub String);

impl fmt::Display for SimpleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl StdError for SimpleError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        None
    }
}

impl<T: Into<String>> From<T> for SimpleError {
    fn from(s: T) -> Self {
        Self(s.into())
    }
}

#[derive(Debug)]
pub enum CommandError {
    Failed(String, Option<Box<dyn StdError + Send + Sync>>),
    BadCall(String, Option<Box<dyn StdError + Send + Sync>>),
    AccessDenied,
    Cme,
    NotFound(String),
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandError::Failed(str, _) => write!(f, "Failed: {}", str),
            CommandError::BadCall(str, _) => write!(f, "BadCall: {}", str),
            CommandError::AccessDenied => write!(f, "Access denied"),
            CommandError::Cme => write!(f, "Create Message Error"),
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

impl From<MessageValidationError> for CommandError {
    fn from(e: MessageValidationError) -> Self {
        CommandError::Cme
    }
}

impl From<LexerError> for CommandError {
    fn from(e: LexerError) -> Self {
        CommandError::BadCall(
            format!("Error parsing arguments: {}", e.to_string()),
            Some(e.into()),
        )
    }
}

impl From<DDNetError> for CommandError {
    fn from(e: DDNetError) -> Self {
        match e {
            DDNetError::Internal(e) => {
                CommandError::Failed(format!("Configuration error: {}", e.to_string()), Some(e))
            }
            DDNetError::BackendError {
                ref endpoint_short, ..
            } => CommandError::Failed(
                format!("Backend error from `{}`", endpoint_short),
                Some(e.into()),
            ),
        }
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

/*impl From<MTError> for CommandError {
    fn from(e: MTError) -> Self {
        CommandError::Failed(format!("MTError {}", e.to_string()), e.1)
    }
}*/

impl From<AccessDenied> for CommandError {
    fn from(_: AccessDenied) -> Self {
        CommandError::AccessDenied
    }
}
