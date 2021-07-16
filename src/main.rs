use std::env;
use std::fmt;
use std::net::AddrParseError;
use std::net::IpAddr;
use std::num::ParseIntError;

use std::error::Error;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use atomic::Atomic;

use chrono::NaiveDateTime;
use chrono::Utc;

use futures::stream::StreamExt;

use prettytable::cell;
use prettytable::row;
use prettytable::Table;

use sqlx::sqlite::SqliteRow;
use sqlx::Sqlite;
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
use twilight_http::request::channel::message::create_message::{
    CreateMessage, CreateMessageError, CreateMessageErrorType,
};
use twilight_http::Client as TwHttpClient;
use twilight_model::channel::Message;
use twilight_model::gateway::Intents;
use twilight_model::id::{ChannelId, GuildId, RoleId, UserId};

use sqlx::sqlite::SqlitePool;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::sqlite::SqliteQueryResult;
use sqlx::Executor;
use sqlx::Row;

use reqwest::{Client as HttpClient, Url};
use twilight_model::user::User;

use std::time::Duration;
use tokio::sync::RwLock;

mod ddnet;
use ddnet::Error as DDNetError;

mod mt;

mod lexer;
use lexer::Lexer;

#[derive(Debug)]
pub enum Ip {
    Addr(IpAddr),
    Range(IpAddr, IpAddr),
}

impl fmt::Display for Ip {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Ip::Addr(a) => a.fmt(f),
            Ip::Range(a, b) => write!(f, "{}-{}", a, b),
        }
    }
}

impl FromStr for Ip {
    type Err = AddrParseError;
    fn from_str(s: &str) -> Result<Self, AddrParseError> {
        let e = match IpAddr::from_str(s) {
            Ok(i) => {
                return Ok(Ip::Addr(i));
            }
            Err(e) => e,
        };

        match s.split_once('-') {
            Some((start, end)) => {
                let start = match IpAddr::from_str(start) {
                    Ok(i) => i,
                    Err(e) => {
                        return Err(e);
                    }
                };
                let end = match IpAddr::from_str(end) {
                    Ok(i) => i,
                    Err(e) => {
                        return Err(e);
                    }
                };
                Ok(Ip::Range(start, end))
            }
            None => Err(e),
        }
    }
}

#[derive(Debug)]
pub struct Ban {
    pub ip: Ip,
    pub name: String,
    pub expires: NaiveDateTime,
    pub reason: String,
    pub moderator: String,
    pub region: Option<String>,
    pub note: Option<String>,
}

impl<'r> sqlx::FromRow<'r, SqliteRow> for Ban {
    fn from_row(row: &'r SqliteRow) -> Result<Self, sqlx::Error> {
        let ip = match Ip::from_str(row.try_get("ip")?) {
            Ok(r) => r,
            Err(e) => {
                return Err(sqlx::Error::Decode(Box::new(e)));
            }
        };
        Ok(Ban {
            ip,
            name: row.try_get("name")?,
            expires: row.try_get("expires")?,
            reason: row.try_get("reason")?,
            moderator: row.try_get("moderator")?,
            region: row.try_get("region")?,
            note: row.try_get("note")?,
        })
    }
}

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

macro_rules! get_all_bans {
    ($pool:expr, $mutator:expr) => {
        sqlx::query_as::<_, Ban>(concat!("SELECT * FROM bans ", $mutator)).fetch($pool)
    };
    ($pool:expr, $sort:ident, ASC) => {
        get_all_bans!($pool, concat!("ORDER BY ", stringify!($sort), " ASC"))
    };
    ($pool:expr, $sort:ident, DESC) => {
        get_all_bans!($pool, concat!("ORDER BY ", stringify!($sort), " DESC"))
    };
    ($pool:expr => $field:ident:$value:expr) => {
        sqlx::query_as::<_, Ban>(concat!(
            "SELECT * FROM bans WHERE ",
            stringify!($field),
            " = ?"
        ))
        .bind($value)
        .fetch($pool)
    };
    ($pool:expr) => {
        get_all_bans!($pool, "")
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

    tokio::spawn(handle_expiries(context.clone(), config.clone()));

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

#[instrument(skip(message, config, context), fields(caller, message = &message.content.as_str()))]
async fn handle_message(
    message: Message,
    context: Arc<Context>,
    config: Arc<Config>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let caller = format!("{}#{}", message.author.name, message.author.discriminator);
    tracing::Span::current().record("caller", &caller.as_str());

    if message.guild_id != Some(config.ddnet_guild)
        || message.author.id == context.bot_id.load(Ordering::SeqCst)
    {
        return Ok(());
    }

    if message.channel_id == config.ddnet_mt_sub_channel {
        mt::handle_submission(message, &config, &context).await;
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

    if let Err(e) = handle_command(&message, &config, &context).await {
        info!("CommandError `{}`", e.0);
        match context
            .discord_http
            .create_message(message.channel_id)
            .reply(message.id)
            .content(e.0)
        {
            Ok(cm) => match cm.await {
                Ok(_) => {}
                Err(e) => {
                    error!("Error sending error message: {}", e.to_string());
                }
            },
            Err(e) => {
                error!("Error creating error message: {}", e.to_string());
            }
        }
    }

    Ok(())
}

struct CommandError(pub String);
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

async fn get_ban<'a, E: Executor<'a, Database = Sqlite>>(
    ip: &Ip,
    executor: E,
) -> Result<Option<Ban>, sqlx::Error> where
{
    let ip = ip.to_string();
    match sqlx::query_as::<_, Ban>("SELECT * FROM bans WHERE ip = ?")
        .bind(ip)
        .fetch_one(executor)
        .await
    {
        Ok(b) => Ok(Some(b)),
        Err(e) => match e {
            sqlx::Error::RowNotFound => Ok(None),
            _ => Err(e),
        },
    }
}

async fn ban_exists<'a, E: Executor<'a, Database = Sqlite>>(
    ip: &Ip,
    executor: E,
) -> Result<bool, sqlx::Error> {
    match get_ban(ip, executor).await {
        Ok(o) => Ok(o.is_some()),
        Err(e) => Err(e),
    }
}

async fn insert_ban<'a, E: Executor<'a, Database = Sqlite>>(
    ban: &Ban,
    executor: E,
) -> Result<SqliteQueryResult, sqlx::Error> {
    let ip = ban.ip.to_string();
    sqlx::query!("INSERT INTO bans (ip, name, expires, reason, moderator, region, note) VALUES(?, ?, ?, ?, ?, ?, ?)",
        ip, ban.name, ban.expires, ban.reason, ban.moderator, ban.region, ban.note).execute(executor).await
}

async fn remove_ban<'a, E: Executor<'a, Database = Sqlite>>(
    ip: &Ip,
    executor: E,
) -> Result<SqliteQueryResult, sqlx::Error> {
    let ip = ip.to_string();
    sqlx::query!("DELETE FROM bans WHERE ip = ?", ip)
        .execute(executor)
        .await
}

async fn handle_command(
    message: &Message,
    config: &Arc<Config>,
    context: &Arc<Context>,
) -> Result<(), CommandError> {
    let discord_http = &context.discord_http;
    let sql_pool = &context.sql_pool;
    let http_client = &context.http_client;

    let cmdline = message.content.strip_prefix("!").unwrap(); // unreachable panic
    let member = discord_http
        .guild_member(message.guild_id.unwrap(), message.author.id)
        .await?;
    if member.is_none() {
        return Ok(());
    }
    let member = member.unwrap();

    if !member.roles.contains(&config.ddnet_admin_role)
        && !member.roles.contains(&config.ddnet_moderator_role)
    {
        return Err(CommandError("Access denied".to_owned()));
    }

    let mut l = Lexer::new(cmdline.to_owned());
    match l.get_string() {
        Ok(cmd) => {
            match cmd {
                "bans" => {
                    let mut table = Table::new();
                    table.set_format(*prettytable::format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
                    table.set_titles(row![
                        "Ip",
                        "Name",
                        "Expires",
                        "Reason",
                        "Moderator",
                        "Region",
                        "Note"
                    ]);

                    let mut rc = 0;
                    {
                        let mut k = get_all_bans!(sql_pool).map(|r| {
                            r.map(|b| {
                                debug!(?b);
                                let expires = b.expires.format("%F %R").to_string();
                                row![
                                    cell!(b.ip.to_string()),
                                    cell!(b.name),
                                    cell!(expires),
                                    cell!(b.reason),
                                    cell!(b.moderator),
                                    cell!(b.region.unwrap_or_default()),
                                    cell!(b.note.unwrap_or_default())
                                ]
                            })
                        });

                        while let Some(r) = k.next().await {
                            match r {
                                Ok(r) => {
                                    debug!(?r);
                                    rc += 1;
                                    table.add_row(r);
                                }
                                Err(e) => {
                                    warn!("Error getting ban: {}", e.to_string());
                                }
                            }
                        }
                    }

                    let table_str = if rc > 0 {
                        let mut buf: Vec<u8> = vec![];
                        if let Err(e) = table.print(&mut buf) {
                            buf = format!("print failure: {}", e.to_string())
                                .as_bytes()
                                .to_vec();
                        }
                        String::from_utf8(buf).unwrap_or_else(|_| "parse failure".to_owned())
                    } else {
                        String::from("No bans on record")
                    };

                    let msg = match discord_http
                        .create_message(message.channel_id)
                        .reply(message.id)
                        .content(format!("```\n{}\n```", table_str))
                    {
                        Ok(m) => m,
                        Err(e) => match e.kind() {
                            CreateMessageErrorType::ContentInvalid { content: _ } => {
                                let content =
                                    ddnet::create_paste(config, &context.http_client, &table_str)
                                        .await?;
                                discord_http
                                    .create_message(message.channel_id)
                                    .reply(message.id)
                                    .content(content)?
                            }
                            CreateMessageErrorType::EmbedTooLarge { embed: _ } => unreachable!(),
                            _ => unreachable!(),
                        },
                    };

                    msg.await?;

                    Ok(())
                }
                // !ban_[rgn] <ip> <name> <duration> <reason>
                bancmd if bancmd.starts_with("ban") => {
                    let mut region = bancmd.strip_prefix("ban").unwrap(); // unreachable panic
                    if !region.is_empty() {
                        if let Some(r) = region.strip_prefix("_") {
                            if !config.ddnet_regions.iter().any(|s| s == r) {
                                return Err(CommandError(format!("Invalid region {}", r)));
                            }
                            region = r;
                        } else {
                            return Err(CommandError("Invalid ban command".to_owned()));
                        }
                    }
                    let region = if region.is_empty() {
                        None
                    } else {
                        Some(region.to_owned())
                    };

                    let ban = {
                        let ip = l.get_ip()?;
                        let name = l.get_string()?.to_owned();
                        let duration = l.get_duration()?;
                        let reason = l.get_rest()?.to_owned();
                        if reason.len() > 39 {
                            return Err(CommandError("Reason too long".to_owned()));
                        }

                        let expires = Utc::now() + duration;
                        let moderator =
                            format!("{}#{}", message.author.name, message.author.discriminator);
                        Ban {
                            ip,
                            name,
                            expires: expires.naive_utc(),
                            reason,
                            moderator,
                            region,
                            note: None,
                        }
                    };

                    if ban_exists(&ban.ip, sql_pool).await? {
                        return Err(CommandError("Ban already exists".to_owned()));
                    }

                    ddnet::ban(config, &http_client, &ban).await?;
                    match insert_ban(&ban, sql_pool).await {
                        Ok(_) => {}
                        Err(e) => {
                            ddnet::unban_ip(config, &http_client, &ban.ip).await?;
                            return Err(CommandError::from(e));
                        }
                    }

                    discord_http
                        .create_message(message.channel_id)
                        .reply(message.id)
                        .content(format!(
                            "Successfully banned `{}` until {}",
                            ban.ip.to_string(),
                            ban.expires.format("%F %T").to_string()
                        ))?
                        .await?;

                    Ok(())
                }
                // !unban <ip|name>
                "unban" => {
                    let bans = match l.get_ip() {
                        Ok(i) => match get_ban(&i, sql_pool).await? {
                            Some(b) => vec![b],
                            None => vec![],
                        },
                        Err(e) => match e {
                            lexer::Error::ParseError(_) => {
                                let name = l.get_string()?;
                                let mut banstream = get_all_bans!(sql_pool => name:name);
                                let mut ban_vec: Vec<Ban> = vec![];
                                while let Some(res) = banstream.next().await {
                                    match res {
                                        Ok(b) => {
                                            ban_vec.push(b);
                                        }
                                        Err(e) => {
                                            return Err(e.into());
                                        }
                                    }
                                }
                                ban_vec
                            }
                            _ => {
                                return Err(e.into());
                            }
                        },
                    };

                    if bans.is_empty() {
                        return Err(CommandError("Ban not found".to_owned()));
                    }

                    let mut err: Option<(Box<dyn Error + Send>, usize)> = None;
                    for (i, b) in bans.iter().enumerate() {
                        if let Err(e) = ddnet::unban(config, &http_client, b).await {
                            warn!("Error while unbanning {:?}: {}", b, e.to_string());
                            if let ddnet::Error::BackendError {
                                endpoint: _,
                                body: _,
                                status,
                            } = e
                            {
                                if status == reqwest::StatusCode::NOT_FOUND {
                                    continue;
                                }
                            }
                            err = Some((Box::new(e), i));
                            break;
                        }
                    }

                    if err.is_none() {
                        let mut t = sql_pool.begin().await?;
                        for (i, b) in bans.iter().enumerate() {
                            if let Err(e) = remove_ban(&b.ip, &mut t).await {
                                match e {
                                    sqlx::Error::RowNotFound => {} //ban could have expired, this is fine
                                    e => {
                                        warn!(
                                            "Database error while trying to remove ban: {}",
                                            e.to_string()
                                        );
                                        err = Some((Box::new(e), i));
                                        break;
                                    }
                                }
                            }
                        }

                        if let Err(e) = t.commit().await {
                            warn!("Database error while commiting: {}", e.to_string());
                            err = Some((Box::new(e), bans.len()));
                        }
                    }

                    if let Some((e, f)) = err {
                        let mut rb_err = false;
                        for b in bans[..f].iter() {
                            if let Err(e) = ddnet::ban(config, &http_client, b).await {
                                rb_err = true;
                                warn!("Error trying to rollback {:?}: {}", b, e.to_string());
                            }
                        }

                        return Err(CommandError(format!(
                            "{} Error: {}",
                            if rb_err { "Unclean" } else { "Clean" },
                            e.to_string()
                        )));
                    }

                    discord_http
                        .create_message(message.channel_id)
                        .reply(message.id)
                        .content("Unbanned")?
                        .await?;

                    Ok(())
                }
                "die" => {
                    if !member.roles.contains(&config.ddnet_admin_role) {
                        return Err(CommandError("Access denied".to_owned()));
                    }

                    context.alive.store(false, Ordering::Relaxed);

                    Ok(())
                }
                unk => Err(CommandError(format!("Command {} not found", unk))),
            }
        }
        Err(lexer::Error::EndOfString) => Ok(()),
        Err(e) => Err(CommandError(e.to_string())),
    }
}

#[instrument(skip(context))]
async fn handle_expiries(context: Arc<Context>, config: Arc<Config>) {
    let mut local_alive = true;
    while local_alive {
        if select! {
            _ = async {
                while context.alive.load(Ordering::Relaxed) { tokio::time::sleep(Duration::from_secs(1)).await; };
            } => { true }
            _ = tokio::time::sleep(Duration::from_secs(60)) => { false }
        } {
            local_alive = false;
        }

        debug!("Starting handling of expiries");
        let mut expired_bans = vec![];
        {
            let mut bans_db = get_all_bans!(&context.sql_pool, expires, ASC);
            let now = Utc::now().naive_utc();

            while let Some(r) = bans_db.next().await {
                match r {
                    Ok(b) => {
                        if b.expires < now {
                            expired_bans.push(b);
                        }
                    }
                    Err(e) => {
                        warn!("Error getting ban {}", e.to_string());
                        continue;
                    }
                }
            }
        }

        if !expired_bans.is_empty() {
            let mut removed_bans = vec![];
            for b in expired_bans.iter() {
                if let Err(e) = ddnet::unban(&config, &context.http_client, &b).await {
                    if let ddnet::Error::BackendError {
                        endpoint: _,
                        body: _,
                        status,
                    } = e
                    {
                        if status == reqwest::StatusCode::NOT_FOUND {
                            removed_bans.push(b);
                            continue;
                        }
                    }
                    warn!("Backend Error removing ban: {} {}", b.ip, e.to_string());
                } else {
                    removed_bans.push(b);
                }
            }

            let mut c = match context.sql_pool.begin().await {
                Ok(t) => t,
                Err(e) => {
                    warn!("Couldn't start transaction: {}", e.to_string());
                    continue;
                }
            };

            let mut failed_removals = vec![];
            for b in removed_bans.iter() {
                if let Err(e) = remove_ban(&b.ip, &mut c).await {
                    warn!("Couldn't remove ban: {} {}", b.ip, e.to_string());
                    failed_removals.push(*b);
                }
            }

            let mut rollback = &failed_removals;
            if let Err(e) = c.commit().await {
                warn!("Couldn't commit transaction: {}", e.to_string());
                warn!("Rolling back state on backend");

                rollback = &removed_bans;
            }

            for b in rollback {
                if let Err(e) = ddnet::ban(&config, &context.http_client, b).await {
                    warn!("Couldn't roll back {:?}: {}", b, e.to_string()); // maybe just die here if everything went so wrong
                }
            }
        }
    }
    debug!("Task done");
}
