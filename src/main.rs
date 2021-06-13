use std::env;
use std::net::AddrParseError;
use std::net::IpAddr;
use std::num::ParseIntError;

use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::stream::StreamExt;

use tracing::{info, info_span, instrument};
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

use twilight_cache_inmemory::{InMemoryCache, ResourceType};
use twilight_gateway::{
    cluster::{Cluster, ShardScheme},
    Event,
};
use twilight_http::request::channel::message::create_message::{CreateMessage, CreateMessageError};
use twilight_http::Client as TwHttpClient;
use twilight_model::channel::Message;
use twilight_model::gateway::Intents;

use sqlx::sqlite::SqlitePool;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::sqlite::SqliteQueryResult;
use sqlx::Row;

mod ddnet;
use ddnet::Error as DDNetError;

mod lexer;
use lexer::Lexer;

#[derive(Debug)]
pub struct Ban {
    pub ip: IpAddr,
    pub name: String,
    pub expires: DateTime<Utc>,
    pub reason: String,
    pub moderator: String,
    pub region: String,
    pub note: String,
}

#[derive(Debug)]
pub struct Config {
    pub discord_token: String,
    pub database_url: String,
    pub ddnet_token: String,
    pub ddnet_ban_endpoint: String,
    pub ddnet_regions: Vec<String>,
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

    dotenv::dotenv().expect("Error loading .env");
    let mut config = Config {
        discord_token: env::var("DISCORD_TOKEN").expect("DISCORD_TOKEN is missing"),
        database_url: env::var("DATABASE_URL").expect("DATABASE_URL is missing"),
        ddnet_token: env::var("DDNET_TOKEN").expect("DDNET_TOKEN is missing"),
        ddnet_ban_endpoint: env::var("DDNET_BAN_ENDPOINT").expect("DDNET_BAN_ENDPOINT is missing"),
        ddnet_regions: vec![],
    };

    let regions = env::var("DDNET_REGIONS").expect("DDNET_REGIONS is missing");
    config.ddnet_regions = regions.split(',').map(String::from).collect();
    if config.ddnet_regions.iter().any(|r| r.len() != 3) {
        panic!("Invalid region in DDNET_REGIONS");
    }

    let config = Arc::new(config);

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

    let http_client = reqwest::Client::new();
    let mut events = cluster.events();
    while let Some((_shard_id, event)) = events.next().await {
        let span = info_span!("eventloop");
        let _enter = span.enter();
        cache.update(&event);

        match event {
            Event::MessageCreate(msg) if !msg.author.bot => {
                tokio::spawn(handle_message(
                    msg.0,
                    config.clone(),
                    discord_http.clone(),
                    http_client.clone(),
                    pool.clone(),
                ));
            }
            Event::Ready(r) => {
                info!("Connected and ready with name: {}", r.user.name);
            }
            _ => {}
        }
    }
}

#[instrument(skip(message, config, discord_http, http_client, sql_pool), fields(caller, message = &message.content.as_str()))]
async fn handle_message(
    message: Message,
    config: Arc<Config>,
    discord_http: TwHttpClient,
    http_client: reqwest::Client,
    sql_pool: SqlitePool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let caller = format!("{}#{}", message.author.name, message.author.discriminator);
    tracing::Span::current().record("caller", &caller.as_str());

    let errorfn = |msg| -> Result<CreateMessage, CreateMessageError> {
        discord_http
            .create_message(message.channel_id)
            .reply(message.id)
            .content(msg)
    };

    if !message.content.starts_with('!') {
        return Ok(());
    }

    if let Err(e) = handle_command(&message, &config, &discord_http, &http_client, &sql_pool).await
    {
        info!("CommandError `{}`", e.0);
        errorfn(e.0)?.await?;
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
        match e {
            DDNetError::Internal(e) => CommandError(format!("Internal Error: {}", e.to_string())),
            DDNetError::BackendError(e) => CommandError(format!("Backend error: {}", e)),
        }
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

async fn get_ban(ip: &IpAddr, sql_pool: &SqlitePool) -> Result<Option<Ban>, sqlx::Error> {
    let ip = ip.to_string();
    match sqlx::query("SELECT * FROM bans WHERE ip = ?")
        .bind(ip)
        .fetch_one(sql_pool)
        .await
    {
        Ok(r) => Ok(Some(Ban {
            ip: IpAddr::from_str(r.get("ip")).unwrap(),
            name: r.get("name"),
            expires: r.get("expires"),
            reason: r.get("reason"),
            moderator: r.get("moderator"),
            region: r.get("region"),
            note: r.get("note"),
        })),
        Err(e) => match e {
            sqlx::Error::RowNotFound => Ok(None),
            _ => Err(e),
        },
    }
}

async fn ban_exists(ip: &IpAddr, sql_pool: &SqlitePool) -> Result<bool, sqlx::Error> {
    let ip = ip.to_string();
    match sqlx::query!("SELECT ip FROM bans WHERE ip = ?", ip)
        .fetch_one(sql_pool)
        .await
    {
        Ok(_) => Ok(true),
        Err(e) => match e {
            sqlx::Error::RowNotFound => Ok(false),
            _ => Err(e),
        },
    }
}

async fn insert_ban(ban: &Ban, sql_pool: &SqlitePool) -> Result<SqliteQueryResult, sqlx::Error> {
    let ip = ban.ip.to_string();
    sqlx::query!("INSERT INTO bans (ip, name, expires, reason, moderator, region, note) VALUES(?, ?, ?, ?, ?, ?, ?)",
        ip, ban.name, ban.expires, ban.reason, ban.moderator, ban.region, ban.note).execute(sql_pool).await
}

async fn remove_ban(ip: &IpAddr, sql_pool: &SqlitePool) -> Result<SqliteQueryResult, sqlx::Error> {
    let ip = ip.to_string();
    sqlx::query!("DELETE FROM bans WHERE ip = ?", ip)
        .execute(sql_pool)
        .await
}

async fn handle_command(
    message: &Message,
    config: &Config,
    discord_http: &TwHttpClient,
    http_client: &reqwest::Client,
    sql_pool: &SqlitePool,
) -> Result<(), CommandError> {
    let cmdline = message.content.strip_prefix("!").unwrap(); // unreachable panic

    let mut l = Lexer::new(cmdline.to_owned());
    match l.get_string() {
        Ok(cmd) => {
            match cmd {
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
                    let region = region.to_owned();

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
                            expires,
                            reason,
                            moderator,
                            region,
                            note: "".to_owned(),
                        }
                    };

                    if ban_exists(&ban.ip, sql_pool).await? {
                        return Err(CommandError("Ban already exists".to_owned()));
                    }

                    ddnet::ban(config, &http_client, &ban).await?;
                    match insert_ban(&ban, sql_pool).await {
                        Ok(_) => {}
                        Err(e) => {
                            ddnet::unban_ip(config, &http_client, ban.ip).await?;
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
                // !unban <ip>
                "unban" => {
                    let ip = l.get_ip()?;

                    let ban = match get_ban(&ip, sql_pool).await? {
                        Some(b) => b,
                        None => {
                            return Err(CommandError("Ban not found".to_owned()));
                        }
                    };

                    ddnet::unban_ip(config, &http_client, ip).await?;
                    match remove_ban(&ip, sql_pool).await {
                        Ok(_) => {}
                        Err(e) => match e {
                            sqlx::Error::RowNotFound => {}
                            e => {
                                ddnet::ban(config, &http_client, &ban).await?;
                                return Err(CommandError::from(e));
                            }
                        },
                    }

                    discord_http
                        .create_message(message.channel_id)
                        .reply(message.id)
                        .content("Unbanned")?
                        .await?;

                    Ok(())
                }
                unk => Err(CommandError(format!("Command {} not found", unk))),
            }
        }
        Err(lexer::Error::EndOfString) => Ok(()),
        Err(e) => Err(CommandError(e.to_string())),
    }
}
