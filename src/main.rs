use std::env;
use std::net::AddrParseError;
use std::net::IpAddr;
use std::num::ParseIntError;
use std::str::FromStr;

use std::sync::Arc;

use chrono::Date;
use chrono::Duration;
use chrono::{DateTime, Utc};
use futures::stream::StreamExt;

use twilight_gateway::{cluster::{Cluster, ShardScheme}, Event};
use twilight_model::gateway::Intents;
use twilight_model::channel::Message;
use twilight_cache_inmemory::{InMemoryCache, ResourceType};
use twilight_http::Client as TwHttpClient;
use twilight_http::request::channel::message::create_message::{CreateMessage, CreateMessageError};

use sqlx::sqlite::SqlitePool;
use sqlx::sqlite::SqlitePoolOptions;

mod ddnet;
use ddnet::Error as DDNetError;

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

pub struct Config {
    pub discord_token: String,
    pub database_url: String,
    pub ddnet_token: String,
    pub ddnet_ban_endpoint: String,
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().expect("Error loading .env");
    let config = Arc::new(Config {
        discord_token: env::var("DISCORD_TOKEN").expect("DISCORD_TOKEN is missing"),
        database_url: env::var("DATABASE_URL").expect("DATABASE_URL is missing"),
        ddnet_token: env::var("DDNET_TOKEN").expect("DDNET_TOKEN is missing"),
        ddnet_ban_endpoint: env::var("DDNET_BAN_ENDPOINT").expect("DDNET_BAN_ENDPOINT is missing"),
    });

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

    println!("Connecting to database: {}", config.database_url);
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&config.database_url)
        .await
        .expect("Failed to connect to db");

    println!("Running migrations");
    sqlx::migrate!()
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    let http_client = reqwest::Client::new();
    let now = Utc::now();
    println!("{:?}", now);
    let strs = sqlite_fromdatetime(now);
    println!("{}", strs);
    let back = datetime_fromsqlite(strs);
    println!("{:?}", back);

    let mut events = cluster.events();
    while let Some((_shard_id, event)) = events.next().await {
        cache.update(&event);

        match event {
            Event::MessageCreate(msg) => {
                tokio::spawn(handle_message( msg.0, config.clone(), discord_http.clone(), http_client.clone(), pool.clone()));
            }
            _ => {}
        }
    }
}

struct TokenError;
fn tokenize(s: &str) -> Result<Vec<&str>, TokenError> {
    enum State {
        Out,
        In(bool, usize),
    }
    let mut tokens = vec![];
    let mut state = State::Out;

    for (i, c) in s.chars().enumerate() {
        match state {
            State::Out => {
                if !c.is_whitespace() {
                    state = State::In(c == '"', i);
                }
            },
            State::In(explicit, start) => {
                if !explicit && (c.is_whitespace() || c == '"') {
                    tokens.push(&s[start..i]);
                    state = if c == '"' { State::In(true, i) } else { State::Out };
                }
                else if explicit && c == '"' {
                    tokens.push(&s[start+1..i]);
                    state = State::Out;
                }
            }
        }
    };

    if let State::In(explicit, start) = state {
        if explicit {
            return Err(TokenError);
        }
        else {
            tokens.push(&s[start..]);
        }
    }

    Ok(tokens)
}


async fn handle_message(message: Message, config: Arc<Config>, discord_http: TwHttpClient, http_client: reqwest::Client, sql_pool: SqlitePool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let errorfn =
        |msg| -> Result<CreateMessage, CreateMessageError> {
            discord_http.create_message(message.channel_id).reply(message.id).content(msg)
        };

    if !message.content.starts_with("!") {
        return Ok(());
    }

    match handle_command(&message, &config, &discord_http, &http_client, &sql_pool).await {
        Err(e) => { errorfn(e.0)?.await?; },
        Ok(_) => { },
    }

    println!("{}: {}", message.author.name, message.content);
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

fn sqlite_fromdatetime(d: DateTime<Utc>) -> String {
    d.format("%F %T%.f").to_string()
}

fn datetime_fromsqlite(s: String) -> DateTime<Utc> {
    DateTime::parse_from_str(&s, "%F %T%.f").expect("Something went verywrong").with_timezone(&Utc)
}

async fn handle_command(message: &Message, config: &Config, discord_http: &TwHttpClient, http_client: &reqwest::Client, sql_pool: &SqlitePool) -> Result<(), CommandError> {
    // !ban_[rgn] <ip> <name> <duration> <reason>
    if let Some(mut s) = message.content.strip_prefix("!ban") {
        let mut region = "";
        if s.starts_with('_') {
            region = &s[1..4];
            s = &s[4..];
        }

        if s.starts_with(' ') {
            s = &s[1..];
        }
        else if s.len() == 0 {
            return Err(CommandError("Too few arguments".to_owned()));
        }
        else {
            return Err(CommandError("Invalid ban command".to_owned()));
        }

        let toks = tokenize(s).unwrap_or(vec![]);
        println!("Tokens: {:?}", toks);
        if toks.len() < 4 {
            return Err(CommandError("Too few arguments".to_owned()));
        }

        let ip = toks[0];
        let name = toks[1];
        let duration = toks[2];
        let reason = toks[3..].join(" ");

        let ip = std::net::IpAddr::from_str(ip)?;
        let duration: u32 = duration.parse()?;
        if reason.len() > 39 {
            return Err(CommandError("Reason too long".to_owned()));
        }
        let expires = Utc::now() + Duration::minutes(duration as i64);
        let moderator = format!("{}#{}", message.author.name, message.author.discriminator);

        let ban = Ban { ip, name: name.to_owned(), expires, reason, moderator, region: region.to_owned(), note: "".to_owned() };
        let ip_string = ban.ip.to_string();
        match sqlx::query!("SELECT Ip FROM bans WHERE Ip = ?", ip_string).fetch_one(sql_pool).await.err() {
            Some(e) => {
                match e {
                    sqlx::Error::RowNotFound => { },
                    _ => { return Err(CommandError::from(e)); }
                }
            },
            None => { return Err(CommandError("Ban already exists".to_owned())); },
        }

        println!("Ban: {:?}", ban);
        ddnet::ban(config, &http_client, &ban).await?;
        sqlx::query!("INSERT INTO bans (Ip, Name, Expires, Reason, Moderator, Region, Note) VALUES(?, ?, ?, ?, ?, ?, ?)", ip_string, ban.name, sqlite_fromdatetime(ban.expires), ban.reason, ban.moderator, ban.region, ban.note);

        discord_http.create_message(message.channel_id).reply(message.id).content(format!("Successfully banned `{}` until {}", ip.to_string(), expires.to_string()))?.await?;
    }
    // !unban <ip>
    else if let Some(s) = message.content.strip_prefix("!unban") {
        let toks = tokenize(s).unwrap_or(vec![]);
        if toks.is_empty() {
            return Err(CommandError("Too few arguments".to_owned()));
        }
        else if toks.len() > 1 {
            return Err(CommandError("Too many arguments".to_owned()));
        }

        let ip = std::net::IpAddr::from_str(toks[0])?;
        ddnet::unban_ip(config, &http_client, ip).await?;

        discord_http.create_message(message.channel_id).reply(message.id).content(format!("Unbanned `{}`", ip.to_string()))?.await?;
    }

    Ok(())
}
