use std::error::Error as StdError;
use std::str::FromStr;
use std::sync::Arc;

use atomic::Ordering;
use futures::StreamExt;
use tokio::{
    select,
    time::{sleep, Duration},
};
use tracing::{debug, instrument, warn};

use sqlx::sqlite::{Sqlite, SqliteQueryResult, SqliteRow};
use sqlx::{Error, Executor, FromRow, Row};

use chrono::{NaiveDateTime, Utc};
use prettytable::{cell, format::consts::FORMAT_NO_LINESEP_WITH_TITLE, row, Table};
use twilight_http::request::prelude::create_message::CreateMessageErrorType;
use twilight_model::channel::Message;

use crate::ddnet;
use crate::lexer::{Error as LexerError, Lexer};
use crate::util::Ip;
use crate::CommandError;
use crate::Context;

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

impl<'r> FromRow<'r, SqliteRow> for Ban {
    fn from_row(row: &'r SqliteRow) -> Result<Self, Error> {
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

async fn get_ban<'a, E: Executor<'a, Database = Sqlite>>(
    ip: &Ip,
    executor: E,
) -> Result<Option<Ban>, sqlx::Error> where
{
    let ip = ip.to_string();
    sqlx::query_as::<_, Ban>("SELECT * FROM bans WHERE ip = ?")
        .bind(ip)
        .fetch_optional(executor)
        .await
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

pub async fn handle_command(message: &Message, context: &Arc<Context>) -> Result<(), CommandError> {
    let discord_http = &context.discord_http;
    let sql_pool = &context.sql_pool;
    let http_client = &context.http_client;
    let config = &context.config;

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
                    table.set_format(*FORMAT_NO_LINESEP_WITH_TITLE);
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
                            LexerError::ParseError(_) => {
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

                    let mut err: Option<(Box<dyn StdError + Send>, usize)> = None;
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
        Err(LexerError::EndOfString) => Ok(()),
        Err(e) => Err(CommandError(e.to_string())),
    }
}

#[instrument(skip(context))]
pub async fn handle_expiries(context: Arc<Context>) {
    let config = &context.config;
    let mut local_alive = true;
    while local_alive {
        if select! {
            _ = async {
                while context.alive.load(Ordering::Relaxed) { sleep(Duration::from_secs(1)).await; };
            } => { true }
            _ = sleep(Duration::from_secs(60)) => { false }
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
