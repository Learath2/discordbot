use std::error::Error as StdError;
use std::fmt::{self, Display};
use std::sync::Arc;

use lazy_static::lazy_static;
use regex::Regex;

use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use sqlx::{error::Error as SqlError, sqlite::SqliteQueryResult, Executor, FromRow, Sqlite};
use twilight_model::channel::Message;
use twilight_model::id::{ChannelId, MessageId, UserId};

use tokio::time::{sleep, Duration};

use crate::{Config, Context};

#[derive(Debug)]
pub struct Error(String, Option<Box<dyn StdError + Send + Sync>>);

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.1.as_ref().map(|err| err.as_ref() as _)
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Self(s.into(), None)
    }
}

impl From<SqlError> for Error {
    fn from(e: SqlError) -> Self {
        Self(String::from("Database Error"), Some(Box::new(e)))
    }
}

enum ComId {
    Message(MessageId),
    Channel(ChannelId),
}

impl ComId {
    fn from_id_state(id: u64, state: State) -> Self {
        match state {
            State::Init => Self::Message(id.into()),
            _ => Self::Channel(id.into()),
        }
    }
}

impl From<ComId> for String {
    fn from(c: ComId) -> Self {
        match c {
            ComId::Message(i) => i.to_string(),
            ComId::Channel(i) => i.to_string(),
        }
    }
}

#[derive(Clone, Copy, FromPrimitive)]
enum State {
    Init,
    Testing,
    Waiting,
    Evaluated,
}

struct Submission {
    name: String,
    com_id: ComId,
    author: String,
    author_id: UserId,
    server: Option<String>,
    file_url: String,
    state: State,
}

impl<'r> FromRow<'r, SqliteRow> for Submission {
    fn from_row(row: &'r SqliteRow) -> Result<Self, SqlError> {
        let state: State = FromPrimitive::from_i32(row.try_get("state")?).ok_or_else(|| {
            SqlError::Decode(Box::new(Error("Invalid submission state".into(), None)))
        })?;

        let com_id: u64 = row
            .try_get::<String, _>("com_id")?
            .parse()
            .map_err(|_| SqlError::Decode(Box::new(Error("Invalid u64 in com_id".into(), None))))?;
        let com_id = ComId::from_id_state(com_id, state);

        let author_id: u64 = row
            .try_get::<String, _>("author_id")?
            .parse()
            .map_err(|_| {
                SqlError::Decode(Box::new(Error("Invalid u64 in author_id".into(), None)))
            })?;
        let author_id = UserId::from(author_id);

        Ok(Self {
            name: row.try_get("name")?,
            com_id,
            author: row.try_get("author")?,
            author_id,
            server: row.try_get("server")?,
            file_url: row.try_get("file_url")?,
            state,
        })
    }
}

impl fmt::Display for ComId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ComId::Message(i) => i.fmt(f),
            ComId::Channel(i) => i.fmt(f),
        }
    }
}

async fn insert_submission<'a, E: Executor<'a, Database = Sqlite>>(
    sub: &Submission,
    executor: E,
) -> Result<SqliteQueryResult, sqlx::Error> {
    let com_id = sub.com_id.to_string();
    let author_id = sub.author_id.to_string();
    let state = sub.state as i32;

    sqlx::query!("INSERT INTO mt_subs (name, com_id, author, author_id, server, file_url, state) VALUES(?, ?, ?, ?, ?, ?, ?)",
        sub.name, com_id, sub.author, author_id, sub.server, sub.file_url, state).execute(executor).await
}

async fn get_submission<'a, 'b, E: Executor<'a, Database = Sqlite>>(
    name: &str,
    executor: E,
) -> Result<Option<Submission>, sqlx::Error> {
    sqlx::query_as("SELECT * FROM mt_subs WHERE name=?")
        .bind(name)
        .fetch_optional(executor)
        .await
}

pub async fn handle_submission(
    message: &Message,
    config: &Arc<Config>,
    context: &Arc<Context>,
) -> Result<(), Error> {
    lazy_static! {
        static ref RE: Regex = Regex::new(r#"^"(.+)" by (\w+)(?: \[(\w+)\])?$"#).unwrap();
    }

    if message.attachments.len() != 1 {
        return Err("Missing attachment".into());
    }

    let caps = RE.captures(message.content.trim());
    let m = match caps.filter(|m| m.len() >= 3) {
        Some(m) => m,
        None => return Err("Invalid submission format".into()),
    };

    let s = Submission {
        name: m.get(1).unwrap().as_str().to_owned(),
        com_id: ComId::Message(message.id),
        author_id: message.author.id,
        author: m.get(2).unwrap().as_str().to_owned(),
        server: m.get(3).map(|m| m.as_str().to_owned()),
        file_url: message.attachments[0].url.clone(),
        state: State::Init,
    };

    static DELAY: [Duration; 3] = [
        Duration::from_millis(1),
        Duration::from_millis(2),
        Duration::from_millis(5),
    ];
    let mut tries = 0;

    // Ideally this would start an IMMEDIATE transaction
    let mut t = loop {
        match context.sql_pool.begin().await {
            Ok(t) => break Ok(t),
            Err(e) if tries == 3 => break Err(e),
            Err(e) => match e {
                SqlError::Database(_) => {
                    sleep(DELAY[tries]).await;
                }
                _ => break Err(e),
            },
        }
        tries += 1;
    }?;

    match get_submission(&s.name, &mut t).await {
        Ok(s) if s.is_some() => return Err("Duplicate map".into()),
        Err(e) => return Err(e.into()),
        _ => {}
    }

    // TODO: Investigate what happens if rollback or commit error out.
    //       Is it even possible to recover from this?
    if let Err(e) = insert_submission(&s, &mut t).await {
        t.rollback().await?;
        return Err(e.into());
    }

    t.commit().await?;

    Ok(())
}
