use std::error::Error as StdError;
use std::fmt::{self, Display};
use std::sync::Arc;

use futures::StreamExt;
use lazy_static::lazy_static;
use regex::Regex;

use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

use sqlx::query::Query;
use sqlx::sqlite::SqliteRow;
use sqlx::{error::Error as SqlError, sqlite::SqliteQueryResult, Executor, FromRow, Sqlite};
use sqlx::Row;
use twilight_model::channel::Message;
use twilight_model::gateway::payload::MessageDeleteBulk;
use twilight_model::id::{ChannelId, MessageId, UserId};

use crate::util::{sql_get_in_string, sql_start_transaction};
use crate::Context;

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

    let mut t = sql_start_transaction(&context.sql_pool).await?;
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

pub async fn handle_message_deletion(
    deleted: &MessageDeleteBulk,
    context: &Arc<Context>,
) -> Result<(), Error> {
    let mut t = sql_start_transaction(&context.sql_pool).await?;

    let in_str = sql_get_in_string(deleted.ids.len());
    let query_string =
        "SELECT name FROM mt_subs WHERE state = ? AND com_id IN ".to_owned() + &in_str;
    let mut q = sqlx::query(&query_string).bind(State::Init as i32);
    for id in deleted.ids.iter() {
        q = q.bind(id.to_string());
    }

    let mut err = None;
    let mut deleted_submissions: Vec<String> = vec![];
    let mut s = q.fetch(&mut t);
    while let Some(r) = s.next().await {
        match r {
            Ok(s) => match s.try_get("name") {
                Ok(str) => deleted_submissions.push(str),
                Err(e) => {
                    err = Some(e);
                }
            },
            Err(e) => {
                err = Some(e);
            }
        }
    }
    drop(s);

    if deleted_submissions.is_empty() {
        return Ok(());
    }

    if let Some(e) = err {
        t.rollback().await?;
        return Err(e.into());
    }

    let in_str = sql_get_in_string(deleted_submissions.len());
    let query_string = "DELETE FROM mt_subs WHERE state = ? AND name IN ".to_owned() + &in_str;
    let mut q: Query<_,_> = sqlx::query(&query_string).bind(State::Init as i32);
    for s in deleted_submissions.iter() {
        q = q.bind(s);
    }

    if let Err(e) = q.execute(&mut t).await {
        t.rollback().await?;
        return Err(e.into());
    }

    t.commit().await?;

    Ok(())
}
