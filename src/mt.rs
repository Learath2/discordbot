use std::error::Error as StdError;
use std::fmt::{self, Display};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use atomic::Ordering;
use tracing::{info, debug, instrument};

use futures::StreamExt;
use lazy_static::lazy_static;
use regex::Regex;

use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

use sqlx::query::Query;
use sqlx::sqlite::SqliteRow;
use sqlx::{error::Error as SqlError, sqlite::SqliteQueryResult, Executor, FromRow, Sqlite};
use sqlx::{Row, Transaction};

use twilight_http::request::guild::create_guild_channel::CreateGuildChannelError;
use twilight_http::request::prelude::RequestReactionType;
use twilight_http::Error as TwError;
use twilight_mention::Mention;
use twilight_model::channel::ChannelType::GuildText as ChannelTypeText;
use twilight_model::channel::{Message, Reaction};
use twilight_model::gateway::payload::{MessageDeleteBulk, Ready};
use twilight_model::id::{ChannelId, MessageId, UserId};

use crate::util::{sql_get_in_string, sql_start_transaction};
use crate::{reply, Context};

// Ideally this would lie in a module struct, held by the bot
//     However, self-reference issues make it very annoying to do this
// This should probably be a OnceCell
static READY: AtomicBool = AtomicBool::new(false);

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
        Self("Database Error".into(), Some(Box::new(e)))
    }
}

impl From<TwError> for Error {
    fn from(e: TwError) -> Self {
        Self("TWError".into(), Some(Box::new(e)))
    }
}

impl From<CreateGuildChannelError> for Error {
    fn from(e: CreateGuildChannelError) -> Self {
        Self("Couldn't create new channel".into(), Some(Box::new(e)))
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

#[derive(Clone, Copy, FromPrimitive, PartialEq)]
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

async fn get_submission_name<'a, E: Executor<'a, Database = Sqlite>>(
    name: &str,
    executor: E,
) -> Result<Option<Submission>, sqlx::Error> {
    sqlx::query_as("SELECT * FROM mt_subs WHERE name=?")
        .bind(name)
        .fetch_optional(executor)
        .await
}

async fn get_submission_id<'a, E: Executor<'a, Database = Sqlite>>(
    id: ComId,
    executor: E,
) -> Result<Option<Submission>, sqlx::Error> {
    match id {
        ComId::Message(_) => sqlx::query_as("SELECT * FROM mt_subs WHERE state=? AND com_id=?")
            .bind(State::Init as i32),
        ComId::Channel(_) => sqlx::query_as("SELECT * FROM mt_subs WHERE state > ? AND com_id=?")
            .bind(State::Testing as i32),
    }
    .bind(id.to_string())
    .fetch_optional(executor)
    .await
}

// DO NOT USE context.sql_pool here, use the transaction already started
// DO NOT COMMIT transaction, let the caller do it
async fn accept_initial_submission(
    s: Submission,
    context: &Arc<Context>,
    transaction: &mut Transaction<'_, Sqlite>,
) -> Result<ChannelId, Error> {
    if s.state != State::Init {
        return Err("Submission in invalid state".into());
    }
    let discord = &context.discord_http;

    let cid = discord
        .create_guild_channel(context.config.ddnet_guild, &s.name)?
        .kind(ChannelTypeText)
        .parent_id(context.config.ddnet_mt_active_cat)
        .await?
        .id();

    let new_state = State::Testing as i32;
    let cid_str = cid.to_string();
    sqlx::query!(
        "UPDATE mt_subs SET state = ?, com_id = ? WHERE name = ?",
        new_state,
        cid_str,
        s.name
    )
    .execute(transaction)
    .await?;

    Ok(cid)
}

const ACCEPT_EMOJI: &str = "\u{1F7E2}";
const DECLINE_EMOJI: &str = "\u{1F534}";

fn is_uemoji<E: Into<RequestReactionType> + Clone>(e: &E, u: &str) -> bool {
    let e: RequestReactionType = e.clone().into();

    match e {
        RequestReactionType::Custom { .. } => false,
        RequestReactionType::Unicode { name } => name == u,
    }
}

fn is_accept<E: Into<RequestReactionType> + Clone>(e: &E) -> bool {
    is_uemoji(e, ACCEPT_EMOJI)
}

fn is_decline<E: Into<RequestReactionType> + Clone>(e: &E) -> bool {
    is_uemoji(e, DECLINE_EMOJI)
}

pub async fn init(ready: Arc<Ready>, context: Arc<Context>) -> Result<(), Error> {
    // TODO: Go through all submissions, if their messages are not there delete them
    //       For submissions that are there, reset the reactions for good measure.

    READY.store(true, Ordering::SeqCst);
    Ok(())
}

pub async fn handle_submission(message: &Message, context: &Arc<Context>) -> Result<(), Error> {
    lazy_static! {
        static ref RE: Regex = Regex::new(r#"^"(.+)" by (\w+)(?: \[(\w+)\])?$"#).unwrap();
    }

    // Ideally these tasks can wait here on the atomic
    //     However tokio has no condition variables...
    if !READY.load(Ordering::SeqCst) {
        return Ok(());
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
    match get_submission_name(&s.name, &mut t).await {
        Ok(s) if s.is_some() => return Err("Duplicate map".into()),
        Err(e) => return Err(e.into()),
        _ => {}
    }

    // TODO: Investigate what happens if rollback or commit error out.
    //       Is it even possible to recover from this?
    if let Err(e) = insert_submission(&s, &mut t).await {
        return Err(e.into());
    }

    let mut err = None;
    if let Err(e) = context
        .discord_http
        .create_reaction(
            message.channel_id,
            message.id,
            RequestReactionType::Unicode {
                name: ACCEPT_EMOJI.to_owned(),
            },
        )
        .await
    {
        err = Some(e);
    }
    if let Err(e) = context
        .discord_http
        .create_reaction(
            message.channel_id,
            message.id,
            RequestReactionType::Unicode {
                name: DECLINE_EMOJI.to_owned(),
            },
        )
        .await
    {
        err = Some(e);
    }
    if let Some(e) = err {
        return Err(e.into());
    }

    t.commit().await?;
    Ok(())
}

pub async fn handle_message_deletion(
    deleted: &MessageDeleteBulk,
    context: &Arc<Context>,
) -> Result<(), Error> {
    // Ideally these tasks can wait here on the atomic
    //     However tokio has no condition variables...
    if !READY.load(Ordering::SeqCst) {
        return Ok(());
    }

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
        return Err(e.into());
    }

    let in_str = sql_get_in_string(deleted_submissions.len());
    let query_string = "DELETE FROM mt_subs WHERE state = ? AND name IN ".to_owned() + &in_str;
    let mut q: Query<_, _> = sqlx::query(&query_string).bind(State::Init as i32);
    for s in deleted_submissions.iter() {
        q = q.bind(s);
    }

    if let Err(e) = q.execute(&mut t).await {
        return Err(e.into());
    }

    t.commit().await?;

    Ok(())
}

#[instrument(skip(reaction, context), fields(reaction, caller))]
pub async fn handle_reaction_add(reaction: &Reaction, context: &Arc<Context>) -> Result<(), Error> {
    // Ideally these tasks can wait here on the atomic
    //     However tokio has no condition variables...
    if !READY.load(Ordering::SeqCst) {
        return Ok(());
    }

    let config = &context.config;
    let discord = &context.discord_http;

    // Safe to unwrap because checked earlier
    let member = match discord
    .guild_member(reaction.guild_id.unwrap(), reaction.user_id)
    .await?
    {
        Some(m) => m,
        None => {
            return Ok(());
        }
    };

    let caller = format!("{}#{}", member.user.name, member.user.discriminator);
    tracing::Span::current().record("caller", &caller.as_str());

    if !member.roles.contains(&config.ddnet_mt_tester_role)
        && !member.roles.contains(&config.ddnet_mt_tester_lead_role)
    {
        discord
            .delete_reaction(
                reaction.channel_id,
                reaction.message_id,
                reaction.emoji.clone().into(),
                reaction.user_id,
            )
            .await?;
        return Ok(());
    }

    let mut t = sql_start_transaction(&context.sql_pool).await?;
    let s = match get_submission_id(ComId::Message(reaction.message_id), &mut t).await? {
        Some(s) => s,
        None => {
            return Ok(());
        }
    };

    if is_accept(&reaction.emoji) {
        info!("Map accepted");
        let cid = accept_initial_submission(s, context, &mut t).await?;
        if let Err(e) = reply(
            crate::Target(reaction.channel_id, reaction.message_id),
            &format!("Accepted, {}", cid.mention()),
            context,
        )
        .await
        {
            t.commit().await?;
            return Err(Error(
                "Success ECR (This needs to be a different error type and dropped)".into(),
                Some(e),
            ));
        }
    }
    else if is_decline(&reaction.emoji) {
        info!("Map declined");
        // Delete submission from db

    }

    t.commit().await?;
    Ok(())
}
