use std::convert::{TryInto, TryFrom};
use std::error::Error as StdError;
use std::fmt::{self, Display};
use std::num::NonZeroU64;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic::Ordering;
use either::Either;
use tokio::time::sleep;
use tracing::{debug, info, instrument};

use futures::{StreamExt, TryStreamExt};
use lazy_static::lazy_static;
use regex::Regex;

use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

use sqlx::query::Query;
use sqlx::sqlite::SqliteRow;
use sqlx::{error::Error as SqlError, sqlite::SqliteQueryResult, Executor, FromRow, Sqlite};
use sqlx::{Row, Transaction};

use twilight_http::Client as TwHttpClient;
use twilight_http::Error as TwError;
use twilight_http::request::channel::reaction::RequestReactionType;
use twilight_http::response::DeserializeBodyError;
use twilight_mention::Mention;
use twilight_model::channel::message::{MessageType, ReactionType};
use twilight_model::channel::ChannelType;
use twilight_model::channel::Message;
use twilight_model::gateway::GatewayReaction;
use twilight_model::gateway::payload::incoming::{MessageDeleteBulk, Ready};
use twilight_model::id::marker::MessageMarker;
use twilight_model::id::{Id, marker::{
    ChannelMarker, UserMarker, RoleMarker
}};

use crate::lexer::{Error as LexerError, Lexer};
use crate::util::{sql_get_in_string, sql_start_transaction};
use crate::{get_referenced_message, reply, Caller, CommandError, Context, Target};

// Ideally this would lie in a module struct, held by the bot
//     However, self-reference issues make it very annoying to do this
// This should probably be a OnceCell
static READY: AtomicBool = AtomicBool::new(false);

#[derive(Debug)]
pub struct Error(pub String, pub Option<Box<dyn StdError + Send + Sync>>);

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

impl From<DeserializeBodyError> for Error {
    fn from(e: DeserializeBodyError) -> Self {
        Self("Error deserializing discord api response".into(), Some(Box::new(e)))
    }
}

impl From<CreateGuildChannelError> for Error {
    fn from(e: CreateGuildChannelError) -> Self {
        Self("Couldn't create new channel".into(), Some(Box::new(e)))
    }
}

enum ComId {
    Message(Id<MessageMarker>),
    Channel(Id<ChannelMarker>),
}

impl ComId {
    fn from_id_state(id: NonZeroU64, state: State) -> Self {
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
    author_id: Id<UserMarker>,
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
        let com_id = ComId::from_id_state(com_id.try_into().map_err(|_| -> SqlError {SqlError::Decode(Box::new(Error("com_id can't be 0".into(), None)))})?, state);

        let author_id: u64 = row
            .try_get::<String, _>("author_id")?
            .parse()
            .map_err(|_| {
                SqlError::Decode(Box::new(Error("Invalid u64 in author_id".into(), None)))
            })?;
        let author_id = Id::try_from(author_id).map_err(|_| -> SqlError {SqlError::Decode(Box::new(Error("com_id can't be 0".into(), None)))})?;

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
) -> Result<Id<ChannelMarker>, Error> {
    if s.state != State::Init {
        return Err("Submission in invalid state".into());
    }
    let discord = &context.discord_http;

    let cid = discord
        .create_guild_channel(context.config.ddnet_guild, &s.name)?
        .kind(ChannelType::GuildText)
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

fn is_uemoji(e: &ReactionType, u: &str) -> bool {
    match e {
        ReactionType::Custom { .. } => false,
        ReactionType::Unicode { name } => name == u,
    }
}

fn is_accept(e: &ReactionType) -> bool {
    is_uemoji(e, ACCEPT_EMOJI)
}

fn is_decline(e: &ReactionType) -> bool {
    is_uemoji(e, DECLINE_EMOJI)
}

#[instrument(skip(_ready, context))]
pub async fn init(
    _ready: Arc<Ready>,
    context: Arc<Context>,
) -> Result<(), Box<dyn StdError + Send + Sync>> {
    let discord = &context.discord_http;
    let config = &context.config;

    let mut t = sql_start_transaction(&context.sql_pool).await?;
    let mut s = sqlx::query_as::<_, Submission>("SELECT * FROM mt_subs").fetch(&mut t);
    let mut deleted = vec![];
    while let Some(s) = s.next().await {
        let s = s?;
        match s.com_id {
            ComId::Message(id) =>
            {
                let msg = discord.message(config.ddnet_mt_sub_channel, id).await?;
                if msg.status().is_success() {
                    let msg = msg.model().await?;
                    discord.delete_all_reactions(msg.channel_id, msg.id).await?;
                    // ratelimiter goof, twilight-rs/twilight#1046
                    sleep(tokio::time::Duration::from_millis(100)).await;
                    create_options(&msg, discord).await?;
                }
                else {
                    deleted.push(s.com_id);
                }
            },
            ComId::Channel(id) => {
                if !discord.channel(id).await?.status().is_success() {
                    deleted.push(s.com_id);
                }
            }
        }
    }
    drop(s);

    // These sql queries could be batched
    for d in deleted {
        info!("Deleting {}", d);
        match d {
            ComId::Message(_) => sqlx::query("DELETE FROM mt_subs WHERE state = ? AND com_id = ?")
                .bind(State::Init as i32),
            ComId::Channel(_) => sqlx::query("DELETE FROM mt_subs WHERE state >= ? AND com_id = ?")
                .bind(State::Testing as i32),
        }
        .bind(d.to_string())
        .execute(&mut t)
        .await?;
    }

    t.commit().await?;
    READY.store(true, Ordering::SeqCst);
    Ok(())
}

pub async fn create_options(message: impl Target, discord: &TwHttpClient) -> Result<(), Error> {
    let message = message.into_tuple();

    let mut err = None;
    if let Err(e) = discord
        .create_reaction(
            message.0,
            message.1,
            &RequestReactionType::Unicode {
                name: ACCEPT_EMOJI,
            },
        )
        .await
    {
        err = Some(e);
    }
    if let Err(e) = discord
        .create_reaction(
            message.0,
            message.1,
            &RequestReactionType::Unicode {
                name: DECLINE_EMOJI,
            },
        )
        .await
    {
        err = Some(e);
    }
    if let Some(e) = err {
        let _ = discord.delete_all_reactions(message.0, message.1).await;
        return Err(e.into());
    }

    Ok(())
}

async fn add_submission(
    message: &Message,
    discord: &TwHttpClient,
    transaction: &mut Transaction<'_, Sqlite>,
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

    match get_submission_name(&s.name, &mut *transaction).await {
        Ok(s) if s.is_some() => return Err("Duplicate map".into()),
        Err(e) => return Err(e.into()),
        _ => {}
    }

    // TODO: Investigate what happens if rollback or commit error out.
    //       Is it even possible to recover from this?
    if let Err(e) = insert_submission(&s, &mut *transaction).await {
        return Err(e.into());
    }

    create_options(message, discord).await?;

    Ok(())
}

pub async fn handle_submission(message: &Message, context: &Arc<Context>) -> Result<(), Error> {
    // Ideally these tasks can wait here on the atomic
    //     However tokio has no condition variables...
    if !READY.load(Ordering::SeqCst) {
        return Ok(());
    }

    let mut t = sql_start_transaction(&context.sql_pool).await?;
    add_submission(message, &context.discord_http, &mut t).await?;
    t.commit().await?;
    Ok(())
}

#[instrument(skip(context))]
pub async fn handle_message_deletion(
    deleted: &MessageDeleteBulk,
    context: &Arc<Context>,
) -> Result<(), Error> {
    // Ideally these tasks can wait here on the atomic
    //     However tokio has no condition variables...
    if !READY.load(Ordering::SeqCst) {
        return Ok(());
    }

    debug!(?deleted);

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

    if let Some(e) = err {
        return Err(e.into());
    }

    if deleted_submissions.is_empty() {
        return Ok(());
    }

    debug!(?deleted_submissions);

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
pub async fn handle_reaction_add(reaction: &GatewayReaction, context: &Arc<Context>) -> Result<(), Error> {
    // Ideally these tasks can wait here on the atomic
    //     However tokio has no condition variables...
    if !READY.load(Ordering::SeqCst) {
        return Ok(());
    }

    let config = &context.config;
    let discord = &context.discord_http;

    // Safe to unwrap because checked earlier
    let member = discord
        .guild_member(reaction.guild_id.unwrap(), reaction.user_id)
        .await?.model().await?;

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
            (reaction.channel_id, reaction.message_id),
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
    } else if is_decline(&reaction.emoji) {
        info!("Map declined");
        // TODO: Delete submission from db
    }

    t.commit().await?;
    Ok(())
}

pub async fn handle_command(
    message: &Message,
    caller: &Caller,
    context: &Arc<Context>,
) -> Result<(), CommandError> {
    let config = &context.config;
    let discord = &context.discord_http;

    // This could be cached but with how fast sqlite is I doubt it's necessary
    let mut channels: Vec<Id<ChannelMarker>> = sqlx::query("SELECT com_id FROM mt_subs WHERE state >= ?")
        .bind(State::Testing as i32)
        .fetch(&context.sql_pool)
        .map_err(CommandError::from)
        .and_then(|r| async move {
            match r.try_get::<String, _>("com_id") {
                Ok(s) => match s.parse::<u64>() {
                    Ok(id) => Ok(Id<ChannelMarker>(id)),
                    Err(e) => Err(SqlError::ColumnDecode {
                        index: "com_id".into(),
                        source: e.into(),
                    }
                    .into()),
                },
                Err(e) => Err(e.into()),
            }
        })
        .try_collect()
        .await?;

    channels.push(context.config.ddnet_mt_sub_channel);
    if !channels.contains(&message.channel_id) {
        return Err(CommandError::NotFound("".into()));
    }

    // Strip '!', guaranteed to work due to caller checking
    let cmdline = &message.content[1..];
    let mut l = Lexer::new(cmdline.to_owned());
    match l.get_string() {
        Ok(cmd) => match cmd {
            // ^!add_sub
            "add_sub" => {
                caller.check_access(
                    &[
                        config.ddnet_mt_tester_role,
                        config.ddnet_mt_tester_lead_role,
                    ],
                    &[],
                )?;

                if !matches!(message.kind, MessageType::Reply) {
                    return Err(CommandError::BadCall(
                        "Command must be used in a reply".to_owned(),
                        None,
                    ));
                }

                let referenced = get_referenced_message(&message, discord)
                    .await?
                    .ok_or_else(|| {
                        CommandError::Failed(
                            "API Promise broken, message.type implies reference exists".to_owned(),
                            None,
                        )
                    })?;

                let mut t = sql_start_transaction(&context.sql_pool).await?;
                let sub = get_submission_id(ComId::Message(referenced.id), &mut t).await?;
                if sub.is_some() {
                    return Err(CommandError::Failed("Already tracked".to_string(), None));
                }

                add_submission(referenced.as_ref(), &discord, &mut t).await?;
                Ok(())
            }
            _ => Err(CommandError::NotFound(cmd.to_owned())),
        },
        Err(LexerError::EndOfString) => Ok(()),
        Err(e) => Err(CommandError::Failed(
            "Lexer error".to_owned(),
            Some(e.into()),
        )),
    }
}
