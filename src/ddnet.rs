use std::fmt;
use std::net::IpAddr;

use tracing::{debug, instrument};

use reqwest::Client as HttpClient;
use reqwest::Error as ReqwestError;
use reqwest::Url;

use crate::{Ban, Config, Ip};

#[derive(Debug)]
pub enum Error {
    Internal(ReqwestError),
    BackendError(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &*self {
            Error::Internal(e) => e.fmt(f),
            Error::BackendError(e) => f.write_fmt(format_args!("BackendError {}: ", e)),
        }
    }
}

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        Error::Internal(e)
    }
}

fn get_final_reason(ban: &Ban) -> String {
    let expiry = ban.expires.format("%b %d %H:%M UTC");
    format!("{}. Until {}", ban.reason, expiry)
}

fn get_final_note(ban: &Ban) -> String {
    let mut note = format!("{}: {}", ban.moderator, ban.name);
    if let Some(inote) = ban.note.as_ref() {
        note.push_str(&format!("({})", inote));
    }

    note
}

#[instrument(level = "debug", skip(http_client))]
pub async fn ban(config: &Config, http_client: &HttpClient, ban: &Ban) -> Result<(), Error> {
    let region = ban.region.clone().unwrap_or_default();
    let req = http_client
        .post(config.ddnet_ban_endpoint.clone())
        .header("x-ddnet-token", config.ddnet_token.clone())
        .query(&[
            ("ip", ban.ip.to_string()),
            ("name", ban.name.clone()),
            ("reason", get_final_reason(&ban)),
            ("region", region),
            ("note", get_final_note(&ban)),
        ])
        .build()?;
    debug!(?req);

    let res = http_client.execute(req).await?;
    debug!(?res);

    if !res.status().is_success() {
        return Err(Error::BackendError(format!(
            "{}: {}",
            res.status(),
            res.text().await?
        )));
    }

    Ok(())
}

#[instrument(level = "debug", skip(http_client))]
pub async fn unban_ip(config: &Config, http_client: &HttpClient, ip: &Ip) -> Result<(), Error> {
    let req = http_client
        .delete(config.ddnet_ban_endpoint.clone())
        .header("x-ddnet-token", config.ddnet_token.clone())
        .query(&[("ip", ip.to_string())])
        .build()?;
    debug!(?req);

    let res = http_client.execute(req).await?;
    debug!(?res);

    if !res.status().is_success() {
        return Err(Error::BackendError(res.text().await?));
    }

    Ok(())
}

pub async fn unban(config: &Config, http_client: &HttpClient, ban: &Ban) -> Result<(), Error> {
    unban_ip(config, http_client, &ban.ip).await
}

#[instrument(level = "debug", skip(http_client))]
pub async fn create_paste(
    config: &Config,
    http_client: &HttpClient,
    content: &str,
) -> Result<String, Error> {
    if config.paste_service.is_none() {
        return Err(Error::BackendError(
            "Paste service not configured".to_owned(),
        ));
    }

    let endpoint = config.paste_service.as_ref().unwrap();
    let req = http_client
        .post(endpoint)
        .body(content.to_string())
        .build()?;
    debug!(?req);

    let res = http_client.execute(req).await?;
    debug!(?res);

    let status = res.status();
    let text = res.text().await?;
    if !status.is_success() || Url::parse(&text).is_err() {
        return Err(Error::BackendError(format!("{}: {}", endpoint, text)));
    }

    Ok(text)
}
