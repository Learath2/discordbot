use std::fmt;
use std::net::IpAddr;

use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use reqwest::Error as ReqwestError;
use reqwest::Client as HttpClient;

use super::Ban;
use super::Config;

#[derive(Debug)]
pub enum Error {
    Internal(ReqwestError),
    BackendError(String)
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &*self {
            Error::Internal(e) => e.fmt(f),
            Error::BackendError(e) => f.write_fmt(format_args!("BackendError {}: ", e))
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
    if !ban.note.is_empty() {
        note.push_str(&format!("({})", ban.note));
    }

    note
}

pub async fn ban(config: &Config, http_client: &HttpClient, ban: &Ban) -> Result<(), Error> {
    let req = http_client.post(config.ddnet_ban_endpoint.clone())
        .header("x-ddnet-token", config.ddnet_token.clone())
        .query(&[
            ("ip", ban.ip.to_string()),
            ("name", ban.name.clone()),
            ("reason", get_final_reason(&ban)),
            ("region", ban.region.clone()),
            ("note", get_final_note(&ban))])
        .build()?;

    println!("{:?}", req);
    let res = http_client.execute(req).await?;

    if !res.status().is_success() {
        return Err(Error::BackendError(format!("{}: {}", res.status(), res.text().await?)));
    }

    Ok(())
}

pub async fn unban_ip(config: &Config, http_client: &HttpClient, ip: IpAddr) -> Result<(), Error> {
    let ban = Ban { ip, name: "".to_owned(), expires: Utc.timestamp(0, 0), reason: "".to_owned(), moderator: "".to_owned(), region: "".to_owned(), note: "".to_owned() };
    unban(config, http_client, &ban).await
}

pub async fn unban(config: &Config, http_client: &HttpClient, ban: &Ban) -> Result<(), Error> {
    let req = http_client.delete(config.ddnet_ban_endpoint.clone())
        .header("x-ddnet-token", config.ddnet_token.clone())
        .query(&[("ip", ban.ip.to_string())])
        .build()?;

    println!("{:?}", req);
    let res = http_client.execute(req).await?;

    if !res.status().is_success() {
        return Err(Error::BackendError(res.text().await?));
    }

    Ok(())
}
