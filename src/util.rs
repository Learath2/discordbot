use std::fmt;
use std::net::{AddrParseError, IpAddr};
use std::str::FromStr;

use sqlx::{Error as SqlError, Sqlite, SqlitePool, Transaction};
use tokio::time::{sleep, Duration};

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

pub fn sql_get_in_string(n: usize) -> String {
    if n == 0 {
        return String::from("");
    }

    let mut n = n;
    let mut s = String::from("(?");
    n -= 1;
    s.push_str(&",?".repeat(n));
    s.push(')');

    s
}

// Ideally this would be generic to allow single connections as well as pools
//     Look into `sqlx::Acquire`
// Ideally this would start an IMMEDIATE transaction
pub async fn sql_start_transaction<'a>(
    c: &SqlitePool,
) -> Result<Transaction<'a, Sqlite>, sqlx::Error> {
    static DELAY: [Duration; 3] = [
        Duration::from_millis(1),
        Duration::from_millis(2),
        Duration::from_millis(5),
    ];
    let mut tries = 0;
    Ok(loop {
        match c.begin().await {
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
    }?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_string() {
        assert_eq!(sql_get_in_string(0), "");
        assert_eq!(sql_get_in_string(1), "(?)");
        assert_eq!(sql_get_in_string(2), "(?,?)");
        assert_eq!(sql_get_in_string(3), "(?,?,?)");
    }
}
