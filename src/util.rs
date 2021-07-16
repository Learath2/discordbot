use std::fmt;
use std::net::{AddrParseError, IpAddr};
use std::str::FromStr;

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
