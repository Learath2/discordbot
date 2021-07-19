use lazy_static::lazy_static;
use regex::Regex;
use std::error::Error as StdError;
use std::{fmt, str::FromStr};

use chrono::Duration;
use num_traits::PrimInt as Integer;

use crate::util::Ip;

#[derive(Debug)]
pub enum Error {
    EndOfString,
    UnmatchedQuote(char),
    ParseError(String),
    InvalidState(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let t;
        f.write_str(match self {
            Error::EndOfString => "Reached end of string while parsing",
            Error::UnmatchedQuote(c) => match c {
                '\'' => "Unmatched single quote in string",
                '"' => "Unmatched double quote in string",
                _ => unreachable!(),
            },
            Error::ParseError(e) => {
                t = format!("Error parsing token: {}", e);
                &t
            }
            Error::InvalidState(e) => {
                t = format!("Invalid Lexer State: {}. This should be unreachable", e);
                &t
            }
        })
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        None
    }
}

pub struct Lexer {
    str: String,
    i: Vec<usize>,
}

impl Lexer {
    pub fn new(str: String) -> Self {
        return Self {
            str: str.trim().to_owned(),
            i: vec![0],
        };
    }

    pub fn reset(&mut self) {
        self.i = vec![0];
    }

    pub fn rewind(&mut self) -> Result<(), Error> {
        if self.i.len() > 1 {
            self.i.pop();
            Ok(())
        } else {
            Err(Error::InvalidState(String::from("Empty history")))
        }
    }

    pub fn get_string(&mut self) -> Result<&str, Error> {
        let gstart = match self.i.last() {
            Some(u) => *u,
            None => {
                return Err(Error::InvalidState(String::from("Empty history")));
            }
        };

        let rest = &self.str[gstart..];
        if rest.is_empty() {
            return Err(Error::EndOfString);
        }

        let mut it = rest.char_indices().peekable();
        let (delimiter, quoted, start) = match it.next() {
            Some((_, c)) if c == '"' || c == '\'' => (c, true, 1usize),
            _ => (' ', false, 0usize),
        };

        let end = match quoted {
            true => {
                let r = it.find(|(_, c)| *c == delimiter);
                if r.is_none() {
                    return Err(Error::UnmatchedQuote(delimiter));
                }
                r
            }
            false => it.find(|(_, c)| *c == '"' || *c == '\'' || c.is_whitespace()),
        };

        let (end, next) = match end {
            None => (rest.len(), rest.len()),
            Some((endidx, endc)) => {
                if quoted || endc.is_whitespace() {
                    match it.find(|(_, c)| !c.is_whitespace()) {
                        Some((i, _)) => (endidx, i),
                        None => (endidx, rest.len()),
                    }
                } else {
                    (endidx, endidx)
                }
            }
        };

        self.i.push(gstart + next);
        Ok(&rest[start..end])
    }

    pub fn get_integer<T: Integer + FromStr>(&mut self) -> Result<T, Error>
    where
        <T as FromStr>::Err: fmt::Display,
    {
        let str = self.get_string()?;
        match str.parse() {
            Ok(i) => Ok(i),
            Err(e) => {
                self.rewind()?;
                Err(Error::ParseError(e.to_string()))
            }
        }
    }

    pub fn get_duration(&mut self) -> Result<Duration, Error> {
        lazy_static! {
            static ref RE: Regex = Regex::new(r"^(\d+)(mo|m|h|d|w|y)$").unwrap();
        }
        let str = self.get_string()?;
        let caps: regex::Captures = match RE.captures(str) {
            Some(m) => m,
            None => {
                return Err(Error::ParseError("Couldn't parse duration".to_owned()));
            }
        };

        match caps.len() {
            2 => {
                let i: i64 = caps[1].parse().unwrap();
                Ok(Duration::minutes(i))
            }
            3 => {
                let i: i64 = caps[1].parse().unwrap();
                Ok(match &caps[2] {
                    "m" => Duration::minutes(i),
                    "h" => Duration::hours(i),
                    "d" => Duration::days(i),
                    "w" => Duration::weeks(i),
                    "y" => Duration::days(i * 365),
                    "mo" => Duration::days(i * 30),
                    _ => unreachable!(),
                })
            }
            _ => Err(Error::ParseError("Couldn't parse duration".to_owned())),
        }
    }

    pub fn get_ip(&mut self) -> Result<Ip, Error> {
        let str = self.get_string()?;
        match Ip::from_str(str) {
            Ok(r) => Ok(r),
            Err(e) => {
                self.rewind()?;
                Err(Error::ParseError(e.to_string()))
            }
        }
    }

    pub fn get_rest(&mut self) -> Result<&str, Error> {
        let current = match self.i.last() {
            Some(u) => *u,
            None => unreachable!(),
        };
        let len = self.str.len();
        if len == current {
            return Err(Error::EndOfString);
        }

        self.i.push(len);
        Ok(&self.str[current..len])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple() {
        let mut l = Lexer::new("Test string here.".to_owned());
        assert_eq!(l.get_string().unwrap(), "Test");
        assert_eq!(l.get_string().unwrap(), "string");
        assert_eq!(l.get_string().unwrap(), "here.");
        assert!(l.get_string().is_err());

        l = Lexer::new(r#"quoted "argumen t"s are"s u"  ch "a pain""#.to_owned());
        assert_eq!(l.get_string().unwrap(), "quoted");
        assert_eq!(l.get_string().unwrap(), "argumen t");
        assert_eq!(l.get_string().unwrap(), "s");
        assert_eq!(l.get_string().unwrap(), "are");
        assert_eq!(l.get_string().unwrap(), "s u");
        assert_eq!(l.get_string().unwrap(), "ch");
        assert_eq!(l.get_string().unwrap(), "a pain");
    }

    #[test]
    fn test_edge() {
        let mut l = Lexer::new("".to_owned());
        assert!(l.get_string().is_err());

        l = Lexer::new("a".to_owned());
        assert_eq!(l.get_string().unwrap(), "a");

        l = Lexer::new(" a  b c    d ".to_owned());
        assert_eq!(l.get_string().unwrap(), "a");
        assert_eq!(l.get_string().unwrap(), "b");
        assert_eq!(l.get_string().unwrap(), "c");
        assert_eq!(l.get_string().unwrap(), "d");
        assert!(l.get_string().is_err());
    }

    #[test]
    fn test_rewind() {
        let mut l = Lexer::new("ban ip name".to_owned());
        assert_eq!(l.get_string().unwrap(), "ban");
        assert!(l.rewind().is_ok());
        assert_eq!(l.get_string().unwrap(), "ban");
        assert_eq!(l.get_string().unwrap(), "ip");
        assert!(l.rewind().is_ok());
        assert_eq!(l.get_string().unwrap(), "ip");
        assert_eq!(l.get_string().unwrap(), "name");
        assert!(l.rewind().is_ok());
        assert_eq!(l.get_string().unwrap(), "name");
        assert!((l.get_string().is_err()));
    }

    #[test]
    fn test_typed() -> Result<(), String> {
        let mut l = Lexer::new("123 abc 12a a12".to_owned());
        let a: i32 = match l.get_integer() {
            Ok(i) => i,
            Err(e) => {
                return Err(e.to_string());
            }
        };
        assert_eq!(a, 123);

        assert!(l.get_integer::<i32>().is_err());
        assert_eq!(l.get_string().unwrap(), "abc");
        assert!(l.get_integer::<i32>().is_err());
        assert_eq!(l.get_string().unwrap(), "12a");
        assert!(l.get_integer::<i32>().is_err());
        assert_eq!(l.get_string().unwrap(), "a12");

        l = Lexer::new("12d".to_owned());
        let a = match l.get_duration() {
            Ok(d) => d,
            Err(e) => {
                return Err(e.to_string());
            }
        };
        assert_eq!(a, Duration::days(12));

        Ok(())
    }
}
