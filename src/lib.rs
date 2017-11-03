//! Tools to record and display what's happening in your program
//!

#![feature(test)]

#[macro_use] extern crate slog;
#[macro_use] extern crate money;

extern crate test;
extern crate zmq;
extern crate influent;
extern crate chrono;
extern crate hyper;
extern crate termion;
//extern crate pub_sub;
extern crate sloggers;
extern crate slog_term;
extern crate shuteye;
//extern crate chashmap;

extern crate windows;
extern crate pubsub as pub_sub;

use std::sync::Arc;

use chrono::{DateTime, Utc};
use sloggers::Build;
use sloggers::types::{Severity, TimeZone};
use sloggers::file::FileLoggerBuilder;

pub mod influx;
pub mod warnings;
pub mod latency;

//pub type FileLogger = slog::Logger<Arc<slog::SendSyncRefUnwindSafeDrain<Ok=(), Err=slog::private::NeverStruct>>>;

/// converts a chrono::DateTime to an integer timestamp (ns)
///
pub fn nanos(t: DateTime<Utc>) -> u64 {
    (t.timestamp() as u64) * 1_000_000_000_u64 + (t.timestamp_subsec_nanos() as u64)
}

pub fn file_logger(path: &str, level: Severity) -> slog::Logger {
    let mut builder = FileLoggerBuilder::new(path);
    builder.level(level);
    builder.timezone(TimeZone::Utc);
    builder.build().unwrap()
}
