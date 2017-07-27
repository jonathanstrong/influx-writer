//! Tools to record and display what's happening in your program
//!

extern crate zmq;
extern crate influent;
extern crate chrono;
extern crate hyper;
extern crate termion;
extern crate pub_sub;

extern crate windows;

use chrono::{DateTime, Utc};

pub mod influx;
pub mod warnings;
pub mod latency;

/// converts a chrono::DateTime to an integer timestamp (ns)
///
pub fn nanos(t: DateTime<Utc>) -> u64 {
    (t.timestamp() as u64) * 1_000_000_000_u64 + (t.timestamp_subsec_nanos() as u64)
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}
