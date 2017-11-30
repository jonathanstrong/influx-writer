//! Utilities to efficiently send data to influx
//! 

use std::iter::FromIterator;
use std::io::{Write, Read};
use std::sync::mpsc::{Sender, Receiver, channel};
use std::thread;
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::time::Duration;

use hyper::status::StatusCode;
use hyper::client::response::Response;
use hyper::Url;
use hyper::client::Client;
use influent::measurement::{Measurement, Value};
use zmq;
use chrono::{DateTime, Utc, TimeZone};
use sloggers::types::Severity;
use shuteye;

use super::{nanos, file_logger};
use warnings::Warning;

const WRITER_ADDR: &'static str = "ipc:///tmp/mm/influx";
//const WRITER_ADDR: &'static str = "tcp://127.0.0.1:17853";
const DB_NAME: &'static str = "mm";
const DB_HOST: &'static str = "http://washington.0ptimus.internal:8086/write";
//const DB_HOST: &'static str = "http://harrison.0ptimus.internal:8086/write";
const ZMQ_RCV_HWM: i32 = 0; 
const ZMQ_SND_HWM: i32 = 0; 

/// # Examples
///
/// ```rust,ignore
///
/// let M = // ... Sender<OwnedMeasurement>
///
/// measure![m; meas_name] {
///     tag   [ "ticker" => "xmr_btc" ],
///     int   [ "len" => 2 ]
///     float [ "x" => 1.234 ]
///     time  [ now() ]
/// };
/// ```
///
/// Resolves to:
///
/// ```rust,ignore
/// let measurements = // ...
///
/// measurements.send(
///     OwnedMeasurement::new("meas_name")
///         .add_tag("ticker", "xmr_btc")
///         .add_field("len", OwnedValue::Integer(2))
///         .add_field("x", OwnedValue::Float(1.234))
///         .set_timestamp(now() as i64));
/// ```
///
macro_rules! measure {
    () => {}
}

pub fn pull(ctx: &zmq::Context) -> Result<zmq::Socket, zmq::Error> {
    let socket = ctx.socket(zmq::PULL)?;
    socket.bind(WRITER_ADDR)?;
    socket.set_rcvhwm(ZMQ_RCV_HWM)?;
    Ok(socket)
}

pub fn push(ctx: &zmq::Context) -> Result<zmq::Socket, zmq::Error> {
    let socket = ctx.socket(zmq::PUSH)?;
    socket.connect(WRITER_ADDR)?;
    socket.set_sndhwm(ZMQ_SND_HWM)?;
    Ok(socket)
}

fn escape(s: &str) -> String {
    s.replace(" ", "\\ ")
     .replace(",", "\\,")
}

fn as_string(s: &str) -> String {
    // the second replace removes double escapes
    //
    format!("\"{}\"", s.replace("\"", "\\\"")
                       .replace(r#"\\""#, r#"\""#))
}

#[test]
fn it_checks_as_string_does_not_double_escape() {
    let raw = "this is \\\"an escaped string\\\" so it's problematic";
    let escaped = as_string(&raw);
    assert_eq!(escaped, format!("\"{}\"", raw).as_ref());
}

fn as_integer(i: &i64) -> String {
    format!("{}i", i)
}

fn as_float(f: &f64) -> String {
    f.to_string()
}

fn as_boolean(b: &bool) -> &str {
    if *b { "t" } else { "f" }
}

pub fn now() -> i64 {
    nanos(Utc::now()) as i64
}

/// Serialize the measurement into influx line protocol
/// and append to the buffer.
/// 
/// # Examples
/// 
/// ```
/// extern crate influent;
/// extern crate logging;
///
/// use influent::measurement::{Measurement, Value};
/// use std::string::String;
/// use logging::influx::serialize;
///
/// fn main() {
///     let mut buf = String::new();
///     let mut m = Measurement::new("test");
///     m.add_field("x", Value::Integer(1));
///     serialize(&m, &mut buf);
/// }
/// 
/// ```
/// 
pub fn serialize(measurement: &Measurement, line: &mut String) {
    line.push_str(&escape(measurement.key));

    for (tag, value) in measurement.tags.iter() {
        line.push_str(",");
        line.push_str(&escape(tag));
        line.push_str("=");
        line.push_str(&escape(value));
    }

    let mut was_spaced = false;

    for (field, value) in measurement.fields.iter() {
        line.push_str({if !was_spaced { was_spaced = true; " " } else { "," }});
        line.push_str(&escape(field));
        line.push_str("=");

        match value {
            &Value::String(ref s)  => line.push_str(&as_string(s)),
            &Value::Integer(ref i) => line.push_str(&as_integer(i)),
            &Value::Float(ref f)   => line.push_str(&as_float(f)),
            &Value::Boolean(ref b) => line.push_str(as_boolean(b))
        };
    }

    match measurement.timestamp {
        Some(t) => {
            line.push_str(" ");
            line.push_str(&t.to_string());
        }
        _ => {}
    }
}

pub fn serialize_owned(measurement: &OwnedMeasurement, line: &mut String) {
    line.push_str(&escape(measurement.key));

    let add_tag = |line: &mut String, key: &str, value: &str| {
        line.push_str(",");
        line.push_str(&escape(key));
        line.push_str("=");
        line.push_str(&escape(value));
    };

    for (key, value) in measurement.tags.iter() {
        add_tag(line, key, value);
    }

    for (key, value) in measurement.string_tags.iter() {
        add_tag(line, key, value);
    }

    //let mut was_spaced = false;
    let mut fields = measurement.fields.iter();

    // first time separate from tags with space
    //
    fields.next().map(|kv| {
        line.push_str(" ");
        line.push_str(&escape(kv.0));
        line.push_str("=");
        match kv.1 {
            &OwnedValue::String(ref s)  => line.push_str(&as_string(s)),
            &OwnedValue::Integer(ref i) => line.push_str(&as_integer(i)),
            &OwnedValue::Float(ref f)   => line.push_str(&as_float(f)),
            &OwnedValue::Boolean(ref b) => line.push_str(as_boolean(b))
        };
    });

    //for (field, value) in measurement.fields.iter() {
    // subsequent times seperate with comma
    for kv in fields {
        //line.push_str({if !was_spaced { was_spaced = true; " " } else { "," }});
        //line.push_str(&escape(field));
        line.push_str(",");
        line.push_str(&escape(kv.0));
        line.push_str("=");

        match kv.1 {
            &OwnedValue::String(ref s)  => line.push_str(&as_string(s)),
            &OwnedValue::Integer(ref i) => line.push_str(&as_integer(i)),
            &OwnedValue::Float(ref f)   => line.push_str(&as_float(f)),
            &OwnedValue::Boolean(ref b) => line.push_str(as_boolean(b))
        };
    }

    //match measurement.timestamp {
    if let Some(t) = measurement.timestamp {
        line.push_str(" ");
        line.push_str(&t.to_string());
    }
}




pub fn writer(warnings: Sender<Warning>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let _ = fs::create_dir("/tmp/mm");
        let ctx = zmq::Context::new();
        let socket = pull(&ctx).expect("influx::writer failed to create pull socket");
        let url = Url::parse_with_params(DB_HOST, &[("db", DB_NAME), ("precision", "ns")]).expect("influx writer url should parse");
        let client = Client::new();
        let mut buf = String::with_capacity(4096);
        let mut server_resp = String::with_capacity(4096);
        let mut count = 0;
        loop {
            if let Ok(bytes) = socket.recv_bytes(0) {
                if let Ok(msg) = String::from_utf8(bytes) {
                    count = match count {
                        0 => { 
                            buf.push_str(&msg);
                            1
                        }
                        n @ 1...40 => {
                            buf.push_str("\n");
                            buf.push_str(&msg);
                            n + 1
                        }
                        _ => {
                            buf.push_str("\n");
                            buf.push_str(&msg);
                            match client.post(url.clone())
                                        .body(&buf)
                                        .send() {

                                Ok(Response { status, .. }) if status == StatusCode::NoContent => {}

                                Ok(mut resp) =>  {
                                    resp.read_to_string(&mut server_resp); //.unwrap_or(0);
                                    warnings.send(
                                        Warning::Error(
                                            format!("Influx server: {}", server_resp)));
                                    server_resp.clear();
                                }

                                Err(why) => {
                                    warnings.send(
                                        Warning::Error(
                                            format!("Influx write error: {}", why)));
                                }
                            }
                            buf.clear();
                            0
                        }
                    }
                }
            }
        }
    })
}

#[derive(Debug, Clone, PartialEq)]
pub enum OwnedValue {
    String(String),
    Float(f64),
    Integer(i64),
    Boolean(bool)
}

#[derive(Clone)]
pub struct OwnedMeasurement {
    pub key: &'static str,
    pub timestamp: Option<i64>,
    pub fields: HashMap<&'static str, OwnedValue>,
    pub tags: HashMap<&'static str, &'static str>,
    pub string_tags: HashMap<&'static str, String>
}

impl OwnedMeasurement {
    pub fn new(key: &'static str) -> Self {
        OwnedMeasurement {
            key,
            timestamp: None,
            fields: HashMap::new(),
            tags: HashMap::new(),
            string_tags: HashMap::new()
        }
    }

    pub fn add_tag(mut self, key: &'static str, value: &'static str) -> Self {
        self.tags.insert(key, value);
        self
    }

    pub fn add_string_tag(mut self, key: &'static str, value: String) -> Self {
        self.string_tags.insert(key, value);
        self
    }

    pub fn add_field(mut self, key: &'static str, value: OwnedValue) -> Self {
        self.fields.insert(key, value);
        self
    }

    pub fn set_timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    pub fn set_tag(mut self, key: &'static str, value: &'static str) -> Self {
        *self.tags.entry(key).or_insert(value) = value;
        self
    }
}

pub fn dur_nanos(d: ::std::time::Duration) -> i64 {
    (d.as_secs() * 1_000_000_000_u64 + (d.subsec_nanos() as u64)) as i64
}

//pub fn now() -> i64 { ::latency::dt_nanos(Utc::now()) }

/// exactly like `writer`, but also returns a `Sender<Measurement>` and accepts
/// incoming `Measurement`s that way *in addition* to the old socket/`String`
/// method
///
pub struct InfluxWriter {
    kill_switch: Sender<()>,
    thread: Option<thread::JoinHandle<()>>,
}

impl InfluxWriter {

    pub fn new(log_path: &str, warnings: Sender<Warning>) -> (Self, Sender<OwnedMeasurement>) {
        let (kill_switch, terminate) = channel();
        let (tx, rx) = channel();
        let logger = file_logger(log_path, Severity::Info);
        let thread = thread::spawn(move || {
            info!(logger, "initializing zmq");
            let _ = fs::create_dir("/tmp/mm");
            let ctx = zmq::Context::new();
            let socket = pull(&ctx).expect("influx::writer failed to create pull socket");
            info!(logger, "initializing url";
                  "DB_HOST" => DB_HOST, 
                  "DB_NAME" => DB_NAME);
            let url = Url::parse_with_params(DB_HOST, &[("db", DB_NAME), ("precision", "ns")]).expect("influx writer url should parse");
            let client = Client::new();
            info!(logger, "initializing buffers");
            let mut meas_buf = String::with_capacity(4096);
            let mut buf = String::with_capacity(4096);
            let mut server_resp = String::with_capacity(4096);
            let mut count = 0;

            let next = |prev: u8, s: &str, buf: &mut String| -> u8 {
                debug!(logger, "appending serialized measurement to buffer";
                       "prev" => prev,
                       "buf.len()" => buf.len());
                match prev {
                    0 => { 
                        buf.push_str(s);
                        1
                    }

                    n @ 1...80 => {
                        buf.push_str("\n");
                        buf.push_str(s);
                        n + 1
                    }

                    _ => {
                        buf.push_str("\n");
                        if s.len() > 0 {
                            buf.push_str(s);
                        }
                        debug!(logger, "sending buffer to influx";
                               "buf.len()" => buf.len());

                        let resp = client.post(url.clone())
                                    .body(buf.as_str())
                                    .send();
                        match resp {

                            Ok(Response { status, .. }) if status == StatusCode::NoContent => {
                                debug!(logger, "server responded ok: 204 NoContent");  
                            }

                            Ok(mut resp) =>  {
                                let mut server_resp = String::with_capacity(1024);
                                //server_resp.push_str(&format!("sent at {}:\n", Utc::now()));
                                //server_resp.push_str(&buf);
                                //server_resp.push_str("\nreceived:\n");
                                resp.read_to_string(&mut server_resp); //.unwrap_or(0);
                                error!(logger, "influx server error";
                                       "status" => resp.status.to_string(),
                                       "body" => server_resp);
                            }

                            Err(why) => {
                                error!(logger, "http request failed: {:?}", why);
                                // warnings.send(
                                //     Warning::Error(
                                //         format!("Influx write error: {}", why)));
                            }
                        }
                        buf.clear();
                        0
                    }
                }
            };

            let mut rcvd_msg = false;

            loop {
                rcvd_msg = false;
                rx.try_recv()
                    .map(|meas| {
                        debug!(logger, "rcvd new OwnedMeasurement";
                               "count" => count);
                        serialize_owned(&meas, &mut meas_buf);
                        count = next(count, &meas_buf, &mut buf);
                        meas_buf.clear();
                        rcvd_msg = true;
                    });

                socket.recv_bytes(zmq::DONTWAIT).ok()
                    .and_then(|bytes| {
                        String::from_utf8(bytes).ok()
                    }).map(|s| {
                        debug!(logger, "rcvd new serialized";
                               "count" => count);
                        count = next(count, &s, &mut buf);
                        rcvd_msg = true;
                    });

                let end = terminate.try_recv()
                    .map(|_| {
                        let _ = next(::std::u8::MAX, "", &mut buf);
                        true
                    }).unwrap_or(false);

                if end { break }

                if !rcvd_msg {
                    #[cfg(feature = "no-thrash")]
                    shuteye::sleep(Duration::new(0, 5000));
                }
            }

            crit!(logger, "goodbye");
        });
        let writer = InfluxWriter {
            kill_switch,
            thread: Some(thread)
        };
        (writer, tx)
    }
}

impl Drop for InfluxWriter {
    fn drop(&mut self) {
        self.kill_switch.send(());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

mod tests {
    use super::*;
    use test::{black_box, Bencher};

    #[test]
    #[ignore]
    fn it_spawns_a_writer_thread_and_sends_dummy_measurement_to_influxdb() {
        let ctx = zmq::Context::new();
        let socket = push(&ctx).unwrap();
        let (tx, rx) = channel();
        let w = writer(tx.clone());
        let mut buf = String::with_capacity(4096);
        let mut meas = Measurement::new("rust_test");
        meas.add_tag("a", "t");
        meas.add_field("c", Value::Float(1.23456));
        let now = now();
        meas.set_timestamp(now);
        serialize(&meas, &mut buf);
        socket.send_str(&buf, 0);
        drop(w);
    }

    #[test]
    fn it_serializes_a_measurement_in_place() {
        let mut buf = String::with_capacity(4096);
        let mut meas = Measurement::new("rust_test");
        meas.add_tag("a", "b");
        meas.add_field("c", Value::Float(1.0));
        let now = now();
        meas.set_timestamp(now);
        serialize(&meas, &mut buf);
        let ans = format!("rust_test,a=b c=1 {}", now);
        assert_eq!(buf, ans);
    }

    #[test]
    fn it_serializes_a_hard_to_serialize_message() {
        let raw = r#"error encountered trying to send krkn order: Other("Failed to send http request: Other("Resource temporarily unavailable (os error 11)")")"#;
        let mut buf = String::new();
        let mut server_resp = String::new();
        let mut m = Measurement::new("rust_test");
        m.add_field("s", Value::String(&raw));
        let now = now();
        m.set_timestamp(now);
        serialize(&m, &mut buf);
        println!("{}", buf);
        buf.push_str("\n");
        let buf_copy = buf.clone();
        buf.push_str(&buf_copy);
        println!("{}", buf);

        let url = Url::parse_with_params(DB_HOST, &[("db", DB_NAME), ("precision", "ns")]).expect("influx writer url should parse");
        let client = Client::new();
        match client.post(url.clone())
                    .body(&buf)
                    .send() {

            Ok(Response { status, .. }) if status == StatusCode::NoContent => {}

            Ok(mut resp) =>  {
                resp.read_to_string(&mut server_resp); //.unwrap_or(0);
                panic!("{}", server_resp);
            }

            Err(why) => {
                panic!(why)
            }
        }

    }

    #[bench]
    fn serialize_owned_longer(b: &mut Bencher) {
        let mut buf = String::with_capacity(1024);
        let m = 
            OwnedMeasurement::new("test")
                .add_tag("one", "a")
                .add_tag("two", "b")
                .add_tag("ticker", "xmr_btc")
                .add_tag("exchange", "plnx")
                .add_tag("side", "bid")
                .add_field("three", OwnedValue::Float(1.2345))
                .add_field("four", OwnedValue::Integer(57))
                .add_field("five", OwnedValue::Boolean(true))
                .add_field("six", OwnedValue::String(String::from("abcdefghijklmnopqrstuvwxyz")))
                .set_timestamp(now());
        b.iter(|| {
            serialize_owned(&m, &mut buf);
            buf.clear()
        });
    }

    #[bench]
    fn serialize_owned_simple(b: &mut Bencher) {
        let mut buf = String::with_capacity(1024);
        let m = 
            OwnedMeasurement::new("test")
                .add_tag("one", "a")
                .add_tag("two", "b")
                .add_field("three", OwnedValue::Float(1.2345))
                .add_field("four", OwnedValue::Integer(57))
                .set_timestamp(now());
        b.iter(|| {
            serialize_owned(&m, &mut buf);
            buf.clear()
        });
    }


    #[test]
    fn it_serializes_a_hard_to_serialize_message_from_owned() {
        let raw = r#"error encountered trying to send krkn order: Other("Failed to send http request: Other("Resource temporarily unavailable (os error 11)")")"#;
        let mut buf = String::new();
        let mut server_resp = String::new();
        let mut m = OwnedMeasurement::new("rust_test")
            .add_field("s", OwnedValue::String(raw.to_string()))
            .set_timestamp(now());
        serialize_owned(&m, &mut buf);
        println!("{}", buf);
        buf.push_str("\n");
        let buf_copy = buf.clone();
        buf.push_str(&buf_copy);
        println!("{}", buf);

        let url = Url::parse_with_params(DB_HOST, &[("db", DB_NAME), ("precision", "ns")]).expect("influx writer url should parse");
        let client = Client::new();
        match client.post(url.clone())
                    .body(&buf)
                    .send() {

            Ok(Response { status, .. }) if status == StatusCode::NoContent => {}

            Ok(mut resp) =>  {
                resp.read_to_string(&mut server_resp); //.unwrap_or(0);
                panic!("{}", server_resp);
            }

            Err(why) => {
                panic!(why)
            }
        }
    }
}
