//! Utilities to efficiently send data to influx
//! 

use std::iter::FromIterator;
use std::io::Read;
use std::sync::mpsc::{Sender, Receiver, channel};
use std::thread;

use hyper::status::StatusCode;
use hyper::client::response::Response;
use hyper::Url;
use hyper::client::Client;
use influent::measurement::{Measurement, Value};
use zmq;
use chrono::{DateTime, Utc, TimeZone};

use super::nanos;
use warnings::Warning;

const WRITER_ADDR: &'static str = "ipc://mm-influx";
//const WRITER_ADDR: &'static str = "tcp://127.0.0.1:17853";
const DB_NAME: &'static str = "mm";
const DB_HOST: &'static str = "http://localhost:8086/write";
const ZMQ_RCV_HWM: i32 = 0; 
const ZMQ_SND_HWM: i32 = 0; 

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
    s
        .replace(" ", "\\ ")
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


pub fn writer(warnings: Sender<Warning>) -> thread::JoinHandle<()> {

    let ctx = zmq::Context::new();

    let socket = pull(&ctx).expect("influx::writer failed to create pull socket");

    let url = Url::parse_with_params(DB_HOST, &[("db", DB_NAME), ("precision", "ns")]).expect("influx writer url should parse");

    let client = Client::new();

    let mut buf = String::with_capacity(4096);

    let mut server_resp = String::with_capacity(4096);

    let mut count = 0;

    thread::spawn(move || {
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
                                    //let mut body = String::with_capacity(4096);
                                    //let _ = 
                                    resp.read_to_string(&mut server_resp); //.unwrap_or(0);
                                    //println!("Influx write error: Server responded {} (sent '{}' to {}):\n{}", 
                                    //warnings.send(Warning::Error(buf.clone()));
                                    //print!("\n\n\n\n\n{}", buf);
                                    warnings.send(
                                        Warning::Error(
                                            format!("Influx server: {}", server_resp)));
                                    server_resp.clear();

                                    //resp.status, String::from_utf8_lossy(&bytes), url, body);
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

mod tests {
    use super::*;

    #[test]
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

}
