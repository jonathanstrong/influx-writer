//! Utilities to efficiently send data to influx
//! 

use std::iter::FromIterator;
use std::io::{Write, Read};
use std::sync::mpsc::{Sender, Receiver, channel, SendError};
use std::thread;
use std::fs::{self, OpenOptions};
use std::time::Duration;
use std::hash::{Hash, BuildHasherDefault};

use hyper::status::StatusCode;
use hyper::client::response::Response;
use hyper::Url;
use hyper::client::Client;
use influent::measurement::{Measurement, Value};
use zmq;
use chrono::{DateTime, Utc, TimeZone};
use sloggers::types::Severity;
use ordermap::OrderMap;
use fnv::FnvHasher;
use decimal::d128;
use uuid::Uuid;

use money::Ticker;

use super::{nanos, file_logger};
use warnings::Warning;

const WRITER_ADDR: &'static str = "ipc:///tmp/mm/influx";
//const WRITER_ADDR: &'static str = "tcp://127.0.0.1:17853";
const DB_NAME: &'static str = "mm";
const DB_HOST: &'static str = "http://washington.0ptimus.internal:8086/write";
//const DB_HOST: &'static str = "http://harrison.0ptimus.internal:8086/write";
const ZMQ_RCV_HWM: i32 = 0; 
const ZMQ_SND_HWM: i32 = 0; 

const BUFFER_SIZE: u8 = 80;

pub use super::{dur_nanos, dt_nanos};

pub type Map<K, V> = OrderMap<K, V, BuildHasherDefault<FnvHasher>>;

pub fn new_map<K, V>(capacity: usize) -> Map<K, V> {
    Map::with_capacity_and_hasher(capacity, Default::default())
}

/// Provides flexible and ergonomic use of `Sender<OwnedMeasurement>`.
///
/// The macro both creates an `OwnedMeasurement` from the supplied tags and
/// values, as well as sends it with the `Sender`.
///
/// Benchmarks show around 600ns for a small measurement and 1u for a medium-sized
/// measurement (see `tests` mod).
///
/// # Examples
///
/// ```
/// #[macro_use] extern crate logging;
/// extern crate decimal;
///
/// use std::sync::mpsc::channel;
/// use decimal::d128;
/// use logging::influx::*;
///
/// fn main() {
///     let (tx, rx) = channel();
///
///     // "shorthand" syntax
///
///     measure!(tx, test, tag[color;"red"], int[n;1]);
///
///     let meas: OwnedMeasurement = rx.recv().unwrap();
///
///     assert_eq!(meas.key, "test");
///     assert_eq!(meas.tags.get("color"), Some(&"red"));
///     assert_eq!(meas.fields.get("n"), Some(&OwnedValue::Integer(1)));
///
///     // alternate syntax ...
///
///     measure!(tx, test,
///         tag [ one => "a" ],
///         tag [ two => "b" ],
///         int [ three => 2 ],
///         float [ four => 1.2345 ],
///         string [ five => String::from("d") ],
///         bool [ six => true ],
///         int [ seven => { 1 + 2 } ],
///         time [ 1 ]
///     );
///
///     let meas: OwnedMeasurement = rx.recv().unwrap();
///
///     assert_eq!(meas.key, "test");
///     assert_eq!(meas.tags.get("one"), Some(&"a"));
///     assert_eq!(meas.tags.get("two"), Some(&"b"));
///     assert_eq!(meas.fields.get("three"), Some(&OwnedValue::Integer(2)));
///     assert_eq!(meas.fields.get("seven"), Some(&OwnedValue::Integer(3)));
///     assert_eq!(meas.timestamp, Some(1));
///
///     // use the @make_meas flag to skip sending a measurement, instead merely
///     // creating it. 
///
///     let meas: OwnedMeasurement = measure!(@make_meas meas_only, tag[color; "red"], int[n; 1]);
///
///     // each variant also has shorthand aliases
///
///     let meas: OwnedMeasurement = 
///         measure!(@make_meas abcd, t[color; "red"], i[n; 1], d[price; d128::zero()]);
/// }
/// ```
///
#[macro_export]
macro_rules! measure {
    (@kv $t:tt, $meas:ident, $k:tt => $v:expr) => { measure!(@ea $t, $meas, stringify!($k), $v) };
    (@kv $t:tt, $meas:ident, $k:tt; $v:expr) => { measure!(@ea $t, $meas, stringify!($k), $v) };
    (@kv $t:tt, $meas:ident, $k:tt, $v:expr) => { measure!(@ea $t, $meas, stringify!($k), $v) };
    (@kv time, $meas:ident, $tm:expr) => { $meas = $meas.set_timestamp($tm as i64) };
    (@kv tm, $meas:ident, $tm:expr) => { $meas = $meas.set_timestamp($tm as i64) };
    (@kv $t:tt, $meas:ident, $k:tt) => { measure!(@ea $t, $meas, stringify!($k), measure!(@as_expr $k)) };
    (@ea tag, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_tag($k, $v); };
    (@ea t, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_tag($k, $v); };
    (@ea int, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_field($k, $crate::influx::OwnedValue::Integer($v as i64)) };
    (@ea i, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_field($k, $crate::influx::OwnedValue::Integer($v as i64)) };
    (@ea float, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_field($k, $crate::influx::OwnedValue::Float($v as f64)) };
    (@ea f, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_field($k, $crate::influx::OwnedValue::Float($v as f64)) };
    (@ea string, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_field($k, $crate::influx::OwnedValue::String($v)) };
    (@ea s, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_field($k, $crate::influx::OwnedValue::String($v)) };
    (@ea d128, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_field($k, $crate::influx::OwnedValue::D128($v)) };
    (@ea d, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_field($k, $crate::influx::OwnedValue::D128($v)) };
    (@ea uuid, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_field($k, $crate::influx::OwnedValue::Uuid($v)) };
    (@ea u, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_field($k, $crate::influx::OwnedValue::Uuid($v)) };
    (@ea bool, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_field($k, $crate::influx::OwnedValue::Boolean($v as bool)) };
    (@ea b, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_field($k, $crate::influx::OwnedValue::Boolean($v as bool)) };

    (@as_expr $e:expr) => {$e};
    
    (@count_tags) => {0usize};
    (@count_tags tag $($tail:tt)*) => {1usize + measure!(@count_tags $($tail)*)};
    (@count_tags $t:tt $($tail:tt)*) => {0usize + measure!(@count_tags $($tail)*)};

    (@count_fields) => {0usize};
    (@count_fields tag $($tail:tt)*) => {0usize + measure!(@count_fields $($tail)*)};
    (@count_fields time $($tail:tt)*) => {0usize + measure!(@count_fields $($tail)*)};
    (@count_fields $t:tt $($tail:tt)*) => {1usize + measure!(@count_fields $($tail)*)};

    (@make_meas $name:tt, $( $t:tt ( $($tail:tt)* ) ),+ $(,)*) => {
        measure!(@make_meas $name, $( $t [ $($tail)* ] ),*)
    };

    (@make_meas $name:tt, $( $t:tt [ $($tail:tt)* ] ),+ $(,)*) => {{
        let n_tags = measure!(@count_tags $($t)*);
        let n_fields = measure!(@count_fields $($t)*);
        let mut meas = 
            $crate::influx::OwnedMeasurement::with_capacity(stringify!($name), n_tags, n_fields);
        $(
            measure!(@kv $t, meas, $($tail)*);
        )*
        meas
    }};

    ($m:expr, $name:tt, $( $t:tt ( $($tail:tt)* ) ),+ $(,)*) => {
        measure!($m, $name, $($t [ $($tail)* ] ),+)
    };

    ($m:tt, $name:tt, $( $t:tt [ $($tail:tt)* ] ),+ $(,)*) => {{
        let measurement = measure!(@make_meas $name, $( $t [ $($tail)* ] ),*);
        let _ = $m.send(measurement);
    }};
}

/// Holds a thread (and provides an interface to it) that serializes `OwnedMeasurement`s 
/// it receives (over a SPSC channel) and inserts to influxdb via http when `BUFFER_SIZE` 
/// measurements have accumulated. 
/// 
pub struct InfluxWriter {
    host: &'static str,
    db: &'static str,
    tx: Sender<OwnedMeasurement>,
    kill_switch: Sender<()>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Default for InfluxWriter {
    fn default() -> Self {
        InfluxWriter::new("washington.0ptimus.internal", "mm_test", "var/default.log", BUFFER_SIZE)
    }
}

impl Clone for InfluxWriter {
    fn clone(&self) -> Self {
        let (tx, _) = channel();
        InfluxWriter {
            host: self.host,
            db: self.db,
            tx: self.tx.clone(),
            kill_switch: tx,
            thread: None,
        }
    }
}

impl InfluxWriter {
    /// Sends the `OwnedMeasurement` to the serialization thread. 
    /// 
    pub fn send(&self, m: OwnedMeasurement) -> Result<(), SendError<OwnedMeasurement>> {
        self.tx.send(m)
    }

    pub fn tx(&self) -> Sender<OwnedMeasurement> {
        self.tx.clone()
    }

    pub fn new(host: &'static str, db: &'static str, log_path: &str, buffer_size: u8) -> Self {
        let (kill_switch, terminate) = channel();
        let (tx, rx) = channel();
        let logger = file_logger(log_path, Severity::Info);
        let thread = thread::spawn(move || {
            info!(logger, "initializing url";
                  "DB_HOST" => host, 
                  "DB_NAME" => db);
            let url = Url::parse_with_params(&format!("http://{}:8086/write", host), &[("db", db), ("precision", "ns")]).expect("influx writer url should parse");
            let client = Client::new();
            info!(logger, "initializing buffers");
            let mut meas_buf = String::with_capacity(32 * 32 * 32);
            let mut buf = String::with_capacity(32 * 32 * 32);
            let mut count = 0;

            let next = |prev: u8, s: &str, buf: &mut String| -> u8 {
                trace!(logger, "appending serialized measurement to buffer";
                       "prev" => prev,
                       "buf.len()" => buf.len());

                match prev {
                    0 if buffer_size > 0 => {
                        buf.push_str(s);
                        1
                    }

                    n if n < buffer_size => {
                        buf.push_str("\n");
                        buf.push_str(s);
                        n + 1
                    }

                    _ => {
                        buf.push_str("\n");
                        if s.len() > 0 {
                            buf.push_str(s);
                        }
                        trace!(logger, "sending buffer to influx";
                               "buf.len()" => buf.len());

                        #[cfg(not(test))]
                        {
                            let resp = client.post(url.clone())
                                        .body(buf.as_str())
                                        .send();
                            match resp {

                                Ok(Response { status, .. }) if status == StatusCode::NoContent => {
                                    trace!(logger, "server responded ok: 204 NoContent");  
                                }

                                Ok(mut resp) =>  {
                                    let mut server_resp = String::with_capacity(1024);
                                    resp.read_to_string(&mut server_resp); //.unwrap_or(0);
                                    error!(logger, "influx server error";
                                           "status" => resp.status.to_string(),
                                           "body" => server_resp);
                                }

                                Err(why) => {
                                    error!(logger, "http request failed: {:?}", why);
                                }
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
                rx.recv_timeout(Duration::from_millis(10))
                    .map(|mut meas: OwnedMeasurement| {
                        // if we didn't set the timestamp, it would end up
                        // being whenever we accumulated `BUFFER_SIZE` messages,
                        // which might be some period of time after we received
                        // the message. 
                        //
                        if meas.timestamp.is_none() {
                            meas.timestamp = Some(now());
                        }

                        trace!(logger, "rcvd new OwnedMeasurement"; "count" => count);
                        serialize_owned(&meas, &mut meas_buf);
                        count = next(count, &meas_buf, &mut buf);
                        meas_buf.clear();
                        rcvd_msg = true;
                    });

                let end = terminate.try_recv()
                    .map(|_| {
                        let _ = next(::std::u8::MAX, "", &mut buf);
                        true
                    }).unwrap_or(false);

                if end { break }

                #[cfg(feature = "no-thrash")]
                {
                    if !rcvd_msg {
                        thread::sleep(Duration::new(0, 5000));
                    }
                }
            }

            crit!(logger, "goodbye");
        });

        InfluxWriter {
            host,
            db,
            tx,
            kill_switch,
            thread: Some(thread)
        }
    }
}

impl Drop for InfluxWriter {
    fn drop(&mut self) {
        let _ = self.kill_switch.send(()).unwrap();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
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

/// This removes offending things rather than escaping them.
///
fn escape_tag(s: &str) -> String {
    s.replace(" ", "")
     .replace(",", "")
     .replace("\"", "")
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

/// Serializes an `&OwnedMeasurement` as influx line protocol into `line`. 
/// 
/// The serialized measurement is appended to the end of the string without
/// any regard for what exited in it previously. 
/// 
pub fn serialize_owned(measurement: &OwnedMeasurement, line: &mut String) {
    line.push_str(&escape_tag(measurement.key));

    let add_tag = |line: &mut String, key: &str, value: &str| {
        line.push_str(",");
        line.push_str(&escape_tag(key));
        line.push_str("=");
        line.push_str(&escape(value));
    };

    for (key, value) in measurement.tags.iter() {
        add_tag(line, key, value);
    }

    let mut fields = measurement.fields.iter();

    let add_field = |line: &mut String, key: &str, value: &OwnedValue, is_first: bool| {
        if is_first { line.push_str(" "); } else { line.push_str(","); }
        line.push_str(&escape_tag(key));
        line.push_str("=");
        match *value {
            OwnedValue::String(ref s)  => line.push_str(&as_string(s)),
            OwnedValue::Integer(ref i) => line.push_str(&format!("{}i", i)),
            OwnedValue::Float(ref f)   => line.push_str(&format!("{}", f)),
            OwnedValue::Boolean(ref b) => line.push_str(as_boolean(b)),
            OwnedValue::D128(ref d)    => line.push_str(&format!("{}", d)),
            OwnedValue::Uuid(ref u)    => line.push_str(&format!("\"{}\"", &u.to_string()[..8])),
        };
    };

    let mut fields = measurement.fields.iter();

    // first time separate from tags with space
    //
    fields.next().map(|kv| {
        add_field(line, kv.0, kv.1, true);
    });

    // then seperate the rest w/ comma
    //
    for kv in fields {
        add_field(line, kv.0, kv.1, false);
    }

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
                                    let _ = warnings.send(
                                        Warning::Error(
                                            format!("Influx server: {}", server_resp)));
                                    server_resp.clear();
                                }

                                Err(why) => {
                                    let _ = warnings.send(
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
    Boolean(bool),
    D128(d128),
    Uuid(Uuid),
}

#[derive(Clone, Debug)]
pub struct OwnedMeasurement {
    pub key: &'static str,
    pub timestamp: Option<i64>,
    pub fields: Map<&'static str, OwnedValue>,
    pub tags: Map<&'static str, &'static str>,
    //pub n_tags: usize,
    //pub n_fields: usize,
    //pub string_tags: HashMap<&'static str, String>
}

impl OwnedMeasurement {
    pub fn with_capacity(key: &'static str, n_tags: usize, n_fields: usize) -> Self {
        OwnedMeasurement {
            key,
            timestamp: None,
            tags: new_map(n_tags),
            fields: new_map(n_fields),
            //n_tags,
            //n_fields,
            //string_tags: HashMap::new()
        }
    }

    pub fn new(key: &'static str) -> Self {
        OwnedMeasurement::with_capacity(key, 4, 4)
    }

    pub fn add_tag(mut self, key: &'static str, value: &'static str) -> Self {
        self.tags.insert(key, value);
        self
    }

    // pub fn add_string_tag(mut self, key: &'static str, value: String) -> Self {
    //     self.string_tags.insert(key, value);
    //     self
    // }

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

#[cfg(test)]
mod tests {
    use super::*;
    use test::{black_box, Bencher};

    #[test]
    fn it_uses_the_new_tag_k_only_shortcut() {
        let tag_value = "one";
        let color = "red";
        let time = now();
        let m = measure!(@make_meas test, t(color), t(tag_value), tm(time));
        assert_eq!(m.tags.get("color"), Some(&"red"));
        assert_eq!(m.tags.get("tag_value"), Some(&"one"));
        assert_eq!(m.timestamp, Some(time));
    }

    #[test]
    fn it_uses_measure_macro_parenthesis_syntax() {
        let m = measure!(@make_meas test, t(a,"b"), i(n,1), f(x,1.1), tm(1));
        assert_eq!(m.key, "test");
        assert_eq!(m.tags.get("a"), Some(&"b"));
        assert_eq!(m.fields.get("n"), Some(&OwnedValue::Integer(1)));
        assert_eq!(m.fields.get("x"), Some(&OwnedValue::Float(1.1)));
        assert_eq!(m.timestamp, Some(1));
    }

    #[test]
    fn it_uses_measure_macro_on_a_self_attribute() {
        struct A {
            pub influx: InfluxWriter,
        }

        impl A {
            fn f(&self) {
                measure!(self.influx, test, t(color, "red"), i(n, 1));
            }
        }

        let a = A { influx: InfluxWriter::default() };

        a.f();
    }

    #[bench]
    fn influx_writer_send_basic(b: &mut Bencher) {
        let m = InfluxWriter::default();
        b.iter(|| {
            measure!(m, test, tag[color; "red"], int[n; 1], float[p; 1.234]);
        });
    }

    #[bench]
    fn influx_writer_send_price(b: &mut Bencher) {
        let m = InfluxWriter::default();
        b.iter(|| {
            measure!(m, test, 
                tag[ticker; t!(xmr-btc).to_str()], 
                tag[exchange; "plnx"],
                d128[bid; d128::zero()],
                d128[ask; d128::zero()],
            );
        });
    }

    #[test]
    fn it_checks_color_tag_error_in_non_doctest() {
        let (tx, rx) = channel();
        measure!(tx, test, tag[color;"red"], int[n;1]);
        let meas: OwnedMeasurement = rx.recv().unwrap();
        assert_eq!(meas.tags.get("color"), Some(&"red"), "meas = \n {:?} \n", meas);
    }

    #[test]
    fn it_uses_the_make_meas_pattern_of_the_measure_macro() {
        let meas = measure!(@make_meas test_measurement,
            tag [ one => "a" ],
            tag [ two => "b" ],
            int [ three => 2 ],
            float [ four => 1.2345 ],
            string [ five => String::from("d") ],
            bool [ six => true ],
            int [ seven => { 1 + 2 } ],
            time [ 1 ]
        );
        assert_eq!(meas.key, "test_measurement");
        assert_eq!(meas.tags.get("one"), Some(&"a"));
        assert_eq!(meas.tags.get("two"), Some(&"b"));
        assert_eq!(meas.fields.get("three"), Some(&OwnedValue::Integer(2)));
        assert_eq!(meas.fields.get("seven"), Some(&OwnedValue::Integer(3)));
        assert_eq!(meas.timestamp, Some(1));
    }

    #[test]
    fn it_uses_the_measure_macro() {
        let (tx, rx) = channel();
        measure!(tx, test_measurement,
            tag [ one => "a" ],
            tag [ two => "b" ],
            int [ three => 2 ],
            float [ four => 1.2345 ],
            string [ five => String::from("d") ],
            bool [ six => true ],
            int [ seven => { 1 + 2 } ],
            time [ 1 ]
        );
        thread::sleep_ms(10);
        let meas: OwnedMeasurement = rx.try_recv().unwrap();
        assert_eq!(meas.key, "test_measurement");
        assert_eq!(meas.tags.get("one"), Some(&"a"));
        assert_eq!(meas.tags.get("two"), Some(&"b"));
        assert_eq!(meas.fields.get("three"), Some(&OwnedValue::Integer(2)));
        assert_eq!(meas.fields.get("seven"), Some(&OwnedValue::Integer(3)));
        assert_eq!(meas.timestamp, Some(1));
    }

    #[test]
    fn it_uses_measure_macro_for_d128_and_uuid() {

        let (tx, rx) = channel();
        let u = Uuid::new_v4();
        let d = d128::zero();
        let t = now();
        measure!(tx, test_measurement,
            tag[one; "a"],
            d128[two; d],
            uuid[three; u],
            time[t]
        );

        thread::sleep_ms(10);
        let meas: OwnedMeasurement = rx.try_recv().unwrap();
        assert_eq!(meas.key, "test_measurement");
        assert_eq!(meas.tags.get("one"), Some(&"a"));
        assert_eq!(meas.fields.get("two"), Some(&OwnedValue::D128(d128::zero())));
        assert_eq!(meas.fields.get("three"), Some(&OwnedValue::Uuid(u)));
        assert_eq!(meas.timestamp, Some(t));
    }

    #[test]
    fn it_uses_the_measure_macro_alt_syntax() {

        let (tx, rx) = channel();
        measure!(tx, test_measurement,
            tag[one; "a"],
            tag[two; "b"],
            int[three; 2],
            float[four; 1.2345],
            string[five; String::from("d")],
            bool [ six => true ],
            int[seven; { 1 + 2 }],
            time[1]
        );

        thread::sleep_ms(10);
        let meas: OwnedMeasurement = rx.try_recv().unwrap();
        assert_eq!(meas.key, "test_measurement");
        assert_eq!(meas.tags.get("one"), Some(&"a"));
        assert_eq!(meas.tags.get("two"), Some(&"b"));
        assert_eq!(meas.fields.get("three"), Some(&OwnedValue::Integer(2)));
        assert_eq!(meas.fields.get("seven"), Some(&OwnedValue::Integer(3)));
        assert_eq!(meas.timestamp, Some(1));
    }

    #[test]
    fn it_checks_that_fields_are_separated_correctly() {
        let m = measure!(@make_meas test, t[a; "one"], t[b; "two"], f[x; 1.1], f[y; -1.1]);
        assert_eq!(m.key, "test");
        assert_eq!(m.tags.get("a"), Some(&"one"));
        assert_eq!(m.fields.get("x"), Some(&OwnedValue::Float(1.1)));

        let mut buf = String::new();
        serialize_owned(&m, &mut buf);
        assert!(buf.contains("b=two x=1.1,y=-1.1"), "buf = {}", buf);
    }

    #[test]
    fn try_to_break_measure_macro() {
        let (tx, _) = channel();
        measure!(tx, one, tag[x=>"y"], int[n;1]);
        measure!(tx, one, tag[x;"y"], int[n;1],);

        struct A {
            pub one: i32,
            pub two: i32,
        }

        struct B {
            pub a: A
        }

        let b = B { a: A { one: 1, two: 2 } };

        let m = measure!(@make_meas test, t(name, "a"), i(a, b.a.one));

        assert_eq!(m.fields.get("a"), Some(&OwnedValue::Integer(1)));
    }

    #[bench]
    fn measure_macro_small(b: &mut Bencher) {
        let (tx, rx) = channel();
        let listener = thread::spawn(move || {
            loop { if rx.recv().is_err() { break } }
        });
        b.iter(|| {
            measure!(tx, test, tag[color; "red"], int[n; 1], time[now()]);
        });
    }

    #[bench]
    fn measure_macro_medium(b: &mut Bencher) {
        let (tx, rx) = channel();
        let listener = thread::spawn(move || {
            loop { if rx.recv().is_err() { break } }
        });
        b.iter(|| {
            measure!(tx, test, 
                tag[color; "red"], 
                tag[mood => "playful"],
                tag [ ticker => "xmr_btc" ],
                float[ price => 1.2345 ],
                float[ amount => 56.323],
                int[n; 1], 
                time[now()]
            );
        });
    }


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

    // macro_rules! make_measurement {
    //     (@kv $t:tt, $meas:ident, $k:tt => $v:expr) => { measure!(@ea $t, $meas, stringify!($k), $v) };
    //     (@kv $t:tt, $meas:ident, $k:tt; $v:expr) => { measure!(@ea $t, $meas, stringify!($k), $v) };
    //     (@kv time, $meas:ident, $tm:expr) => { $meas = $meas.set_timestamp($tm as i64) };
    //     (@ea tag, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_tag($k, $v); };
    //     (@ea int, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_field($k, $crate::influx::OwnedValue::Integer($v)) };
    //     (@ea float, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_field($k, $crate::influx::OwnedValue::Float($v)) };
    //     (@ea string, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_field($k, $crate::influx::OwnedValue::String($v)) };
    //     (@ea bool, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_field($k, $crate::influx::OwnedValue::Boolean($v)) };
    //     
    //     (@count_tags) => {0usize};
    //     (@count_tags tag $($tail:tt)*) => {1usize + measure!(@count_tags $($tail)*)};
    //     (@count_tags $t:tt $($tail:tt)*) => {0usize + measure!(@count_tags $($tail)*)};
    // 
    //     (@count_fields) => {0usize};
    //     (@count_fields tag $($tail:tt)*) => {0usize + measure!(@count_fields $($tail)*)};
    //     (@count_fields time $($tail:tt)*) => {0usize + measure!(@count_fields $($tail)*)};
    //     (@count_fields $t:tt $($tail:tt)*) => {1usize + measure!(@count_fields $($tail)*)};
    // 
    //     ($m:tt, $name:tt, $( $t:tt [ $($tail:tt)* ] ),+ $(,)*) => {{
    //         let n_tags = measure!(@count_tags $($t)*);
    //         let n_fields = measure!(@count_fields $($t)*);
    //         let mut meas = 
    //             $crate::influx::OwnedMeasurement::with_capacity(stringify!($name), n_tags, n_fields);
    //         $(
    //             measure!(@kv $t, meas, $($tail)*);
    //         )*
    //         //let _ = $m.send(meas);
    //         meas
    //     }};
    // }
    // 
    // #[test]
    // fn it_checks_n_tags_is_correct() {
    //     let (tx, _): (Sender<OwnedMeasurement>, Receiver<OwnedMeasurement>) = channel();
    //     assert_eq!(make_measurement!(tx, test, tag[a;"b"]).n_tags, 1);
    //     assert_eq!(make_measurement!(tx, test, tag[a;"b"], tag[c;"d"]).n_tags, 2);
    //     assert_eq!(make_measurement!(tx, test, int[a;1]).n_tags, 0);
    //     assert_eq!(make_measurement!(tx, test, tag[a;"b"], tag[c;"d"]).n_fields, 0);
    // 
    //     let m4 = 
    //         make_measurement!(tx, test, 
    //             tag[a;"b"],
    //             tag[c;"d"],
    //             int[n; 1],
    //             tag[e;"f"],
    //             float[x; 1.234],
    //             tag[g;"h"],
    //             time[1],
    //         );
    //     assert_eq!(m4.n_tags, 4);
    //     assert_eq!(m4.n_fields, 2);
    // }


}
