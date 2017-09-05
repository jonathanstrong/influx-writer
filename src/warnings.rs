//! An object to handle everyone's errors
//! 

use std::thread::{self, JoinHandle};
use std::sync::{Arc, Mutex, RwLock};
use std::sync::mpsc::{self, Sender, Receiver, channel};
use std::collections::{BTreeMap, VecDeque};
use std::fmt::{self, Display, Error as FmtError, Formatter};

use zmq;
use chrono::{DateTime, Utc, TimeZone};
use termion::color::{self, Fg, Bg};
use influent::measurement::{Measurement, Value as InfluentValue};
use slog::{self, OwnedKVList, Drain, Key, KV};
use sloggers::types::Severity;

use super::{nanos, file_logger};
use influx;


const N_WARNINGS: usize = 150;

#[macro_export]
macro_rules! confirmed {
    ($warnings:ident, $($args:tt)*) => (
        {
            $warnings.send(Warning::Confirmed( ( format!($($args)*) ) ) ).unwrap();
        }
    )
}

/// logs a `Warning::Awesome` message to the `WarningsManager`
#[macro_export]
macro_rules! awesome {
    ($warnings:ident, $($args:tt)*) => (
        {
            $warnings.send(Warning::Awesome( ( format!($($args)*) ) ) ).unwrap();
        }
    )
}

#[macro_export]
macro_rules! critical {
    ($warnings:ident, $($args:tt)*) => (
        {
            $warnings.send(Warning::Critical( ( format!($($args)*) ) ) ).unwrap();
        }
    )
}

#[macro_export]
macro_rules! notice {
    ($warnings:ident, $($args:tt)*) => (
        {
            $warnings.send(Warning::Notice( ( format!($($args)*) ) ) ).unwrap();
        }
    )
}

#[macro_export]
macro_rules! error {
    ($warnings:ident, $($args:tt)*) => (
        {
            $warnings.send(Warning::Error( ( format!($($args)*) ) ) ).unwrap();
        }
    )
}

/// represents a non-fatal error somewhere in
/// the system to report either to the program interface
/// or in logs.
/// 
#[derive(Debug, Clone, PartialEq)]
pub enum Warning {
    Notice(String),

    Error(String),

    DegradedService(String),

    Critical(String),

    Confirmed(String),

    Awesome(String),

    Debug {
        msg: String,
        kv: MeasurementRecord,
    },

    Terminate
}

impl Warning {
    pub fn msg(&self) -> String {
        match *self {
            Warning::Notice(ref s) | Warning::Error(ref s) | 
            Warning::DegradedService(ref s) | Warning::Critical(ref s) | 
            Warning::Awesome(ref s) | Warning::Confirmed(ref s) |
            Warning::Debug { msg: ref s, .. } => 
                s.clone(),

            Warning::Terminate => "".to_owned()
        }
    }
    pub fn msg_str(&self) -> &str {
        match *self {
            Warning::Notice(ref s) | Warning::Error(ref s) |
            Warning::DegradedService(ref s) | Warning::Critical(ref s) |
            Warning::Awesome(ref s) | Warning::Confirmed(ref s) |
            Warning::Debug { msg: ref s, .. } => 

                s.as_ref(),

            Warning::Terminate => "Terminate"
        }
    }

    pub fn category_str(&self) -> &str {
        match self {
            &Warning::Notice(_) => "notice",
            &Warning::Error(_) => "error",
            &Warning::Critical(_) => "critical",
            &Warning::DegradedService(_) => "degraded_service",
            &Warning::Confirmed(_) => "confirmed",
            &Warning::Awesome(_) => "awesome",
            &Warning::Debug { .. } => "debug",
            &Warning::Terminate => "terminate",
        }
    }

    pub fn category(&self, f: &mut Formatter) {
        match self {
            &Warning::Notice(_) => {
                write!(f, "[ Notice ]");
            }

            &Warning::Error(_) => {
                write!(f, "{yellow}[{title}]{reset}", 
                    yellow = Fg(color::LightYellow),
                    title = " Error--",
                    reset = Fg(color::Reset));
            }
            &Warning::Critical(_) => {
                write!(f, "{bg}{fg}{title}{resetbg}{resetfg}", 
                        bg = Bg(color::Red),
                        fg = Fg(color::White),
                        title = " CRITICAL ",
                        resetbg = Bg(color::Reset),
                        resetfg = Fg(color::Reset));
            }

            &Warning::Awesome(_) => {
                write!(f, "{color}[{title}]{reset}", 
                        color = Fg(color::Green),
                        title = "Awesome!",
                        reset = Fg(color::Reset));
            }

            &Warning::DegradedService(_) => {
                write!(f, "{color}[{title}] {reset}", 
                        color = Fg(color::Blue),
                        title = "Degraded Service ",
                        reset = Fg(color::Reset));
            }
            &Warning::Confirmed(_) => {
                write!(f, "{bg}{fg}{title}{resetbg}{resetfg}", 
                        bg = Bg(color::Blue),
                        fg = Fg(color::White),
                        title = "Confirmed ",
                        resetbg = Bg(color::Reset),
                        resetfg = Fg(color::Reset));
            }


            _ => {}
        }
    }
}

impl Display for Warning {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        self.category(f);
        write!(f, " {}", self.msg())
    }
}

// impl Message for Warning {
//     fn kill_switch() -> Self {
//         Warning::Terminate
//     }
// }

#[derive(Debug, Clone)]
pub struct Record {
    pub time: DateTime<Utc>,
    pub msg: Warning
}

impl Record {
    pub fn new(msg: Warning) -> Self {
        let time = Utc::now();
        Record { time, msg }
    }

    pub fn to_measurement(&self, name: &'static str) -> Measurement {
        let cat = self.msg.category_str();
        let body = self.msg.msg_str();
        let mut m = Measurement::new(name);
        m.add_tag("category", cat);
        m.add_field("msg", InfluentValue::String(body));
        m.set_timestamp(nanos(self.time) as i64);
        m
    }
}

impl Display for Record {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        write!(f, "{} | {}", self.time.format("%H:%M:%S"), self.msg)
    }
}

pub type SlogResult = Result<(), slog::Error>;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(String),
    Float(f64),
    Integer(i64),
    Boolean(bool)
}

impl Value {
    pub fn to_influent<'a>(&'a self) -> InfluentValue<'a> {
        match self {
            &Value::String(ref s) => InfluentValue::String(s),
            &Value::Float(n) => InfluentValue::Float(n),
            &Value::Integer(i) => InfluentValue::Integer(i),
            &Value::Boolean(b) => InfluentValue::Boolean(b),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MeasurementRecord {
    fields: Vec<(Key, Value)>,
    //measurement: &'a mut Measurement<'a>,
    tags: Vec<(Key, String)>,
}

impl MeasurementRecord {
    pub fn new() -> Self {
        MeasurementRecord {
            fields: Vec::new(),
            tags: Vec::new(),
        }
    }

    pub fn add_field(&mut self, key: Key, val: Value) -> SlogResult {
        self.fields.push((key, val));
        Ok(())
    }

    pub fn add_tag(&mut self, key: Key, val: String) -> SlogResult {
        self.tags.push((key, val));
        Ok(())
    }

    pub fn serialize_values(&mut self, record: &slog::Record, values: &OwnedKVList) {
        let mut builder = TagBuilder { mrec: self };
        values.serialize(record, &mut builder);
    }

    pub fn to_measurement<'a>(&'a self, name: &'a str) -> Measurement<'a> {
        let fields: BTreeMap<&'a str, InfluentValue<'a>> =
            self.fields.iter()
                .map(|&(k, ref v)| {
                    (k, v.to_influent())
                }).collect();

        let tags: BTreeMap<&'a str, &'a str> = 
            self.tags.iter()
                .map(|&(k, ref v)| {
                    (k, v.as_ref())
                }).collect();

        Measurement {
            key: name,
            timestamp: Some(nanos(Utc::now()) as i64),
            fields,
            tags,
        }
    }
}

impl slog::Serializer for MeasurementRecord {
    fn emit_usize(&mut self, key: Key, val: usize) -> SlogResult { self.add_field(key, Value::Integer(val as i64)) }
    fn emit_isize(&mut self, key: Key, val: isize) -> SlogResult { self.add_field(key, Value::Integer(val as i64)) }
    fn emit_bool(&mut self, key: Key, val: bool)   -> SlogResult { self.add_field(key, Value::Boolean(val)); Ok(()) }
    fn emit_u8(&mut self, key: Key, val: u8)       -> SlogResult { self.add_field(key, Value::Integer(val as i64)) }
    fn emit_i8(&mut self, key: Key, val: i8)       -> SlogResult { self.add_field(key, Value::Integer(val as i64)) }
    fn emit_u16(&mut self, key: Key, val: u16)     -> SlogResult { self.add_field(key, Value::Integer(val as i64)) } 
    fn emit_i16(&mut self, key: Key, val: i16)     -> SlogResult { self.add_field(key, Value::Integer(val as i64)) }
    fn emit_u32(&mut self, key: Key, val: u32)     -> SlogResult { self.add_field(key, Value::Integer(val as i64)) }
    fn emit_i32(&mut self, key: Key, val: i32)     -> SlogResult { self.add_field(key, Value::Integer(val as i64)) }
    fn emit_f32(&mut self, key: Key, val: f32)     -> SlogResult { self.add_field(key, Value::Float(val as f64)) }
    fn emit_u64(&mut self, key: Key, val: u64)     -> SlogResult { self.add_field(key, Value::Integer(val as i64)) }
    fn emit_i64(&mut self, key: Key, val: i64)     -> SlogResult { self.add_field(key, Value::Integer(val)) }
    fn emit_f64(&mut self, key: Key, val: f64)     -> SlogResult { self.add_field(key, Value::Float(val)) }
    fn emit_str(&mut self, key: Key, val: &str)    -> SlogResult { self.add_field(key, Value::String(val.to_string())) }
    fn emit_unit(&mut self, key: Key)              -> SlogResult { self.add_field(key, Value::Boolean(true)) }
    fn emit_none(&mut self, key: Key)              -> SlogResult { Ok(()) } //self.add_field(key, Value::String("none".into())) }
    fn emit_arguments(&mut self, key: Key, val: &fmt::Arguments) -> SlogResult { self.add_field(key, Value::String(val.to_string())) } 
}

pub struct TagBuilder<'a> {
    mrec: &'a mut MeasurementRecord
}

impl<'a> slog::Serializer for TagBuilder<'a> {
    fn emit_str(&mut self, key: Key, val: &str)    -> SlogResult { 
        self.mrec.add_tag(key, val.to_string())
    }

    fn emit_arguments(&mut self, key: Key, val: &fmt::Arguments) -> SlogResult { 
        self.mrec.add_tag(key, val.to_string())
    }
}

pub struct WarningsDrain<D: Drain> {
    tx: Arc<Mutex<Sender<Warning>>>,
    drain: D
}

impl WarningsDrain<slog::Discard> {
    pub fn new(tx: Sender<Warning>) -> Self {
        let tx = Arc::new(Mutex::new(tx));
        let drain = slog::Discard;
        WarningsDrain { tx, drain }
    }
}


impl<D: Drain> Drain for WarningsDrain<D> {
    type Ok = ();
    type Err = D::Err;

    fn log(&self, record: &slog::Record, values: &OwnedKVList) -> Result<Self::Ok, Self::Err> {
        //let mut meas = Measurement::new("warnings");
        //println!("{:?}", values);
        let mut ser = MeasurementRecord::new();
        ser.serialize_values(record, values);
        //values.serialize(record, &mut ser);
        record.kv().serialize(record, &mut ser);
        //println!("{:?}", ser);
        let msg = record.msg().to_string();
        if let Ok(lock) = self.tx.lock() {
            lock.send(Warning::Debug { msg, kv: ser });
        }
        Ok(())
    }
}


#[derive(Debug)]
pub struct WarningsManager {
    pub tx: Sender<Warning>,
    pub warnings: Arc<RwLock<VecDeque<Record>>>,
    thread: Option<JoinHandle<()>>
}

impl WarningsManager {
    /// `measurement_name` is the name of the influxdb measurement
    /// we will save log entries to.
    ///
    pub fn new(measurement_name: &'static str) -> Self {
        let warnings = Arc::new(RwLock::new(VecDeque::new()));
        let warnings_copy = warnings.clone();
        let (tx, rx) = channel();
        let mut buf = String::with_capacity(4096);
        let ctx = zmq::Context::new();
        let socket = influx::push(&ctx).unwrap();
        let thread = thread::spawn(move || { 
            let path = format!("var/log/warnings-manager-{}.log", measurement_name);
            let logger = file_logger(&path, Severity::Info);
            info!(logger, "entering loop");
            loop {
                if let Ok(msg) = rx.recv() {
                    match msg {
                        Warning::Terminate => {
                            crit!(logger, "terminating");
                            break;
                        }

                        Warning::Debug { msg, kv } => {
                            debug!(logger, "new Warning::Debug arrived";
                                   "msg" => &msg);
                            let mut meas = kv.to_measurement(measurement_name);
                            meas.add_field("msg", InfluentValue::String(msg.as_ref()));
                            meas.add_tag("category", "debug");
                            influx::serialize(&meas, &mut buf);
                            socket.send_str(&buf, 0);
                            buf.clear();
                            // and don't push to warnings
                            // bc it's debug
                        }

                        other => {
                            debug!(logger, "new {} arrived", other.category_str();
                                   "msg" => other.category_str());
                            let rec = Record::new(other);
                            {
                                let m = rec.to_measurement(measurement_name);
                                influx::serialize(&m, &mut buf);
                                socket.send_str(&buf, 0);
                                buf.clear();
                            }
                            if let Ok(mut lock) = warnings.write() {
                                lock.push_front(rec);
                                lock.truncate(N_WARNINGS);
                            }
                        }
                    }
                }
            } 
        });

        WarningsManager {
            warnings: warnings_copy,
            thread: Some(thread),
            tx
        }
    }
}

impl Drop for WarningsManager {
    fn drop(&mut self) {
        self.tx.send(Warning::Terminate);
        if let Some(thread) = self.thread.take() {
            thread.join();
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use test::{black_box, Bencher};

    #[test]
    fn it_creates_a_logger() {
        let wm = WarningsManager::new();
        let im = influx::writer(wm.tx.clone());
        let drain = WarningsDrain { tx: Arc::new(Mutex::new(wm.tx.clone())), drain: slog::Discard };
        let logger = slog::Logger::root(drain, o!());
        //for _ in 0..60 {
        //    debug!(logger, "test 123"; "exchange" => "plnx");
        //}
    }

    #[bench]
    fn it_sends_integers_with_a_sender_behind_a_mutex(b: &mut Bencher) {
        let (tx, rx) = channel();
        enum Msg {
            Val(usize),
            Terminate
        }
        let worker = thread::spawn(move || {
            let mut xs = Vec::new();
            loop {
                match rx.recv().unwrap() {
                    Msg::Val(x) => { xs.push(x); }
                    Msg::Terminate => break,
                }
            }
            xs.len()
        });
        let tx = Arc::new(Mutex::new(tx));
        b.iter(|| {
            let lock = tx.lock().unwrap();
            lock.send(Msg::Val(1));
        });
        tx.lock().unwrap().send(Msg::Terminate);
        let len = worker.join().unwrap();
        println!("{}", len);

    }
}
