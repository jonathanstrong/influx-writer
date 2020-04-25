//! An object to handle everyone's errors
//! 

use std::thread::{self, JoinHandle};
use std::sync::{Arc, Mutex, RwLock};
use std::sync::mpsc::{Sender, channel};
use std::collections::{BTreeMap, VecDeque};
use std::fmt::{self, Display, Error as FmtError, Formatter};
use std::io::{self, Write};
use std::fs;

use chrono::{DateTime, Utc};
use termion::color::{self, Fg, Bg};
use influent::measurement::{Measurement, Value as InfluentValue};
use slog::{self, OwnedKVList, Drain, Key, KV, Level, Logger};
use sloggers::types::Severity;

use super::{nanos, file_logger};
use influx;


const N_WARNINGS: usize = 500;

#[macro_export]
macro_rules! confirmed {
    ($warnings:ident, $($args:tt)*) => (
        {
            let _ = warnings.send(Warning::Confirmed( ( format!($($args)*) ) ) ).unwrap();
        }
    )
}

/// logs a `Warning::Awesome` message to the `WarningsManager`
#[macro_export]
macro_rules! awesome {
    ($warnings:ident, $($args:tt)*) => (
        {
            let _ = $warnings.send(Warning::Awesome( ( format!($($args)*) ) ) ).unwrap();
        }
    )
}

#[macro_export]
macro_rules! critical {
    ($warnings:ident, $($args:tt)*) => (
        {
            let _ = $warnings.send(Warning::Critical( ( format!($($args)*) ) ) ).unwrap();
        }
    )
}

#[macro_export]
macro_rules! notice {
    ($warnings:ident, $($args:tt)*) => (
        {
            let _ = $warnings.send(Warning::Notice( ( format!($($args)*) ) ) ).unwrap();
        }
    )
}

#[macro_export]
macro_rules! error_w {
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

    Log {
        level: Level,
        module: &'static str,
        function: &'static str,
        line: u32,
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
            Warning::Log { msg: ref s, .. } => 
                s.clone(),

            Warning::Terminate => "".to_owned()
        }
    }
    pub fn msg_str(&self) -> &str {
        match *self {
            Warning::Notice(ref s) | Warning::Error(ref s) |
            Warning::DegradedService(ref s) | Warning::Critical(ref s) |
            Warning::Awesome(ref s) | Warning::Confirmed(ref s) |
            Warning::Log { msg: ref s, .. } => 

                s.as_ref(),

            Warning::Terminate => "Terminate"
        }
    }

    pub fn category_str(&self) -> &str {
        match self {
            &Warning::Notice(_) => "NOTC",
            &Warning::Error(_) => "ERRO",
            &Warning::Critical(_) => "CRIT",
            &Warning::DegradedService(_) => "DGRD",
            &Warning::Confirmed(_) => "CNFD",
            &Warning::Awesome(_) => "AWSM",
            &Warning::Log { ref level, .. } => level.as_short_str(),
            &Warning::Terminate => "TERM",
        }
    }

    pub fn category(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Warning::Notice(_) => {
                write!(f, "[ Notice ]")
            }

            Warning::Error(_) => {
                write!(f, "{yellow}[{title}]{reset}", 
                    yellow = Fg(color::LightYellow),
                    title = " Error--",
                    reset = Fg(color::Reset))
            }

            Warning::Critical(_) => {
                write!(f, "{bg}{fg}{title}{resetbg}{resetfg}", 
                        bg = Bg(color::Red),
                        fg = Fg(color::White),
                        title = " CRITICAL ",
                        resetbg = Bg(color::Reset),
                        resetfg = Fg(color::Reset))
            }

            Warning::Awesome(_) => {
                write!(f, "{color}[{title}]{reset}", 
                        color = Fg(color::Green),
                        title = "Awesome!",
                        reset = Fg(color::Reset))
            }

            Warning::DegradedService(_) => {
                write!(f, "{color}[{title}] {reset}", 
                        color = Fg(color::Blue),
                        title = "Degraded Service ",
                        reset = Fg(color::Reset))
            }

            Warning::Confirmed(_) => {
                write!(f, "{bg}{fg}{title}{resetbg}{resetfg}", 
                        bg = Bg(color::Blue),
                        fg = Fg(color::White),
                        title = "Confirmed ",
                        resetbg = Bg(color::Reset),
                        resetfg = Fg(color::Reset))
            }

            _ => Ok(())
        }
    }
}

impl Display for Warning {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        self.category(f)?;
        write!(f, " {}", self.msg())
    }
}

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
        match key {
            "exchange" | "thread" | "ticker" | "category" => {
                self.tags.push((key, val));
            }

            other => {
                self.add_field(other, Value::String(val)).unwrap();
            }
        }

        Ok(())
    }

    pub fn serialize_values(&mut self, record: &slog::Record, values: &OwnedKVList) {
        let mut builder = TagBuilder { mrec: self };
        let _ = values.serialize(record, &mut builder);
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
    fn emit_bool(&mut self, key: Key, val: bool)   -> SlogResult { self.add_field(key, Value::Boolean(val)) }
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
    fn emit_none(&mut self, _: Key)                -> SlogResult { Ok(()) } //self.add_field(key, Value::String("none".into())) }
    fn emit_arguments(&mut self, key: Key, val: &fmt::Arguments) -> SlogResult { self.add_field(key, Value::String(val.to_string())) } 
}

pub struct TagBuilder<'a> {
    mrec: &'a mut MeasurementRecord
}

impl<'a> slog::Serializer for TagBuilder<'a> {
    fn emit_str(&mut self, key: Key, val: &str)    -> SlogResult { 
        match key {
            "exchange" | "thread" | "ticker" | "category" => {
                self.mrec.add_tag(key, val.to_string())
            }

            other => {
                self.mrec.add_field(other, Value::String(val.to_string()))
            }
        }
    }

    fn emit_arguments(&mut self, key: Key, val: &fmt::Arguments) -> SlogResult { 
        match key {
            "exchange" | "thread" | "ticker" | "category" => {
                self.mrec.add_tag(key, val.to_string())
            }

            other => {
                self.mrec.add_field(other, Value::String(val.to_string()))
            }
        }

    }
}

pub struct WarningsDrain<D: Drain> {
    level: Level,
    tx: Arc<Mutex<Sender<Warning>>>,
    drain: D,
    to_file: Logger,
}

impl<D> WarningsDrain<D> 
    where D: Drain
{
    pub fn new(tx: Sender<Warning>, level: Level, drain: D) -> Self {
        let tx = Arc::new(Mutex::new(tx));
        let to_file = file_logger("var/log/mm.log", Severity::Warning);
        WarningsDrain { tx, drain, level, to_file }
    }
}

impl From<Sender<Warning>> for WarningsDrain<slog::Fuse<slog::Discard>> {
    fn from(tx: Sender<Warning>) -> Self {
        WarningsDrain::new(tx, Level::Debug, slog::Discard.fuse())
    }
}

impl<D: Drain> Drain for WarningsDrain<D> {
    type Ok = ();
    type Err = D::Err;

    fn log(&self, record: &slog::Record, values: &OwnedKVList) -> Result<Self::Ok, Self::Err> {
        if record.level() <= self.level {
            let mut ser = MeasurementRecord::new();
            ser.serialize_values(record, values);
            let _ = record.kv().serialize(record, &mut ser);
            let msg = record.msg().to_string();
            if let Ok(lock) = self.tx.lock() {
                let _ = lock.send(Warning::Log { 
                    level: record.level(),
                    module: record.module(),
                    function: record.function(),
                    line: record.line(),
                    msg, 
                    kv: ser 
                });
            }
        }
        if record.level() <= Level::Warning {
            let _ = self.to_file.log(record);
        }
        let _ = self.drain.log(record, values)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct WarningsManager {
    pub tx: Sender<Warning>,
    pub warnings: Arc<RwLock<VecDeque<Record>>>,
    thread: Option<JoinHandle<()>>
}

impl Drop for WarningsManager {
    fn drop(&mut self) {
        let _ = self.tx.send(Warning::Terminate);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

const TIMESTAMP_FORMAT: &'static str = "%b %d %H:%M:%S%.3f";

/// Serializes *only* KV pair with `key == "thread"`
///
struct ThreadSer<'a>(&'a mut Vec<u8>);

impl<'a> slog::ser::Serializer for ThreadSer<'a> {
    fn emit_arguments(&mut self, _: &str, _: &fmt::Arguments) -> slog::Result {
        Ok(())
    }

    fn emit_str(&mut self, key: &str, val: &str) -> slog::Result {
        if key == "thread" {
            write!(self.0, " {:<20}", val)?;
        }
        Ok(())
    }
}


/// Serializes KV pairs as ", k: v"
///
struct KvSer<'a>(&'a mut Vec<u8>);

macro_rules! s(
    ($s:expr, $k:expr, $v:expr) => {
        try!(write!($s.0, ", {}: {}", $k, $v));
    };
);

impl<'a> slog::ser::Serializer for KvSer<'a> {
    fn emit_none(&mut self, key: &str) -> slog::Result {
        s!(self, key, "None");
        Ok(())
    }
    fn emit_unit(&mut self, key: &str) -> slog::Result {
        s!(self, key, "()");
        Ok(())
    }

    fn emit_bool(&mut self, key: &str, val: bool) -> slog::Result {
        s!(self, key, val);
        Ok(())
    }

    fn emit_char(&mut self, key: &str, val: char) -> slog::Result {
        s!(self, key, val);
        Ok(())
    }

    fn emit_usize(&mut self, key: &str, val: usize) -> slog::Result {
        s!(self, key, val);
        Ok(())
    }
    fn emit_isize(&mut self, key: &str, val: isize) -> slog::Result {
        s!(self, key, val);
        Ok(())
    }

    fn emit_u8(&mut self, key: &str, val: u8) -> slog::Result {
        s!(self, key, val);
        Ok(())
    }
    fn emit_i8(&mut self, key: &str, val: i8) -> slog::Result {
        s!(self, key, val);
        Ok(())
    }
    fn emit_u16(&mut self, key: &str, val: u16) -> slog::Result {
        s!(self, key, val);
        Ok(())
    }
    fn emit_i16(&mut self, key: &str, val: i16) -> slog::Result {
        s!(self, key, val);
        Ok(())
    }
    fn emit_u32(&mut self, key: &str, val: u32) -> slog::Result {
        s!(self, key, val);
        Ok(())
    }
    fn emit_i32(&mut self, key: &str, val: i32) -> slog::Result {
        s!(self, key, val);
        Ok(())
    }
    fn emit_f32(&mut self, key: &str, val: f32) -> slog::Result {
        s!(self, key, val);
        Ok(())
    }
    fn emit_u64(&mut self, key: &str, val: u64) -> slog::Result {
        s!(self, key, val);
        Ok(())
    }
    fn emit_i64(&mut self, key: &str, val: i64) -> slog::Result {
        s!(self, key, val);
        Ok(())
    }
    fn emit_f64(&mut self, key: &str, val: f64) -> slog::Result {
        s!(self, key, val);
        Ok(())
    }
    fn emit_str(&mut self, key: &str, val: &str) -> slog::Result {
        s!(self, key, val);
        Ok(())
    }
    fn emit_arguments(
        &mut self,
        key: &str,
        val: &fmt::Arguments,
    ) -> slog::Result {
        s!(self, key, val);
        Ok(())
    }
}

#[allow(unused_variables, unused_imports)]
#[cfg(test)]
mod tests {
    use super::*;
    use test::{black_box, Bencher};

    #[test]
    #[ignore]
    fn it_creates_a_logger() {
        let wm = WarningsManager::new("rust-test");
        let im = influx::writer(wm.tx.clone());
        let drain = 
            WarningsDrain { 
                tx: Arc::new(Mutex::new(wm.tx.clone())), 
                drain: slog::Discard,
                to_file: Logger::root(slog::Discard, o!()),
                level: Level::Trace,
            };
        let logger = slog::Logger::root(drain, o!());
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
            let _ = lock.send(Msg::Val(1));
        });
        let _ = tx.lock().unwrap().send(Msg::Terminate);
        let len = worker.join().unwrap();
        //println!("{}", len);
    }
}
