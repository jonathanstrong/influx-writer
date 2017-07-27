//! An object to handle everyone's errors
//! 

use std::thread::{self, JoinHandle};
use std::sync::{Arc, Mutex, RwLock};
use std::sync::mpsc::{self, Sender, Receiver, channel};
use std::collections::VecDeque;
use std::fmt::{Display, Error as FmtError, Formatter};

use zmq;
use chrono::{DateTime, Utc, TimeZone};
use termion::color::{self, Fg, Bg};
use influent::measurement::{Measurement, Value};

use super::nanos;
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

    Terminate
}

impl Warning {
    pub fn msg(&self) -> String {
        match *self {
            Warning::Notice(ref s) | Warning::Error(ref s) | 
            Warning::DegradedService(ref s) | Warning::Critical(ref s) | 
            Warning::Awesome(ref s) | Warning::Confirmed(ref s) => 
                s.clone(),

            Warning::Terminate => "".to_owned()
        }
    }
    pub fn msg_str(&self) -> &str {
        match *self {
            Warning::Notice(ref s) | Warning::Error(ref s) |
            Warning::DegradedService(ref s) | Warning::Critical(ref s) |
            Warning::Awesome(ref s) | Warning::Confirmed(ref s) =>
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

    pub fn to_measurement(&self) -> Measurement {
        let cat = self.msg.category_str();
        let body = self.msg.msg_str();
        let mut m = Measurement::new("warnings");
        m.add_tag("category", cat);
        m.add_field("msg", Value::String(body));
        m.set_timestamp(nanos(self.time) as i64);
        m
    }
}

impl Display for Record {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        write!(f, "{} | {}", self.time.format("%H:%M:%S"), self.msg)
    }
}

#[derive(Debug)]
pub struct WarningsManager {
    pub tx: Sender<Warning>,
    pub warnings: Arc<RwLock<VecDeque<Record>>>,
    thread: Option<JoinHandle<()>>
}

impl WarningsManager {
    pub fn new() -> Self {
        let warnings = Arc::new(RwLock::new(VecDeque::new()));
        let warnings_copy = warnings.clone();
        let (tx, rx) = channel();
        let mut buf = String::with_capacity(4096);
        let ctx = zmq::Context::new();
        let socket = influx::push(&ctx).unwrap();
        let thread = thread::spawn(move || { loop {
            if let Ok(msg) = rx.recv() {
                match msg {
                    Warning::Terminate => {
                        println!("warnings manager terminating");
                        break;
                    }
                    other => {
                        let rec = Record::new(other);
                        {
                            let m = rec.to_measurement();
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
        } });

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



