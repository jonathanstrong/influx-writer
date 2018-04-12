use std::thread::{self, JoinHandle};
use std::sync::mpsc::{Sender, channel};
use std::fmt;
use std::time::{Instant, Duration};

use chrono::{self, DateTime, Utc};
use pub_sub::PubSub;
use sloggers::types::Severity;

//use windows::{DurationWindow, Incremental, Window};
use money::{Ticker, Side, Exchange};

use super::file_logger;
use influx::{self, OwnedMeasurement, OwnedValue};

use self::windows::Incremental;

pub type Nanos = u64;

pub const SECOND: u64 = 1e9 as u64;
pub const MINUTE: u64 = SECOND * 60;
pub const HOUR: u64 = MINUTE * 60;
pub const MILLISECOND: u64 = SECOND / 1000;
pub const MICROSECOND: u64 = MILLISECOND / 1000;

pub fn nanos(d: Duration) -> Nanos {
    d.as_secs() * 1_000_000_000 + (d.subsec_nanos() as u64)
}

pub fn dt_nanos(t: DateTime<Utc>) -> i64 {
    (t.timestamp() as i64) * 1_000_000_000_i64 + (t.timestamp_subsec_nanos() as i64)
}

pub fn now() -> i64 { dt_nanos(Utc::now()) }

pub fn tfmt(ns: Nanos) -> String {
    match ns {
        t if t <= MICROSECOND => {
            format!("{}ns", t)
        }

        t if t > MICROSECOND && t < MILLISECOND => {
            format!("{}u", t / MICROSECOND)
        }
        t if t > MILLISECOND && t < SECOND => {
            format!("{}ms", t / MILLISECOND)
        }

        t => {
            format!("{}.{}sec", t / SECOND, t / MILLISECOND)
        }
    }
}

pub fn tfmt_dur(d: Duration) -> String {
    tfmt(nanos(d))
}

pub fn tfmt_dt(dt: DateTime<Utc>) -> String {
    Utc::now().signed_duration_since(dt)
        .to_std()
        .map(|dur| {
            tfmt_dur(dur)
        }).unwrap_or("?".into())
}


pub fn tfmt_write(ns: Nanos, f: &mut fmt::Formatter) -> fmt::Result {
    match ns {
        t if t <= MICROSECOND => {
            write!(f, "{}ns", t)
        }

        t if t > MICROSECOND && t < MILLISECOND => {
            write!(f, "{}u", t / MICROSECOND)
        }

        t if t > MILLISECOND && t < SECOND => {
            write!(f, "{}ms", t / MILLISECOND)
        }

        t => {
            write!(f, "{}.{}sec", t / SECOND, t / MILLISECOND)
        }
    }
}

#[doc(hide)]
mod windows {
    use super::*;
    use std::ops::{Div, Mul, Sub, SubAssign, AddAssign};
    use std::collections::VecDeque;
    use num::Float;

    const INITIAL_CAPACITY: usize = 1000;

    #[derive(Clone, Debug)]
    pub struct Point<T> 
        //where T: Default
    {
        time: Instant, 
        value: T
    }

    #[derive(Debug, Clone)]
    pub struct Window<T> 
        where T: Default
    {
        pub size: Duration, // window size
            mean: T,
            ps: T,
            psa: T,
            var: T,
            sum: T,
            count: u32,
            items: VecDeque<Point<T>>,
    }

    #[derive(Default)]
    pub struct DurationWindow {
        pub size: Duration,
            mean: Duration,
            sum: Duration,
            count: u32, 
            items: VecDeque<Point<Duration>>
    }

    impl<T> Point<T> 
        where T: Default + Copy
    {
        fn new(time: Instant, value: T) -> Self {
            Point { time, value }
        }

        fn value(&self) -> T {
            self.value
        }
    }

    impl<T> Window<T> 
        where T: Default + Zero
    {
        pub fn new(size: Duration) -> Self {
            Window {
                size,
                mean: T::default(),
                psa: T::default(),
                ps: T::default(),
                sum: T::default(),
                count: 0,
                var: T::default(),
                items: VecDeque::with_capacity(INITIAL_CAPACITY),
            }
        }

        pub fn with_size_and_capacity(size: Duration, capacity: usize) -> Self {
            Window {
                size,
                mean: T::default(),
                psa: T::default(),
                ps: T::default(),
                sum: T::default(),
                count: 0,
                var: T::default(),
                items: VecDeque::with_capacity(capacity),
            }
        }
    }

    impl<T> From<Duration> for Window<T>
        where T: Default + Zero
    {
        fn from(size: Duration) -> Self {
            Window::new(size)
        }
    }

    impl From<Duration> for DurationWindow {
        fn from(size: Duration) -> Self {
            DurationWindow::new(size)
        }
    }

    pub trait Incremental<T> {
        /// Purge expired items. 
        /// 
        #[inline]
        fn refresh(&mut self, t: Instant) -> &Self;

        /// Add a new item. 
        /// 
        #[inline]
        fn add(&mut self, time: Instant, value: T);

        /// Add a new item and purge expired items. 
        /// 
        #[inline]
        fn update(&mut self, time: Instant, value: T) {
            self.refresh(time);
            self.add(time, value);
        }
    }

    pub trait Zero {
        fn zero() -> Self;
    }

    pub trait One {
        fn one() -> Self;
    }

    macro_rules! zero {
        ($t:ty, $body:expr) => {

            impl Zero for $t {
                fn zero() -> $t { $body }
            }
        }
    }

    macro_rules! one {
        ($t:ty, $body:expr) => {

            impl One for $t {
                fn one() -> $t { $body }
            }
        }
    }

    zero!(f64, 0.0);
    zero!(f32, 0.0);
    zero!(u128, 0);
    zero!(i128, 0);
    zero!(u64, 0);
    zero!(i64, 0);
    zero!(i32, 0);
    zero!(u32, 0);
    zero!(u16, 0);
    one!(f64, 1.0);
    one!(f32, 1.0);
    one!(u128, 1);
    one!(i128, 1);
    one!(u64, 1);
    one!(i64, 1);
    one!(i32, 1);
    one!(u32, 1);
    one!(u16, 1);

    impl<T> Incremental<T> for Window<T> 
        where T: Default + AddAssign<T> + SubAssign<T> + From<u32> + Div<Output = T> + 
                 Mul<Output = T> + Sub<Output = T> + Copy 
    {
        #[inline]
        fn refresh(&mut self, t: Instant) -> &Self {
            if !self.items.is_empty() {
                let (n_remove, sum, ps, count) = 
                    self.items.iter()
                        .take_while(|x| t - x.time > self.size)
                        .fold((0, self.sum, self.ps, self.count), |(n_remove, sum, ps, count), x| {
                            (n_remove + 1, sum - x.value, ps - x.value * x.value, count - 1)
                        });
                self.sum = sum;
                self.ps = ps;
                self.count = count;
                for _ in 0..n_remove {
                    self.items.pop_front();
                }
            }

            if self.count > 0 {
                self.mean = self.sum / self.count.into();
                self.psa = self.ps / self.count.into();
                let c: T = self.count.into();
                self.var = (self.psa * c - c * self.mean * self.mean) / c;
            }
            self
        }

        /// Creates `Point { time, value }` and pushes to `self.items`.
        /// 
        #[inline]
        fn add(&mut self, time: Instant, value: T) {
            let p = Point::new(time, value);
            self.sum += p.value;
            self.ps += p.value * p.value;
            self.count += 1;
            self.items.push_back(p);
        }

        #[inline]
        fn update(&mut self, time: Instant, value: T) {
            self.add(time, value);
            self.refresh(time);
        }
    }

    impl Incremental<Duration> for DurationWindow {
        #[inline]
        fn refresh(&mut self, t: Instant) -> &Self {
            if !self.items.is_empty() {
                let (n_remove, sum, count) = 
                    self.items.iter()
                        .take_while(|x| t - x.time > self.size)
                        .fold((0, self.sum, self.count), |(n_remove, sum, count), x| {
                            (n_remove + 1, sum - x.value, count - 1)
                        });
                self.sum = sum;
                self.count = count;
                for _ in 0..n_remove {
                    self.items.pop_front();
                }
            }

            if self.count > 0 {
                self.mean = self.sum / self.count.into();
            }

            self
        }

        #[inline]
        fn add(&mut self, time: Instant, value: Duration) {
            let p = Point::new(time, value);
            self.sum += p.value;
            self.count += 1;
            self.items.push_back(p);
        }
    }


    impl<T> Window<T> 
        where T: Default + Copy
    {
        pub fn mean(&self)      -> T { self.mean }
        pub fn var(&self)       -> T { self.var }
        pub fn psa(&self)       -> T { self.psa }
        pub fn ps(&self)        -> T { self.ps }
        pub fn count(&self)     -> u32 { self.count }
        pub fn len(&self)       -> usize { self.items.len() }
        pub fn is_empty(&self)  -> bool { self.items.is_empty() }

        /// Returns the `Duration` between `t` and the first `Point` in `self.items`.
        ///
        /// If there are no items, returns `Duration { secs: 0, nanos: 0 }`.
        ///
        /// # Panics
        ///
        /// This function will panic if `t` is earlier than the first `Point`'s `Instant`.
        ///
        #[inline]
        pub fn elapsed(&self, t: Instant) -> Duration {
            self.items.front()
                .map(|p| {
                    t - p.time
                }).unwrap_or_else(|| Duration::new(0, 0))
        }
    }

    impl<T> Window<T>
        where T: Float + Default
    {
        #[inline]
        pub fn std(&self) -> T { self.var.sqrt() }
    }

    impl DurationWindow {
        pub fn new(size: Duration)  -> Self { DurationWindow { size, ..Default::default() } }
        pub fn mean(&self)          -> Duration { self.mean }
        pub fn count(&self)         -> u32 { self.count }
        pub fn len(&self)           -> usize { self.items.len() }
        pub fn is_empty(&self)      -> bool { self.items.is_empty() }

        #[inline]
        pub fn nanos(d: Duration)   -> u64 { d.as_secs() * 1_000_000_000 + (d.subsec_nanos() as u64) }

        /// Returns number of microseconds as `u32` if `d <= Duration::new(4_294, 967_295_000)`.
        ///
        /// Any duration above ~4,295 seconds as micros is larger than `u32::MAX`. 4,295 seconds
        /// is about 71.5 minutes.
        ///
        /// # Examples
        ///
        /// ```
        /// use windows::DurationWindow;
        /// use std::time::Duration;
        ///
        /// assert_eq!(DurationWindow::micros(Duration::new(1, 0)), Some(1_000_000));
        /// assert_eq!(DurationWindow::micros(Duration::new(4_295, 0)), None);
        /// ```
        ///
        #[inline]
        pub fn micros(d: Duration)  -> Option<u32> {
            if d <= Duration::new(4_294, 967_295_000) {
                Some((d.as_secs() * 1_000_000) as u32 + d.subsec_nanos() / 1_000u32)
            } else {
                None
            }
        }

        #[inline]
        pub fn mean_nanos(&self)    -> u64 { DurationWindow::nanos(self.mean()) }

        #[inline]
        pub fn max(&self) -> Option<Duration> {
            self.items.iter()
                .map(|p| p.value)
                .max()
        }

        #[inline]
        pub fn max_nanos(&self) -> Option<u64> {
            self.max()
                .map(|x| DurationWindow::nanos(x))
        }

        #[inline]
        pub fn first(&self) -> Option<Duration> {
            self.items
                .front()
                .map(|pt| pt.value())
        }

        /// Returns the `Duration` between `t` and the first `Point` in `self.items`.
        ///
        /// If there are no items, returns `Duration { secs: 0, nanos: 0 }`.
        ///
        /// # Panics
        ///
        /// This function will panic if `t` is earlier than the first `Point`'s `Instant`.
        ///
        #[inline]
        pub fn elapsed(&self, t: Instant) -> Duration {
            self.items.front()
                .map(|p| {
                    t - p.time
                }).unwrap_or_else(|| Duration::new(0, 0))
        }
    }
}

#[derive(Debug)]
pub enum Latency {
    Ws(Exchange, Ticker, Duration),
    Http(Exchange, Duration),
    Trade(Exchange, Ticker, Duration),
    Terminate
}

#[derive(Debug)]
pub enum ExperiencedLatency {

    GdaxWebsocket(Duration),
    GdaxHttpPublic(Duration),
    GdaxHttpPrivate(Duration),
    PlnxHttpPublic(Duration),
    PlnxHttpPrivate(Duration),
    PlnxOrderBook(Duration),
    KrknHttpPublic(Duration),
    KrknHttpPrivate(Duration),
    KrknTrade(Duration, &'static str, Option<Ticker>, Option<Side>),
    PlnxWs(Ticker),

    Terminate
}

#[derive(Debug, Clone)]
pub struct Update {
    pub gdax_ws: Nanos,
    pub gdax_trade: Nanos,
    pub gdax_last: DateTime<Utc>
}

impl Default for Update {
    fn default() -> Self {
        Update {
            gdax_ws: 0,
            gdax_trade: 0,
            gdax_last: Utc::now(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LatencyUpdate {
    pub gdax_ws: Nanos,
    pub krkn_pub: Nanos,
    pub krkn_priv: Nanos, 
    pub plnx_pub: Nanos,
    pub plnx_priv: Nanos,
    pub plnx_order: Nanos,
    pub krkn_trade_30_mean: Nanos,
    pub krkn_trade_30_max: Nanos,

    pub krkn_trade_300_mean: Nanos,
    pub krkn_trade_300_max: Nanos,

    pub plnx_last: DateTime<Utc>,
    pub krkn_last: DateTime<Utc>,

    pub plnx_ws_count: u64,
}

impl Default for LatencyUpdate {
    fn default() -> Self {
        LatencyUpdate {
            gdax_ws                 : 0,
            krkn_pub                : 0,
            krkn_priv               : 0, 
            plnx_pub                : 0,
            plnx_priv               : 0,
            plnx_order              : 0,
            krkn_trade_30_mean      : 0,
            krkn_trade_30_max       : 0,
            krkn_trade_300_mean     : 0,
            krkn_trade_300_max      : 0,
            plnx_ws_count           : 0,

            plnx_last               : Utc::now(),
            krkn_last               : Utc::now(),
        }
    }
}

pub struct Manager {
    pub tx: Sender<Latency>,
    pub channel: PubSub<Update>,
        thread: Option<JoinHandle<()>>,
}

pub struct LatencyManager {
    pub tx: Sender<ExperiencedLatency>,
    pub channel: PubSub<LatencyUpdate>,
        thread: Option<JoinHandle<()>>,
}

/// returns a DateTime equal to now - `dur`
///
pub fn dt_from_dur(dur: Duration) -> DateTime<Utc> {
    let old_dur = chrono::Duration::nanoseconds(nanos(dur) as i64);
    Utc::now() - old_dur
}

struct Last {
    broadcast: Instant,
    plnx: Instant,
    krkn: Instant,
    gdax: Instant,
}

impl Default for Last {
    fn default() -> Self {
        Last {
            broadcast: Instant::now(),
            plnx: Instant::now(),
            krkn: Instant::now(),
            gdax: Instant::now(),
        }
    }
}

impl Manager {
    pub fn new(window: Duration,
           log_path: &'static str, 
           measurements: Sender<OwnedMeasurement>) -> Self {

        let (tx, rx) = channel();
        let channel = PubSub::new();
        let channel_copy = channel.clone();
        let logger = file_logger(log_path, Severity::Info);
        
        info!(logger, "initializing");
 
        let mut gdax_ws = windows::DurationWindow::new(window);
        let mut gdax_trade = windows::DurationWindow::new(window);

        let mut last = Last::default();

        info!(logger, "entering loop");

        let thread = Some(thread::spawn(move || { 
            loop {

                let loop_time = Instant::now();

                if let Ok(msg) = rx.recv_timeout(Duration::from_millis(1)) {
                    debug!(logger, "rcvd {:?}", msg);

                    match msg {
                        Latency::Ws(_, _, dur) => {
                            gdax_ws.update(loop_time, dur);
                            last.gdax = loop_time;
                        }

                        Latency::Trade(_, ticker, dur) => {
                            gdax_trade.update(loop_time, dur);
                            last.gdax = loop_time;
                            let nanos = windows::DurationWindow::nanos(dur);
                            measurements.send(
                                OwnedMeasurement::new("gdax_trade_api")
                                    .add_tag("ticker", ticker.as_str())
                                    .add_field("nanos", OwnedValue::Integer(nanos as i64))
                                    .set_timestamp(influx::now())).unwrap();
                        }

                        Latency::Terminate => break,

                        _ => {}
                    }
                }

                if loop_time - last.broadcast > Duration::from_millis(100) {
                    debug!(logger, "initalizing broadcast");

                    let update = Update {
                        gdax_ws: gdax_ws.refresh(loop_time).mean_nanos(),
                        gdax_trade: gdax_trade.refresh(loop_time).mean_nanos(),
                        gdax_last: dt_from_dur(loop_time - last.gdax)
                    };
                    channel.send(update).unwrap();
                    last.broadcast = loop_time;
                    debug!(logger, "sent broadcast");
                } 

            }
            debug!(logger, "latency manager terminating");
        }));

        Manager {
            tx,
            channel: channel_copy,
            thread,
        }
    }
}

impl Drop for LatencyManager {
    fn drop(&mut self) {
        for _ in 0..100 { self.tx.send(ExperiencedLatency::Terminate).unwrap(); }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for Manager {
    fn drop(&mut self) {
        for _ in 0..100 { self.tx.send(Latency::Terminate).unwrap(); }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl LatencyManager {
    pub fn new(d: Duration) -> Self {
        let (tx, rx) = channel();
        let tx_copy = tx.clone();
        let channel = PubSub::new();
        let channel_copy = channel.clone();
        //let w = w.clone();

        let thread = Some(thread::spawn(move || { 
            let logger = file_logger("var/log/latency-manager.log", Severity::Info);
            info!(logger, "initializing zmq");

            info!(logger, "initializing DurationWindows");
            let mut gdax_ws = windows::DurationWindow::new(d);
            let mut gdax_priv = windows::DurationWindow::new(d);
            let mut krkn_pub = windows::DurationWindow::new(d);
            let mut krkn_priv = windows::DurationWindow::new(d);
            let mut plnx_pub = windows::DurationWindow::new(d);
            let mut plnx_priv = windows::DurationWindow::new(d);
            let mut plnx_order = windows::DurationWindow::new(d);
            let mut plnx_ws_count: windows::Window<u32> = windows::Window::new(d);

            // yes I am intentionally breaking from the hard-typed duration
            // window ... that was a stupid idea
            //
            let mut krkn_trade_30 = windows::DurationWindow::new(Duration::from_secs(30));
            let mut krkn_trade_300 = windows::DurationWindow::new(Duration::from_secs(300));

            let mut last = Last::default();

            thread::sleep(Duration::from_millis(1));

            info!(logger, "entering loop");
            loop {
                let loop_time = Instant::now();

                if let Ok(msg) = rx.recv() {
                    debug!(logger, "new msg: {:?}", msg);

                    match msg {
                        ExperiencedLatency::Terminate => {
                            crit!(logger, "terminating");
                            break;
                        }

                        ExperiencedLatency::GdaxWebsocket(d) => gdax_ws.update(loop_time, d),

                        ExperiencedLatency::GdaxHttpPrivate(d) => gdax_priv.update(loop_time, d),

                        ExperiencedLatency::KrknHttpPublic(d) => {
                            last.krkn = loop_time;
                            krkn_pub.update(loop_time, d)
                        }

                        ExperiencedLatency::KrknHttpPrivate(d) => {
                            last.krkn = loop_time;
                            krkn_priv.update(loop_time, d)
                        }

                        ExperiencedLatency::PlnxHttpPublic(d) => {
                            last.plnx = loop_time;
                            plnx_pub.update(loop_time, d)
                        }

                        ExperiencedLatency::PlnxHttpPrivate(d) => {
                            last.plnx = loop_time;
                            plnx_priv.update(loop_time, d)
                        }

                        ExperiencedLatency::PlnxOrderBook(d) => {
                            last.plnx = loop_time;
                            plnx_order.update(loop_time, d)
                        }

                        ExperiencedLatency::PlnxWs(_) => {
                            last.plnx = loop_time;
                            plnx_ws_count.update(loop_time, 1_u32);
                        }

                        ExperiencedLatency::KrknTrade(d, cmd, _, _) => {
                            debug!(logger, "new KrknTrade";
                                   "cmd" => cmd);
                            last.krkn = loop_time;
                            krkn_trade_30.update(loop_time, d);
                            krkn_trade_300.update(loop_time, d);
                        }

                        other => {
                            warn!(logger, "unexpected msg: {:?}", other);
                        }
                    }
                }

                if loop_time - last.broadcast > Duration::from_millis(100) {
                    debug!(logger, "initalizing broadcast");
                    // note - because we mutated the Window instances
                    // above, we need a fresh Instant to avoid less than other
                    // panic
                    //
                    krkn_trade_30.refresh(loop_time);
                    krkn_trade_300.refresh(loop_time);
                    let update = LatencyUpdate {
                        gdax_ws: gdax_ws.refresh(loop_time).mean_nanos(),
                        krkn_pub: krkn_pub.refresh(loop_time).mean_nanos(),
                        krkn_priv: krkn_priv.refresh(loop_time).mean_nanos(),
                        plnx_pub: plnx_pub.refresh(loop_time).mean_nanos(),
                        plnx_priv: plnx_priv.refresh(loop_time).mean_nanos(),
                        plnx_order: plnx_order.refresh(loop_time).mean_nanos(),

                        krkn_trade_30_mean: krkn_trade_30.mean_nanos(),
                        krkn_trade_30_max: krkn_trade_30.max_nanos().unwrap_or(0),

                        krkn_trade_300_mean: krkn_trade_300.mean_nanos(),
                        krkn_trade_300_max: krkn_trade_300.max_nanos().unwrap_or(0),

                        plnx_last: dt_from_dur(loop_time - last.plnx),
                        krkn_last: dt_from_dur(loop_time - last.krkn),

                        plnx_ws_count: plnx_ws_count.refresh(loop_time).count() as u64,

                    };
                    channel.send(update).unwrap();
                    last.broadcast = loop_time;
                    debug!(logger, "sent broadcast");
                }
            }
            crit!(logger, "goodbye");
        }));

        LatencyManager {
            tx: tx_copy,
            channel: channel_copy,
            thread
        }
    }
}
