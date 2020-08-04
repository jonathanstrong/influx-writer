//! Utilities to efficiently send data to influx
//!

#![feature(test)]

#[cfg(test)]
extern crate test;
#[macro_use]
extern crate slog;

use std::io::Read;
use std::sync::Arc;
use crossbeam_channel::{Sender, Receiver, bounded, SendError};
use std::{thread, mem};
use std::time::*;
use std::collections::VecDeque;
use hyper::status::StatusCode;
use hyper::client::response::Response;
use hyper::Url;
use hyper::client::Client;
use slog::Drain;
use chrono::prelude::*;
use decimal::d128;
use uuid::Uuid;
use smallvec::SmallVec;
use slog::Logger;
use pretty_toa::ThousandsSep;

pub type Credentials = hyper::header::Authorization<hyper::header::Basic>;

/// Created this so I know what types can be passed through the
/// `measure!` macro, which used to convert with `as i64` and
/// `as f64` until I accidentally passed a function name, and it
/// still compiled, but with garbage numbers.
pub trait AsI64 {
    fn as_i64(x: Self) -> i64;
}

impl AsI64 for i64 { fn as_i64(x: Self) -> i64 { x } }
impl AsI64 for i32 { fn as_i64(x: Self) -> i64 { x as i64 } }
impl AsI64 for u32 { fn as_i64(x: Self) -> i64 { x as i64 } }
impl AsI64 for u64 { fn as_i64(x: Self) -> i64 { x as i64 } }
impl AsI64 for usize { fn as_i64(x: Self) -> i64 { x as i64 } }
impl AsI64 for f64 { fn as_i64(x: Self) -> i64 { x as i64 } }
impl AsI64 for f32 { fn as_i64(x: Self) -> i64 { x as i64 } }
impl AsI64 for u16 { fn as_i64(x: Self) -> i64 { x as i64 } }
impl AsI64 for i16 { fn as_i64(x: Self) -> i64 { x as i64 } }
impl AsI64 for u8 { fn as_i64(x: Self) -> i64 { x as i64 } }
impl AsI64 for i8 { fn as_i64(x: Self) -> i64 { x as i64 } }

/// Created this so I know what types can be passed through the
/// `measure!` macro, which used to convert with `as i64` and
/// `as f64` until I accidentally passed a function name, and it
/// still compiled, but with garbage numbers.
pub trait AsF64 {
    fn as_f64(x: Self) -> f64;
}

impl AsF64 for f64 { fn as_f64(x: Self) -> f64 { x } }
impl AsF64 for i64 { fn as_f64(x: Self) -> f64 { x as f64 } }
impl AsF64 for i32 { fn as_f64(x: Self) -> f64 { x as f64 } }
impl AsF64 for u32 { fn as_f64(x: Self) -> f64 { x as f64 } }
impl AsF64 for u64 { fn as_f64(x: Self) -> f64 { x as f64 } }
impl AsF64 for usize { fn as_f64(x: Self) -> f64 { x as f64 } }
impl AsF64 for f32 { fn as_f64(x: Self) -> f64 { x as f64 } }

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
/// #[macro_use]
/// extern crate influx_writer;
///
/// use influx_writer::{OwnedValue, OwnedMeasurement, AsI64};
///
/// use decimal::d128;
///
/// fn main() {
///     let (tx, rx) = crossbeam_channel::bounded(1024);
///
///     // "shorthand" syntax
///
///     measure!(tx, test, t(color, "red"), i(n, 1));
///
///     let meas: OwnedMeasurement = rx.recv().unwrap();
///
///     assert_eq!(meas.key, "test");
///     assert_eq!(meas.get_tag("color"), Some("red"));
///     assert_eq!(meas.get_field("n"), Some(&OwnedValue::Integer(1)));
///
///     measure!(tx, test,
///         t(one, "a"), t(two, "b"), i(three, 2),
///         f(four, 1.2345), s(five, String::from("d")),
///         b(six, true), i(seven, 1 + 2),
///         tm(1)
///     );
///
///     let meas: OwnedMeasurement = rx.recv().unwrap();
///
///     assert_eq!(meas.key, "test");
///     assert_eq!(meas.get_tag("one"), Some("a"));
///     assert_eq!(meas.get_tag("two"), Some("b"));
///     assert_eq!(meas.get_field("three"), Some(&OwnedValue::Integer(2)));
///     assert_eq!(meas.get_field("seven"), Some(&OwnedValue::Integer(3)));
///     assert_eq!(meas.timestamp, Some(1));
///
///     // use the @make_meas flag to skip sending a measurement, instead merely
///     // creating it.
///
///     let meas: OwnedMeasurement = measure!(@make_meas meas_only, t(color, "red"), i(n, 1));
///
///     // each variant also has shorthand aliases
///
///     let meas: OwnedMeasurement = measure!(@make_meas abcd, t(color, "red"), i(n, 1), d(price, d128::zero()));
/// }
/// ```
///
#[macro_export]
macro_rules! measure {
    (@kv $t:tt, $meas:ident, $k:tt => $v:expr) => { measure!(@ea $t, $meas, stringify!($k), $v) };
    (@kv $t:tt, $meas:ident, $k:tt; $v:expr) => { measure!(@ea $t, $meas, stringify!($k), $v) };
    (@kv $t:tt, $meas:ident, $k:tt, $v:expr) => { measure!(@ea $t, $meas, stringify!($k), $v) };
    //(@kv time, $meas:ident, $tm:expr) => { $meas = $meas.set_timestamp(AsI64::as_i64($tm)) };
    (@kv tm, $meas:ident, $tm:expr) => { $meas = $meas.set_timestamp(AsI64::as_i64($tm)) };
    (@kv utc, $meas:ident, $tm:expr) => { $meas = $meas.set_timestamp(AsI64::as_i64($crate::nanos($tm))) };
    (@kv v, $meas:ident, $k:expr) => { measure!(@ea t, $meas, "version", $k) };
    (@kv $t:tt, $meas:ident, $k:tt) => { measure!(@ea $t, $meas, stringify!($k), measure!(@as_expr $k)) };
    (@ea t, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_tag($k, $v); };
    (@ea i, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_field($k, $crate::OwnedValue::Integer(AsI64::as_i64($v))) };
    (@ea f, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_field($k, $crate::OwnedValue::Float(AsF64::as_f64($v))) };
    (@ea s, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_field($k, $crate::OwnedValue::String($v)) };
    (@ea d, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_field($k, $crate::OwnedValue::D128($v)) };
    (@ea u, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_field($k, $crate::OwnedValue::Uuid($v)) };
    (@ea b, $meas:ident, $k:expr, $v:expr) => { $meas = $meas.add_field($k, $crate::OwnedValue::Boolean(bool::from($v))) };

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
            $crate::OwnedMeasurement::with_capacity(stringify!($name), n_tags, n_fields);
        $(
            measure!(@kv $t, meas, $($tail)*);
        )*
        meas
    }};

    ($m:expr, $name:tt, $( $t:tt ( $($tail:tt)* ) ),+ $(,)*) => {
        measure!($m, $name, $($t [ $($tail)* ] ),+)
    };

    ($m:tt, $name:tt, $( $t:tt [ $($tail:tt)* ] ),+ $(,)*) => {{
        #[allow(unused_imports)]
        use $crate::{AsI64, AsF64};
        let measurement = measure!(@make_meas $name, $( $t [ $($tail)* ] ),*);
        let _ = $m.send(measurement);
    }};
}

/// converts a chrono::DateTime to an integer timestamp (ns)
///
#[inline]
pub fn nanos(t: DateTime<Utc>) -> u64 {
    (t.timestamp() as u64) * 1_000_000_000_u64 + (t.timestamp_subsec_nanos() as u64)
}

#[inline]
pub fn secs(d: Duration) -> f64 {
    d.as_secs() as f64 + d.subsec_nanos() as f64 / 1_000_000_000_f64
}

#[inline]
pub fn inanos(t: DateTime<Utc>) -> i64 {
    t.timestamp() * 1_000_000_000i64 + t.timestamp_subsec_nanos() as i64
}

//#[deprecated(since="0.4.3", note="Use `nanos(DateTime<Utc>) -> u64` instead")]
pub fn dt_nanos(t: DateTime<Utc>) -> i64 {
    (t.timestamp() as i64) * 1_000_000_000_i64 + (t.timestamp_subsec_nanos() as i64)
}

#[inline]
pub fn dur_nanos(d: ::std::time::Duration) -> i64 {
    (d.as_secs() * 1_000_000_000_u64 + (d.subsec_nanos() as u64)) as i64
}

#[inline]
pub fn nanos_utc(t: i64) -> DateTime<Utc> {
    Utc.timestamp(t / 1_000_000_000, (t % 1_000_000_000) as u32)
}

#[derive(Clone, Debug)]
struct Point<T, V> {
    pub time: T,
    pub value: V
}

struct DurationWindow {
    pub size: Duration,
    pub mean: Duration,
    pub sum: Duration,
    pub count: u32,
    pub items: VecDeque<Point<Instant, Duration>>
}

#[allow(dead_code)]
impl DurationWindow {
    #[inline]
    pub fn update(&mut self, time: Instant, value: Duration) {
        self.add(time, value);
        self.refresh(time);
    }

    #[inline]
    pub fn refresh(&mut self, t: Instant) -> &Self {
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
    pub fn add(&mut self, time: Instant, value: Duration) {
        let p = Point { time, value };
        self.sum += p.value;
        self.count += 1;
        self.items.push_back(p);
    }
}

/// Holds a thread (and provides an interface to it) that serializes `OwnedMeasurement`s
/// it receives (over a SPSC channel) and inserts to influxdb via http when `BUFFER_SIZE`
/// measurements have accumulated.
///
#[derive(Debug)]
pub struct InfluxWriter {
    host: String,
    db: String,
    tx: Sender<Option<OwnedMeasurement>>,
    thread: Option<Arc<thread::JoinHandle<()>>>,
}

impl Default for InfluxWriter {
    fn default() -> Self {
        InfluxWriter::new("localhost", "test")
    }
}

impl Clone for InfluxWriter {
    fn clone(&self) -> Self {
        debug_assert!(self.thread.is_some());
        let thread = self.thread.as_ref().map(|x| Arc::clone(x));
        InfluxWriter {
            host: self.host.to_string(),
            db: self.db.to_string(),
            tx: self.tx.clone(),
            thread,
        }
    }
}

impl InfluxWriter {
    pub fn host(&self) -> &str { self.host.as_str() }

    pub fn db(&self) -> &str { self.db.as_str() }

    /// Sends the `OwnedMeasurement` to the serialization thread.
    ///
    #[inline]
    pub fn send(&self, m: OwnedMeasurement) -> Result<(), SendError<Option<OwnedMeasurement>>> {
        self.tx.send(Some(m))
    }

    #[inline]
    pub fn nanos(&self, d: DateTime<Utc>) -> i64 { nanos(d) as i64 }

    #[inline]
    pub fn dur_nanos(&self, d: Duration) -> i64 { dur_nanos(d) as i64 }

    #[inline]
    pub fn dur_nanos_u64(&self, d: Duration) -> u64 { dur_nanos(d).max(0) as u64 }

    #[inline]
    pub fn rsecs(&self, d: Duration) -> f64 {
        ((d.as_secs() as f64 + (d.subsec_nanos() as f64 / 1_000_000_000_f64))
            * 1000.0)
            .round()
            / 1000.0
    }

    #[inline]
    pub fn secs(&self, d: Duration) -> f64 {
        d.as_secs() as f64 + d.subsec_nanos() as f64 / 1_000_000_000_f64
    }

    pub fn tx(&self) -> Sender<Option<OwnedMeasurement>> {
        self.tx.clone()
    }

    #[inline]
    pub fn is_full(&self) -> bool { self.tx.is_full() }

    pub fn placeholder() -> Self {
        let (tx, _) = bounded(1024);
        Self {
            host: String::new(),
            db: String::new(),
            tx,
            thread: None,
        }
    }

    pub fn new(host: &str, db: &str) -> Self {
        let noop_logger = slog::Logger::root(slog::Discard.fuse(), o!());
        Self::with_logger_and_opt_creds(host, db, None, &noop_logger)
    }

    pub fn get_credentials(username: String, password: Option<String>) -> Credentials {
        hyper::header::Authorization(
            hyper::header::Basic { username, password }
        )
    }

    fn http_req<'a>(client: &'a Client, url: Url, body: &'a str, creds: &Option<Credentials>) -> hyper::client::RequestBuilder<'a> {
        let req = client.post(url.clone())
            .body(body);
        if let Some(auth) = creds {
            req.header(auth.clone())
        } else {
            req
        }
    }

    #[deprecated(since = "0.9.0", note = "replaced by with_logger_and_opt_creds")]
    #[allow(unused_assignments)]
    pub fn with_logger(host: &str, db: &str, logger: &Logger) -> Self {
        Self::with_logger_and_opt_creds(host, db, None, logger)
    }

    pub fn with_logger_and_opt_creds(host: &str, db: &str, creds: Option<Credentials>, logger: &Logger) -> Self {
        let logger = logger.new(o!(
            "host" => host.to_string(),
            "db" => db.to_string()));
        let (tx, rx): (Sender<Option<OwnedMeasurement>>, Receiver<Option<OwnedMeasurement>>) = bounded(4096);
        let url =
            Url::parse_with_params(&format!("http://{}:8086/write", host),
                                   &[("db", db), ("precision", "ns")])
                .expect("influx writer url should parse");
        let thread = thread::Builder::new().name(format!("mm:inflx:{}", db)).spawn(move || {
            use std::time::*;
            use crossbeam_channel as chan;

            #[cfg(feature = "no-influx-buffer")]
            const N_BUFFER_LINES: usize = 0;

            const N_BUFFER_LINES: usize = 1024;
            const MAX_PENDING: Duration = Duration::from_secs(3);
            const INITIAL_BUFFER_CAPACITY: usize = 4096;
            const MAX_BACKLOG: usize = 1024;
            const MAX_OUTSTANDING_HTTP: usize = 64;
            const DEBUG_HB_EVERY: usize = 1024 * 96;
            const INFO_HB_EVERY: usize = 1024 * 1024;
            const N_HTTP_ATTEMPTS: u32 = 15;
            const INITIAL_BACKLOG: usize = MAX_OUTSTANDING_HTTP * 2;

            let client = Arc::new(Client::new());
            let creds = Arc::new(creds);

            info!(logger, "initializing InfluxWriter ...";
                "N_BUFFER_LINES" => N_BUFFER_LINES,
                "MAX_PENDING" => %format_args!("{:?}", MAX_PENDING),
                "MAX_OUTSTANDING_HTTP" => MAX_OUTSTANDING_HTTP,
                "INITIAL_BUFFER_CAPACITY" => INITIAL_BUFFER_CAPACITY,
                "INITIAL_BACKLOG" => INITIAL_BACKLOG,
                "MAX_BACKLOG" => MAX_BACKLOG,
            );

            // pre-allocated buffers ready for use if the active one is stasheed
            // during an outage
            let mut spares: VecDeque<String> = VecDeque::with_capacity(INITIAL_BACKLOG);

            // queue failed sends here until problem resolved, then send again. in worst
            // case scenario, loop back around on buffers queued in `backlog`, writing
            // over the oldest first.
            //
            let mut backlog: VecDeque<String> = VecDeque::with_capacity(INITIAL_BACKLOG);

            for _ in 0..INITIAL_BACKLOG {
                spares.push_back(String::with_capacity(INITIAL_BUFFER_CAPACITY));
            }

            struct Resp {
                pub buf: String,
                pub took: Duration,
            }

            let mut db_health = DurationWindow {
                size: Duration::from_secs(120),
                mean: Duration::new(10, 0),
                sum: Duration::new(0, 0),
                count: 0,
                items: VecDeque::with_capacity(MAX_OUTSTANDING_HTTP),
            };

            let (http_tx, http_rx) = chan::bounded(32);

            let mut buf = spares.pop_front().unwrap();
            let mut count = 0;
            let mut extras = 0; // any new Strings we intro to the system
            let mut n_rcvd = 0;
            let mut in_flight_buffer_bytes = 0;
            let mut last = Instant::now();
            let mut active: bool;
            let mut last_clear = Instant::now();
            let mut last_memory_check = Instant::now();
            let mut loop_time: Instant;

            let n_out = |s: &VecDeque<String>, b: &VecDeque<String>, extras: usize| -> usize {
                INITIAL_BACKLOG + extras - s.len() - b.len() - 1
            };

            assert_eq!(n_out(&spares, &backlog, extras), 0);

            let count_allocated_memory = |spares: &VecDeque<String>, backlog: &VecDeque<String>, in_flight_buffer_bytes: &usize| -> usize {
                spares.iter().map(|x| x.capacity()).sum::<usize>()
                + backlog.iter().map(|x| x.capacity()).sum::<usize>()
                + (*in_flight_buffer_bytes)
            };

            let send = |mut buf: String, backlog: &mut VecDeque<String>, n_outstanding: usize, in_flight_buffer_bytes: &mut usize| {
                if n_outstanding >= MAX_OUTSTANDING_HTTP {
                    backlog.push_back(buf);
                    return
                }
                let url = url.clone(); // Arc would be faster, but `hyper::Client::post` consumes url
                let tx = http_tx.clone();
                let thread_logger = logger.new(o!("thread" => "InfluxWriter:http", "in flight req at spawn time" => n_outstanding)); // re `thread_logger` name: disambiguating for `logger` after thread closure
                let client = Arc::clone(&client);
                let creds = Arc::clone(&creds);
                *in_flight_buffer_bytes = *in_flight_buffer_bytes + buf.capacity();
                debug!(logger, "launching http thread");
                let thread_res = thread::Builder::new().name(format!("inflx-http{}", n_outstanding)).spawn(move || {
                    let logger = thread_logger;
                    debug!(logger, "preparing to send http request to influx"; "buf.len()" => buf.len());
                    let start = Instant::now();
                    for n_req in 0..N_HTTP_ATTEMPTS {
                        let throttle = Duration::from_secs(2) * n_req * n_req;
                        if n_req > 0 {
                            warn!(logger, "InfluxWriter http thread: pausing before next request";
                                  "n_req" => n_req,
                                  "throttle" => %format_args!("{:?}", throttle),
                                  "elapsed" => %format_args!("{:?}", Instant::now() - start));
                            thread::sleep(throttle); // 0, 2, 8, 16, 32
                        }
                        let sent = Instant::now();
                        let req = Self::http_req(&client, url.clone(), buf.as_str(), &creds);
                        let resp = req.send();
                        let rcvd = Instant::now();
                        let took = rcvd - sent;
                        let mut n_tx = 0u32;
                        match resp {
                            Ok(Response { status, .. }) if status == StatusCode::NoContent => {
                                debug!(logger, "server responded ok: 204 NoContent");
                                buf.clear();
                                let mut resp = Some(Ok(Resp { buf, took }));
                                loop {
                                    n_tx += 1;
                                    match tx.try_send(resp.take().unwrap()) {
                                        Ok(_) => {
                                            if n_req > 0 {
                                                info!(logger, "successfully recovered from failed request with retry";
                                                      "n_req" => n_req,
                                                      "n_tx" => n_tx,
                                                      "elapsed" => %format_args!("{:?}", Instant::now() - start));
                                            }
                                            return
                                        }

                                        Err(chan::TrySendError::Full(r)) => {
                                            let throttle = Duration::from_millis(1000) * n_tx;
                                            warn!(logger, "channel full: InfluxWriter http thread failed to return buf";
                                                  "n_tx" => n_tx, "n_req" => n_req, "until next" => %format_args!("{:?}", throttle));
                                            resp = Some(r);
                                            thread::sleep(throttle);
                                        }

                                        Err(chan::TrySendError::Disconnected(_)) => {
                                            warn!(logger, "InfluxWriter http thread: channel disconnected, aborting buffer return";
                                                  "n_tx" => n_tx, "n_req" => n_req);
                                            return
                                        }
                                    }
                                }
                            }

                            Ok(mut resp) =>  {
                                let mut server_resp = String::new();
                                let _ = resp.read_to_string(&mut server_resp); //.unwrap_or(0);
                                error!(logger, "influx server error (request took {:?})", took;
                                       "status" => %resp.status,
                                       "body" => server_resp);
                            }

                            Err(e) => {
                                error!(logger, "http request failed: {:?} (request took {:?})", e, took; "err" => %e);
                            }
                        }

                    }
                    let took = Instant::now() - start;
                    warn!(logger, "InfluxWriter http thread: aborting http req, returning buffer";
                        "took" => %format_args!("{:?}", took));
                    let buflen = buf.len();
                    let n_lines = buf.lines().count();
                    if let Err(e) = tx.send(Err(Resp { buf, took })) {
                        crit!(logger, "failed to send Err(Resp {{ .. }}) back on abort: {:?}", e;
                              "err" => %e, "buf.len()" => buflen, "n_lines" => n_lines);
                    }
                });

                if let Err(e) = thread_res {
                    crit!(logger, "failed to spawn thread: {}", e);
                }
            };

            let next = |prev: usize, m: &OwnedMeasurement, buf: &mut String, loop_time: Instant, last: Instant| -> Result<usize, usize> {
                match prev {
                    0 if N_BUFFER_LINES > 0 => {
                        serialize_owned(m, buf);
                        Ok(1)
                    }

                    n if n < N_BUFFER_LINES && loop_time - last < MAX_PENDING => {
                        buf.push_str("\n");
                        serialize_owned(m, buf);
                        Ok(n + 1)
                    }

                    n => {
                        buf.push_str("\n");
                        serialize_owned(m, buf);
                        Err(n + 1)
                    }
                }
            };

            'event: loop {
                loop_time = Instant::now();
                active = false;

                if loop_time - last_memory_check > Duration::from_secs(60) {
                    let allocated_bytes = count_allocated_memory(&spares, &backlog, &in_flight_buffer_bytes);
                    let allocated_mb = allocated_bytes as f64 / 1024.0 / 1024.0;
                    info!(logger, "allocated memory: {:.1}MB", allocated_mb;
                        "allocated bytes" => allocated_bytes,
                        "in flight buffer bytes" => in_flight_buffer_bytes,
                        "spares.len()" => spares.len(),
                        "backlog.len()" => backlog.len(),
                    );
                    last_memory_check = loop_time;
                }
                match rx.recv() {
                    Ok(Some(mut meas)) => {
                        n_rcvd += 1;
                        active = true;

                        if n_rcvd % INFO_HB_EVERY == 0 {
                            let n_outstanding = n_out(&spares, &backlog, extras);
                            let allocated_bytes = count_allocated_memory(&spares, &backlog, &in_flight_buffer_bytes);
                            let allocated_mb = allocated_bytes as f64 / 1024.0 / 1024.0;
                            info!(logger, "rcvd {} measurements", n_rcvd.thousands_sep();
                                "n_outstanding" => n_outstanding,
                                "spares.len()" => spares.len(),
                                "n_rcvd" => n_rcvd,
                                "n_active_buf" => count,
                                "db_health" => %format_args!("{:?}", db_health.mean),
                                "allocated buffer memory" => %format_args!("{:.1}MB", allocated_mb),
                                "backlog.len()" => backlog.len());
                        } else if n_rcvd % DEBUG_HB_EVERY == 0 {
                            let n_outstanding = n_out(&spares, &backlog, extras);
                            let allocated_bytes = count_allocated_memory(&spares, &backlog, &in_flight_buffer_bytes);
                            let allocated_mb = allocated_bytes as f64 / 1024.0 / 1024.0;
                            debug!(logger, "rcvd {} measurements", n_rcvd.thousands_sep();
                                "n_outstanding" => n_outstanding,
                                "spares.len()" => spares.len(),
                                "n_rcvd" => n_rcvd,
                                "n_active_buf" => count,
                                "db_health" => %format_args!("{:?}", db_health.mean),
                                "allocated buffer memory" => %format_args!("{:.1}MB", allocated_mb),
                                "backlog.len()" => backlog.len());
                        }

                        if meas.timestamp.is_none() { meas.timestamp = Some(now()) }

                        if meas.fields.is_empty() {
                            meas.fields.push(("n", OwnedValue::Integer(1)));
                        }

                        //#[cfg(feature = "trace")] { if count % 10 == 0 { trace!(logger, "rcvd new measurement"; "count" => count, "key" => meas.key); } }

                        count = match next(count, &meas, &mut buf, loop_time, last) {
                            Ok(n) => n,
                            Err(_n) => {
                                let mut count = 0;
                                let mut next: String = match spares.pop_front() {
                                    Some(x) => x,

                                    None => {
                                        let n_outstanding = n_out(&spares, &backlog, extras);
                                        if n_outstanding > MAX_BACKLOG {
                                            warn!(logger, "no available buffers in `spares`, pulling from backlog";
                                                  "n_outstanding" => n_outstanding,
                                                  "spares.len()" => spares.len(),
                                                  "n_rcvd" => n_rcvd,
                                                  "backlog.len()" => backlog.len());
                                            match backlog.pop_front() {
                                                // Note: this does not clear the backlog buffer,
                                                // instead we will just write more and more until
                                                // we are out of memory. I expect that will never
                                                // happen.
                                                //
                                                Some(x) => {
                                                    count = 1;  // otherwise, no '\n' added in `next(..)` - we are
                                                                // sending a "full" buffer to be extended
                                                    x
                                                }

                                                None => {
                                                    extras += 1;
                                                    crit!(logger, "failed to pull from backlog, too!! WTF #!(*#(* ... creating new String";
                                                        "n_outstanding" => n_outstanding,
                                                        "spares.len()" => spares.len(),
                                                        "backlog.len()" => backlog.len(),
                                                        "n_rcvd" => n_rcvd,
                                                        "extras" => extras);
                                                    String::new()
                                                }
                                            }
                                        } else {
                                            extras += 1;
                                            let allocated_bytes = count_allocated_memory(&spares, &backlog, &in_flight_buffer_bytes) + INITIAL_BUFFER_CAPACITY;
                                            let allocated_mb = allocated_bytes as f64 / 1024.0 / 1024.0;
                                            info!(logger, "allocating new buffer: zero spares avail";
                                                "allocated buffer memory" => %format_args!("{:.1}MB", allocated_mb),
                                                "n_outstanding" => n_outstanding,
                                                "extras" => extras,
                                            );
                                            String::with_capacity(INITIAL_BUFFER_CAPACITY)
                                        }
                                    }
                                };
                                // after swap, buf in next, so want to send next
                                //
                                mem::swap(&mut buf, &mut next);
                                let n_outstanding = n_out(&spares, &backlog, extras);
                                send(next, &mut backlog, n_outstanding, &mut in_flight_buffer_bytes);
                                last = loop_time;
                                count
                            }
                        };
                    }

                    Ok(None) => {
                        let start = Instant::now();
                        let mut hb = Instant::now();
                        warn!(logger, "terminate signal rcvd"; "count" => count);
                        if buf.len() > 0 {
                            info!(logger, "sending remaining buffer to influx on terminate"; "count" => count);
                            let meas = OwnedMeasurement::new("influx_writer").add_field("n", OwnedValue::Integer(1));
                            let _ = next(N_BUFFER_LINES, &meas, &mut buf, loop_time, last);
                            let n_outstanding = n_out(&spares, &backlog, extras);
                            let mut placeholder = spares.pop_front().unwrap_or_else(String::new);
                            mem::swap(&mut buf, &mut placeholder);
                            send(placeholder, &mut backlog, n_outstanding, &mut in_flight_buffer_bytes);
                        }
                        let mut n_ok = 0;
                        let mut n_err = 0;
                        loop {
                            loop_time = Instant::now();
                            let n_outstanding = n_out(&spares, &backlog, extras);
                            if backlog.is_empty() && n_outstanding < 1 {
                                info!(logger, "cleared any remaining backlog";
                                    "n_outstanding" => n_outstanding,
                                    "spares.len()" => spares.len(),
                                    "backlog.len()" => backlog.len(),
                                    "n_cleared_ok" => n_ok,
                                    "n_cleared_err" => n_err,
                                    "n_rcvd" => n_rcvd,
                                    "extras" => extras,
                                    "elapsed" => %format_args!("{:?}", loop_time - start));
                                break 'event
                            }
                            if loop_time - hb > Duration::from_secs(5) {
                                info!(logger, "InfluxWriter still clearing backlog ..";
                                      "n_outstanding" => n_outstanding,
                                      "spares.len()" => spares.len(),
                                      "backlog.len()" => backlog.len(),
                                      "n_cleared_ok" => n_ok,
                                      "n_cleared_err" => n_err,
                                      "extras" => extras,
                                      "n_rcvd" => n_rcvd,
                                      "elapsed" => %format_args!("{:?}", loop_time - start));
                                hb = loop_time;
                            }
                            if let Some(buf) = backlog.pop_front() {
                                let n_outstanding = n_out(&spares, &backlog, extras);
                                debug!(logger, "resending queued buffer from backlog";
                                       "backlog.len()" => backlog.len(),
                                       "spares.len()" => spares.len(),
                                       "n_rcvd" => n_rcvd,
                                       "n_outstanding" => n_outstanding);
                                send(buf, &mut backlog, n_outstanding, &mut in_flight_buffer_bytes);
                            }

                            'rx: loop {
                                match http_rx.try_recv() {
                                    Ok(Ok(Resp { buf, .. })) => {
                                        n_ok += 1;
                                        in_flight_buffer_bytes = in_flight_buffer_bytes.saturating_sub(buf.capacity());
                                        if spares.len() <= INITIAL_BACKLOG {
                                            spares.push_back(buf); // needed so `n_outstanding` count remains accurate
                                        } else {
                                            extras = extras.saturating_sub(1);
                                        }
                                    }
                                    Ok(Err(Resp { buf, .. })) => {
                                        warn!(logger, "requeueing failed request"; "buf.len()" => buf.len());
                                        n_err += 1;
                                        in_flight_buffer_bytes = in_flight_buffer_bytes.saturating_sub(buf.capacity());
                                        backlog.push_front(buf);
                                    }
                                    Err(chan::TryRecvError::Disconnected) => {
                                        crit!(logger, "trying to clear backlog, but http_rx disconnected! aborting";
                                            "n_outstanding" => n_outstanding,
                                            "backlog.len()" => backlog.len(),
                                            "n_cleared_ok" => n_ok,
                                            "n_cleared_err" => n_err,
                                            "extras" => extras,
                                            "n_rcvd" => n_rcvd,
                                            "elapsed" => %format_args!("{:?}", loop_time - start));
                                        break 'event
                                    }
                                    Err(_) => break 'rx
                                }
                            }
                            thread::sleep(Duration::from_millis(1));
                        }
                    }

                    _ => {}
                }

                db_health.refresh(loop_time);
                let n_outstanding = n_out(&spares, &backlog, extras);
                let healthy = db_health.count == 0 || db_health.mean < Duration::from_secs(200);
                if (n_outstanding < MAX_OUTSTANDING_HTTP 
                    || loop_time.saturating_duration_since(last_clear) > Duration::from_secs(60)) 
                    && healthy {

                    if let Some(queued) = backlog.pop_front() {
                        let n_outstanding = n_out(&spares, &backlog, extras);
                        send(queued, &mut backlog, n_outstanding, &mut in_flight_buffer_bytes);
                        active = true;
                    }
                    last_clear = loop_time;
                }

                loop {
                    match http_rx.try_recv() {
                        Ok(Ok(Resp { buf, took })) => {
                            db_health.add(loop_time, took);
                            let in_flight_before = in_flight_buffer_bytes.clone();
                            in_flight_buffer_bytes = in_flight_buffer_bytes.saturating_sub(buf.capacity());
                            if spares.len() <= INITIAL_BACKLOG {
                                spares.push_back(buf);
                            } else {
                                extras = extras.saturating_sub(1);
                                debug!(logger, "dropping buffer to reduce memory back to INITIAL_BACKLOG size";
                                    "spares.len()" => spares.len(),
                                    "extras" => extras,
                                    "in flight before" => in_flight_before,
                                    "in in_flight_buffer_bytes" => in_flight_buffer_bytes,
                                );
                            }

                            //spares.push_back(buf);
                            active = true;
                        }

                        Ok(Err(Resp { buf, took })) => {
                            db_health.add(loop_time, took);
                            in_flight_buffer_bytes = in_flight_buffer_bytes.saturating_sub(buf.capacity());
                            backlog.push_front(buf);
                            active = true;
                        }

                        Err(chan::TryRecvError::Disconnected) => {
                            crit!(logger, "trying to recover buffers, but http_rx disconnected! aborting";
                                "n_outstanding" => n_outstanding,
                                "backlog.len()" => backlog.len(),
                                "n_rcvd" => n_rcvd,
                                "extras" => extras);
                            break 'event
                        }

                        Err(_) => break
                    }
                }

                if !active {
                    thread::sleep(Duration::new(0, 1))
                }
            }
            thread::sleep(Duration::from_millis(10));
        }).unwrap();

        InfluxWriter {
            host: host.to_string(),
            db: db.to_string(),
            tx,
            thread: Some(Arc::new(thread))
        }
    }
}

impl Drop for InfluxWriter {
    fn drop(&mut self) {
        if let Some(arc) = self.thread.take() {
            if let Ok(thread) = Arc::try_unwrap(arc) {
                let _ = self.tx.send(None);
                let _ = thread.join();
            }
        }
    }
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

fn as_boolean(b: &bool) -> &str {
    if *b { "t" } else { "f" }
}

pub fn now() -> i64 {
    nanos(Utc::now()) as i64
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
        #[cfg(not(feature = "string-tags"))]
        add_tag(line, key, value);

        #[cfg(feature = "string-tags")]
        add_tag(line, key, value.as_str());
    }

    let add_field = |line: &mut String, key: &str, value: &OwnedValue, is_first: bool| {
        if is_first { line.push_str(" "); } else { line.push_str(","); }
        line.push_str(&escape_tag(key));
        line.push_str("=");
        match *value {
            OwnedValue::String(ref s)  => line.push_str(&as_string(s)),
            OwnedValue::Integer(ref i) => line.push_str(&format!("{}i", i)),
            OwnedValue::Boolean(ref b) => line.push_str(as_boolean(b)),

            OwnedValue::D128(ref d) => {
                if d.is_finite() {
                    line.push_str(&format!("{}", d));
                } else {
                    line.push_str("0.0");
                }
            }

            OwnedValue::Float(ref f)   => {
                if f.is_finite() {
                    line.push_str(&format!("{}", f));
                } else {
                    line.push_str("-999.0");
                }
            }

            OwnedValue::Uuid(ref u)    => line.push_str(&format!("\"{}\"", u)),
        };
    };

    let mut fields = measurement.fields.iter();

    // first time separate from tags with space
    //
    fields.next().map(|kv| {
        add_field(line, &kv.0, &kv.1, true);
    });

    // then seperate the rest w/ comma
    //
    for kv in fields {
        add_field(line, kv.0, &kv.1, false);
    }

    if let Some(t) = measurement.timestamp {
        line.push_str(" ");
        line.push_str(&t.to_string());
    }
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

/// Holds data meant for an influxdb measurement in transit to the
/// writing thread.
///
#[derive(Clone, Debug)]
pub struct OwnedMeasurement {
    pub key: &'static str,
    pub timestamp: Option<i64>,
    //pub fields: Map<&'static str, OwnedValue>,
    //pub tags: Map<&'static str, &'static str>,
    pub fields: SmallVec<[(&'static str, OwnedValue); 8]>,
    #[cfg(not(feature = "string-tags"))]
    pub tags: SmallVec<[(&'static str, &'static str); 8]>,
    #[cfg(feature = "string-tags")]
    pub tags: SmallVec<[(&'static str, String); 8]>,
}

impl OwnedMeasurement {
    pub fn with_capacity(key: &'static str, n_tags: usize, n_fields: usize) -> Self {
        OwnedMeasurement {
            key,
            timestamp: None,
            tags: SmallVec::with_capacity(n_tags),
            fields: SmallVec::with_capacity(n_fields),
        }
    }

    pub fn new(key: &'static str) -> Self {
        OwnedMeasurement {
            key,
            timestamp: None,
            tags: SmallVec::new(),
            fields: SmallVec::new(),
        }
    }

    /// Unusual consuming `self` signature because primarily used by
    /// the `measure!` macro.
    #[cfg(not(feature = "string-tags"))]
    pub fn add_tag(mut self, key: &'static str, value: &'static str) -> Self {
        self.tags.push((key, value));
        self
    }

    #[cfg(feature = "string-tags")]
    pub fn add_tag<S: ToString>(mut self, key: &'static str, value: S) -> Self {
        self.tags.push((key, value.to_string()));
        self
    }

    /// Unusual consuming `self` signature because primarily used by
    /// the `measure!` macro.
    pub fn add_field(mut self, key: &'static str, value: OwnedValue) -> Self {
        self.fields.push((key, value));
        self
    }

    pub fn set_timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    #[cfg(not(feature = "string-tags"))]
    pub fn set_tag(mut self, key: &'static str, value: &'static str) -> Self {
        match self.tags.iter().position(|kv| kv.0 == key) {
            Some(i) => {
                self.tags.get_mut(i)
                    .map(|x| {
                        x.0 = value;
                    });
                self
            }

            None => {
                self.add_tag(key, value)
            }
        }
    }

    pub fn get_field(&self, key: &'static str) -> Option<&OwnedValue> {
        self.fields.iter()
            .find(|kv| kv.0 == key)
            .map(|kv| &kv.1)
    }

    #[cfg(feature = "string-tags")]
    pub fn get_tag(&self, key: &'static str) -> Option<&str> {
        self.tags.iter()
            .find(|kv| kv.0 == key)
            .map(|kv| kv.1.as_str())
    }

    #[cfg(not(feature = "string-tags"))]
    pub fn get_tag(&self, key: &'static str) -> Option<&'static str> {
        self.tags.iter()
            .find(|kv| kv.0 == key)
            .map(|kv| kv.1)
    }
}

#[allow(unused)]
#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "unstable")]
    use test::{black_box, Bencher};

    #[ignore]
    #[cfg(feature = "unstable")]
    #[bench]
    fn measure_ten(b: &mut Bencher) {
        let influx = InfluxWriter::new("localhost", "test");
        let mut n = 0;
        b.iter(|| {
            for _ in 0..10 {
                let time = influx.nanos(Utc::now());
                n += 1;
                measure!(influx, million, i(n), tm(time));
            }
        });
    }

    #[test]
    fn it_uses_the_utc_shortcut_to_convert_a_datetime_utc() {
        const VERSION: &str = "0.3.90";
        let tag_value = "one";
        let color = "red";
        let time = Utc::now();
        let m = measure!(@make_meas test, i(n, 1), t(color), v(VERSION), utc(time));
        assert_eq!(m.get_tag("color"), Some("red"));
        assert_eq!(m.get_tag("version"), Some(VERSION));
        assert_eq!(m.timestamp, Some(nanos(time) as i64));
    }

    #[test]
    fn it_uses_the_v_for_version_shortcut() {
        const VERSION: &str = "0.3.90";
        let tag_value = "one";
        let color = "red";
        let time = now();
        let m = measure!(@make_meas test, i(n, 1), t(color), v(VERSION), tm(time));
        assert_eq!(m.get_tag("color"), Some("red"));
        assert_eq!(m.get_tag("version"), Some(VERSION));
        assert_eq!(m.timestamp, Some(time));
    }

    #[test]
    fn it_uses_the_new_tag_k_only_shortcut() {
        let tag_value = "one";
        let color = "red";
        let time = now();
        let m = measure!(@make_meas test, t(color), t(tag_value), tm(time));
        assert_eq!(m.get_tag("color"), Some("red"));
        assert_eq!(m.get_tag("tag_value"), Some("one"));
        assert_eq!(m.timestamp, Some(time));
    }

    #[test]
    fn it_uses_measure_macro_parenthesis_syntax() {
        let m = measure!(@make_meas test, t(a,"b"), i(n,1), f(x,1.1), tm(1));
        assert_eq!(m.key, "test");
        assert_eq!(m.get_tag("a"), Some("b"));
        assert_eq!(m.get_field("n"), Some(&OwnedValue::Integer(1)));
        assert_eq!(m.get_field("x"), Some(&OwnedValue::Float(1.1)));
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

    #[test]
    fn it_clones_an_influx_writer_to_check_both_drop() {
        let influx = InfluxWriter::default();
        measure!(influx, drop_test, i(a, 1), i(b, 2));
        {
            let influx = influx.clone();
            thread::spawn(move || {
                measure!(influx, drop_test, i(a, 3), i(b, 4));
            });
        }
    }

    #[cfg(feature = "unstable")]
    #[bench]
    fn influx_writer_send_basic(b: &mut Bencher) {
        let m = InfluxWriter::new("localhost", "test");
        b.iter(|| {
            measure!(m, test, t(color; "red"), i(n, 1)); //, float[p; 1.234]);
        });
    }

    #[cfg(feature = "unstable")]
    #[bench]
    fn influx_writer_send_price(b: &mut Bencher) {
        let m = InfluxWriter::new("localhost", "test");
        b.iter(|| {
            measure!(m, test,
                t(ticker, "xmr_btc"),
                t(exchange, "plnx"),
                d(bid, d128::zero()),
                d(ask, d128::zero()),
            );
        });
    }

    #[test]
    fn it_checks_color_tag_error_in_non_doctest() {
        let (tx, rx) = bounded(1024);
        measure!(tx, test, t(color,"red"), i(n,1));
        let meas: OwnedMeasurement = rx.recv().unwrap();
        assert_eq!(meas.get_tag("color"), Some("red"), "meas = \n {:?} \n", meas);
    }

    #[test]
    fn it_uses_the_make_meas_pattern_of_the_measure_macro() {
        let meas = measure!(@make_meas test_measurement,
            t(one, "a"), t(two, "b"), i(three, 2),
            f(four, 1.2345), s(five, String::from("d")),
            b(six, true), i(seven, 1 + 2),
            tm(1)
        );
        assert_eq!(meas.key, "test_measurement");
        assert_eq!(meas.get_tag("one"), Some("a"));
        assert_eq!(meas.get_tag("two"), Some("b"));
        assert_eq!(meas.get_field("three"), Some(&OwnedValue::Integer(2)));
        assert_eq!(meas.get_field("seven"), Some(&OwnedValue::Integer(3)));
        assert_eq!(meas.timestamp, Some(1));
    }

    #[test]
    fn it_uses_measure_macro_for_d128_and_uuid() {

        let (tx, rx) = bounded(1024);
        let one = "a";
        let two = d128::zero();
        let three = Uuid::new_v4();
        let time = now();
        measure!(tx, test_measurement, t(one), d(two), u(three), tm(time));

        thread::sleep(Duration::from_millis(10));
        let meas: OwnedMeasurement = rx.try_recv().unwrap();
        assert_eq!(meas.key, "test_measurement");
        assert_eq!(meas.get_tag("one"), Some("a"));
        assert_eq!(meas.get_field("two"), Some(&OwnedValue::D128(d128::zero())));
        assert_eq!(meas.get_field("three"), Some(&OwnedValue::Uuid(three)));
        assert_eq!(meas.timestamp, Some(time));
    }

    #[test]
    fn it_uses_the_measure_macro_alt_syntax() {

        let (tx, rx) = bounded(1024);
        measure!(tx, test_measurement,
            t(one, "a"), t(two, "b"), i(three, 2),
            f(four, 1.2345), s(five, String::from("d")),
            b(six, true), i(seven, 1 + 2),
            tm(1)
        );

        thread::sleep(Duration::from_millis(10));
        let meas: OwnedMeasurement = rx.try_recv().unwrap();
        assert_eq!(meas.key, "test_measurement");
        assert_eq!(meas.get_tag("one"), Some("a"));
        assert_eq!(meas.get_tag("two"), Some("b"));
        assert_eq!(meas.get_field("three"), Some(&OwnedValue::Integer(2)));
        assert_eq!(meas.get_field("seven"), Some(&OwnedValue::Integer(3)));
        assert_eq!(meas.timestamp, Some(1));
    }

    #[test]
    fn it_checks_that_fields_are_separated_correctly() {
        let m = measure!(@make_meas test, t[a; "one"], t[b; "two"], f[x; 1.1], f[y; -1.1]);
        assert_eq!(m.key, "test");
        assert_eq!(m.get_tag("a"), Some("one"));
        assert_eq!(m.get_field("x"), Some(&OwnedValue::Float(1.1)));

        let mut buf = String::new();
        serialize_owned(&m, &mut buf);
        assert!(buf.contains("b=two x=1.1,y=-1.1"), "buf = {}", buf);
    }

    #[test]
    fn try_to_break_measure_macro() {
        let (tx, _) = bounded(1024);
        measure!(tx, one, t(x,"y"), i(n,1));
        measure!(tx, one, t(x,"y"), i(n,1),);

        struct A {
            pub one: i32,
            pub two: i32,
        }

        struct B {
            pub a: A
        }

        let b = B { a: A { one: 1, two: 2 } };

        let m = measure!(@make_meas test, t(name, "a"), i(a, b.a.one));

        assert_eq!(m.get_field("a"), Some(&OwnedValue::Integer(1)));
    }

    #[cfg(feature = "unstable")]
    #[bench]
    fn measure_macro_small(b: &mut Bencher) {
        let (tx, rx) = bounded(1024);
        let listener = thread::spawn(move || {
            loop { if rx.recv().is_err() { break } }
        });
        b.iter(|| {
            measure!(tx, test, t(color, "red"), i(n, 1), tm(now()));
        });
    }

    #[cfg(feature = "unstable")]
    #[bench]
    fn measure_macro_medium(b: &mut Bencher) {
        let (tx, rx) = bounded(1024);
        let listener = thread::spawn(move || {
            loop { if rx.recv().is_err() { break } }
        });
        b.iter(|| {
            measure!(tx, test, t(color, "red"), t(mood, "playful"),
                t(ticker, "xmr_btc"), f(price, 1.2345), f(amount, 56.322),
                i(n, 1), tm(now()));
        });
    }

    #[cfg(feature = "unstable")]
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

    #[cfg(feature = "unstable")]
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

    #[cfg(feature = "unstable")]
    #[bench]
    fn clone_url_for_thread(b: &mut Bencher) {
        let host = "ahmes";
        let db = "mlp";
        let url =
            Url::parse_with_params(&format!("http://{}:8086/write", host),
                                   &[("db", db), ("precision", "ns")]).unwrap();
        b.iter(|| {
            url.clone()
        })
    }

    #[cfg(feature = "unstable")]
    #[bench]
    fn clone_arc_url_for_thread(b: &mut Bencher) {
        let host = "ahmes";
        let db = "mlp";
        let url =
            Url::parse_with_params(&format!("http://{}:8086/write", host),
                                   &[("db", db), ("precision", "ns")]).unwrap();
        let url = Arc::new(url);
        b.iter(|| {
            Arc::clone(&url)
        })
    }

    #[test]
    fn it_serializes_a_hard_to_serialize_message_from_owned() {
        let raw = r#"error encountered trying to send krkn order: Other("Failed to send http request: Other("Resource temporarily unavailable (os error 11)")")"#;
        let mut buf = String::new();
        let mut server_resp = String::new();
        let m = OwnedMeasurement::new("rust_test")
            .add_field("s", OwnedValue::String(raw.to_string()))
            .set_timestamp(now());
        serialize_owned(&m, &mut buf);
        println!("{}", buf);
        buf.push_str("\n");
        let buf_copy = buf.clone();
        buf.push_str(&buf_copy);
        println!("{}", buf);

        let url = Url::parse_with_params("http://localhost:8086/write", &[("db", "test"), ("precision", "ns")]).expect("influx writer url should parse");
        let client = Client::new();
        let req = InfluxWriter::http_req(&client, url.clone(), &buf, &None);
        match req.send() {

            Ok(Response { status, .. }) if status == StatusCode::NoContent => {}

            Ok(mut resp) =>  {
                resp.read_to_string(&mut server_resp).unwrap();
                panic!("{}", server_resp);
            }

            Err(why) => {
                panic!(why)
            }
        }
    }

    #[cfg(feature = "auth-tests")]
    #[test]
    fn it_sends_authenticated_measurements() {
        let creds = InfluxWriter::get_credentials("auth_test_user".into(), Some("hot dog".into()));
        let noop_logger = slog::Logger::root(slog::Discard.fuse(), o!());
        //let decorator = slog_term::TermDecorator::new().stdout().force_color().build();
        //let drain = slog_term::FullFormat::new(decorator).use_utc_timestamp().build().fuse();
        //let drain = slog_async::Async::new(drain).chan_size(1024 * 64).thread_name("recv".into()).build().fuse();
        //let root = slog::Logger::root(drain, o!("version" => "0.1"));
        //let influx = InfluxWriter::with_logger_and_opt_creds("localhost", "auth_test", Some(creds), &root);
        measure!(influx, auth_test_meas, i(n, 1));
        drop(influx);
    }
}
