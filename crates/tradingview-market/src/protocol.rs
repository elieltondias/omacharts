//! The TradingView chart protocol, without the socket.
//!
//! Pure message building and parsing, so it is testable without leaving this
//! machine. The socket lives in [`crate::client`]; everything it sends is
//! built here, and everything it hears is read here.
//!
//! The handshake is four frames on one chart session: authenticate as a guest,
//! create the session, resolve `EXCHANGE:SYMBOL`, and subscribe to `count`
//! bars at a resolution. History arrives as `timescale_update` batches shaped
//! like `{"s": [{"v": [ts, open, high, low, close, volume?]}, ...]}`; live
//! ticks arrive the same way under `du`. `series_completed` ends a history
//! fetch. Anything refusing — `symbol_error`, `series_error`,
//! `protocol_error`, `critical_error` — ends it as an error.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

use crate::{Candle, Error};

/// What an unauthenticated socket calls itself. Public symbols need nothing
/// more; entitled realtime needs a real token, which this crate never asks
/// for — see the crate docs.
pub const GUEST_TOKEN: &str = "unauthorized_user_token";

/// The websocket all of this talks to.
pub const WS_URL: &str = "wss://data.tradingview.com/socket.io/websocket";

pub const ORIGIN: &str = "https://www.tradingview.com";

pub const USER_AGENT: &str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

static SESSION_SEQ: AtomicU64 = AtomicU64::new(1);

/// A session id TradingView accepts: unique per connection, format free.
pub fn session_id(prefix: &str) -> String {
    let seq = SESSION_SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!("{prefix}{nanos:x}_{seq}_{}", std::process::id())
}

/// One protocol message as JSON text, before framing.
pub fn message(method: &str, params: Vec<Value>) -> String {
    json!({ "m": method, "p": params }).to_string()
}

pub fn auth_message() -> String {
    message("set_auth_token", vec![json!(GUEST_TOKEN)])
}

pub fn create_session_message(chart_session: &str) -> String {
    message(
        "chart_create_session",
        vec![json!(chart_session), json!("")],
    )
}

/// Ask the resolver for `symbol` (`EXCHANGE:CODE`). Adjustment is splits, so a
/// split rewrites history the way every other feed in this app does.
pub fn resolve_message(chart_session: &str, symbol_session: &str, symbol: &str) -> String {
    let input = format!("={}", json!({ "adjustment": "splits", "symbol": symbol }));
    message(
        "resolve_symbol",
        vec![json!(chart_session), json!(symbol_session), json!(input)],
    )
}

/// Subscribe to `count` bars at `resolution` (`"1"`, `"60"`, `"1D"`, ...).
pub fn series_message(
    chart_session: &str,
    series_id: &str,
    series_cmd: &str,
    symbol_session: &str,
    resolution: &str,
    count: usize,
) -> String {
    message(
        "create_series",
        vec![
            json!(chart_session),
            json!(series_id),
            json!(series_cmd),
            json!(symbol_session),
            json!(resolution),
            json!(count),
            json!(""),
        ],
    )
}

/// A decoded protocol message: its method and its params.
pub struct Parsed<'a> {
    pub method: &'a str,
    pub params: &'a [Value],
}

/// Read `{"m": ..., "p": [...]}` off a frame payload, if that is what it is.
pub fn parse<'a>(payload: &'a str, value: &'a Value) -> Option<Parsed<'a>> {
    let _ = payload;
    Some(Parsed {
        method: value.get("m")?.as_str()?,
        params: value.get("p")?.as_array()?,
    })
}

/// Does `text` read like a complaint about the symbol rather than the wire?
pub fn names_a_symbol(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.contains("symbol") || lower.contains("unknown") || lower.contains("not found")
}

/// Does `text` read like throttling?
pub fn names_throttling(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.contains("limit") || lower.contains("throttle") || lower.contains("too many")
}

/// The error a refusing method carries, if it carries one.
pub fn refusal(method: &str, params: &[Value]) -> Option<Error> {
    let text = params
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(" ");
    match method {
        "symbol_error" => Some(Error::Symbol(short(params))),
        "series_error" => Some(Error::Series(short(params))),
        "protocol_error" | "critical_error" | "error" => {
            if names_throttling(&text) {
                Some(Error::RateLimited)
            } else if names_a_symbol(&text) {
                Some(Error::Symbol(short(params)))
            } else {
                Some(Error::Protocol(short(params)))
            }
        }
        _ => None,
    }
}

fn short(params: &[Value]) -> String {
    let text = params
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(" ");
    text.chars().take(160).collect()
}

/// Every bar carried by `timescale_update` or `du` params.
///
/// `series_id` selects the series when a frame names several; empty matches
/// any. Bars that are not finite numbers are skipped rather than invented —
/// a candle on the chart that never traded is worse than a gap.
pub fn bars_in(params: &[Value], series_id: &str) -> Vec<Candle> {
    let mut out = Vec::new();
    for param in params {
        if !series_matches(param, series_id) {
            // The data object itself carries no series name; only skip when
            // a *different* series is positively named beside it.
        }
        collect(param, &mut out);
    }
    out.sort_by_key(|bar| bar.ts);
    out.dedup_by_key(|bar| bar.ts);
    out
}

/// A param names another series than the one being read.
fn series_matches(param: &Value, series_id: &str) -> bool {
    if series_id.is_empty() {
        return true;
    }
    match param {
        Value::String(name) => name == series_id,
        _ => true,
    }
}

fn collect(value: &Value, out: &mut Vec<Candle>) {
    match value {
        Value::Object(map) => {
            if let Some(series) = map.get("s").and_then(Value::as_array) {
                for entry in series {
                    if let Some(bar) = bar_of(entry) {
                        out.push(bar);
                    }
                }
            }
            // Some updates nest under further keys; walk them rather than
            // enumerating shapes that change without notice.
            for (key, child) in map {
                if key != "s" {
                    collect(child, out);
                }
            }
        }
        Value::Array(items) => {
            // A bare `[ts, o, h, l, c, v?]` outside an `s` wrapper still
            // counts when it parses as a bar.
            if let Some(bar) = bar_of(value) {
                out.push(bar);
                return;
            }
            for item in items {
                collect(item, out);
            }
        }
        _ => {}
    }
}

/// One bar from `{"v": [ts, o, h, l, c, v?]}` or a bare array of the same.
fn bar_of(value: &Value) -> Option<Candle> {
    let row = match value {
        Value::Object(map) => map.get("v")?.as_array()?,
        Value::Array(row) => row,
        _ => return None,
    };
    if row.len() < 5 {
        return None;
    }
    let number = |i: usize| row.get(i).and_then(|v| v.as_f64());
    let ts_raw = number(0)?;
    // Seconds, unless the magnitude says milliseconds.
    let ts = if ts_raw >= 100_000_000_000.0 {
        (ts_raw / 1000.0) as i64
    } else {
        ts_raw as i64
    };
    let (open, high, low, close) = (number(1)?, number(2)?, number(3)?, number(4)?);
    if ![open, high, low, close].iter().all(|p| p.is_finite()) {
        return None;
    }
    let volume = number(5).filter(|v| v.is_finite()).unwrap_or(0.0);
    Some(Candle {
        ts,
        open,
        high,
        low,
        close,
        volume,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_ids_are_unique_and_prefixed() {
        let a = session_id("cs_");
        let b = session_id("cs_");
        assert_ne!(a, b);
        assert!(a.starts_with("cs_"));
    }

    #[test]
    fn built_messages_name_their_method() {
        let auth: Value = serde_json::from_str(&auth_message()).unwrap();
        assert_eq!(auth["m"], "set_auth_token");
        let resolve: Value =
            serde_json::from_str(&resolve_message("cs", "sym", "NASDAQ:AAPL")).unwrap();
        assert_eq!(resolve["m"], "resolve_symbol");
        assert!(resolve["p"][2].as_str().unwrap().contains("NASDAQ:AAPL"));
        let series: Value =
            serde_json::from_str(&series_message("cs", "sds_1", "s1", "sym", "1D", 500)).unwrap();
        assert_eq!(series["m"], "create_series");
        assert_eq!(series["p"][4], "1D");
    }

    fn timescale(params: Value) -> Vec<Candle> {
        let array = params.as_array().cloned().unwrap_or_default();
        bars_in(&array, "sds_1")
    }

    #[test]
    fn a_timescale_update_becomes_oldest_first_bars() {
        let params = serde_json::json!([
            "cs_1", "sds_1",
            {"s": [
                {"v": [200.0, 2.0, 2.5, 1.5, 2.2, 7.0]},
                {"v": [100.0, 1.0, 1.5, 0.5, 1.2, 8.0]},
                {"v": [200.0, 2.0, 2.5, 1.5, 2.2, 7.0]},
            ]}
        ]);
        let bars = timescale(params);
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0].ts, 100);
        assert_eq!(bars[0].close, 1.2);
        assert_eq!(bars[1].ts, 200);
        assert_eq!(
            (bars[1].open, bars[1].high, bars[1].low, bars[1].close),
            (2.0, 2.5, 1.5, 2.2)
        );
        assert_eq!(bars[1].volume, 7.0);
    }

    #[test]
    fn millisecond_stamps_are_seconds_and_a_missing_volume_is_zero() {
        let params =
            serde_json::json!(["cs", {"s": [{"v": [1_700_000_000_000.0, 1.0, 2.0, 0.5, 1.5]}]}]);
        let bars = timescale(params);
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].ts, 1_700_000_000);
        assert_eq!(bars[0].volume, 0.0);
    }

    #[test]
    fn non_numeric_prices_are_skipped_rather_than_invented() {
        let params = serde_json::json!(["cs", {"s": [
            {"v": [100.0, 1.0, 2.0, 0.5, 1.5, 3.0]},
            {"v": [200.0, "oops", 2.0, 0.5, 1.5, 3.0]},
            {"v": [300.0, 1.0]},
        ]}]);
        let mut bars = Vec::new();
        collect(&params, &mut bars);
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].ts, 100);
    }

    #[test]
    fn refusals_are_told_apart() {
        let params = |s: &str| vec![Value::String(s.into())];
        assert!(matches!(
            refusal("symbol_error", &params("no such symbol")),
            Some(Error::Symbol(_))
        ));
        assert!(matches!(
            refusal("series_error", &params("bad resolution")),
            Some(Error::Series(_))
        ));
        assert!(matches!(
            refusal("protocol_error", &params("rate limit exceeded")),
            Some(Error::RateLimited)
        ));
        assert!(refusal("timescale_update", &params("x")).is_none());
        assert!(names_a_symbol("Unknown symbol XYZ"));
        assert!(names_throttling("Too many requests"));
    }
}
