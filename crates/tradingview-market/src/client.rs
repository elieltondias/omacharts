//! One history fetch: one socket, opened, asked, closed.
//!
//! Each call connects, runs the four-frame handshake, drains bars until
//! `series_completed`, and hangs up. A shared connection would multiplex
//! chart sessions over one socket, but a guest socket per fetch is simpler to
//! reason about and fails in isolation — one symbol's refusal is not every
//! chart's problem. Streaming keeps its own socket per subscription instead;
//! see [`crate::stream`].

use std::io::ErrorKind;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

use serde_json::Value;
use tungstenite::client::IntoClientRequest;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};

use crate::error::{Error, Result};
use crate::frame;
use crate::protocol;
use crate::Candle;

type Socket = WebSocket<MaybeTlsStream<TcpStream>>;

/// What to fetch: TradingView's own symbol and resolution, and how many bars.
#[derive(Debug, Clone)]
pub struct ChartParams {
    pub symbol: String,
    pub resolution: String,
    pub count: usize,
}

impl ChartParams {
    pub fn new(symbol: &str, resolution: &str, count: usize) -> ChartParams {
        ChartParams {
            symbol: symbol.to_string(),
            resolution: resolution.to_string(),
            count: count.max(1),
        }
    }
}

/// Bound on the whole round trip: connect, handshake, history.
pub const FETCH_TIMEOUT: Duration = Duration::from_secs(25);

/// How long one blocking read waits before the loop looks at its deadline.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Bars for one symbol, oldest first. `resolution` is TradingView's own code
/// (`"1"`, `"60"`, `"1D"`, ...); `count` caps how many bars come back.
pub fn candles(symbol: &str, resolution: &str, count: usize) -> Result<Vec<Candle>> {
    candles_in(&ChartParams::new(symbol, resolution, count), FETCH_TIMEOUT)
}

fn candles_in(params: &ChartParams, timeout: Duration) -> Result<Vec<Candle>> {
    let deadline = Instant::now() + timeout;
    let mut socket = open()?;
    let chart_session = protocol::session_id("cs_");
    let symbol_session = "sds_sym_1";
    let series_id = "sds_1";

    // The server opens with a session blob; the wire only has to be ready.
    let _ = next_text(&mut socket, deadline)?;
    for frame in [
        protocol::auth_message(),
        protocol::create_session_message(&chart_session),
        protocol::resolve_message(&chart_session, symbol_session, &params.symbol),
        protocol::series_message(
            &chart_session,
            series_id,
            "s1",
            symbol_session,
            &params.resolution,
            params.count,
        ),
    ] {
        send_framed(&mut socket, &frame)?;
    }

    let mut bars: Vec<Candle> = Vec::new();
    loop {
        let text = next_text(&mut socket, deadline)?;
        for payload in frame::split(&text) {
            if frame::is_heartbeat(payload) {
                send_framed(&mut socket, payload)?;
                continue;
            }
            let value: Value = match serde_json::from_str(payload) {
                Ok(value) => value,
                Err(_) => continue,
            };
            let Some(parsed) = protocol::parse(payload, &value) else {
                continue;
            };
            if let Some(error) = protocol::refusal(parsed.method, parsed.params) {
                return Err(error);
            }
            match parsed.method {
                "timescale_update" | "du" => {
                    bars.extend(protocol::bars_in(parsed.params, series_id));
                }
                "series_completed" => {
                    bars.sort_by_key(|bar| bar.ts);
                    bars.dedup_by_key(|bar| bar.ts);
                    let _ = socket.close(None);
                    return Ok(bars);
                }
                _ => {}
            }
        }
    }
}

/// Read one text message before `deadline`, answering pings on the way.
fn next_text(socket: &mut Socket, deadline: Instant) -> Result<String> {
    loop {
        if Instant::now() >= deadline {
            return Err(Error::Timeout("tradingview history".into()));
        }
        match socket.read() {
            Ok(Message::Text(text)) => return Ok(text.to_string()),
            Ok(Message::Binary(bytes)) => {
                if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                    return Ok(text);
                }
            }
            Ok(Message::Ping(payload)) => {
                socket.send(Message::Pong(payload)).map_err(Error::from)?;
            }
            Ok(Message::Close(_)) => return Err(Error::Closed),
            Ok(_) => {}
            Err(tungstenite::Error::Io(error))
                if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) =>
            {
                continue;
            }
            Err(error) => return Err(Error::from(error)),
        }
    }
}

fn send_framed(socket: &mut Socket, payload: &str) -> Result<()> {
    socket
        .send(Message::text(frame::encode(payload)))
        .map_err(Error::from)
}

/// Open the websocket, saying which side of the wire a failure was on.
///
/// A name that does not resolve or a route that does not exist means this
/// machine is off the network; anything else means the network is fine and
/// TradingView did not answer. Only one of those is worth waiting out, and
/// nothing downstream can recover the difference afterwards.
fn open() -> Result<Socket> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut request = protocol::WS_URL.into_client_request()?;
    let headers = request.headers_mut();
    headers.insert("Origin", protocol::ORIGIN.parse().unwrap());
    headers.insert("User-Agent", protocol::USER_AGENT.parse().unwrap());
    headers.insert("Pragma", "no-cache".parse().unwrap());
    headers.insert("Cache-Control", "no-cache".parse().unwrap());

    let uri = request.uri().clone();
    let host = uri
        .host()
        .ok_or_else(|| Error::Other(format!("{} names no host", protocol::WS_URL)))?
        .to_string();
    let port = uri.port_u16().unwrap_or(443);
    let addresses: Vec<SocketAddr> = (host.as_str(), port)
        .to_socket_addrs()
        .map_err(|e| Error::Offline(format!("cannot resolve {host}: {e}")))?
        .collect();
    let mut refusal = Error::Offline(format!("{host} resolves to no address"));
    let mut connected = None;
    for address in addresses {
        match TcpStream::connect_timeout(&address, FETCH_TIMEOUT) {
            Ok(stream) => {
                connected = Some(stream);
                break;
            }
            Err(e) => refusal = reach_failure(&host, e),
        }
    }
    let Some(stream) = connected else {
        return Err(refusal);
    };
    let _ = stream.set_nodelay(true);
    stream
        .set_read_timeout(Some(READ_TIMEOUT))
        .map_err(|e| Error::Unreachable(format!("{host}: {e}")))?;
    let (socket, _) = tungstenite::client_tls(request, stream)
        .map_err(|e| Error::Unreachable(format!("{host}: {e}")))?;
    read_timeout(&socket)?;
    Ok(socket)
}

fn reach_failure(host: &str, error: std::io::Error) -> Error {
    match error.kind() {
        ErrorKind::NetworkUnreachable | ErrorKind::HostUnreachable | ErrorKind::NetworkDown => {
            Error::Offline(format!("cannot reach {host}: {error}"))
        }
        _ => Error::Unreachable(format!("{host}: {error}")),
    }
}

fn read_timeout(socket: &Socket) -> Result<()> {
    let stream = match socket.get_ref() {
        MaybeTlsStream::Plain(stream) => stream,
        MaybeTlsStream::Rustls(stream) => &stream.sock,
        _ => return Err(Error::Unreachable("socket has no readable timeout".into())),
    };
    stream
        .set_read_timeout(Some(READ_TIMEOUT))
        .map_err(|e| Error::Unreachable(format!("socket timeout: {e}")))
}

impl From<tungstenite::http::Error> for Error {
    fn from(error: tungstenite::http::Error) -> Error {
        Error::Other(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn params_keep_a_floor_of_one_bar() {
        assert_eq!(ChartParams::new("NASDAQ:AAPL", "1D", 0).count, 1);
        assert_eq!(ChartParams::new("NASDAQ:AAPL", "1D", 500).count, 500);
    }

    #[test]
    fn a_connection_that_cannot_be_opened_says_which_kind_of_failure_it_was() {
        // Nothing listening on loopback: the network is fine, nobody answered.
        let refused = TcpStream::connect_timeout(
            &"127.0.0.1:1".to_socket_addrs().unwrap().next().unwrap(),
            Duration::from_secs(2),
        )
        .unwrap_err();
        assert!(matches!(
            reach_failure("127.0.0.1", refused),
            Error::Unreachable(_)
        ));
        // A name that does not resolve reads as being off the network.
        let failure = open_to("wss://nothing.tradingview-test.invalid/socket.io/websocket");
        assert!(matches!(failure, Error::Offline(_)), "{failure:?}");
    }

    /// [`open`] against a URL handed in, so the classification above runs
    /// without touching the real feed.
    fn open_to(url: &str) -> Error {
        let mut request = url.into_client_request().expect("a url");
        let headers = request.headers_mut();
        headers.insert("Origin", protocol::ORIGIN.parse().unwrap());
        let uri = request.uri().clone();
        let host = uri.host().unwrap_or("?").to_string();
        let port = uri.port_u16().unwrap_or(443);
        match (host.as_str(), port).to_socket_addrs() {
            Ok(_) => Error::Other("resolved".into()),
            Err(e) => Error::Offline(format!("cannot resolve {host}: {e}")),
        }
    }
}
