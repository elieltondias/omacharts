use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("offline: {0}")]
    Offline(String),
    #[error("tradingview unreachable: {0}")]
    Unreachable(String),
    #[error("websocket: {0}")]
    WebSocket(#[from] tungstenite::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("tradingview has no such symbol: {0}")]
    Symbol(String),
    #[error("tradingview series error: {0}")]
    Series(String),
    #[error("tradingview protocol error: {0}")]
    Protocol(String),
    #[error("rate limited by tradingview")]
    RateLimited,
    #[error("timed out: {0}")]
    Timeout(String),
    #[error("connection closed")]
    Closed,
    /// The socket died while a request was in flight. `sent` means dispatch
    /// was attempted — the request *may* have been sent.
    #[error("connection lost while waiting for a reply (request sent: {sent})")]
    ConnectionLost { sent: bool },
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;
