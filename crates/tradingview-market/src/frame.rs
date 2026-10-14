//! `~m~N~m~<payload>` frame encoding / decoding.
//!
//! TradingView prefixes each logical message with its byte length in a fixed
//! text header. A single websocket text message can carry several concatenated
//! frames, so the client splits them before decoding the JSON payload. A
//! heartbeat (`~h~<counter>`) is echoed back verbatim inside a new frame.

const HEADER: &str = "~m~";

/// Encode one payload as a framed message.
pub fn encode(payload: &str) -> String {
    // The length is UTF-8 bytes, not chars.
    format!("{HEADER}{}{HEADER}{}", payload.len(), payload)
}

/// Split a wire message into its framed payloads.
///
/// Drops a truncated trailing frame; an invalid length ends parsing.
pub fn split(raw: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut cursor = 0;
    while cursor < raw.len() {
        let Some(start) = raw[cursor..].find(HEADER) else {
            break;
        };
        let after_first = cursor + start + HEADER.len();
        let Some(len_end) = raw[after_first..].find(HEADER) else {
            break;
        };
        let Ok(len) = raw[after_first..after_first + len_end].parse::<usize>() else {
            break;
        };
        let payload_start = after_first + len_end + HEADER.len();
        let payload_end = payload_start + len;
        if payload_end > raw.len() {
            break;
        }
        out.push(&raw[payload_start..payload_end]);
        cursor = payload_end;
    }
    out
}

/// Is this a heartbeat (`~h~<counter>`)?
pub fn is_heartbeat(payload: &str) -> bool {
    payload.starts_with("~h~")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_counts_bytes_not_chars() {
        assert_eq!(encode("hi"), "~m~2~m~hi");
        assert_eq!(encode(""), "~m~0~m~");
        // "é" is 2 bytes in UTF-8 but 1 char.
        assert_eq!(encode("é"), "~m~2~m~é");
    }

    #[test]
    fn split_single_and_concatenated_frames() {
        assert_eq!(split("~m~2~m~hi"), vec!["hi"]);
        assert_eq!(split("~m~2~m~hi~m~5~m~world"), vec!["hi", "world"]);
    }

    #[test]
    fn split_drops_a_truncated_trailing_frame() {
        assert_eq!(split("~m~2~m~ok~m~10~m~abc"), vec!["ok"]);
        assert!(split("").is_empty());
    }

    #[test]
    fn heartbeats_are_recognised_and_echoed_through_encode() {
        assert_eq!(split("~m~4~m~~h~1"), vec!["~h~1"]);
        assert!(is_heartbeat("~h~42"));
        assert!(!is_heartbeat("{\"m\":\"x\"}"));
        assert_eq!(encode("~h~42"), "~m~5~m~~h~42");
    }

    #[test]
    fn a_payload_containing_tildes_does_not_confuse_the_split() {
        let json = r#"{"m":"quote_add_symbols","p":["test~m~fake"]}"#;
        assert_eq!(split(&encode(json)), vec![json]);
    }
}
