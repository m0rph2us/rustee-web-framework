//! Deterministic stdio MCP fixture used by the platform integration test.

use std::io::{self, BufRead, BufWriter, Write};

const PROTOCOL_VERSION: &str = "2025-11-25";

fn main() {
    let stdin = io::stdin();
    let mut stdout = BufWriter::new(io::stdout().lock());
    for (index, line) in stdin.lock().lines().enumerate() {
        if line.is_err() {
            return;
        }
        let reply = match index {
            0 => Some(format!(
                "{{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{{\"protocolVersion\":\"{PROTOCOL_VERSION}\",\"capabilities\":{{}},\"serverInfo\":{{\"name\":\"platform-fixture\",\"version\":\"0.1.0\"}}}}}}"
            )),
            1 => None,
            2 => Some(
                "{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"tools\":[{\"name\":\"orders.platform\",\"inputSchema\":{\"type\":\"object\"}}]}}".to_owned(),
            ),
            3 => Some(
                "{\"jsonrpc\":\"2.0\",\"id\":3,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"platform fixture result\"}]}}".to_owned(),
            ),
            _ => return,
        };
        if let Some(reply) = reply
            && (writeln!(stdout, "{reply}").is_err() || stdout.flush().is_err())
        {
            return;
        }
    }
}
