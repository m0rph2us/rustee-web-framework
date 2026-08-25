//! Canonical Prometheus text-exposition formatting helpers.

/// Prometheus text exposition media type for version 0.0.4.
pub const PROMETHEUS_TEXT_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Appends one complete Prometheus text-exposition line.
pub fn append_line(output: &mut String, line: &str) {
    output.push_str(line);
    output.push('\n');
}

/// Escapes a value for a quoted Prometheus label.
#[must_use]
pub fn escape_label_value(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            _ => escaped.push(character),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::{PROMETHEUS_TEXT_CONTENT_TYPE, append_line, escape_label_value};

    #[test]
    fn text_helpers_preserve_line_boundaries_and_escape_label_metacharacters() {
        let mut output = String::new();
        append_line(&mut output, "first");
        append_line(&mut output, "second");

        assert_eq!(output, "first\nsecond\n");
        assert_eq!(
            escape_label_value("quote=\" slash=\\ newline=\n"),
            "quote=\\\" slash=\\\\ newline=\\n"
        );
        assert_eq!(
            PROMETHEUS_TEXT_CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8"
        );
    }
}
