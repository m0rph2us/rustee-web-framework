//! Bounded context-response materialization accounting.

/// Tracks the response budget consumed while context models materialize JSON values.
pub(crate) struct ContextWireBudget {
    remaining: usize,
}

impl ContextWireBudget {
    pub(crate) const fn new(max_bytes: usize) -> Self {
        Self {
            remaining: max_bytes,
        }
    }

    pub(crate) fn reserve_text(&mut self, value: &str) -> Option<()> {
        self.reserve(value.len())
    }

    pub(crate) fn reserve_optional_text(&mut self, value: Option<&str>) -> Option<()> {
        if let Some(value) = value {
            self.reserve_text(value)?;
        }
        Some(())
    }

    pub(crate) fn reserve_base64(&mut self, value: &[u8]) -> Option<()> {
        let encoded_length = value.len().checked_add(2)?.checked_div(3)?.checked_mul(4)?;
        self.reserve(encoded_length)
    }

    fn reserve(&mut self, bytes: usize) -> Option<()> {
        self.remaining = self.remaining.checked_sub(bytes)?;
        Some(())
    }
}
