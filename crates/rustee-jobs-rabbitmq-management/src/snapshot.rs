use std::{collections::BTreeMap, fmt};

use serde::Deserialize;
use serde_json::Value;

/// Minimal management queue response used for topology auditing.
#[derive(Clone, Deserialize)]
pub struct QueueSnapshot {
    #[serde(rename = "type")]
    pub(crate) queue_type: String,
    pub(crate) durable: bool,
    pub(crate) auto_delete: bool,
    #[serde(default)]
    arguments: BTreeMap<String, Value>,
    #[serde(default)]
    effective_policy_definition: BTreeMap<String, Value>,
}

impl fmt::Debug for QueueSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueueSnapshot")
            .field("queue_type", &"[REDACTED]")
            .field("durable", &self.durable)
            .field("auto_delete", &self.auto_delete)
            .field("argument_count", &self.arguments.len())
            .field(
                "effective_policy_value_count",
                &self.effective_policy_definition.len(),
            )
            .finish()
    }
}

impl QueueSnapshot {
    pub(crate) fn effective_values(&self) -> EffectiveValues<'_> {
        EffectiveValues {
            policy: &self.effective_policy_definition,
            arguments: &self.arguments,
        }
    }
}

pub(crate) struct EffectiveValues<'a> {
    policy: &'a BTreeMap<String, Value>,
    arguments: &'a BTreeMap<String, Value>,
}

impl EffectiveValues<'_> {
    pub(crate) fn text(&self, key: &str) -> Option<&str> {
        self.value(key)?.as_str()
    }

    pub(crate) fn integer(&self, key: &str) -> Option<i64> {
        self.value(key)?.as_i64()
    }

    fn value(&self, key: &str) -> Option<&Value> {
        self.arguments
            .get(&format!("x-{key}"))
            .or_else(|| self.policy.get(key))
    }
}
