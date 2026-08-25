//! Per-token-key refresh coordination without provider I/O under a blocking lock.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex as StdMutex, Weak},
};

use crate::{McpOAuthError, McpOAuthTokenStoreKey};

/// A process-local registry of asynchronous refresh gates keyed by one stored token set.
#[derive(Clone, Default)]
pub(super) struct RefreshGateRegistry {
    gates: Arc<StdMutex<BTreeMap<McpOAuthTokenStoreKey, Weak<tokio::sync::Mutex<()>>>>>,
}

/// A leased gate that removes its registry entry once all local refreshers release it.
pub(super) struct RefreshGateLease {
    registry: RefreshGateRegistry,
    key: McpOAuthTokenStoreKey,
    gate: Arc<tokio::sync::Mutex<()>>,
}

impl RefreshGateRegistry {
    /// Leases the gate for one token-store key.
    pub(super) fn lease(
        &self,
        key: &McpOAuthTokenStoreKey,
    ) -> Result<RefreshGateLease, McpOAuthError> {
        // This mutex only selects a per-key async gate; provider I/O never holds it.
        let mut gates = self
            .gates
            .lock()
            .map_err(|_| McpOAuthError::TokenStoreUnavailable)?;
        let gate = gates.get(key).and_then(Weak::upgrade).unwrap_or_else(|| {
            let gate = Arc::new(tokio::sync::Mutex::new(()));
            gates.insert(key.clone(), Arc::downgrade(&gate));
            gate
        });
        Ok(RefreshGateLease {
            registry: self.clone(),
            key: key.clone(),
            gate,
        })
    }
}

impl RefreshGateLease {
    /// Returns the asynchronous gate selected for this lease.
    pub(super) const fn gate(&self) -> &Arc<tokio::sync::Mutex<()>> {
        &self.gate
    }
}

impl Drop for RefreshGateLease {
    fn drop(&mut self) {
        let Ok(mut gates) = self.registry.gates.lock() else {
            return;
        };
        if Arc::strong_count(&self.gate) == 1
            && gates
                .get(&self.key)
                .is_some_and(|stored| Weak::ptr_eq(stored, &Arc::downgrade(&self.gate)))
        {
            gates.remove(&self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::RefreshGateRegistry;
    use crate::McpOAuthTokenStoreKey;

    #[test]
    fn active_leases_share_one_key_gate_and_remove_it_when_idle() {
        let registry = RefreshGateRegistry::default();
        let key = McpOAuthTokenStoreKey::new("tenant-a:user-a:connection-a")
            .expect("test token-store key must be valid");

        let first = registry.lease(&key).expect("first lease must be available");
        let second = registry
            .lease(&key)
            .expect("second lease must be available");
        assert!(Arc::ptr_eq(first.gate(), second.gate()));
        assert_eq!(registry.gates.lock().unwrap().len(), 1);

        drop(first);
        assert_eq!(registry.gates.lock().unwrap().len(), 1);
        drop(second);
        assert!(registry.gates.lock().unwrap().is_empty());
    }
}
