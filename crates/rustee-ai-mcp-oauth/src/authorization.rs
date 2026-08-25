//! Authorization-code flow facade.

mod flow;
mod protocol;
mod refresh_gate;
#[cfg(test)]
mod tests;
mod transaction;

pub use flow::McpOAuthAuthorizationFlow;
pub use protocol::{
    McpOAuthAuthorizationCallback, McpOAuthAuthorizationRedirect, McpOAuthTokenExchangeRequest,
};
pub use transaction::{
    InMemoryMcpOAuthTransactionStore, McpOAuthPendingAuthorization, McpOAuthTransactionStore,
    McpOAuthValueGenerator, UuidMcpOAuthValueGenerator,
};

#[cfg(test)]
pub(super) use protocol::MAX_AUTHORIZATION_REDIRECT_BYTES;
pub(super) use protocol::{MAX_AUTHORIZATION_CODE_BYTES, MAX_PROVIDER_ERROR_BYTES, pkce_challenge};
#[cfg(test)]
pub(super) use transaction::MAX_IN_MEMORY_TRANSACTIONS;
pub(super) use transaction::unix_seconds;
