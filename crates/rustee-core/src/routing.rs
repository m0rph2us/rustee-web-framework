//! Router-produced connection and route metadata.

use std::{fmt, net::SocketAddr};

/// Transport-provided connection metadata for the current request.
///
/// Network adapters insert this extension from the accepted connection, never from request
/// headers. Middleware can use it as the first input to an explicit proxy-trust policy.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ConnectionInfo {
    peer_addr: SocketAddr,
}

impl fmt::Debug for ConnectionInfo {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectionInfo")
            .field("peer_addr", &"[REDACTED]")
            .finish()
    }
}

impl ConnectionInfo {
    /// Creates connection metadata from an adapter-observed peer address.
    #[must_use]
    pub const fn new(peer_addr: SocketAddr) -> Self {
        Self { peer_addr }
    }

    /// Returns the directly connected peer address.
    #[must_use]
    pub const fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }
}

/// The configured template of a route selected by the application router.
///
/// Network adapters must not construct this from a request URI. The router derives it from its
/// configured route table, so observability can use it without recording a user-controlled path.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RouteTemplate(String);

impl RouteTemplate {
    /// Creates route metadata from a router-configured template.
    #[must_use]
    pub fn new(template: impl Into<String>) -> Self {
        Self(template.into())
    }

    /// Returns the configured route template.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A low-cardinality router outcome for response-layer observability.
///
/// The special values are framework-reserved and do not contain a request URI. A matched route
/// holds the router-configured [`RouteTemplate`].
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RouteClassification {
    /// A route selected for the request method and path.
    Matched(RouteTemplate),
    /// An application-defined fallback handled the request.
    Fallback,
    /// The path matched a route but not the request method.
    MethodNotAllowed,
    /// Neither a route nor an application fallback handled the path.
    NotFound,
}

impl RouteClassification {
    /// Builds the classification for a matched configured route.
    #[must_use]
    pub const fn matched(template: RouteTemplate) -> Self {
        Self::Matched(template)
    }

    /// Builds the classification for an application fallback.
    #[must_use]
    pub const fn fallback() -> Self {
        Self::Fallback
    }

    /// Builds the classification for a method mismatch.
    #[must_use]
    pub const fn method_not_allowed() -> Self {
        Self::MethodNotAllowed
    }

    /// Builds the classification for an unmatched path.
    #[must_use]
    pub const fn not_found() -> Self {
        Self::NotFound
    }

    /// Returns the configured template or a framework-reserved outcome label.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Matched(template) => template.as_str(),
            Self::Fallback => "<fallback>",
            Self::MethodNotAllowed => "<method-not-allowed>",
            Self::NotFound => "<not-found>",
        }
    }
}

/// Matched route parameters, kept separate from query and request state.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct RouteParams(Vec<(String, String)>);

impl RouteParams {
    /// Builds parameters from a matched route.
    #[must_use]
    pub const fn new(params: Vec<(String, String)>) -> Self {
        Self(params)
    }

    /// Looks up a named parameter.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0
            .iter()
            .find_map(|(key, value)| (key == name).then_some(value.as_str()))
    }

    /// Returns the matched parameters in route order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }
}

impl std::fmt::Debug for RouteParams {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RouteParams")
            .field("parameter_count", &self.0.len())
            .finish()
    }
}
