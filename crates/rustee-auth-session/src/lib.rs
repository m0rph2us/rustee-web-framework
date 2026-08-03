//! Server-side browser sessions and CSRF protection for Rustee.
//!
//! Cookies contain only a random opaque session identifier. Identity and CSRF state remain in a
//! [`SessionStore`], which production applications replace with a durable provider adapter.

use std::{
    collections::BTreeMap,
    convert::Infallible,
    fmt,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{SystemTime, UNIX_EPOCH},
};

use futures_util::future::BoxFuture;
use http::{
    HeaderValue, Method, StatusCode,
    header::{COOKIE, SET_COOKIE},
};
use rustee_auth::Principal;
use rustee_core::{Error, FromRequest, IntoResponse, Request, Response, RouteParams, StateStore};
use tower::{Layer, Service, util::BoxCloneService};
use uuid::Uuid;

/// Opaque, randomly generated server-side session identifier.
#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct SessionId(Uuid);

impl SessionId {
    /// Generates a random version-4 session identifier.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    fn parse(value: &str) -> Option<Self> {
        Uuid::parse_str(value).ok().map(Self)
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A server-side session record with an expiry and a CSRF token.
#[derive(Clone, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Session {
    id: SessionId,
    principal: Principal,
    csrf_token: String,
    expires_at_unix_seconds: u64,
}

impl Session {
    fn new(principal: Principal, ttl_seconds: u64) -> Self {
        Self {
            id: SessionId::new(),
            principal,
            csrf_token: Uuid::new_v4().to_string(),
            expires_at_unix_seconds: unix_seconds().saturating_add(ttl_seconds),
        }
    }

    /// Returns the authenticated principal held by this session.
    #[must_use]
    pub fn principal(&self) -> &Principal {
        &self.principal
    }

    /// Returns the opaque identifier persisted as the cookie value.
    #[must_use]
    pub const fn id(&self) -> SessionId {
        self.id
    }

    /// Returns whether the session is expired at the current system time.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.expires_at_unix_seconds <= unix_seconds()
    }

    /// Returns the remaining persistence TTL, or `None` when the session is expired.
    #[must_use]
    pub fn remaining_ttl_seconds(&self) -> Option<u64> {
        self.expires_at_unix_seconds
            .checked_sub(unix_seconds())
            .filter(|ttl| *ttl > 0)
    }
}

impl fmt::Debug for Session {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Session")
            .field("id", &"[REDACTED]")
            .field("principal", &"[REDACTED]")
            .field("csrf_token", &"[REDACTED]")
            .field("expires_at_unix_seconds", &self.expires_at_unix_seconds)
            .finish()
    }
}

/// Persistence contract for opaque server-side sessions.
pub trait SessionStore: Clone + Send + Sync + 'static {
    /// Store-specific failure type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Persists or replaces one session record.
    fn save(&self, session: Session) -> BoxFuture<'static, Result<(), Self::Error>>;
    /// Loads one session record by its opaque identifier.
    fn load(&self, id: SessionId) -> BoxFuture<'static, Result<Option<Session>, Self::Error>>;
    /// Deletes one server-side session record.
    fn delete(&self, id: SessionId) -> BoxFuture<'static, Result<(), Self::Error>>;
}

/// Cookie `SameSite` policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SameSite {
    /// Prevent cross-site cookie delivery whenever the browser supports it.
    Strict,
    /// Allow top-level safe navigation while protecting ordinary cross-site requests.
    Lax,
    /// Allow cross-site delivery; requires `Secure` in modern browsers.
    None,
}

impl SameSite {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "Strict",
            Self::Lax => "Lax",
            Self::None => "None",
        }
    }
}

/// Browser-cookie configuration for server-side sessions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionCookieConfig {
    name: String,
    ttl_seconds: u64,
    secure: bool,
    same_site: SameSite,
}

impl SessionCookieConfig {
    /// Creates a secure, HTTP-only, `SameSite=Lax` cookie configuration.
    ///
    /// # Errors
    ///
    /// Returns [`CookieConfigError::InvalidName`] when `name` is not a valid cookie token.
    pub fn new(name: impl Into<String>, ttl_seconds: u64) -> Result<Self, CookieConfigError> {
        let name = name.into();
        if !valid_cookie_name(&name) {
            return Err(CookieConfigError::InvalidName);
        }
        if ttl_seconds == 0 {
            return Err(CookieConfigError::ZeroTtl);
        }
        Ok(Self {
            name,
            ttl_seconds,
            secure: true,
            same_site: SameSite::Lax,
        })
    }

    /// Changes the `SameSite` policy.
    ///
    /// # Errors
    ///
    /// Returns [`CookieConfigError::SameSiteNoneRequiresSecure`] when a cross-site cookie would
    /// be sent without secure transport.
    pub fn with_same_site(mut self, same_site: SameSite) -> Result<Self, CookieConfigError> {
        if matches!(same_site, SameSite::None) && !self.secure {
            return Err(CookieConfigError::SameSiteNoneRequiresSecure);
        }
        self.same_site = same_site;
        Ok(self)
    }

    /// Allows an explicitly insecure cookie for local development only.
    ///
    /// # Errors
    ///
    /// Returns [`CookieConfigError::SameSiteNoneRequiresSecure`] when the current policy requires
    /// a secure cookie.
    pub fn with_secure(mut self, secure: bool) -> Result<Self, CookieConfigError> {
        if !secure && matches!(self.same_site, SameSite::None) {
            return Err(CookieConfigError::SameSiteNoneRequiresSecure);
        }
        self.secure = secure;
        Ok(self)
    }

    fn set_cookie(&self, id: SessionId) -> HeaderValue {
        HeaderValue::from_str(&format!(
            "{}={id}; Path=/; Max-Age={}; HttpOnly{}; SameSite={}",
            self.name,
            self.ttl_seconds,
            if self.secure { "; Secure" } else { "" },
            self.same_site.as_str(),
        ))
        .expect("validated cookie configuration produces a valid header")
    }

    fn clear_cookie(&self) -> HeaderValue {
        HeaderValue::from_str(&format!(
            "{}=; Path=/; Max-Age=0; HttpOnly{}; SameSite={}",
            self.name,
            if self.secure { "; Secure" } else { "" },
            self.same_site.as_str(),
        ))
        .expect("validated cookie configuration produces a valid header")
    }
}

/// Invalid browser session cookie configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CookieConfigError {
    /// The cookie name cannot be represented safely in a Cookie header.
    #[error("session cookie name must be a non-empty HTTP token")]
    InvalidName,
    /// A zero TTL would immediately invalidate every issued session.
    #[error("session cookie TTL must be non-zero")]
    ZeroTtl,
    /// Modern browsers require a secure transport for cross-site cookies.
    #[error("SameSite=None session cookies require Secure")]
    SameSiteNoneRequiresSecure,
}

/// Result of establishing or rotating a server-side session.
#[derive(Clone)]
pub struct IssuedSession {
    csrf_token: String,
    set_cookie: HeaderValue,
}

impl fmt::Debug for IssuedSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IssuedSession")
            .field("csrf_token", &"[REDACTED]")
            .field("set_cookie", &"[REDACTED]")
            .finish()
    }
}

impl IssuedSession {
    /// Returns the CSRF token to render into same-origin forms or client code.
    #[must_use]
    pub fn csrf_token(&self) -> &str {
        &self.csrf_token
    }

    /// Adds the secure session cookie to an HTTP response.
    pub fn apply_to(&self, response: &mut Response) {
        response
            .headers_mut()
            .append(SET_COOKIE, self.set_cookie.clone());
    }
}

/// Creates, rotates, and invalidates sessions without exposing store details to request handlers.
#[derive(Clone, Debug)]
pub struct SessionManager<S> {
    store: S,
    cookie: SessionCookieConfig,
}

impl<S> SessionManager<S>
where
    S: SessionStore,
{
    /// Creates a session manager from one durable session store and cookie policy.
    #[must_use]
    pub fn new(store: S, cookie: SessionCookieConfig) -> Self {
        Self { store, cookie }
    }

    /// Establishes a new session, suitable after login or privilege elevation.
    ///
    /// # Errors
    ///
    /// Returns the underlying store failure when the new session cannot be persisted.
    pub async fn establish(&self, principal: Principal) -> Result<IssuedSession, S::Error> {
        let session = Session::new(principal, self.cookie.ttl_seconds);
        self.store.save(session.clone()).await?;
        Ok(IssuedSession {
            csrf_token: session.csrf_token,
            set_cookie: self.cookie.set_cookie(session.id),
        })
    }

    /// Rotates a session by deleting the old ID before issuing a new one.
    ///
    /// # Errors
    ///
    /// Returns the underlying store failure when deletion or new-session persistence fails.
    pub async fn rotate(
        &self,
        previous: SessionId,
        principal: Principal,
    ) -> Result<IssuedSession, S::Error> {
        self.store.delete(previous).await?;
        self.establish(principal).await
    }

    /// Invalidates a server-side session and expires the browser cookie.
    ///
    /// # Errors
    ///
    /// Returns the underlying store failure when the session cannot be deleted.
    pub async fn invalidate(&self, id: SessionId) -> Result<HeaderValue, S::Error> {
        self.store.delete(id).await?;
        Ok(self.cookie.clear_cookie())
    }
}

/// Request extension inserted only after a valid server-side session lookup.
#[derive(Clone)]
pub struct SessionContext {
    id: SessionId,
    csrf_token: String,
}

impl fmt::Debug for SessionContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionContext")
            .field("id", &"[REDACTED]")
            .field("csrf_token", &"[REDACTED]")
            .finish()
    }
}

impl SessionContext {
    /// Returns the opaque session identifier for logout or rotation.
    #[must_use]
    pub const fn id(&self) -> SessionId {
        self.id
    }
}

/// Extracts a principal and opaque session ID restored by [`SessionLayer`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionUser {
    principal: Principal,
    id: SessionId,
}

impl SessionUser {
    /// Returns the authenticated principal.
    #[must_use]
    pub fn principal(&self) -> &Principal {
        &self.principal
    }

    /// Returns the validated opaque ID for session rotation or invalidation.
    #[must_use]
    pub const fn id(&self) -> SessionId {
        self.id
    }
}

impl FromRequest for SessionUser {
    fn from_request<'a>(
        request: &'a mut Request,
        _params: &'a RouteParams,
        _state: &'a StateStore,
    ) -> BoxFuture<'a, rustee_core::Result<Self>> {
        Box::pin(async move {
            let principal = request.extensions().get::<Principal>().cloned();
            let context = request.extensions().get::<SessionContext>().cloned();
            match (principal, context) {
                (Some(principal), Some(context)) => Ok(Self {
                    principal,
                    id: context.id,
                }),
                _ => Err(Error::new(
                    StatusCode::UNAUTHORIZED,
                    "unauthenticated",
                    "authentication is required",
                )),
            }
        })
    }
}

/// Middleware that restores a valid server-side session into the request extensions.
#[derive(Clone, Debug)]
pub struct SessionLayer<S> {
    store: S,
    cookie: SessionCookieConfig,
}

impl<S> SessionLayer<S> {
    /// Creates session-restoration middleware.
    #[must_use]
    pub fn new(store: S, cookie: SessionCookieConfig) -> Self {
        Self { store, cookie }
    }
}

#[derive(Clone)]
pub struct SessionService<S> {
    inner: BoxCloneService<Request, Response, Infallible>,
    store: S,
    cookie_name: String,
}

impl<S> fmt::Debug for SessionService<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionService")
            .field("store", &std::any::type_name::<S>())
            .finish_non_exhaustive()
    }
}

impl<S, T> Layer<T> for SessionLayer<S>
where
    S: SessionStore,
    T: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    T::Future: Send + 'static,
{
    type Service = SessionService<S>;

    fn layer(&self, inner: T) -> Self::Service {
        SessionService {
            inner: BoxCloneService::new(inner),
            store: self.store.clone(),
            cookie_name: self.cookie.name.clone(),
        }
    }
}

impl<S> Service<Request> for SessionService<S>
where
    S: SessionStore,
{
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, mut request: Request) -> Self::Future {
        let store = self.store.clone();
        let cookie_name = self.cookie_name.clone();
        let mut inner = self.inner.clone();
        Box::pin(async move {
            let Some(id) = session_id(request.headers().get(COOKIE), &cookie_name) else {
                return inner.call(request).await;
            };
            let Ok(session) = store.load(id).await else {
                return Ok(session_store_response());
            };
            let Some(session) = session.filter(|session| !session.is_expired()) else {
                return inner.call(request).await;
            };
            request.extensions_mut().insert(session.principal);
            request.extensions_mut().insert(SessionContext {
                id,
                csrf_token: session.csrf_token,
            });
            inner.call(request).await
        })
    }
}

/// Middleware that requires a matching CSRF header for unsafe session-authenticated methods.
#[derive(Clone, Debug, Default)]
pub struct CsrfLayer;

#[derive(Clone)]
pub struct CsrfService {
    inner: BoxCloneService<Request, Response, Infallible>,
}

impl fmt::Debug for CsrfService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CsrfService")
            .finish_non_exhaustive()
    }
}

impl<T> Layer<T> for CsrfLayer
where
    T: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    T::Future: Send + 'static,
{
    type Service = CsrfService;

    fn layer(&self, inner: T) -> Self::Service {
        CsrfService {
            inner: BoxCloneService::new(inner),
        }
    }
}

impl Service<Request> for CsrfService {
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Response, Infallible>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let mut inner = self.inner.clone();
        Box::pin(async move {
            if is_safe_method(request.method()) {
                return inner.call(request).await;
            }
            let Some(session) = request.extensions().get::<SessionContext>() else {
                return Ok(Error::new(
                    StatusCode::UNAUTHORIZED,
                    "unauthenticated",
                    "authentication is required",
                )
                .into_response());
            };
            let valid = request
                .headers()
                .get("x-csrf-token")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|token| constant_time_eq(token, &session.csrf_token));
            if !valid {
                return Ok(Error::new(
                    StatusCode::FORBIDDEN,
                    "csrf_rejected",
                    "CSRF validation failed",
                )
                .into_response());
            }
            inner.call(request).await
        })
    }
}

/// In-memory session store for tests and local development only.
#[derive(Clone, Default)]
pub struct InMemorySessionStore {
    sessions: Arc<Mutex<BTreeMap<SessionId, Session>>>,
}

impl fmt::Debug for InMemorySessionStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InMemorySessionStore")
            .finish_non_exhaustive()
    }
}

impl SessionStore for InMemorySessionStore {
    type Error = Infallible;

    fn save(&self, session: Session) -> BoxFuture<'static, Result<(), Self::Error>> {
        let sessions = Arc::clone(&self.sessions);
        Box::pin(async move {
            sessions.lock().unwrap().insert(session.id, session);
            Ok(())
        })
    }

    fn load(&self, id: SessionId) -> BoxFuture<'static, Result<Option<Session>, Self::Error>> {
        let sessions = Arc::clone(&self.sessions);
        Box::pin(async move { Ok(sessions.lock().unwrap().get(&id).cloned()) })
    }

    fn delete(&self, id: SessionId) -> BoxFuture<'static, Result<(), Self::Error>> {
        let sessions = Arc::clone(&self.sessions);
        Box::pin(async move {
            sessions.lock().unwrap().remove(&id);
            Ok(())
        })
    }
}

fn session_id(cookie: Option<&HeaderValue>, name: &str) -> Option<SessionId> {
    let cookie = cookie?.to_str().ok()?;
    let mut matching_values = cookie
        .split(';')
        .filter_map(|part| part.trim().split_once('='))
        .filter_map(|(key, value)| (key == name).then_some(value));
    let value = matching_values.next()?;
    matching_values
        .next()
        .is_none()
        .then_some(value)
        .and_then(SessionId::parse)
}

fn is_safe_method(method: &Method) -> bool {
    matches!(
        *method,
        Method::GET | Method::HEAD | Method::OPTIONS | Method::TRACE
    )
}

fn constant_time_eq(left: &str, right: &str) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.bytes()
        .zip(right.bytes())
        .fold(0_u8, |diff, (a, b)| diff | (a ^ b))
        == 0
}

fn session_store_response() -> Response {
    Error::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "session_unavailable",
        "session service is unavailable",
    )
    .into_response()
}

fn valid_cookie_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use http::{HeaderValue, Request as HttpRequest, StatusCode};
    use rustee_router::App;
    use tower::{Layer, ServiceExt};

    use super::{
        CsrfLayer, InMemorySessionStore, Principal, SessionCookieConfig, SessionLayer,
        SessionManager, SessionStore, SessionUser,
    };
    use rustee_core::empty_body;

    #[tokio::test]
    async fn session_layer_restores_a_principal_and_csrf_layer_protects_post() {
        let store = InMemorySessionStore::default();
        let cookie = SessionCookieConfig::new("rustee_session", 60)
            .unwrap()
            .with_secure(false)
            .unwrap();
        let manager = SessionManager::new(store.clone(), cookie.clone());
        let issued = manager
            .establish(Principal::new("alice").unwrap())
            .await
            .unwrap();
        let set_cookie = issued.set_cookie.to_str().unwrap();
        let session_cookie = set_cookie.split(';').next().unwrap();
        let service = SessionLayer::new(store, cookie).layer(CsrfLayer.layer(App::new().post(
            "/profile",
            |user: SessionUser| async move {
                assert_eq!(user.principal().subject(), "alice");
                "updated"
            },
        )));

        let rejected = service
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/profile")
                    .header("cookie", session_cookie)
                    .body(empty_body())
                    .unwrap(),
            )
            .await
            .unwrap();
        let accepted = service
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/profile")
                    .header("cookie", session_cookie)
                    .header("x-csrf-token", issued.csrf_token())
                    .body(empty_body())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(rejected.status(), StatusCode::FORBIDDEN);
        assert_eq!(accepted.status(), StatusCode::OK);
    }

    #[test]
    fn cross_site_cookies_cannot_disable_secure_transport() {
        let config = SessionCookieConfig::new("session", 60)
            .unwrap()
            .with_secure(false)
            .unwrap();

        assert_eq!(
            config.with_same_site(super::SameSite::None),
            Err(super::CookieConfigError::SameSiteNoneRequiresSecure)
        );
    }

    #[test]
    fn duplicate_session_cookies_are_rejected() {
        let id = super::SessionId::new();
        let cookie = HeaderValue::from_str(&format!("session={id}; session={id}")).unwrap();

        assert!(super::session_id(Some(&cookie), "session").is_none());
    }

    #[tokio::test]
    async fn session_serialization_and_debug_do_not_expose_credentials() {
        let issued = SessionManager::new(
            InMemorySessionStore::default(),
            SessionCookieConfig::new("rustee_session", 60).unwrap(),
        )
        .establish(Principal::new("alice").unwrap())
        .await
        .unwrap();
        let store = InMemorySessionStore::default();
        let manager = SessionManager::new(
            store.clone(),
            SessionCookieConfig::new("session", 60).unwrap(),
        );
        let second_issued = manager
            .establish(Principal::new("bob").unwrap())
            .await
            .unwrap();
        let second_cookie = second_issued.set_cookie.to_str().unwrap();
        let id = second_cookie
            .split(';')
            .next()
            .and_then(|cookie| cookie.split_once('='))
            .and_then(|(_, value)| super::SessionId::parse(value))
            .unwrap();
        let session = store.load(id).await.unwrap().unwrap();

        let encoded = serde_json::to_string(&session).unwrap();
        let decoded: super::Session = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded.id(), session.id());
        assert!(decoded.remaining_ttl_seconds().is_some());
        assert!(!format!("{session:?}").contains(&session.csrf_token));
        assert!(!format!("{issued:?}").contains(issued.csrf_token()));
    }
}
