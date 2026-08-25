//! HTTP session restoration, extraction, and CSRF middleware.

use std::{
    convert::Infallible,
    fmt,
    task::{Context, Poll},
};

use futures_util::future::BoxFuture;
use http::{HeaderMap, StatusCode, header::COOKIE};
use rustee_auth::Principal;
use rustee_core::{
    BoxCloneServiceExt, Error, FromRequest, IntoResponse, Request, Response, RouteParams,
    StateStore,
};
use tower::{Layer, Service, util::BoxCloneService};

use crate::model::{SessionCookieConfig, SessionId, SessionStore};

mod csrf;

pub use csrf::{CsrfLayer, CsrfService};

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

    pub(super) fn csrf_token(&self) -> &str {
        &self.csrf_token
    }
}

/// Extracts a principal and opaque session ID restored by [`SessionLayer`].
#[derive(Clone, Eq, PartialEq)]
pub struct SessionUser {
    principal: Principal,
    id: SessionId,
}

impl fmt::Debug for SessionUser {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionUser")
            .field("principal", &"[REDACTED]")
            .field("id", &"[REDACTED]")
            .finish()
    }
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
#[derive(Clone)]
pub struct SessionLayer<S> {
    store: S,
    cookie: SessionCookieConfig,
}

impl<S> fmt::Debug for SessionLayer<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionLayer")
            .field("store_type", &std::any::type_name::<S>())
            .field("cookie", &self.cookie)
            .finish_non_exhaustive()
    }
}

impl<S> SessionLayer<S> {
    /// Creates session-restoration middleware.
    #[must_use]
    pub fn new(store: S, cookie: SessionCookieConfig) -> Self {
        Self { store, cookie }
    }
}

/// Service produced by [`SessionLayer`] that restores an authenticated session per request.
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
            cookie_name: self.cookie.name().to_owned(),
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
        let inner = self.inner.clone();
        Box::pin(async move {
            let Some(id) = session_id(request.headers(), &cookie_name) else {
                return inner.call_ready(request).await;
            };
            let Ok(session) = store.load(id).await else {
                return Ok(session_store_response());
            };
            let Some(session) =
                session.filter(|session| session.id() == id && !session.is_expired())
            else {
                return inner.call_ready(request).await;
            };
            let (principal, csrf_token) = session.into_authenticated_context();
            request.extensions_mut().insert(principal);
            request
                .extensions_mut()
                .insert(SessionContext { id, csrf_token });
            inner.call_ready(request).await
        })
    }
}

fn session_id(headers: &HeaderMap, name: &str) -> Option<SessionId> {
    let mut cookie_headers = headers.get_all(COOKIE).iter();
    let cookie = cookie_headers.next()?.to_str().ok()?;
    if cookie_headers.next().is_some() {
        return None;
    }
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

fn session_store_response() -> Response {
    Error::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "session_unavailable",
        "session service is unavailable",
    )
    .into_response()
}

#[cfg(test)]
mod tests {
    use http::{HeaderMap, HeaderValue, header::COOKIE};
    use rustee_auth::Principal;

    use super::{SessionId, SessionUser, session_id};

    #[test]
    fn duplicate_session_cookies_are_rejected() {
        let id = SessionId::new();
        let mut headers = HeaderMap::new();
        headers.insert(
            COOKIE,
            HeaderValue::from_str(&format!("session={id}; session={id}")).unwrap(),
        );

        assert!(session_id(&headers, "session").is_none());
    }

    #[test]
    fn duplicate_cookie_headers_are_rejected() {
        let id = SessionId::new();
        let mut cookies = HeaderMap::new();
        cookies.insert(
            COOKIE,
            HeaderValue::from_str(&format!("session={id}")).unwrap(),
        );
        cookies.append(COOKIE, HeaderValue::from_static("other=value"));

        assert!(session_id(&cookies, "session").is_none());
    }

    #[test]
    fn session_user_debug_redacts_the_principal_and_session_id() {
        let user = SessionUser {
            principal: Principal::new("alice").unwrap(),
            id: SessionId::new(),
        };
        let id = user.id().to_string();

        let output = format!("{user:?}");

        assert!(!output.contains("alice"));
        assert!(!output.contains(&id));
    }
}
