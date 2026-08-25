//! CSRF admission for restored server-side session requests.

use std::{
    convert::Infallible,
    fmt,
    task::{Context, Poll},
};

use futures_util::future::BoxFuture;
use http::{HeaderMap, Method, StatusCode};
use rustee_auth::constant_time_eq;
use rustee_core::{BoxCloneServiceExt, Error, IntoResponse, Request, Response};
use tower::{Layer, Service, util::BoxCloneService};

use super::SessionContext;

/// Middleware that requires a matching CSRF header for unsafe session-authenticated methods.
///
/// [`super::SessionLayer`] must wrap this layer so it can restore [`SessionContext`] before this
/// service evaluates an unsafe request. Requests without session authentication pass through
/// unchanged.
#[derive(Clone, Debug, Default)]
pub struct CsrfLayer;

/// Service produced by [`CsrfLayer`] that validates CSRF tokens for unsafe methods.
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
        let inner = self.inner.clone();
        Box::pin(async move {
            if is_safe_method(request.method()) {
                return inner.call_ready(request).await;
            }
            let Some(session) = request.extensions().get::<SessionContext>() else {
                return inner.call_ready(request).await;
            };
            let valid = csrf_header_value(request.headers()).is_some_and(|token| {
                constant_time_eq(token.as_bytes(), session.csrf_token().as_bytes())
            });
            if !valid {
                return Ok(Error::new(
                    StatusCode::FORBIDDEN,
                    "csrf_rejected",
                    "CSRF validation failed",
                )
                .into_response());
            }
            inner.call_ready(request).await
        })
    }
}

fn csrf_header_value(headers: &HeaderMap) -> Option<&str> {
    let mut values = headers.get_all("x-csrf-token").iter();
    let value = values.next()?.to_str().ok()?;
    values.next().is_none().then_some(value)
}

fn is_safe_method(method: &Method) -> bool {
    matches!(
        *method,
        Method::GET | Method::HEAD | Method::OPTIONS | Method::TRACE
    )
}

#[cfg(test)]
mod tests {
    use http::{HeaderMap, HeaderValue};

    use super::csrf_header_value;

    #[test]
    fn duplicate_csrf_headers_are_rejected() {
        let mut headers = HeaderMap::new();
        headers.insert("x-csrf-token", HeaderValue::from_static("first"));
        headers.append("x-csrf-token", HeaderValue::from_static("second"));

        assert!(csrf_header_value(&headers).is_none());
    }
}
