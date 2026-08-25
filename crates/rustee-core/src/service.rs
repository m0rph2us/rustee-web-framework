//! Tower service helpers shared by Rustee integration boundaries.

use std::future::poll_fn;

use futures_util::future::BoxFuture;
use tower::{Service, util::BoxCloneService};

use crate::{Request, Response};

/// Waits for a cloned, type-erased Rustee HTTP service to become ready, then calls it once.
///
/// `poll_ready` state belongs to an individual service instance, so readiness observed on the
/// original does not make a clone ready to receive a call. Framework layers use this extension
/// after cloning their stored [`BoxCloneService`].
#[doc(hidden)]
pub trait BoxCloneServiceExt {
    /// Waits for this exact service instance to become ready, then calls it once.
    fn call_ready(
        self,
        request: Request,
    ) -> BoxFuture<'static, Result<Response, std::convert::Infallible>>;
}

impl BoxCloneServiceExt for BoxCloneService<Request, Response, std::convert::Infallible> {
    fn call_ready(
        mut self,
        request: Request,
    ) -> BoxFuture<'static, Result<Response, std::convert::Infallible>> {
        Box::pin(async move {
            poll_fn(|context| self.poll_ready(context)).await?;
            self.call(request).await
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        future::{Ready, ready},
        task::{Context, Poll},
    };

    use tower::{Service, util::BoxCloneService};

    use super::BoxCloneServiceExt;
    use crate::{IntoResponse, Request, Response};

    #[derive(Default)]
    struct CloneRequiresReadiness {
        ready: bool,
    }

    impl Clone for CloneRequiresReadiness {
        fn clone(&self) -> Self {
            Self::default()
        }
    }

    impl Service<Request> for CloneRequiresReadiness {
        type Response = Response;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.ready = true;
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: Request) -> Self::Future {
            assert!(self.ready, "clone-local poll_ready must precede call");
            self.ready = false;
            ready(Ok("ready".into_response()))
        }
    }

    #[tokio::test]
    async fn cloned_services_are_readied_before_the_extension_calls_them() {
        let service = BoxCloneService::new(CloneRequiresReadiness::default()).clone();
        let request = http::Request::builder()
            .body(crate::empty_body())
            .expect("test request is valid");

        assert_eq!(
            service.call_ready(request).await.unwrap().status(),
            http::StatusCode::OK
        );
    }
}
