//! Governor-backed outbound rate-limit middleware.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use tower::{Layer, Service, ServiceExt};

/// Tower layer that shares one governor limiter across all service clones.
#[derive(Debug, Clone)]
pub struct GovernorLayer {
    limiter: Arc<DefaultDirectRateLimiter>,
}

impl GovernorLayer {
    /// Create a layer from a governor quota.
    pub fn new(quota: Quota) -> Self {
        Self {
            limiter: Arc::new(RateLimiter::direct(quota)),
        }
    }
}

impl<S> Layer<S> for GovernorLayer {
    type Service = GovernorService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GovernorService {
            inner,
            limiter: Arc::clone(&self.limiter),
        }
    }
}

/// Tower service produced by [`GovernorLayer`].
#[derive(Debug, Clone)]
pub struct GovernorService<S> {
    inner: S,
    limiter: Arc<DefaultDirectRateLimiter>,
}

impl<S, Request> Service<Request> for GovernorService<S>
where
    S: Service<Request> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    S::Error: Send + 'static,
    Request: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<S::Response, S::Error>> + Send>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let limiter = Arc::clone(&self.limiter);
        let replacement = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, replacement);

        Box::pin(async move {
            limiter.until_ready().await;
            inner.ready().await?.call(request).await
        })
    }
}
