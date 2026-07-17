//! Governor-backed outbound rate-limit middleware.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use tower::{Layer, Service};

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

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let limiter = Arc::clone(&self.limiter);
        let replacement = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, replacement);

        Box::pin(async move {
            limiter.until_ready().await;
            inner.call(request).await
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        future::{Ready, ready},
        num::NonZeroU32,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, Waker},
        time::Duration,
    };

    use governor::Quota;
    use tower::{Layer, Service};

    use super::GovernorLayer;

    #[derive(Clone, Copy)]
    struct PendingService;

    impl Service<()> for PendingService {
        type Response = ();
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn call(&mut self, _request: ()) -> Self::Future {
            ready(Ok(()))
        }
    }

    #[derive(Clone)]
    struct ReadyOnceService {
        readiness_polls: Arc<AtomicUsize>,
    }

    impl Service<()> for ReadyOnceService {
        type Response = ();
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            if self.readiness_polls.fetch_add(1, Ordering::SeqCst) == 0 {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }

        fn call(&mut self, _request: ()) -> Self::Future {
            ready(Ok(()))
        }
    }

    #[test]
    fn governor_service_forwards_inner_readiness() {
        let mut service =
            GovernorLayer::new(Quota::per_second(NonZeroU32::MIN)).layer(PendingService);
        let mut context = Context::from_waker(Waker::noop());

        assert!(service.poll_ready(&mut context).is_pending());
    }

    #[tokio::test]
    async fn governor_service_calls_the_ready_inner_without_repolling()
    -> Result<(), Box<dyn std::error::Error>> {
        let readiness_polls = Arc::new(AtomicUsize::new(0));
        let inner = ReadyOnceService {
            readiness_polls: Arc::clone(&readiness_polls),
        };
        let mut service = GovernorLayer::new(Quota::per_second(NonZeroU32::MIN)).layer(inner);
        std::future::poll_fn(|context| service.poll_ready(context)).await?;

        tokio::time::timeout(Duration::from_millis(50), service.call(())).await??;

        assert_eq!(readiness_polls.load(Ordering::SeqCst), 1);
        Ok(())
    }
}
