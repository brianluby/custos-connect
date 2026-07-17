//! Composed reqwest transport service.

use std::{
    task::{Context, Poll},
    time::Duration,
};

use reqwest::{Request, Response};
use thiserror::Error;
use tower::{
    Layer, Service, ServiceExt,
    retry::{Retry, RetryLayer},
    util::BoxCloneSyncService,
};

use crate::{
    AuthError, AuthLayer, AuthService, Authenticator, GovernorLayer, GovernorService, NoAuth,
    Quota, RetryPolicy,
};

/// Concrete default service stack built by [`HttpClientBuilder`].
pub type DefaultHttpService<A> =
    AuthService<Retry<RetryPolicy, GovernorService<reqwest::Client>>, A>;

/// Default timeout for establishing a connection.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Default maximum idle time between response-body reads.
pub const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Default total deadline from connection start through response-body completion.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

/// Connect, read-idle, and total request timeouts for the default reqwest client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpTimeouts {
    connect: Duration,
    read: Duration,
    request: Duration,
}

impl HttpTimeouts {
    /// Create explicit connect, read-idle, and total request timeouts.
    pub const fn new(connect: Duration, read: Duration, request: Duration) -> Self {
        Self {
            connect,
            read,
            request,
        }
    }

    /// Return the connection-establishment timeout.
    pub const fn connect(self) -> Duration {
        self.connect
    }

    /// Return the maximum idle time between response-body reads.
    pub const fn read(self) -> Duration {
        self.read
    }

    /// Return the total request deadline.
    pub const fn request(self) -> Duration {
        self.request
    }
}

impl Default for HttpTimeouts {
    fn default() -> Self {
        Self::new(
            DEFAULT_CONNECT_TIMEOUT,
            DEFAULT_READ_TIMEOUT,
            DEFAULT_REQUEST_TIMEOUT,
        )
    }
}

/// Failure to construct the default reqwest client.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HttpClientBuildError {
    /// Reqwest could not initialize its client or TLS backend.
    #[error("failed to build default reqwest client: {0}")]
    DefaultClient(#[source] reqwest::Error),
}

/// Authentication or reqwest transport failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HttpError {
    /// Authentication middleware could not attach credentials.
    #[error(transparent)]
    Authentication(#[from] AuthError),

    /// Reqwest could not execute the request after the retry policy completed.
    #[error("HTTP transport failed: {0}")]
    Transport(#[source] reqwest::Error),
}

/// Cloneable Tower service that executes reqwest requests.
#[derive(Clone)]
pub struct HttpClient {
    inner: BoxCloneSyncService<Request, Response, HttpError>,
}

impl HttpClient {
    /// Erase a caller-composed Tower service for use by [`SyncSession`](crate::SyncSession).
    ///
    /// This keeps the session API independent of a concrete middleware stack.
    /// Layers added by callers must preserve [`HttpError`] as the service error.
    pub fn from_service<S>(service: S) -> Self
    where
        S: Service<Request, Response = Response, Error = HttpError> + Clone + Send + Sync + 'static,
        S::Future: Send + 'static,
    {
        Self {
            inner: BoxCloneSyncService::new(service),
        }
    }

    /// Execute one request through authentication, retry, and rate limiting.
    ///
    /// # Errors
    ///
    /// Returns [`HttpError`] when authentication or reqwest transport fails.
    pub async fn execute(&self, request: Request) -> Result<Response, HttpError> {
        self.clone().oneshot(request).await
    }
}

impl std::fmt::Debug for HttpClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("HttpClient").finish_non_exhaustive()
    }
}

impl Service<Request> for HttpClient {
    type Response = Response;
    type Error = HttpError;
    type Future = <BoxCloneSyncService<Request, Response, HttpError> as Service<Request>>::Future;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        self.inner.call(request)
    }
}

/// Builder for [`HttpClient`].
///
/// The SDK-built client uses [`HttpTimeouts::default`] and does not follow
/// redirects. Injecting a reqwest client transfers both policies to the caller.
pub struct HttpClientBuilder<A = NoAuth> {
    client: Option<reqwest::Client>,
    timeouts: HttpTimeouts,
    authenticator: A,
    quota: Quota,
    retry_policy: RetryPolicy,
}

impl HttpClientBuilder<NoAuth> {
    /// Start a client with an explicit outbound quota.
    pub fn new(quota: Quota, retry_policy: RetryPolicy) -> Self {
        Self {
            client: None,
            timeouts: HttpTimeouts::default(),
            authenticator: NoAuth,
            quota,
            retry_policy,
        }
    }
}

impl<A> HttpClientBuilder<A> {
    /// Use a preconfigured reqwest client.
    ///
    /// The caller owns timeout and redirect policy for an injected client.
    /// [`HttpTimeouts`] applies only to the SDK-built default client.
    /// If the injected client follows redirects, it must not forward custom
    /// credential headers to an insecure destination.
    pub fn with_reqwest_client(mut self, client: reqwest::Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Override timeouts for the SDK-built default reqwest client.
    ///
    /// This setting is ignored when [`Self::with_reqwest_client`] is used.
    pub fn with_timeouts(mut self, timeouts: HttpTimeouts) -> Self {
        self.timeouts = timeouts;
        self
    }

    /// Replace the authentication strategy.
    pub fn with_authenticator<B>(self, authenticator: B) -> HttpClientBuilder<B> {
        HttpClientBuilder {
            client: self.client,
            timeouts: self.timeouts,
            authenticator,
            quota: self.quota,
            retry_policy: self.retry_policy,
        }
    }
}

impl<A> HttpClientBuilder<A>
where
    A: Authenticator,
{
    /// Build the concrete Tower service without type erasure.
    ///
    /// Use this when applying additional error-preserving Tower layers. The
    /// resulting service can be converted back with [`HttpClient::from_service`].
    ///
    /// # Errors
    ///
    /// Returns [`HttpClientBuildError`] if the default reqwest client or TLS
    /// backend cannot be initialized. An injected client is already built.
    pub fn build_service(self) -> Result<DefaultHttpService<A>, HttpClientBuildError> {
        let client = match self.client {
            Some(client) => client,
            None => default_reqwest_client(self.timeouts)?,
        };
        let limited = GovernorLayer::new(self.quota).layer(client);
        let retried = RetryLayer::new(self.retry_policy).layer(limited);
        Ok(AuthLayer::new(self.authenticator).layer(retried))
    }

    /// Build the composed Tower service.
    ///
    /// Layer order is authentication → retry → governor → reqwest. Consequently,
    /// every retry consumes rate-limit capacity and all retries reuse the same
    /// authenticated request snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`HttpClientBuildError`] if the default reqwest client or TLS
    /// backend cannot be initialized.
    pub fn build(self) -> Result<HttpClient, HttpClientBuildError> {
        Ok(HttpClient::from_service(self.build_service()?))
    }
}

fn default_reqwest_client(timeouts: HttpTimeouts) -> Result<reqwest::Client, HttpClientBuildError> {
    reqwest::Client::builder()
        .connect_timeout(timeouts.connect())
        .read_timeout(timeouts.read())
        .timeout(timeouts.request())
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(HttpClientBuildError::DefaultClient)
}
