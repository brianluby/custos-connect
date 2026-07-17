//! Authentication strategies and Tower middleware.

use std::{
    future::Future,
    net::IpAddr,
    pin::Pin,
    task::{Context, Poll},
};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use reqwest::{
    Request, Response, Url,
    header::{AUTHORIZATION, HeaderName, HeaderValue},
};
use secrecy::{ExposeSecret, SecretString};
use thiserror::Error;
use tower::{Layer, Service, ServiceExt};

use crate::transport::HttpError;

/// Failure to attach credentials to an outbound request.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AuthError {
    /// A credential cannot be represented as an HTTP header value.
    #[error("credential contains bytes that are invalid in an HTTP header")]
    InvalidHeaderValue(#[source] reqwest::header::InvalidHeaderValue),

    /// An HTTP Basic username contains the reserved credential delimiter.
    #[error("Basic authentication username cannot contain ':'")]
    InvalidBasicUsername,

    /// Credentials would be attached to a non-HTTPS, non-loopback request.
    #[error("refusing to attach credentials to an insecure non-loopback URL")]
    InsecureTransport,

    /// A custom authenticator could not obtain or refresh credentials.
    #[error("authentication provider failed: {0}")]
    Provider(String),
}

/// Transport policy applied before an authenticator can attach credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum CredentialTransport {
    /// Permit HTTPS and loopback HTTP only.
    #[default]
    HttpsOrLoopbackHttp,
    /// Permit HTTPS and non-loopback HTTP.
    AllowInsecureHttp,
    /// Skip transport validation because the authenticator never attaches credentials.
    NoCredentials,
}

/// Attaches credentials to an outbound request.
///
/// Implementations may refresh credentials asynchronously. The method runs once
/// per logical request, before the retry layer clones that authenticated request.
/// Retries reuse the resulting snapshot and do not invoke the authenticator
/// again. A refreshing implementation must return a credential expected to
/// remain valid for the configured retry sequence. Unauthorized responses are
/// returned to the connector rather than retried.
#[async_trait]
pub trait Authenticator: Clone + Send + Sync + 'static {
    /// Return the transport policy enforced before [`Self::authenticate`].
    ///
    /// Custom credential authenticators are fail-closed by default.
    fn credential_transport(&self) -> CredentialTransport {
        CredentialTransport::default()
    }

    /// Authenticate `request` in place.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError`] when credentials cannot be obtained or encoded.
    async fn authenticate(&self, request: &mut Request) -> Result<(), AuthError>;
}

/// Authenticator that leaves requests unchanged.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoAuth;

#[async_trait]
impl Authenticator for NoAuth {
    fn credential_transport(&self) -> CredentialTransport {
        CredentialTransport::NoCredentials
    }

    async fn authenticate(&self, _request: &mut Request) -> Result<(), AuthError> {
        Ok(())
    }
}

/// Bearer-token authentication requiring HTTPS or loopback HTTP by default.
#[derive(Clone)]
pub struct BearerAuth {
    token: SecretString,
    credential_transport: CredentialTransport,
}

impl BearerAuth {
    /// Create bearer authentication from a secret token.
    pub fn new(token: SecretString) -> Self {
        Self {
            token,
            credential_transport: CredentialTransport::default(),
        }
    }

    /// Permit the bearer token on non-loopback HTTP requests.
    ///
    /// This opt-out is intended only for controlled lab environments.
    pub fn allow_insecure_http(mut self) -> Self {
        self.credential_transport = CredentialTransport::AllowInsecureHttp;
        self
    }
}

impl std::fmt::Debug for BearerAuth {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("BearerAuth").finish_non_exhaustive()
    }
}

#[async_trait]
impl Authenticator for BearerAuth {
    fn credential_transport(&self) -> CredentialTransport {
        self.credential_transport
    }

    async fn authenticate(&self, request: &mut Request) -> Result<(), AuthError> {
        validate_auth_transport(request.url(), self.credential_transport)?;
        insert_sensitive_header(
            request,
            AUTHORIZATION,
            &format!("Bearer {}", self.token.expose_secret()),
        )
    }
}

/// API-key authentication requiring HTTPS or loopback HTTP by default.
#[derive(Clone)]
pub struct ApiKeyAuth {
    header: HeaderName,
    value: SecretString,
    credential_transport: CredentialTransport,
}

impl ApiKeyAuth {
    /// Create header-based API-key authentication.
    pub fn new(header: HeaderName, value: SecretString) -> Self {
        Self {
            header,
            value,
            credential_transport: CredentialTransport::default(),
        }
    }

    /// Permit the API key on non-loopback HTTP requests.
    ///
    /// This opt-out is intended only for controlled lab environments.
    pub fn allow_insecure_http(mut self) -> Self {
        self.credential_transport = CredentialTransport::AllowInsecureHttp;
        self
    }
}

impl std::fmt::Debug for ApiKeyAuth {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ApiKeyAuth")
            .field("header", &self.header)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Authenticator for ApiKeyAuth {
    fn credential_transport(&self) -> CredentialTransport {
        self.credential_transport
    }

    async fn authenticate(&self, request: &mut Request) -> Result<(), AuthError> {
        validate_auth_transport(request.url(), self.credential_transport)?;
        insert_sensitive_header(request, self.header.clone(), self.value.expose_secret())
    }
}

/// HTTP Basic authentication requiring HTTPS or loopback HTTP by default.
#[derive(Clone)]
pub struct BasicAuth {
    username: String,
    password: SecretString,
    credential_transport: CredentialTransport,
}

impl BasicAuth {
    /// Create Basic authentication.
    pub fn new(username: impl Into<String>, password: SecretString) -> Self {
        Self {
            username: username.into(),
            password,
            credential_transport: CredentialTransport::default(),
        }
    }

    /// Permit the Basic credential on non-loopback HTTP requests.
    ///
    /// This opt-out is intended only for controlled lab environments.
    pub fn allow_insecure_http(mut self) -> Self {
        self.credential_transport = CredentialTransport::AllowInsecureHttp;
        self
    }
}

impl std::fmt::Debug for BasicAuth {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BasicAuth")
            .field("username", &self.username)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Authenticator for BasicAuth {
    fn credential_transport(&self) -> CredentialTransport {
        self.credential_transport
    }

    async fn authenticate(&self, request: &mut Request) -> Result<(), AuthError> {
        validate_auth_transport(request.url(), self.credential_transport)?;
        if self.username.contains(':') {
            return Err(AuthError::InvalidBasicUsername);
        }
        let credential = format!("{}:{}", self.username, self.password.expose_secret());
        let encoded = STANDARD.encode(credential);
        insert_sensitive_header(request, AUTHORIZATION, &format!("Basic {encoded}"))
    }
}

fn insert_sensitive_header(
    request: &mut Request,
    name: HeaderName,
    value: &str,
) -> Result<(), AuthError> {
    let mut value = HeaderValue::from_str(value).map_err(AuthError::InvalidHeaderValue)?;
    value.set_sensitive(true);
    request.headers_mut().insert(name, value);
    Ok(())
}

fn validate_auth_transport(url: &Url, policy: CredentialTransport) -> Result<(), AuthError> {
    let permitted = match policy {
        CredentialTransport::NoCredentials => true,
        CredentialTransport::AllowInsecureHttp => matches!(url.scheme(), "http" | "https"),
        CredentialTransport::HttpsOrLoopbackHttp => {
            url.scheme() == "https" || is_loopback_http(url)
        }
    };
    if permitted {
        Ok(())
    } else {
        Err(AuthError::InsecureTransport)
    }
}

fn is_loopback_http(url: &Url) -> bool {
    url.scheme() == "http" && url.host_str().is_some_and(is_loopback_host)
}

fn is_loopback_host(host: &str) -> bool {
    let host = host.trim_end_matches('.');
    let normalized = host.to_ascii_lowercase();
    let ip_literal = normalized.trim_start_matches('[').trim_end_matches(']');
    normalized == "localhost"
        || normalized.ends_with(".localhost")
        || ip_literal
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

/// Tower layer that authenticates reqwest requests.
#[derive(Debug, Clone)]
pub struct AuthLayer<A> {
    authenticator: A,
}

impl<A> AuthLayer<A> {
    /// Create an authentication layer.
    pub fn new(authenticator: A) -> Self {
        Self { authenticator }
    }
}

impl<S, A> Layer<S> for AuthLayer<A>
where
    A: Clone,
{
    type Service = AuthService<S, A>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthService {
            inner,
            authenticator: self.authenticator.clone(),
        }
    }
}

/// Tower service produced by [`AuthLayer`].
///
/// The service enforces [`Authenticator::credential_transport`] before
/// credentials can be attached. It validates the original request URL; an
/// inner client that follows redirects owns forwarded-header policy.
#[derive(Debug, Clone)]
pub struct AuthService<S, A> {
    inner: S,
    authenticator: A,
}

impl<S, A> Service<Request> for AuthService<S, A>
where
    S: Service<Request, Response = Response, Error = reqwest::Error> + Clone + Send + 'static,
    S::Future: Send + 'static,
    A: Authenticator,
{
    type Response = Response;
    type Error = HttpError;
    type Future = Pin<Box<dyn Future<Output = Result<Response, HttpError>> + Send>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut request: Request) -> Self::Future {
        let authenticator = self.authenticator.clone();
        let replacement = self.inner.clone();
        let inner = std::mem::replace(&mut self.inner, replacement);

        Box::pin(async move {
            validate_auth_transport(request.url(), authenticator.credential_transport())?;
            authenticator.authenticate(&mut request).await?;
            inner.oneshot(request).await.map_err(HttpError::Transport)
        })
    }
}

#[cfg(test)]
mod tests {
    use reqwest::{
        Method, Request, Url,
        header::{AUTHORIZATION, HeaderName},
    };
    use secrecy::SecretString;

    use super::{
        ApiKeyAuth, AuthError, Authenticator, BasicAuth, BearerAuth, CredentialTransport, NoAuth,
        validate_auth_transport,
    };

    fn test_request() -> Result<Request, Box<dyn std::error::Error>> {
        Ok(Request::new(
            Method::GET,
            Url::parse("https://example.test")?,
        ))
    }

    #[tokio::test]
    async fn api_key_auth_inserts_a_sensitive_header() -> Result<(), Box<dyn std::error::Error>> {
        let authenticator = ApiKeyAuth::new(
            HeaderName::from_static("x-api-key"),
            SecretString::from("key-secret".to_owned()),
        );
        let mut request = test_request()?;

        authenticator.authenticate(&mut request).await?;

        let value = request
            .headers()
            .get("x-api-key")
            .ok_or("API key header is missing")?;
        assert!(value == "key-secret" && value.is_sensitive());
        Ok(())
    }

    #[tokio::test]
    async fn basic_auth_inserts_a_sensitive_header() -> Result<(), Box<dyn std::error::Error>> {
        let authenticator =
            BasicAuth::new("alice", SecretString::from("password-secret".to_owned()));
        let mut request = test_request()?;

        authenticator.authenticate(&mut request).await?;

        let value = request
            .headers()
            .get(AUTHORIZATION)
            .ok_or("authorization header is missing")?;
        assert!(value == "Basic YWxpY2U6cGFzc3dvcmQtc2VjcmV0" && value.is_sensitive());
        Ok(())
    }

    #[tokio::test]
    async fn basic_auth_rejects_a_colon_in_the_username() -> Result<(), Box<dyn std::error::Error>>
    {
        let authenticator = BasicAuth::new("alice:x", SecretString::from("password".to_owned()));
        let mut request = test_request()?;

        let result = authenticator.authenticate(&mut request).await;

        assert!(matches!(result, Err(AuthError::InvalidBasicUsername)));
        Ok(())
    }

    #[tokio::test]
    async fn no_auth_leaves_headers_unchanged() -> Result<(), Box<dyn std::error::Error>> {
        let mut request = test_request()?;

        NoAuth.authenticate(&mut request).await?;

        assert!(request.headers().is_empty());
        Ok(())
    }

    #[test]
    fn secure_transport_accepts_https() -> Result<(), Box<dyn std::error::Error>> {
        let url = Url::parse("https://api.example.test")?;

        let result = validate_auth_transport(&url, CredentialTransport::HttpsOrLoopbackHttp);

        assert!(result.is_ok());
        Ok(())
    }

    #[test]
    fn secure_transport_accepts_loopback_http() -> Result<(), Box<dyn std::error::Error>> {
        for raw in [
            "http://localhost:8080",
            "http://service.localhost",
            "http://127.0.0.2",
            "http://[::1]",
        ] {
            let url = Url::parse(raw)?;
            assert!(
                validate_auth_transport(&url, CredentialTransport::HttpsOrLoopbackHttp).is_ok(),
                "loopback URL was rejected: {url}"
            );
        }
        Ok(())
    }

    #[test]
    fn secure_transport_rejects_remote_http() -> Result<(), Box<dyn std::error::Error>> {
        let url = Url::parse("http://api.example.test")?;

        let result = validate_auth_transport(&url, CredentialTransport::HttpsOrLoopbackHttp);

        assert!(matches!(result, Err(AuthError::InsecureTransport)));
        Ok(())
    }

    #[test]
    fn explicit_opt_out_accepts_remote_http() -> Result<(), Box<dyn std::error::Error>> {
        let authenticator =
            BearerAuth::new(SecretString::from("token".to_owned())).allow_insecure_http();
        let url = Url::parse("http://api.example.test")?;

        let result = validate_auth_transport(&url, authenticator.credential_transport());

        assert!(result.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn bearer_auth_direct_call_rejects_remote_http() -> Result<(), Box<dyn std::error::Error>>
    {
        let authenticator = BearerAuth::new(SecretString::from("token".to_owned()));
        let mut request = Request::new(Method::GET, Url::parse("http://api.example.test")?);

        let result = authenticator.authenticate(&mut request).await;

        assert!(matches!(result, Err(AuthError::InsecureTransport)));
        Ok(())
    }
}
