//! End-to-end tests for the public connector SDK.

use std::{
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use custos_connect::{
    AuthError, BearerAuth, CursorPagination, GovernorLayer, HttpClient, HttpClientBuilder,
    HttpError, HttpTimeouts, IngestError, Jitter, Page, PageMetadata, Pagination, PaginationError,
    Quota, Request, Response, RetryMode, RetryPolicy, Source, SyncPosition, SyncSession, Url,
};
use reqwest::{Method, header::HeaderValue};
use secrecy::SecretString;
use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;
use tower::{Layer, ServiceExt, service_fn, util::MapRequestLayer};
use wiremock::{
    Mock, MockServer, Request as MockRequest, Respond, ResponseTemplate,
    matchers::{header, method, path, query_param, query_param_is_missing},
};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct Event {
    id: u64,
    updated_at: String,
}

#[derive(Debug, Error)]
enum TestSourceError {
    #[error(transparent)]
    Transport(#[from] reqwest::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Pagination(#[from] PaginationError),
    #[error("upstream returned {0}")]
    Status(reqwest::StatusCode),
}

struct TestSource {
    endpoint: Url,
    pagination: CursorPagination,
}

impl TestSource {
    fn new(endpoint: Url) -> Result<Self, PaginationError> {
        Ok(Self {
            endpoint,
            pagination: CursorPagination::new("cursor", "/next_cursor")?,
        })
    }
}

#[async_trait]
impl Source for TestSource {
    type Record = Event;
    type Cursor = String;
    type Checkpoint = String;
    type Error = TestSourceError;

    async fn build_request(
        &self,
        position: SyncPosition<'_, Self::Cursor, Self::Checkpoint>,
    ) -> Result<Request, Self::Error> {
        let mut request = Request::new(Method::GET, self.endpoint.clone());
        if let Some(checkpoint) = position.checkpoint() {
            request
                .url_mut()
                .query_pairs_mut()
                .append_pair("since", checkpoint);
        }
        self.pagination.apply(&mut request, position.cursor())?;
        Ok(request)
    }

    async fn decode_page(
        &self,
        response: Response,
        position: SyncPosition<'_, Self::Cursor, Self::Checkpoint>,
    ) -> Result<Page<Self::Record, Self::Cursor, Self::Checkpoint>, Self::Error> {
        let status = response.status();
        if !status.is_success() {
            return Err(TestSourceError::Status(status));
        }
        let headers = response.headers().clone();
        let body: Value = response.json().await?;
        let records: Vec<Event> = serde_json::from_value(body["data"].clone())?;
        let next = self.pagination.next(
            position.cursor(),
            PageMetadata {
                headers: &headers,
                body: &body,
                record_count: records.len(),
            },
        )?;
        let checkpoint = records.iter().map(|event| event.updated_at.clone()).max();
        Ok(Page::new(records)
            .with_next_cursor(next)
            .with_checkpoint(checkpoint))
    }
}

fn fast_quota() -> Quota {
    Quota::per_second(NonZeroU32::new(10_000).unwrap_or(NonZeroU32::MIN))
}

fn no_retry() -> RetryPolicy {
    RetryPolicy::new(NonZeroU32::MIN).with_jitter(Jitter::None)
}

fn assert_send_static<T: Send + 'static>(_value: T) {}

#[test]
fn default_http_timeouts_are_finite() {
    let timeouts = HttpTimeouts::default();

    assert!(
        timeouts.connect() > Duration::ZERO
            && timeouts.read() > Duration::ZERO
            && timeouts.request() > Duration::ZERO
    );
}

#[test]
fn session_future_is_send_and_static() -> Result<(), Box<dyn std::error::Error>> {
    let source = TestSource::new(Url::parse("https://example.test/events")?)?;
    let client = HttpClientBuilder::new(fast_quota(), no_retry()).build()?;
    let mut session = SyncSession::new(source, client);

    assert_send_static(async move { session.next_page().await });
    Ok(())
}

#[tokio::test]
async fn session_applies_auth_paginates_and_emits_incremental_checkpoints()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/events"))
        .and(header("authorization", "Bearer integration-token"))
        .and(query_param("since", "2026-07-14"))
        .and(query_param_is_missing("cursor"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": 1, "updated_at": "2026-07-15"}],
            "next_cursor": "page-2"
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/events"))
        .and(header("authorization", "Bearer integration-token"))
        .and(query_param("since", "2026-07-14"))
        .and(query_param("cursor", "page-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": 2, "updated_at": "2026-07-16"}],
            "next_cursor": null
        })))
        .expect(1)
        .mount(&server)
        .await;

    let source = TestSource::new(Url::parse(&format!("{}/events", server.uri()))?)?;
    let client = HttpClientBuilder::new(fast_quota(), no_retry())
        .with_authenticator(BearerAuth::new(SecretString::from(
            "integration-token".to_owned(),
        )))
        .build()?;
    let mut session = SyncSession::resume(source, client, "2026-07-14".to_owned());

    let first = session.next_page().await?.ok_or("expected first page")?;
    let second = session.next_page().await?.ok_or("expected second page")?;

    assert_eq!(first.records()[0].id, 1);
    assert_eq!(first.checkpoint().map(String::as_str), Some("2026-07-15"));
    assert_eq!(second.records()[0].id, 2);
    assert_eq!(second.checkpoint().map(String::as_str), Some("2026-07-16"));
    assert!(session.next_page().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn authenticated_remote_http_is_rejected_before_transport()
-> Result<(), Box<dyn std::error::Error>> {
    let client = HttpClientBuilder::new(fast_quota(), no_retry())
        .with_authenticator(BearerAuth::new(SecretString::from("token".to_owned())))
        .build()?;
    let request = Request::new(Method::GET, Url::parse("http://api.example.test/events")?);

    let result = client.execute(request).await;

    assert!(matches!(
        result,
        Err(HttpError::Authentication(AuthError::InsecureTransport))
    ));
    Ok(())
}

#[tokio::test]
async fn insecure_http_opt_out_is_explicit_and_operational()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/lab"))
        .and(header("authorization", "Bearer lab-token"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    let reqwest_client = reqwest::Client::builder()
        .no_proxy()
        .resolve("lab.example.test", *server.address())
        .build()?;
    let authenticator =
        BearerAuth::new(SecretString::from("lab-token".to_owned())).allow_insecure_http();
    let client = HttpClientBuilder::new(fast_quota(), no_retry())
        .with_reqwest_client(reqwest_client)
        .with_authenticator(authenticator)
        .build()?;
    let request = Request::new(
        Method::GET,
        Url::parse(&format!(
            "http://lab.example.test:{}/lab",
            server.address().port()
        ))?,
    );

    let response = client.execute(request).await?;

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    Ok(())
}

#[tokio::test]
async fn default_client_does_not_follow_authenticated_redirects()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/redirect"))
        .and(header("authorization", "Bearer redirect-token"))
        .respond_with(ResponseTemplate::new(302).insert_header("location", "/target"))
        .expect(1)
        .mount(&server)
        .await;
    let client = HttpClientBuilder::new(fast_quota(), no_retry())
        .with_authenticator(BearerAuth::new(SecretString::from(
            "redirect-token".to_owned(),
        )))
        .build()?;
    let request = Request::new(
        Method::GET,
        Url::parse(&format!("{}/redirect", server.uri()))?,
    );

    let response = client.execute(request).await?;

    assert_eq!(response.status(), reqwest::StatusCode::FOUND);
    Ok(())
}

#[tokio::test]
async fn session_rejects_a_repeated_cursor() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/loop"))
        .and(query_param_is_missing("cursor"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": 1, "updated_at": "2026-07-15"}],
            "next_cursor": "same"
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/loop"))
        .and(query_param("cursor", "same"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": 2, "updated_at": "2026-07-16"}],
            "next_cursor": "same"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let source = TestSource::new(Url::parse(&format!("{}/loop", server.uri()))?)?;
    let client = HttpClientBuilder::new(fast_quota(), no_retry()).build()?;
    let mut session = SyncSession::new(source, client);

    assert!(session.next_page().await?.is_some());
    assert!(matches!(
        session.next_page().await,
        Err(IngestError::RepeatedCursor)
    ));
    Ok(())
}

#[derive(Clone)]
struct FlakyResponder {
    calls: Arc<AtomicUsize>,
    retry_after: Option<&'static str>,
}

impl Respond for FlakyResponder {
    fn respond(&self, _request: &MockRequest) -> ResponseTemplate {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.retry_after.map_or_else(
                || ResponseTemplate::new(503),
                |retry_after| ResponseTemplate::new(503).insert_header("retry-after", retry_after),
            )
        } else {
            ResponseTemplate::new(200).set_body_json(json!({"ok": true}))
        }
    }
}

#[tokio::test]
async fn retry_layer_retries_transient_statuses() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let calls = Arc::new(AtomicUsize::new(0));
    Mock::given(method("GET"))
        .and(path("/flaky"))
        .respond_with(FlakyResponder {
            calls: Arc::clone(&calls),
            retry_after: None,
        })
        .expect(2)
        .mount(&server)
        .await;
    let attempts = NonZeroU32::new(2).ok_or("attempt count must be nonzero")?;
    let policy = RetryPolicy::new(attempts)
        .with_backoff(Duration::from_millis(1), Duration::from_millis(1))
        .with_jitter(Jitter::None);
    let client = HttpClientBuilder::new(fast_quota(), policy).build()?;
    let request = Request::new(Method::GET, Url::parse(&format!("{}/flaky", server.uri()))?);

    let response = client.execute(request).await?;

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn retry_layer_does_not_retry_post_by_default() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let calls = Arc::new(AtomicUsize::new(0));
    Mock::given(method("POST"))
        .and(path("/unsafe"))
        .respond_with(FlakyResponder {
            calls: Arc::clone(&calls),
            retry_after: None,
        })
        .expect(1)
        .mount(&server)
        .await;
    let attempts = NonZeroU32::new(2).ok_or("attempt count must be nonzero")?;
    let policy = RetryPolicy::new(attempts)
        .with_backoff(Duration::from_millis(1), Duration::from_millis(1))
        .with_jitter(Jitter::None);
    let client = HttpClientBuilder::new(fast_quota(), policy).build()?;
    let request = Request::new(
        Method::POST,
        Url::parse(&format!("{}/unsafe", server.uri()))?,
    );

    let response = client.execute(request).await?;

    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn retry_layer_retries_post_when_all_methods_are_enabled()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let calls = Arc::new(AtomicUsize::new(0));
    Mock::given(method("POST"))
        .and(path("/idempotent-post"))
        .respond_with(FlakyResponder {
            calls: Arc::clone(&calls),
            retry_after: None,
        })
        .expect(2)
        .mount(&server)
        .await;
    let attempts = NonZeroU32::new(2).ok_or("attempt count must be nonzero")?;
    let policy = RetryPolicy::new(attempts)
        .with_backoff(Duration::from_millis(1), Duration::from_millis(1))
        .with_jitter(Jitter::None)
        .with_mode(RetryMode::AllMethods);
    let client = HttpClientBuilder::new(fast_quota(), policy).build()?;
    let request = Request::new(
        Method::POST,
        Url::parse(&format!("{}/idempotent-post", server.uri()))?,
    );

    let response = client.execute(request).await?;

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn retry_after_is_bounded_by_max_backoff() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let calls = Arc::new(AtomicUsize::new(0));
    Mock::given(method("GET"))
        .and(path("/hostile-retry-after"))
        .respond_with(FlakyResponder {
            calls: Arc::clone(&calls),
            retry_after: Some("86400"),
        })
        .expect(2)
        .mount(&server)
        .await;
    let attempts = NonZeroU32::new(2).ok_or("attempt count must be nonzero")?;
    let policy = RetryPolicy::new(attempts)
        .with_backoff(Duration::from_millis(1), Duration::from_millis(5))
        .with_jitter(Jitter::None);
    let client = HttpClientBuilder::new(fast_quota(), policy).build()?;
    let request = Request::new(
        Method::GET,
        Url::parse(&format!("{}/hostile-retry-after", server.uri()))?,
    );

    let response =
        tokio::time::timeout(Duration::from_millis(250), client.execute(request)).await??;

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    Ok(())
}

#[tokio::test]
async fn configured_request_timeout_bounds_stalled_transport()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/stalled"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(100)))
        .expect(1)
        .mount(&server)
        .await;
    let timeouts = HttpTimeouts::new(
        Duration::from_millis(20),
        Duration::from_millis(20),
        Duration::from_millis(20),
    );
    let client = HttpClientBuilder::new(fast_quota(), no_retry())
        .with_timeouts(timeouts)
        .build()?;
    let request = Request::new(
        Method::GET,
        Url::parse(&format!("{}/stalled", server.uri()))?,
    );

    let result = client.execute(request).await;

    assert!(matches!(
        result,
        Err(HttpError::Transport(error)) if error.is_timeout()
    ));
    Ok(())
}

#[tokio::test]
async fn caller_can_compose_an_extra_tower_layer() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/layered"))
        .and(header("x-caller-layer", "present"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    let base_service = HttpClientBuilder::new(fast_quota(), no_retry()).build_service()?;
    let service = MapRequestLayer::new(|mut request: Request| {
        request
            .headers_mut()
            .insert("x-caller-layer", HeaderValue::from_static("present"));
        request
    })
    .layer(base_service);
    let client = HttpClient::from_service(service);
    let request = Request::new(
        Method::GET,
        Url::parse(&format!("{}/layered", server.uri()))?,
    );

    let response = client.execute(request).await?;

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    Ok(())
}

#[tokio::test]
async fn governor_layer_limits_each_service_call() -> Result<(), Box<dyn std::error::Error>> {
    let quota =
        Quota::with_period(Duration::from_millis(30)).ok_or("quota period must be nonzero")?;
    let service = GovernorLayer::new(quota).layer(service_fn(|value: u8| async move {
        Ok::<_, std::convert::Infallible>(value)
    }));
    let start = Instant::now();

    let first = service.clone().oneshot(1).await?;
    let second = service.oneshot(2).await?;

    assert_eq!((first, second), (1, 2));
    assert!(start.elapsed() >= Duration::from_millis(20));
    Ok(())
}
