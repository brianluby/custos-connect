//! Retry classification and Tower retry policy.

use std::{
    future::Future,
    num::NonZeroU32,
    pin::Pin,
    time::{Duration, SystemTime},
};

use reqwest::{Method, Request, Response, StatusCode};
use tower::retry::Policy;

/// Methods eligible for automatic retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RetryMode {
    /// Retry only HTTP methods defined as idempotent.
    #[default]
    Idempotent,
    /// Retry every method. Use only when upstream operations are idempotent.
    AllMethods,
}

/// Backoff jitter strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Jitter {
    /// Choose a uniformly random delay between zero and the calculated backoff.
    #[default]
    Full,
    /// Use the calculated backoff without jitter.
    None,
}

/// Retry policy for reqwest requests and responses.
///
/// The first request counts as one attempt. Cloneable request bodies are
/// required for a retry; reqwest streaming bodies therefore run once.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    max_attempts: NonZeroU32,
    initial_backoff: Duration,
    max_backoff: Duration,
    jitter: Jitter,
    mode: RetryMode,
    attempt: u32,
}

impl RetryPolicy {
    /// Create a retry policy with exponential backoff.
    pub fn new(max_attempts: NonZeroU32) -> Self {
        Self {
            max_attempts,
            initial_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(30),
            jitter: Jitter::default(),
            mode: RetryMode::default(),
            attempt: 1,
        }
    }

    /// Configure the initial backoff and maximum delay.
    ///
    /// The maximum bounds both exponential backoff and server-provided
    /// `Retry-After` values.
    pub fn with_backoff(mut self, initial: Duration, maximum: Duration) -> Self {
        self.initial_backoff = initial;
        self.max_backoff = maximum;
        self
    }

    /// Configure the backoff jitter strategy.
    pub fn with_jitter(mut self, jitter: Jitter) -> Self {
        self.jitter = jitter;
        self
    }

    /// Configure which HTTP methods are eligible for retry.
    pub fn with_mode(mut self, mode: RetryMode) -> Self {
        self.mode = mode;
        self
    }

    fn should_retry(&self, request: &Request, result: &Result<Response, reqwest::Error>) -> bool {
        if self.attempt >= self.max_attempts.get() || !self.method_is_retryable(request.method()) {
            return false;
        }

        match result {
            Ok(response) => retryable_status(response.status()),
            Err(error) => error.is_connect() || error.is_timeout() || error.is_body(),
        }
    }

    fn method_is_retryable(&self, method: &Method) -> bool {
        self.mode == RetryMode::AllMethods
            || matches!(
                *method,
                Method::GET
                    | Method::HEAD
                    | Method::PUT
                    | Method::DELETE
                    | Method::OPTIONS
                    | Method::TRACE
            )
    }

    fn delay(&self, response: Option<&Response>) -> Duration {
        if let Some(delay) = response.and_then(retry_after) {
            return delay.min(self.max_backoff);
        }

        let exponent = self.attempt.saturating_sub(1).min(31);
        let delay = self
            .initial_backoff
            .saturating_mul(1_u32 << exponent)
            .min(self.max_backoff);
        match self.jitter {
            Jitter::Full => delay.mul_f64(fastrand::f64()),
            Jitter::None => delay,
        }
    }
}

impl Policy<Request, Response, reqwest::Error> for RetryPolicy {
    type Future = Pin<Box<dyn Future<Output = ()> + Send>>;

    fn retry(
        &mut self,
        request: &mut Request,
        result: &mut Result<Response, reqwest::Error>,
    ) -> Option<Self::Future> {
        if !self.should_retry(request, result) {
            return None;
        }
        let delay = self.delay(result.as_ref().ok());
        self.attempt = self.attempt.saturating_add(1);
        Some(Box::pin(tokio::time::sleep(delay)))
    }

    fn clone_request(&mut self, request: &Request) -> Option<Request> {
        request.try_clone()
    }
}

fn retryable_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

fn retry_after(response: &Response) -> Option<Duration> {
    let raw = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?;
    parse_retry_after(raw, SystemTime::now())
}

fn parse_retry_after(raw: &str, now: SystemTime) -> Option<Duration> {
    let raw = raw.trim();
    if let Ok(seconds) = raw.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let when = httpdate::parse_http_date(raw).ok()?;
    Some(when.duration_since(now).unwrap_or(Duration::ZERO))
}

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroU32,
        time::{Duration, SystemTime},
    };

    use reqwest::Method;

    use super::{Jitter, RetryMode, RetryPolicy, parse_retry_after};

    #[test]
    fn idempotent_mode_rejects_post() {
        let policy = RetryPolicy::new(NonZeroU32::MIN);

        assert!(!policy.method_is_retryable(&Method::POST));
    }

    #[test]
    fn all_methods_mode_accepts_post() {
        let policy = RetryPolicy::new(NonZeroU32::MIN).with_mode(RetryMode::AllMethods);

        assert!(policy.method_is_retryable(&Method::POST));
    }

    #[test]
    fn exponential_backoff_is_capped() {
        let mut policy = RetryPolicy::new(NonZeroU32::MIN)
            .with_backoff(Duration::from_secs(2), Duration::from_secs(3))
            .with_jitter(Jitter::None);
        policy.attempt = 4;

        assert_eq!(policy.delay(None), Duration::from_secs(3));
    }

    #[test]
    fn retry_after_parses_numeric_seconds() {
        let now = SystemTime::UNIX_EPOCH;

        assert_eq!(
            parse_retry_after(" 42 ", now),
            Some(Duration::from_secs(42))
        );
    }

    #[test]
    fn retry_after_parses_http_date() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let raw = httpdate::fmt_http_date(now + Duration::from_secs(7));

        assert_eq!(parse_retry_after(&raw, now), Some(Duration::from_secs(7)));
    }

    #[test]
    fn retry_after_rejects_garbage() {
        assert_eq!(
            parse_retry_after("eventually", SystemTime::UNIX_EPOCH),
            None
        );
    }
}
