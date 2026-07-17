#![doc = include_str!("../README.md")]

mod auth;
mod pagination;
mod rate_limit;
mod retry;
mod source;
mod transport;

pub use auth::{
    ApiKeyAuth, AuthError, AuthLayer, AuthService, Authenticator, BasicAuth, BearerAuth,
    CredentialTransport, NoAuth,
};
pub use governor::Quota;
pub use pagination::{
    CursorPagination, NoPagination, PageMetadata, PageNumberPagination, Pagination, PaginationError,
};
pub use rate_limit::{GovernorLayer, GovernorService};
pub use retry::{Jitter, RetryMode, RetryPolicy};
pub use source::{IngestError, Page, Source, SyncPosition, SyncSession};
pub use transport::{
    DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT, DEFAULT_REQUEST_TIMEOUT, DefaultHttpService,
    HttpClient, HttpClientBuildError, HttpClientBuilder, HttpError, HttpTimeouts,
};

pub use reqwest::{Method, Request, Response, StatusCode, Url};
