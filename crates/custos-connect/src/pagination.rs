//! Reusable pagination state machines.

use std::num::NonZeroUsize;

use reqwest::{Request, header::HeaderMap};
use serde_json::Value;
use thiserror::Error;

/// Response information used to derive the next page cursor.
#[derive(Debug, Clone, Copy)]
pub struct PageMetadata<'a> {
    /// Response headers captured before consuming the body.
    pub headers: &'a HeaderMap,
    /// Decoded JSON response body.
    pub body: &'a Value,
    /// Number of records extracted from the response.
    pub record_count: usize,
}

/// Pagination configuration or response error.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PaginationError {
    /// A query parameter name is empty.
    #[error("pagination query parameter cannot be empty")]
    EmptyParameter,

    /// A response pointer is not valid RFC 6901 JSON Pointer syntax.
    #[error("pagination JSON Pointer is not valid RFC 6901 syntax")]
    InvalidJsonPointer,

    /// The next cursor exists but is not a string or null.
    #[error("pagination cursor at '{pointer}' must be a string or null")]
    InvalidCursorType {
        /// JSON Pointer used to locate the cursor.
        pointer: String,
    },

    /// The page number overflowed `u64`.
    #[error("page number overflowed u64")]
    PageNumberOverflow,
}

/// Applies and advances one pagination strategy.
pub trait Pagination: Clone + Send + Sync + 'static {
    /// Cursor carried between requests in a sync session.
    type Cursor: Clone + PartialEq + Send + Sync + 'static;

    /// Apply the current cursor to an outbound request.
    ///
    /// # Errors
    ///
    /// Returns [`PaginationError`] when the cursor cannot be represented by the
    /// configured strategy.
    fn apply(
        &self,
        request: &mut Request,
        cursor: Option<&Self::Cursor>,
    ) -> Result<(), PaginationError>;

    /// Derive the next cursor after decoding a page.
    ///
    /// # Errors
    ///
    /// Returns [`PaginationError`] when response pagination metadata is invalid.
    fn next(
        &self,
        current: Option<&Self::Cursor>,
        page: PageMetadata<'_>,
    ) -> Result<Option<Self::Cursor>, PaginationError>;
}

/// Pagination strategy for a single-request source.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoPagination;

impl Pagination for NoPagination {
    type Cursor = ();

    fn apply(
        &self,
        _request: &mut Request,
        _cursor: Option<&Self::Cursor>,
    ) -> Result<(), PaginationError> {
        Ok(())
    }

    fn next(
        &self,
        _current: Option<&Self::Cursor>,
        _page: PageMetadata<'_>,
    ) -> Result<Option<Self::Cursor>, PaginationError> {
        Ok(None)
    }
}

/// String cursor read from a JSON response and sent as a query parameter.
#[derive(Debug, Clone)]
pub struct CursorPagination {
    query_parameter: String,
    response_pointer: String,
}

impl CursorPagination {
    /// Create cursor pagination.
    ///
    /// `response_pointer` uses RFC 6901 JSON Pointer syntax.
    ///
    /// # Errors
    ///
    /// Returns [`PaginationError::EmptyParameter`] for an empty query parameter
    /// or [`PaginationError::InvalidJsonPointer`] for invalid pointer syntax.
    pub fn new(
        query_parameter: impl Into<String>,
        response_pointer: impl Into<String>,
    ) -> Result<Self, PaginationError> {
        let query_parameter = query_parameter.into();
        let response_pointer = response_pointer.into();
        validate_parameter(&query_parameter)?;
        validate_pointer(&response_pointer)?;
        Ok(Self {
            query_parameter,
            response_pointer,
        })
    }
}

impl Pagination for CursorPagination {
    type Cursor = String;

    fn apply(
        &self,
        request: &mut Request,
        cursor: Option<&Self::Cursor>,
    ) -> Result<(), PaginationError> {
        replace_query_parameter(request, &self.query_parameter, cursor.map(String::as_str));
        Ok(())
    }

    fn next(
        &self,
        _current: Option<&Self::Cursor>,
        page: PageMetadata<'_>,
    ) -> Result<Option<Self::Cursor>, PaginationError> {
        match page.body.pointer(&self.response_pointer) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(cursor)) if cursor.is_empty() => Ok(None),
            Some(Value::String(cursor)) => Ok(Some(cursor.clone())),
            Some(Value::Bool(_) | Value::Number(_) | Value::Array(_) | Value::Object(_)) => {
                Err(PaginationError::InvalidCursorType {
                    pointer: self.response_pointer.clone(),
                })
            }
        }
    }
}

/// One-based or zero-based page-number pagination.
#[derive(Debug, Clone)]
pub struct PageNumberPagination {
    query_parameter: String,
    first_page: u64,
    page_size: NonZeroUsize,
}

impl PageNumberPagination {
    /// Create page-number pagination.
    ///
    /// Pagination stops when a response contains fewer than `page_size` records.
    ///
    /// # Errors
    ///
    /// Returns [`PaginationError::EmptyParameter`] when the query parameter is empty.
    pub fn new(
        query_parameter: impl Into<String>,
        first_page: u64,
        page_size: NonZeroUsize,
    ) -> Result<Self, PaginationError> {
        let query_parameter = query_parameter.into();
        validate_parameter(&query_parameter)?;
        Ok(Self {
            query_parameter,
            first_page,
            page_size,
        })
    }
}

impl Pagination for PageNumberPagination {
    type Cursor = u64;

    fn apply(
        &self,
        request: &mut Request,
        cursor: Option<&Self::Cursor>,
    ) -> Result<(), PaginationError> {
        let page = cursor.copied().unwrap_or(self.first_page);
        replace_query_parameter(request, &self.query_parameter, Some(&page.to_string()));
        Ok(())
    }

    fn next(
        &self,
        current: Option<&Self::Cursor>,
        page: PageMetadata<'_>,
    ) -> Result<Option<Self::Cursor>, PaginationError> {
        if page.record_count < self.page_size.get() {
            return Ok(None);
        }
        current
            .copied()
            .unwrap_or(self.first_page)
            .checked_add(1)
            .map(Some)
            .ok_or(PaginationError::PageNumberOverflow)
    }
}

fn replace_query_parameter(request: &mut Request, parameter: &str, value: Option<&str>) {
    let retained_pairs = request
        .url()
        .query_pairs()
        .filter(|(name, _value)| name != parameter)
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    let mut query = request.url_mut().query_pairs_mut();
    query.clear();
    query.extend_pairs(retained_pairs);
    if let Some(value) = value {
        query.append_pair(parameter, value);
    }
}

fn validate_parameter(parameter: &str) -> Result<(), PaginationError> {
    if parameter.is_empty() {
        Err(PaginationError::EmptyParameter)
    } else {
        Ok(())
    }
}

fn validate_pointer(pointer: &str) -> Result<(), PaginationError> {
    let valid = pointer.is_empty()
        || pointer
            .strip_prefix('/')
            .is_some_and(|tokens| tokens.split('/').all(pointer_token_has_valid_escapes));
    if valid {
        Ok(())
    } else {
        Err(PaginationError::InvalidJsonPointer)
    }
}

fn pointer_token_has_valid_escapes(token: &str) -> bool {
    let mut bytes = token.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'~' && !matches!(bytes.next(), Some(b'0' | b'1')) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use reqwest::{Method, Request, Url, header::HeaderMap};
    use serde_json::json;

    use super::{
        CursorPagination, PageMetadata, PageNumberPagination, Pagination, PaginationError,
    };

    #[test]
    fn cursor_pagination_applies_and_extracts_cursor() -> Result<(), Box<dyn std::error::Error>> {
        let pagination = CursorPagination::new("after", "/paging/next")?;
        let mut request = Request::new(Method::GET, Url::parse("https://example.test/items")?);
        pagination.apply(&mut request, Some(&"opaque".to_owned()))?;
        let headers = HeaderMap::new();
        let body = json!({"paging": {"next": "following"}});

        let next = pagination.next(
            Some(&"opaque".to_owned()),
            PageMetadata {
                headers: &headers,
                body: &body,
                record_count: 1,
            },
        )?;

        assert!(
            request
                .url()
                .query_pairs()
                .any(|(name, value)| name == "after" && value == "opaque")
        );
        assert_eq!(next.as_deref(), Some("following"));
        Ok(())
    }

    #[test]
    fn cursor_pagination_replaces_existing_query_parameters()
    -> Result<(), Box<dyn std::error::Error>> {
        let pagination = CursorPagination::new("after", "/paging/next")?;
        let mut request = Request::new(
            Method::GET,
            Url::parse("https://example.test/items?after=stale&keep=yes&after=older")?,
        );

        pagination.apply(&mut request, Some(&"current".to_owned()))?;

        let pairs = request
            .url()
            .query_pairs()
            .map(|(name, value)| (name.into_owned(), value.into_owned()))
            .collect::<Vec<_>>();
        assert_eq!(
            pairs,
            vec![
                ("keep".to_owned(), "yes".to_owned()),
                ("after".to_owned(), "current".to_owned())
            ]
        );

        pagination.apply(&mut request, None)?;

        assert_eq!(request.url().query(), Some("keep=yes"));
        Ok(())
    }

    #[test]
    fn page_number_stops_on_a_partial_page() -> Result<(), Box<dyn std::error::Error>> {
        let page_size = NonZeroUsize::new(2).ok_or("page size must be nonzero")?;
        let pagination = PageNumberPagination::new("page", 1, page_size)?;
        let mut request = Request::new(Method::GET, Url::parse("https://example.test/items")?);
        pagination.apply(&mut request, None)?;
        let headers = HeaderMap::new();
        let body = json!(null);

        let next = pagination.next(
            None,
            PageMetadata {
                headers: &headers,
                body: &body,
                record_count: 1,
            },
        )?;

        assert!(
            request
                .url()
                .query_pairs()
                .any(|(name, value)| name == "page" && value == "1")
        );
        assert_eq!(next, None);
        Ok(())
    }

    #[test]
    fn page_number_replaces_existing_query_parameters() -> Result<(), Box<dyn std::error::Error>> {
        let page_size = NonZeroUsize::new(2).ok_or("page size must be nonzero")?;
        let pagination = PageNumberPagination::new("page", 1, page_size)?;
        let mut request = Request::new(
            Method::GET,
            Url::parse("https://example.test/items?page=99&keep=yes&page=100")?,
        );

        pagination.apply(&mut request, None)?;

        let pairs = request
            .url()
            .query_pairs()
            .map(|(name, value)| (name.into_owned(), value.into_owned()))
            .collect::<Vec<_>>();
        assert_eq!(
            pairs,
            vec![
                ("keep".to_owned(), "yes".to_owned()),
                ("page".to_owned(), "1".to_owned())
            ]
        );
        Ok(())
    }

    #[test]
    fn page_number_reports_overflow() -> Result<(), Box<dyn std::error::Error>> {
        let page_size = NonZeroUsize::new(2).ok_or("page size must be nonzero")?;
        let pagination = PageNumberPagination::new("page", 1, page_size)?;
        let headers = HeaderMap::new();
        let body = json!(null);

        let result = pagination.next(
            Some(&u64::MAX),
            PageMetadata {
                headers: &headers,
                body: &body,
                record_count: 2,
            },
        );

        assert!(matches!(result, Err(PaginationError::PageNumberOverflow)));
        Ok(())
    }

    #[test]
    fn cursor_pagination_rejects_an_invalid_pointer() {
        for pointer in ["next", "/next~2cursor", "/next~"] {
            let result = CursorPagination::new("cursor", pointer);

            assert!(matches!(result, Err(PaginationError::InvalidJsonPointer)));
        }
    }

    #[test]
    fn cursor_pagination_accepts_valid_pointer_escapes() -> Result<(), Box<dyn std::error::Error>> {
        let _pagination = CursorPagination::new("cursor", "/a~0b/~1c")?;

        Ok(())
    }
}
