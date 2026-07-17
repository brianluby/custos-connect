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

    /// A JSON Pointer is not empty and does not start with `/`.
    #[error("pagination JSON Pointer must be empty or start with '/'")]
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
        if let Some(cursor) = cursor {
            request
                .url_mut()
                .query_pairs_mut()
                .append_pair(&self.query_parameter, cursor);
        }
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
        request
            .url_mut()
            .query_pairs_mut()
            .append_pair(&self.query_parameter, &page.to_string());
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

fn validate_parameter(parameter: &str) -> Result<(), PaginationError> {
    if parameter.is_empty() {
        Err(PaginationError::EmptyParameter)
    } else {
        Ok(())
    }
}

fn validate_pointer(pointer: &str) -> Result<(), PaginationError> {
    if pointer.is_empty() || pointer.starts_with('/') {
        Ok(())
    } else {
        Err(PaginationError::InvalidJsonPointer)
    }
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
        let result = CursorPagination::new("cursor", "next");

        assert!(matches!(result, Err(PaginationError::InvalidJsonPointer)));
    }
}
