//! Typed boundary used to evaluate Faucet as the ingestion substrate.
//!
//! Faucet deliberately represents source records and bookmarks as JSON values.
//! This spike tests whether a small adapter can preserve its streaming and
//! checkpoint semantics while exposing connector-specific Rust records.

use std::{collections::HashMap, pin::Pin};

use faucet_core::{FaucetError, Source, StreamPage};
use futures::{Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde_json::Value;
use thiserror::Error;

/// A decoded source page whose checkpoint remains in Faucet's portable JSON form.
#[derive(Debug, Clone, PartialEq)]
pub struct TypedPage<T> {
    /// Records decoded into the connector's domain type.
    pub records: Vec<T>,
    /// Checkpoint to persist only after the page has been durably consumed.
    pub checkpoint: Option<Value>,
}

/// Errors produced while adapting a Faucet source to typed records.
#[derive(Debug, Error)]
pub enum TypedSourceError {
    /// The underlying source failed before producing a page.
    #[error(transparent)]
    Source(#[from] FaucetError),

    /// A source record did not match the connector's declared type.
    #[error("record {index} did not match the connector type: {source}")]
    Decode {
        /// Zero-based position within the source page.
        index: usize,
        /// JSON decoding failure.
        #[source]
        source: serde_json::Error,
    },
}

/// Stream returned by [`typed_pages`].
pub type TypedPageStream<'a, T> =
    Pin<Box<dyn Stream<Item = Result<TypedPage<T>, TypedSourceError>> + Send + 'a>>;

impl<T> TryFrom<StreamPage> for TypedPage<T>
where
    T: DeserializeOwned,
{
    type Error = TypedSourceError;

    fn try_from(page: StreamPage) -> Result<Self, Self::Error> {
        let records = page
            .records
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                serde_json::from_value(value)
                    .map_err(|source| TypedSourceError::Decode { index, source })
            })
            .collect::<Result<_, _>>()?;

        Ok(Self {
            records,
            checkpoint: page.bookmark,
        })
    }
}

/// Decode pages from any Faucet source without buffering the complete ingestion.
///
/// The caller must durably consume `TypedPage::records` before persisting the
/// corresponding `TypedPage::checkpoint`.
///
/// # Errors
///
/// Each stream item returns [`TypedSourceError::Source`] when Faucet fails to
/// fetch a page or [`TypedSourceError::Decode`] when a record has the wrong shape.
pub fn typed_pages<'a, S, T>(
    source: &'a S,
    context: &'a HashMap<String, Value>,
    batch_size: usize,
) -> TypedPageStream<'a, T>
where
    S: Source + ?Sized,
    T: DeserializeOwned + Send + 'a,
{
    let pages = source.stream_pages(context, batch_size).map(|result| {
        result
            .map_err(TypedSourceError::from)
            .and_then(TypedPage::try_from)
    });

    Box::pin(pages)
}

#[cfg(test)]
mod tests {
    use faucet_core::StreamPage;
    use serde::Deserialize;
    use serde_json::json;

    use super::{TypedPage, TypedSourceError};

    #[derive(Debug, Deserialize, PartialEq)]
    struct Record {
        id: u64,
    }

    #[test]
    fn typed_page_preserves_records_and_checkpoint() -> Result<(), TypedSourceError> {
        let page = StreamPage {
            records: vec![json!({"id": 7})],
            bookmark: Some(json!("next")),
        };

        let typed = TypedPage::<Record>::try_from(page)?;

        assert_eq!(
            typed,
            TypedPage {
                records: vec![Record { id: 7 }],
                checkpoint: Some(json!("next")),
            }
        );
        Ok(())
    }

    #[test]
    fn typed_page_reports_the_bad_record_index() {
        let page = StreamPage {
            records: vec![json!({"id": 7}), json!({"id": "not-a-number"})],
            bookmark: None,
        };

        let result = TypedPage::<Record>::try_from(page);

        assert!(matches!(
            result,
            Err(TypedSourceError::Decode { index: 1, .. })
        ));
    }
}
