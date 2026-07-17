//! Source contract and incremental pagination session.

use std::error::Error;

use async_trait::async_trait;
use reqwest::{Request, Response};
use thiserror::Error;

use crate::{HttpClient, HttpError};

/// A decoded source page and the state needed after consuming it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use = "a page checkpoint must be handled after its records are consumed"]
pub struct Page<Record, Cursor, Checkpoint> {
    records: Vec<Record>,
    next_cursor: Option<Cursor>,
    checkpoint: Option<Checkpoint>,
}

impl<Record, Cursor, Checkpoint> Page<Record, Cursor, Checkpoint> {
    /// Create a terminal page with no checkpoint.
    pub fn new(records: Vec<Record>) -> Self {
        Self {
            records,
            next_cursor: None,
            checkpoint: None,
        }
    }

    /// Set the cursor used to request the following page.
    pub fn with_next_cursor(mut self, cursor: Option<Cursor>) -> Self {
        self.next_cursor = cursor;
        self
    }

    /// Set the incremental checkpoint produced by this page.
    pub fn with_checkpoint(mut self, checkpoint: Option<Checkpoint>) -> Self {
        self.checkpoint = checkpoint;
        self
    }

    /// Borrow the decoded records.
    pub fn records(&self) -> &[Record] {
        &self.records
    }

    /// Borrow the next page cursor.
    pub fn next_cursor(&self) -> Option<&Cursor> {
        self.next_cursor.as_ref()
    }

    /// Borrow the checkpoint that becomes durable after consuming this page.
    pub fn checkpoint(&self) -> Option<&Checkpoint> {
        self.checkpoint.as_ref()
    }

    /// Consume the page into records, cursor, and checkpoint.
    pub fn into_parts(self) -> (Vec<Record>, Option<Cursor>, Option<Checkpoint>) {
        (self.records, self.next_cursor, self.checkpoint)
    }
}

/// Current position supplied while building and decoding a request.
#[derive(Debug)]
pub struct SyncPosition<'a, Cursor, Checkpoint> {
    cursor: Option<&'a Cursor>,
    checkpoint: Option<&'a Checkpoint>,
}

impl<Cursor, Checkpoint> Clone for SyncPosition<'_, Cursor, Checkpoint> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Cursor, Checkpoint> Copy for SyncPosition<'_, Cursor, Checkpoint> {}

impl<'a, Cursor, Checkpoint> SyncPosition<'a, Cursor, Checkpoint> {
    fn new(cursor: Option<&'a Cursor>, checkpoint: Option<&'a Checkpoint>) -> Self {
        Self { cursor, checkpoint }
    }

    /// Borrow the current page cursor.
    pub fn cursor(self) -> Option<&'a Cursor> {
        self.cursor
    }

    /// Borrow the checkpoint supplied when the session began.
    pub fn checkpoint(self) -> Option<&'a Checkpoint> {
        self.checkpoint
    }
}

/// Connector-specific request construction and page decoding.
#[async_trait]
pub trait Source: Send + Sync {
    /// Record emitted by this source.
    type Record: Send + 'static;
    /// Ephemeral cursor used to paginate one sync run.
    type Cursor: Clone + PartialEq + Send + Sync + 'static;
    /// Durable incremental-sync checkpoint owned by the caller.
    type Checkpoint: Clone + Send + Sync + 'static;
    /// Source-specific construction or decoding error.
    type Error: Error + Send + Sync + 'static;

    /// Build the next outbound HTTP request.
    ///
    /// Authentication middleware runs after this method returns.
    ///
    /// # Errors
    ///
    /// Returns the source error when configuration or sync state cannot be
    /// represented as a request.
    async fn build_request(
        &self,
        position: SyncPosition<'_, Self::Cursor, Self::Checkpoint>,
    ) -> Result<Request, Self::Error>;

    /// Decode one HTTP response into records and sync state.
    ///
    /// # Errors
    ///
    /// Returns the source error for rejected status codes, malformed responses,
    /// or invalid pagination metadata.
    async fn decode_page(
        &self,
        response: Response,
        position: SyncPosition<'_, Self::Cursor, Self::Checkpoint>,
    ) -> Result<Page<Self::Record, Self::Cursor, Self::Checkpoint>, Self::Error>;
}

/// Failure while advancing a [`SyncSession`].
#[derive(Debug, Error)]
pub enum IngestError<E>
where
    E: Error + 'static,
{
    /// The source could not build or decode a page.
    #[error("source failed: {0}")]
    Source(#[source] E),

    /// Authentication or HTTP transport failed.
    #[error(transparent)]
    Http(#[from] HttpError),

    /// A source returned the same cursor as the request it just decoded.
    ///
    /// Only adjacent repetition is detected; longer cursor cycles require a
    /// connector-specific pagination strategy or a hashable cursor guard.
    #[error("source returned the current pagination cursor again")]
    RepeatedCursor,
}

/// Stateful driver for one paginated incremental sync.
///
/// A session retains the initial resume checkpoint for every request and tracks
/// page cursors in memory. It never persists a checkpoint.
pub struct SyncSession<S>
where
    S: Source,
{
    source: S,
    client: HttpClient,
    resume_checkpoint: Option<S::Checkpoint>,
    cursor: Option<S::Cursor>,
    finished: bool,
}

impl<S> SyncSession<S>
where
    S: Source,
{
    /// Begin a sync without a durable resume checkpoint.
    pub fn new(source: S, client: HttpClient) -> Self {
        Self::with_checkpoint(source, client, None)
    }

    /// Resume a sync from a durable checkpoint.
    pub fn resume(source: S, client: HttpClient, checkpoint: S::Checkpoint) -> Self {
        Self::with_checkpoint(source, client, Some(checkpoint))
    }

    fn with_checkpoint(source: S, client: HttpClient, checkpoint: Option<S::Checkpoint>) -> Self {
        Self {
            source,
            client,
            resume_checkpoint: checkpoint,
            cursor: None,
            finished: false,
        }
    }

    /// Borrow the source.
    pub fn source(&self) -> &S {
        &self.source
    }

    /// Return whether the terminal page has been emitted.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Fetch and decode the next page.
    ///
    /// Returns `Ok(None)` after the terminal page. The caller must durably
    /// consume a returned page's records before persisting its checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`IngestError::Source`] for connector-specific failures,
    /// [`IngestError::Http`] for middleware or transport failures, and
    /// [`IngestError::RepeatedCursor`] when the source returns the current
    /// cursor again. Longer cursor cycles are not detected.
    pub async fn next_page(
        &mut self,
    ) -> Result<Option<Page<S::Record, S::Cursor, S::Checkpoint>>, IngestError<S::Error>> {
        if self.finished {
            return Ok(None);
        }

        let position = SyncPosition::new(self.cursor.as_ref(), self.resume_checkpoint.as_ref());
        let request = self
            .source
            .build_request(position)
            .await
            .map_err(IngestError::Source)?;
        let response = self.client.execute(request).await?;
        let page = self
            .source
            .decode_page(response, position)
            .await
            .map_err(IngestError::Source)?;

        if let (Some(current), Some(next)) = (self.cursor.as_ref(), page.next_cursor())
            && current == next
        {
            return Err(IngestError::RepeatedCursor);
        }

        self.cursor.clone_from(&page.next_cursor);
        self.finished = self.cursor.is_none();
        Ok(Some(page))
    }
}
