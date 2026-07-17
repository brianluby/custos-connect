//! Black-box acceptance tests for Faucet's REST ingestion behavior.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use custos_connect_faucet_spike::{TypedSourceError, typed_pages};
use faucet_core::{
    FaucetError, FileStateStore, Pipeline, ReplicationMethod, Sink, StateStore, Value,
};
use faucet_source_rest::{Auth, PaginationStyle, RestStream, RestStreamConfig};
use futures::StreamExt;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::Mutex;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path, query_param, query_param_is_missing},
};

#[derive(Debug, Deserialize, PartialEq)]
struct Event {
    id: u64,
    updated_at: String,
}

#[derive(Default)]
struct RecordingSink {
    records: Mutex<Vec<Value>>,
}

impl RecordingSink {
    async fn snapshot(&self) -> Vec<Value> {
        self.records.lock().await.clone()
    }
}

#[async_trait]
impl Sink for RecordingSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.records.lock().await.extend_from_slice(records);
        Ok(records.len())
    }
}

fn source_config(server: &MockServer) -> RestStreamConfig {
    RestStreamConfig::new(&server.uri(), "/events")
        .auth(Auth::Bearer {
            token: "test-token".to_owned(),
        })
        .records_path("$.data[*]")
        .pagination(PaginationStyle::Cursor {
            next_token_path: "$.next_cursor".to_owned(),
            param_name: "cursor".to_owned(),
        })
        .replication_method(ReplicationMethod::Incremental)
        .replication_key("updated_at")
        .state_key("events")
}

async fn mount_paginated_feed(server: &MockServer, newest_id: u64, newest_date: &str) {
    Mock::given(method("GET"))
        .and(path("/events"))
        .and(header("authorization", "Bearer test-token"))
        .and(query_param_is_missing("cursor"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [
                {"id": 1, "updated_at": "2026-07-15"},
                {"id": 2, "updated_at": "2026-07-16"}
            ],
            "next_cursor": "page-2"
        })))
        .expect(1)
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/events"))
        .and(header("authorization", "Bearer test-token"))
        .and(query_param("cursor", "page-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [
                {"id": newest_id, "updated_at": newest_date}
            ],
            "next_cursor": null
        })))
        .expect(1)
        .mount(server)
        .await;
}

#[tokio::test]
async fn typed_stream_preserves_authenticated_pagination_and_checkpoint()
-> Result<(), TypedSourceError> {
    let server = MockServer::start().await;
    mount_paginated_feed(&server, 3, "2026-07-17").await;
    let source = RestStream::new(source_config(&server))?;
    let context = HashMap::new();
    let mut pages = typed_pages::<_, Event>(&source, &context, 100);

    let first = pages
        .next()
        .await
        .transpose()?
        .ok_or_else(|| FaucetError::Config("expected the first source page".to_owned()))?;
    let second = pages
        .next()
        .await
        .transpose()?
        .ok_or_else(|| FaucetError::Config("expected the second source page".to_owned()))?;

    assert_eq!(first.records.len(), 2);
    assert_eq!(first.checkpoint, None);
    assert_eq!(second.records[0].id, 3);
    assert_eq!(second.records[0].updated_at, "2026-07-17");
    assert_eq!(second.checkpoint, Some(json!("2026-07-17")));
    assert!(pages.next().await.is_none());
    Ok(())
}

#[tokio::test]
async fn pipeline_resumes_from_a_durable_bookmark() -> Result<(), Box<dyn std::error::Error>> {
    let state_dir = tempfile::tempdir()?;
    let store: Arc<dyn StateStore> = Arc::new(FileStateStore::new(state_dir.path()));

    let first_server = MockServer::start().await;
    mount_paginated_feed(&first_server, 3, "2026-07-17").await;
    let first_source = RestStream::new(source_config(&first_server))?;
    let first_sink = RecordingSink::default();
    let first_result = Pipeline::new(&first_source, &first_sink)
        .with_state_store(Arc::clone(&store))
        .run()
        .await?;

    assert_eq!(first_result.records_written, 3);
    assert_eq!(store.get("events").await?, Some(json!("2026-07-17")));

    let second_server = MockServer::start().await;
    mount_paginated_feed(&second_server, 4, "2026-07-18").await;
    let second_source = RestStream::new(source_config(&second_server))?;
    let second_sink = RecordingSink::default();
    let second_result = Pipeline::new(&second_source, &second_sink)
        .with_state_store(Arc::clone(&store))
        .run()
        .await?;

    assert_eq!(second_result.records_written, 1);
    assert_eq!(second_sink.snapshot().await[0]["id"], 4);
    assert_eq!(store.get("events").await?, Some(json!("2026-07-18")));
    Ok(())
}
