//! Which read failures demote a server and mark the read request (issue
//! #803), against a stub ClickHouse: a read that times out (the client's
//! own stream deadline, or the server's code 159) neither demotes its
//! server nor marks the request; a network-class code does both.
//!
//! No database: the stub is a std-thread server on an ephemeral loopback port
//! (`common/stub_server.rs`).

#[path = "common/stub_server.rs"]
mod stub_server;

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{
    ChClient, ChConnConfig, ChError, ChPool, ChProto, QuerySettings, Row, ServerFailure,
};
use stub_server::{Mode, StubServer};

#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct CountRow {
    c: u64,
}

/// One read through a client over the stub alone, with a one-second
/// deadline: the read's outcome, the endpoint's health afterwards, and the
/// client's recorded server failure.
async fn read_once(mode: Mode) -> (Result<(), ChError>, Vec<bool>, Option<ServerFailure>) {
    let stub = StubServer::start(mode);
    let cfg = ChConnConfig {
        server: "127.0.0.1".to_string(),
        http_port: stub.addr.port(),
        database: "default".to_string(),
        proto: ChProto::Http,
        pool_size: 1,
        query_timeout: Duration::from_secs(1),
        ..ChConnConfig::default()
    };
    let pool = Arc::new(ChPool::connect(cfg).await.expect("connect to the stub"));
    let client = ChClient::from_shared_pool(Arc::clone(&pool), Duration::from_secs(1));
    let result = async {
        let mut stream = client
            .query_stream::<CountRow>("SELECT toUInt64(1) AS c", &QuerySettings::new())
            .await?;
        while let Some(row) = stream.next().await {
            row?;
        }
        Ok(())
    }
    .await;
    let health = pool.endpoint_health().into_iter().map(|(_, h)| h).collect();
    (result, health, client.server_failure())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_deadline_neither_demotes_nor_fails_the_request() {
    let (result, health, failure) = read_once(Mode::Silent).await;
    assert!(
        matches!(result, Err(ChError::Timeout(_))),
        "the stream deadline ends the read: {result:?}"
    );
    assert_eq!(health, vec![true], "the server stays up");
    assert_eq!(failure, None, "the request is not marked");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_server_side_read_timeout_neither_demotes_nor_fails_the_request() {
    let (result, health, failure) = read_once(Mode::Code(159)).await;
    assert!(
        matches!(result, Err(ChError::Server { code: 159, .. })),
        "the server's timeout is the error: {result:?}"
    );
    assert_eq!(health, vec![true], "the server stays up");
    assert_eq!(failure, None, "the request is not marked");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_network_error_demotes_and_fails_the_request() {
    let (result, health, failure) = read_once(Mode::Code(210)).await;
    assert!(
        matches!(result, Err(ChError::Server { code: 210, .. })),
        "the network code is the error: {result:?}"
    );
    assert_eq!(health, vec![false], "the server is demoted");
    assert!(failure.is_some(), "the request is marked");
}
