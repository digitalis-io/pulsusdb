//! One read request, run once more on another server when its own fails
//! (issue #803).
//!
//! Every ClickHouse statement of one PulsusDB read request goes to the one
//! server its client was bound to when the request started. If that server
//! fails during the request, the attempt's partial results are dropped, the
//! server is demoted, and the whole request runs once more from the start on
//! the next healthy server. If that also fails, the error is returned.
//!
//! Only the accepted attempt's value leaves [`run_read_request`]: a caller
//! saves whatever it keeps from the request (a cache entry, a snapshot)
//! after `run_read_request` returns, so nothing a discarded attempt built can
//! be saved.

use std::future::Future;

use crate::pool::{ChPool, ServerFailure};

/// One run of a read request: its result, and the first server failure any
/// of its reads met (its client's [`crate::ChClient::server_failure`]).
pub struct Attempt<T, E> {
    pub result: Result<T, E>,
    pub failure: Option<ServerFailure>,
}

impl<T, E> Attempt<T, E> {
    /// An attempt that failed before it sent a statement (its engine or
    /// client could not be built): its error, and no server failure.
    pub fn failed_to_start(e: E) -> Self {
        Self {
            result: Err(e),
            failure: None,
        }
    }
}

/// Runs `attempt`, and once more when it met a server failure and another
/// endpoint is healthy. Never a third time.
///
/// An attempt that met a server failure is not accepted even when its
/// result is `Ok`: with no other healthy endpoint, or on the second attempt,
/// such an `Ok` becomes `Err(failed_after_success(failure))`, and its value
/// is dropped.
///
/// `attempt` is given the endpoint it must not bind: `None` for the first
/// attempt, and the failed server's for the second, so the second never goes
/// back to the server that just failed even when it is healthy again.
pub async fn run_read_request<T, E, F, Fut>(
    pool: &ChPool,
    mut attempt: F,
    failed_after_success: impl FnOnce(ServerFailure) -> E,
) -> Result<T, E>
where
    F: FnMut(Option<usize>) -> Fut,
    Fut: Future<Output = Attempt<T, E>>,
{
    let first = attempt(None).await;
    let Some(failure) = first.failure else {
        return first.result;
    };
    if !pool.other_healthy(failure.endpoint) {
        return settle(first.result, failure, failed_after_success);
    }
    // Everything attempt 1 built is dropped before attempt 2 starts.
    drop(first.result);
    let second = attempt(Some(failure.endpoint)).await;
    match second.failure {
        None => second.result,
        Some(failure) => settle(second.result, failure, failed_after_success),
    }
}

/// The result of an attempt that met a server failure and is not run
/// again: its own error, or the failure's when it was `Ok`.
fn settle<T, E>(
    result: Result<T, E>,
    failure: ServerFailure,
    failed_after_success: impl FnOnce(ServerFailure) -> E,
) -> Result<T, E> {
    match result {
        Err(e) => Err(e),
        Ok(_) => Err(failed_after_success(failure)),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::config::ReadSpread;
    use crate::pool::test_pool;

    const AFTER: &str = "failed after success";

    fn f() -> ServerFailure {
        ServerFailure {
            endpoint: 0,
            message: "reset".to_string(),
        }
    }

    fn two_endpoints() -> ChPool {
        test_pool(
            vec![(None, "http://a:8123"), (None, "http://b:8123")],
            None,
            ReadSpread::All,
            8,
        )
    }

    fn one_endpoint() -> ChPool {
        test_pool(vec![(None, "http://a:8123")], None, ReadSpread::All, 8)
    }

    /// Runs `script` through [`run_read_request`], one entry per attempt,
    /// and returns the result and the number of attempts made.
    async fn run(
        pool: &ChPool,
        script: Vec<(Result<i32, &'static str>, Option<ServerFailure>)>,
    ) -> (Result<i32, &'static str>, usize) {
        let script = Mutex::new(VecDeque::from(script));
        let calls = AtomicUsize::new(0);
        let (script, calls_ref) = (&script, &calls);
        let out = run_read_request(
            pool,
            move |_avoid| async move {
                calls_ref.fetch_add(1, Ordering::SeqCst);
                let (result, failure) = script
                    .lock()
                    .expect("script lock")
                    .pop_front()
                    .expect("no more attempts were scripted");
                Attempt { result, failure }
            },
            |_| AFTER,
        )
        .await;
        (out, calls.load(Ordering::SeqCst))
    }

    #[tokio::test]
    async fn a_request_without_failure_runs_once() {
        let (out, calls) = run(&two_endpoints(), vec![(Ok(1), None)]).await;
        assert_eq!(out, Ok(1));
        assert_eq!(calls, 1);
    }

    #[tokio::test]
    async fn a_failed_request_runs_once_more_and_the_first_result_is_dropped() {
        let (out, calls) = run(&two_endpoints(), vec![(Ok(1), Some(f())), (Ok(2), None)]).await;
        assert_eq!(out, Ok(2));
        assert_eq!(calls, 2);
    }

    #[tokio::test]
    async fn a_second_failure_returns_the_second_error() {
        let (out, calls) = run(
            &two_endpoints(),
            vec![(Err("e1"), Some(f())), (Err("e2"), Some(f()))],
        )
        .await;
        assert_eq!(out, Err("e2"));
        assert_eq!(calls, 2);
    }

    #[tokio::test]
    async fn a_second_failure_after_success_is_an_error() {
        let (out, calls) = run(
            &two_endpoints(),
            vec![(Ok(1), Some(f())), (Ok(2), Some(f()))],
        )
        .await;
        assert_eq!(out, Err(AFTER));
        assert_eq!(calls, 2);
    }

    #[tokio::test]
    async fn one_endpoint_does_not_run_again() {
        let (out, calls) = run(&one_endpoint(), vec![(Err("e1"), Some(f()))]).await;
        assert_eq!(out, Err("e1"));
        assert_eq!(calls, 1);
    }

    #[tokio::test]
    async fn one_endpoint_ok_with_a_failure_is_an_error() {
        let (out, calls) = run(&one_endpoint(), vec![(Ok(1), Some(f()))]).await;
        assert_eq!(out, Err(AFTER));
        assert_eq!(calls, 1);
    }
}
