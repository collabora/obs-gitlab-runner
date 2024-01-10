use std::time::Duration;

use backoff::ExponentialBackoff;
use futures_util::Future;

use color_eyre::{eyre::Result, Report};
use open_build_service_api as obs;
use tracing::instrument;

const INITIAL_INTERVAL: Duration = Duration::from_millis(300);

fn is_client_error(err: &(dyn std::error::Error + 'static)) -> bool {
    err.downcast_ref::<reqwest::Error>()
        .and_then(|e| e.status())
        .map_or(false, |status| status.is_client_error())
        || err
            .downcast_ref::<obs::Error>()
            .map_or(false, |err| matches!(err, obs::Error::ApiError(_)))
}

fn is_caused_by_client_error(report: &Report) -> bool {
    report.chain().any(|err| {
        is_client_error(err)
            // Custom IO errors do not return the custom error itself when its
            // .source() is called (instead returning the custom error's
            // source()), so we need to examine that error directly.
            || err
                .downcast_ref::<std::io::Error>()
                .and_then(|err| err.get_ref())
                .map_or(false, |err| is_client_error(err))
    })
}

#[instrument(skip(func))]
pub async fn retry_request<T, E, Fut, Func>(func: Func) -> Result<T>
where
    Fut: Future<Output = Result<T, E>>,
    Func: Fn() -> Fut,
    E: Into<Report>,
{
    backoff::future::retry(
        ExponentialBackoff {
            max_elapsed_time: None,
            initial_interval: INITIAL_INTERVAL,
            ..Default::default()
        },
        || async {
            func().await.map_err(|err| {
                let report = err.into();
                if is_caused_by_client_error(&report) {
                    backoff::Error::permanent(report)
                } else {
                    backoff::Error::transient(report)
                }
            })
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI32, Ordering};

    use claim::*;
    use open_build_service_api as obs;
    use rstest::*;
    use wiremock::{
        matchers::{method, path_regex},
        Mock, MockServer, ResponseTemplate,
    };

    use crate::test_support::*;

    use super::*;

    fn wrap_in_io_error(err: obs::Error) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::Other, err)
    }

    #[fixture]
    async fn server() -> MockServer {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path_regex("^/source/403/.*"))
            .respond_with(ResponseTemplate::new(403).set_body_raw(
                b"<status code='403'><summary>forbidden</summary></status>".to_vec(),
                "application/xml",
            ))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path_regex("^/source/500/.*"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        server
    }

    #[rstest]
    #[tokio::test]
    async fn test_retry_on_non_client_errors(server: impl Future<Output = MockServer>) {
        let server = server.await;
        let client = obs::Client::new(
            server.uri().parse().unwrap(),
            TEST_USER.to_owned(),
            TEST_PASS.to_owned(),
        );

        let attempts = AtomicI32::new(0);
        assert_err!(
            tokio::time::timeout(
                Duration::from_millis(2000),
                retry_request(|| {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async { client.project("500".to_owned()).meta().await }
                })
            )
            .await
        );
        assert_gt!(attempts.load(Ordering::SeqCst), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_retry_on_nested_non_client_errors(server: impl Future<Output = MockServer>) {
        let server = server.await;
        let client = obs::Client::new(
            server.uri().parse().unwrap(),
            TEST_USER.to_owned(),
            TEST_PASS.to_owned(),
        );

        let attempts = AtomicI32::new(0);
        assert_err!(
            tokio::time::timeout(
                Duration::from_millis(2000),
                retry_request(|| {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async {
                        client
                            .project("500".to_owned())
                            .meta()
                            .await
                            .map_err(wrap_in_io_error)
                    }
                })
            )
            .await
        );
        assert_gt!(attempts.load(Ordering::SeqCst), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_no_retry_on_client_errors(server: impl Future<Output = MockServer>) {
        let server = server.await;
        let client = obs::Client::new(
            server.uri().parse().unwrap(),
            TEST_USER.to_owned(),
            TEST_PASS.to_owned(),
        );

        let attempts = AtomicI32::new(0);
        assert_err!(
            retry_request(|| {
                attempts.fetch_add(1, Ordering::SeqCst);
                async { client.project("403".to_owned()).meta().await }
            })
            .await
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_no_retry_on_nested_client_errors(server: impl Future<Output = MockServer>) {
        let server = server.await;
        let client = obs::Client::new(
            server.uri().parse().unwrap(),
            TEST_USER.to_owned(),
            TEST_PASS.to_owned(),
        );

        let attempts = AtomicI32::new(0);
        assert_err!(
            retry_request(|| {
                attempts.fetch_add(1, Ordering::SeqCst);
                async {
                    client
                        .project("403".to_owned())
                        .meta()
                        .await
                        .map_err(wrap_in_io_error)
                }
            })
            .await
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}
