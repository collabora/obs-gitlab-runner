use std::time::Duration;

use color_eyre::{Report, eyre::Result};
use open_build_service_api as obs;
use tokio_retry2::strategy::{FibonacciBackoff, jitter};

fn is_retriable_error(err: &(dyn std::error::Error + 'static)) -> bool {
    if let Some(err) = err.downcast_ref::<reqwest::Error>() {
        return err.status().is_none_or(|status| !status.is_client_error());
    }

    if let Some(err) = err.downcast_ref::<obs::Error>() {
        return !matches!(*err, obs::Error::ApiError(_));
    }

    false
}

fn is_caused_by_retriable_error(report: &Report) -> bool {
    report.chain().any(|err| {
        is_retriable_error(err)
            // Custom IO errors do not return the custom error itself when its
            // .source() is called (instead returning the custom error's
            // source()), so we need to examine that error directly.
            || err
                .downcast_ref::<std::io::Error>()
                .and_then(|err| err.get_ref()).is_some_and(|err| is_retriable_error(err))
    })
}

pub struct Retrier {
    backoff: std::iter::Map<FibonacciBackoff, fn(Duration) -> Duration>,
}

impl Default for Retrier {
    fn default() -> Self {
        Self {
            backoff: FibonacciBackoff::from_millis(900).map(jitter),
        }
    }
}

impl Retrier {
    pub async fn handle_request_error(&mut self, report: Report) -> Result<()> {
        if !is_caused_by_retriable_error(&report) {
            return Err(report);
        }

        let Some(delay) = self.backoff.next() else {
            return Err(report);
        };

        tokio::time::sleep(delay).await;
        Ok(())
    }
}

#[macro_export]
macro_rules! retry_request {
    ($expr:expr) => {
        'outer: {
            let mut retrier = $crate::retry::Retrier::default();
            loop {
                let ret: Result<_, _> = async { $expr }.await;
                match ret {
                    Ok(ret) => break 'outer Ok(ret),
                    Err(err) => {
                        if let Err(report) = retrier.handle_request_error(err.into()).await {
                            break 'outer Err(report);
                        }
                    }
                }
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use claims::*;
    use futures_util::Future;
    use open_build_service_api as obs;
    use rstest::*;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path_regex},
    };

    use crate::test_support::*;

    use super::*;

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

        let mut attempts = 0;

        assert_err!(
            tokio::time::timeout(Duration::from_millis(2000), async {
                retry_request!({
                    attempts += 1;
                    client.project("500".to_owned()).meta().await
                })
            })
            .await
        );

        assert_gt!(attempts, 1);
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

        let mut attempts = 0;
        assert_err!(
            tokio::time::timeout(Duration::from_millis(2000), async {
                retry_request!({
                    attempts += 1;
                    client
                        .project("500".to_owned())
                        .meta()
                        .await
                        .map_err(std::io::Error::other)
                })
            })
            .await
        );
        assert_gt!(attempts, 1);
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

        let mut attempts = 0;
        let _ = assert_err!(retry_request!({
            attempts += 1;
            client.project("403".to_owned()).meta().await
        }));
        assert_eq!(attempts, 1);
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

        let mut attempts = 0;
        let _ = assert_err!(retry_request!({
            attempts += 1;
            client
                .project("403".to_owned())
                .meta()
                .await
                .map_err(std::io::Error::other)
        }));
        assert_eq!(attempts, 1);
    }
}
