use std::{sync::Arc, time::Duration};

use backoff::ExponentialBackoff;
use futures_util::Future;

use color_eyre::{eyre::Result, Report};
use open_build_service_api as obs;
use tokio::sync::Mutex;
use tracing::{error, instrument};

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

async fn retry_request_impl<T, E, Fut, Func>(backoff_limit: Duration, func: Func) -> Result<T>
where
    Fut: Future<Output = Result<T, E>>,
    Func: FnMut() -> Fut,
    E: Into<Report>,
{
    let func = Arc::new(Mutex::new(func));
    backoff::future::retry(
        ExponentialBackoff {
            max_elapsed_time: Some(backoff_limit),
            ..Default::default()
        },
        move || {
            let func = func.clone();
            async move {
                let mut func = func.lock().await;
                func().await.map_err(|err| {
                    let report = err.into();
                    error!(?report);
                    if is_caused_by_client_error(&report) {
                        backoff::Error::permanent(report)
                    } else {
                        backoff::Error::transient(report)
                    }
                })
            }
        },
    )
    .await
}

#[instrument(skip(func))]
pub async fn retry_request<T, E, Fut, Func>(func: Func) -> Result<T>
where
    Fut: Future<Output = Result<T, E>>,
    Func: FnMut() -> Fut,
    E: Into<Report>,
{
    const BACKOFF_LIMIT: Duration = Duration::from_secs(10 * 60); // 10 minutes
    retry_request_impl(BACKOFF_LIMIT, func).await
}

#[instrument(skip(func))]
pub async fn retry_large_request<T, E, Fut, Func>(func: Func) -> Result<T>
where
    Fut: Future<Output = Result<T, E>>,
    Func: FnMut() -> Fut,
    E: Into<Report>,
{
    const BACKOFF_LIMIT: Duration = Duration::from_secs(60 * 60); // 1 hour
    retry_request_impl(BACKOFF_LIMIT, func).await
}

#[cfg(test)]
mod tests {
    use claim::*;
    use open_build_service_api as obs;
    use wiremock::{
        matchers::{method, path_regex},
        Mock, MockServer, ResponseTemplate,
    };

    use crate::test_support::*;

    use super::*;

    const LIMIT: Duration = Duration::from_secs(1);

    #[tokio::test]
    async fn test_retry_on_non_client_errors() {
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

        let client = obs::Client::new(
            server.uri().parse().unwrap(),
            TEST_USER.to_owned(),
            TEST_PASS.to_owned(),
        );

        let mut attempts = 0;
        assert_err!(
            retry_request_impl(LIMIT, || {
                attempts += 1;
                async { client.project("500".to_owned()).meta().await }
            })
            .await
        );
        assert_gt!(attempts, 1);

        let mut attempts = 0;
        assert_err!(
            retry_request_impl(LIMIT, || {
                attempts += 1;
                async {
                    client
                        .project("500".to_owned())
                        .meta()
                        .await
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                }
            })
            .await
        );
        assert_gt!(attempts, 1);

        attempts = 0;
        assert_err!(
            retry_request_impl(LIMIT, || {
                attempts += 1;
                async { client.project("403".to_owned()).meta().await }
            })
            .await
        );
        assert_eq!(attempts, 1);

        attempts = 0;
        assert_err!(
            retry_request_impl(LIMIT, || {
                attempts += 1;
                async {
                    client
                        .project("403".to_owned())
                        .meta()
                        .await
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                }
            })
            .await
        );
        assert_eq!(attempts, 1);
    }
}
