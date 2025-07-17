use open_build_service_api as obs;
use open_build_service_mock::*;

pub const TEST_USER: &str = "user";
pub const TEST_PASS: &str = "pass";

pub const TEST_PROJECT: &str = "foo";
pub const TEST_PACKAGE_1: &str = "bar";
pub const TEST_PACKAGE_2: &str = "baz";
pub const TEST_REPO: &str = "repo";
pub const TEST_ARCH_1: &str = "aarch64";
pub const TEST_ARCH_2: &str = "x86_64";

pub async fn create_default_mock() -> ObsMock {
    ObsMock::start(TEST_USER, TEST_PASS).await
}

pub fn create_default_client(mock: &ObsMock) -> obs::Client {
    obs::Client::new(mock.uri(), TEST_USER.to_owned(), TEST_PASS.to_owned())
}
