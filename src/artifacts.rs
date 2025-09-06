use std::{future::Future, os::fd::AsRawFd};

use async_trait::async_trait;
use camino::{Utf8Path, Utf8PathBuf};
use color_eyre::eyre::{Report, Result};
use tokio::{
    fs::File as AsyncFile,
    io::{AsyncReadExt, AsyncWriteExt},
};
use tracing::instrument;

#[instrument]
pub async fn async_tempfile() -> Result<AsyncFile> {
    let file = tokio::task::spawn_blocking(tempfile::tempfile).await??;
    Ok(AsyncFile::from_std(file))
}

#[derive(thiserror::Error, Debug)]
#[error("Could not find artifact '{0}'")]
pub struct MissingArtifact(pub Utf8PathBuf);

pub trait MissingArtifactToNone<T> {
    fn missing_artifact_to_none(self) -> Result<Option<T>>;
}

impl<T> MissingArtifactToNone<T> for Report {
    fn missing_artifact_to_none(self) -> Result<Option<T>> {
        if self.is::<MissingArtifact>() {
            Ok(None)
        } else {
            Err(self)
        }
    }
}

impl<T> MissingArtifactToNone<T> for Result<T> {
    fn missing_artifact_to_none(self) -> Result<Option<T>> {
        self.map(Some)
            .or_else(MissingArtifactToNone::missing_artifact_to_none)
    }
}

#[async_trait]
pub trait AsyncFileReopen: Sized {
    async fn reopen(&self) -> Result<Self>;
}

#[async_trait]
impl AsyncFileReopen for AsyncFile {
    // This is to "deep"-clone a file handle, such that the seek positions are
    // fully independent.
    async fn reopen(&self) -> Result<Self> {
        AsyncFile::options()
            .read(true)
            .write(true)
            .open(format!("/proc/self/fd/{}", self.as_raw_fd()))
            .await
            .map_err(|e| e.into())
    }
}

pub trait Callback<'a, T, E>: FnOnce(&'a mut AsyncFile) -> Self::Fut + Send {
    type Fut: Future<Output = Result<T, E>> + Send;
}

impl<'a, T, E, Out, F> Callback<'a, T, E> for F
where
    Out: Future<Output = Result<T, E>> + Send,
    F: FnOnce(&'a mut AsyncFile) -> Out + Send,
{
    type Fut = Out;
}

#[async_trait]
pub trait ArtifactDirectory: Send + Sync {
    async fn open(&self, path: impl AsRef<Utf8Path> + Send) -> Result<AsyncFile>;

    async fn read(&self, path: impl AsRef<Utf8Path> + Send) -> Result<Vec<u8>> {
        let mut buf = vec![];
        let file = self.open(path).await?;
        tokio::pin!(file);
        file.read_to_end(&mut buf).await?;
        Ok(buf)
    }

    async fn read_string(&self, path: impl AsRef<Utf8Path> + Send) -> Result<String> {
        self.read(path)
            .await
            .and_then(|data| String::from_utf8(data).map_err(|e| e.into()))
    }

    async fn save_with<Ret, Err, F, P>(&mut self, path: P, func: F) -> Result<Ret>
    where
        Report: From<Err>,
        Ret: Send,
        Err: Send,
        F: for<'a> Callback<'a, Ret, Err> + Send,
        P: AsRef<Utf8Path> + Send;

    async fn write(&mut self, path: impl AsRef<Utf8Path> + Send, data: &[u8]) -> Result<()> {
        self.save_with(path, async |file: &mut AsyncFile| {
            file.write_all(data).await
        })
        .await?;
        Ok(())
    }
}

#[cfg(test)]
pub mod test_support {
    use std::collections::HashMap;

    use camino::Utf8PathBuf;
    use color_eyre::eyre::eyre;
    use tokio::io::AsyncSeekExt;

    use super::*;

    #[derive(Default)]
    pub struct MockArtifactDirectory {
        artifacts: HashMap<Utf8PathBuf, AsyncFile>,
    }

    #[async_trait]
    impl ArtifactDirectory for MockArtifactDirectory {
        async fn open(&self, path: impl AsRef<Utf8Path> + Send) -> Result<AsyncFile> {
            let file = self
                .artifacts
                .get(path.as_ref())
                .ok_or_else(|| eyre!(MissingArtifact(path.as_ref().to_owned())))?;
            file.reopen().await
        }

        async fn save_with<Ret, Err, F, P>(&mut self, path: P, func: F) -> Result<Ret>
        where
            Report: From<Err>,
            Ret: Send,
            Err: Send,
            F: for<'a> Callback<'a, Ret, Err> + Send,
            P: AsRef<Utf8Path> + Send,
        {
            let mut file = async_tempfile().await?;
            let ret = func(&mut file).await?;

            file.flush().await?;
            file.rewind().await?;
            self.artifacts
                .insert(path.as_ref().to_owned(), file.reopen().await?);
            Ok(ret)
        }
    }
}
