use std::{future::Future, os::fd::AsRawFd, pin::Pin};

use async_trait::async_trait;
use camino::{Utf8Path, Utf8PathBuf};
use color_eyre::eyre::{Report, Result};
use tokio::{
    fs::File as AsyncFile,
    io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt},
};
use tracing::instrument;

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

#[derive(Debug)]
pub struct ArtifactWriter {
    inner: AsyncFile,
}

impl ArtifactWriter {
    pub fn new(inner: AsyncFile) -> Self {
        Self { inner }
    }

    #[instrument]
    pub async fn new_anon() -> Result<Self> {
        let file = tokio::task::spawn_blocking(tempfile::tempfile).await??;
        Ok(Self {
            inner: AsyncFile::from_std(file),
        })
    }

    pub async fn into_reader(mut self) -> Result<ArtifactReader> {
        self.flush().await?;
        self.rewind().await?;
        Ok(ArtifactReader { inner: self.inner })
    }

    pub async fn get_reader(&mut self) -> Result<ArtifactReader> {
        ArtifactReader::from_async_file(&self.inner).await
    }
}

impl AsyncRead for ArtifactWriter {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl AsyncSeek for ArtifactWriter {
    fn start_seek(self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
        Pin::new(&mut self.get_mut().inner).start_seek(position)
    }

    fn poll_complete(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        Pin::new(&mut self.get_mut().inner).poll_complete(cx)
    }
}

impl AsyncWrite for ArtifactWriter {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::result::Result<usize, std::io::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[derive(Debug)]
pub struct ArtifactReader {
    inner: AsyncFile,
}

impl ArtifactReader {
    pub fn new(inner: AsyncFile) -> Self {
        Self { inner }
    }

    pub async fn from_async_file(file: &AsyncFile) -> Result<Self> {
        let inner = AsyncFile::options()
            .read(true)
            .write(true)
            .open(format!("/proc/self/fd/{}", file.as_raw_fd()))
            .await?;
        Ok(Self { inner })
    }

    pub async fn try_clone(&self) -> Result<Self> {
        Self::from_async_file(&self.inner).await
    }
}

impl From<ArtifactReader> for reqwest::Body {
    fn from(value: ArtifactReader) -> Self {
        Self::from(value.inner)
    }
}

impl AsyncRead for ArtifactReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl AsyncSeek for ArtifactReader {
    fn start_seek(self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
        Pin::new(&mut self.get_mut().inner).start_seek(position)
    }

    fn poll_complete(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        Pin::new(&mut self.get_mut().inner).poll_complete(cx)
    }
}

pub trait SaveCallback<'a, T, E>: FnOnce(&'a mut ArtifactWriter) -> Self::Fut + Send {
    type Fut: Future<Output = Result<T, E>> + Send;
}

impl<'a, T, E, Out, F> SaveCallback<'a, T, E> for F
where
    Out: Future<Output = Result<T, E>> + Send,
    F: FnOnce(&'a mut ArtifactWriter) -> Out + Send,
{
    type Fut = Out;
}

#[async_trait]
pub trait ArtifactDirectory: Send + Sync {
    async fn open(&self, path: impl AsRef<Utf8Path> + Send) -> Result<ArtifactReader>;

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
        F: for<'a> SaveCallback<'a, Ret, Err> + Send,
        P: AsRef<Utf8Path> + Send;

    async fn write(&mut self, path: impl AsRef<Utf8Path> + Send, data: &[u8]) -> Result<()> {
        self.save_with(path, async |file: &mut ArtifactWriter| {
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

    use super::*;

    #[derive(Default)]
    pub struct MockArtifactDirectory {
        artifacts: HashMap<Utf8PathBuf, ArtifactReader>,
    }

    #[async_trait]
    impl ArtifactDirectory for MockArtifactDirectory {
        async fn open(&self, path: impl AsRef<Utf8Path> + Send) -> Result<ArtifactReader> {
            let file = self
                .artifacts
                .get(path.as_ref())
                .ok_or_else(|| eyre!(MissingArtifact(path.as_ref().to_owned())))?;
            file.try_clone().await
        }

        async fn save_with<Ret, Err, F, P>(&mut self, path: P, func: F) -> Result<Ret>
        where
            Report: From<Err>,
            Ret: Send,
            Err: Send,
            F: for<'a> SaveCallback<'a, Ret, Err> + Send,
            P: AsRef<Utf8Path> + Send,
        {
            let mut writer = ArtifactWriter::new_anon().await?;
            let ret = func(&mut writer).await?;
            self.artifacts
                .insert(path.as_ref().to_owned(), writer.into_reader().await?);
            Ok(ret)
        }
    }
}
