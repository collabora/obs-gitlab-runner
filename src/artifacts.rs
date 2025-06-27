use std::{
    fs::File,
    io::{Read, Seek},
};

use async_trait::async_trait;
use color_eyre::eyre::{Result, eyre};
use tokio::fs::File as AsyncFile;
use tracing::instrument;

#[instrument(skip(reader))]
pub fn save_to_tempfile<R: Read>(reader: &mut R) -> Result<File> {
    let mut temp = tempfile::tempfile()?;
    std::io::copy(reader, &mut temp)?;
    temp.rewind()?;
    Ok(temp)
}

#[instrument(skip(reader))]
pub async fn async_save_to_tempfile<R: Read + Send + 'static>(mut reader: R) -> Result<AsyncFile> {
    let file = tokio::task::spawn_blocking(move || save_to_tempfile(&mut reader)).await??;
    Ok(AsyncFile::from_std(file))
}

#[async_trait]
pub trait ArtifactDirectory: Send + Sync {
    type Reader: Read + Send + 'static;

    async fn get_or_none(&self, filename: &str) -> Result<Option<Self::Reader>>;

    async fn get(&self, filename: &str) -> Result<Self::Reader> {
        self.get_or_none(filename)
            .await?
            .ok_or_else(|| eyre!("Could not find artifact '{}'", filename))
    }

    async fn get_file_or_none(&self, filename: &str) -> Result<Option<AsyncFile>> {
        let reader = self.get_or_none(filename).await?;
        Ok(if let Some(reader) = reader {
            Some(async_save_to_tempfile(reader).await?)
        } else {
            None
        })
    }

    async fn get_file(&self, filename: &str) -> Result<AsyncFile> {
        let reader = self.get(filename).await?;
        async_save_to_tempfile(reader).await
    }

    async fn get_data_or_none(&self, filename: &str) -> Result<Option<Vec<u8>>> {
        Ok(
            if let Some(mut reader) = self.get_or_none(filename).await? {
                let mut data = Vec::new();
                reader.read_to_end(&mut data)?;
                Some(data)
            } else {
                None
            },
        )
    }

    async fn get_data(&self, filename: &str) -> Result<Vec<u8>> {
        let mut reader = self.get(filename).await?;

        let mut data = Vec::new();
        reader.read_to_end(&mut data)?;
        Ok(data)
    }
}

#[cfg(test)]
pub mod test_support {
    use std::{collections::HashMap, sync::Arc};

    use super::*;

    pub struct MockArtifactReader {
        artifact: Arc<Vec<u8>>,
        pos: usize,
    }

    impl Read for MockArtifactReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let bytes_remaining = self.artifact.len() - self.pos;
            let bytes_to_read = std::cmp::min(bytes_remaining, buf.len());
            let end_pos = self.pos + bytes_to_read;

            buf[..bytes_to_read].copy_from_slice(&self.artifact[self.pos..end_pos]);
            self.pos = end_pos;
            Ok(bytes_to_read)
        }
    }

    pub struct MockArtifactDirectory {
        pub artifacts: HashMap<String, Arc<Vec<u8>>>,
    }

    #[async_trait]
    impl ArtifactDirectory for MockArtifactDirectory {
        type Reader = MockArtifactReader;

        async fn get_or_none(&self, filename: &str) -> Result<Option<Self::Reader>> {
            Ok(self
                .artifacts
                .get(filename)
                .cloned()
                .map(|artifact| MockArtifactReader { artifact, pos: 0 }))
        }
    }
}
