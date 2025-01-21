use serde::{
    de::{IntoDeserializer, Visitor},
    Deserialize, Deserializer,
};
use std::marker::PhantomData;

#[derive(Debug)]
pub struct FileEntry {
    pub hash: String,
    pub size: usize,
    pub filename: String,
}

impl<'de> Deserialize<'de> for FileEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct FileEntryVisitor;

        impl Visitor<'_> for FileEntryVisitor {
            type Value = FileEntry;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a file entry line")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let parts: Vec<&str> = v.split(' ').collect();
                if parts.len() != 3 {
                    return Err(E::custom("invalid number of components in line"));
                }

                let size = match parts[1].parse() {
                    Ok(size) => size,
                    Err(_) => return Err(E::custom("invalid size value")),
                };

                Ok(FileEntry {
                    hash: parts[0].to_owned(),
                    size,
                    filename: parts[2].to_owned(),
                })
            }
        }

        deserializer.deserialize_string(FileEntryVisitor)
    }
}

fn de_line_delimited<'de, T, D>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    struct SplitVisitor<T> {
        phantom: PhantomData<T>,
    }

    impl<'de, T> Visitor<'de> for SplitVisitor<T>
    where
        T: Deserialize<'de>,
    {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a string")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            let mut result = vec![];
            for part in v.trim().split('\n') {
                result.push(T::deserialize(part.into_deserializer())?);
            }
            Ok(result)
        }
    }

    deserializer.deserialize_string(SplitVisitor {
        phantom: PhantomData,
    })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Dsc {
    pub source: String,
    // Note that this is 'default' solely so the tests can avoid creating dummy
    // file lists.
    #[serde(default, deserialize_with = "de_line_delimited")]
    pub files: Vec<FileEntry>,
}

#[cfg(test)]
mod tests {
    use claim::*;

    use super::*;

    const TEST_DSC: &str = "Source: abc
SomeUnknownAttribute: 10
Files:
 hash1 10 file1
 hash2 27 file2";

    #[test]
    fn test_de() {
        let dsc: Dsc = assert_ok!(rfc822_like::from_str(TEST_DSC));

        assert_eq!(dsc.source, "abc");
        assert_eq!(dsc.files.len(), 2);

        assert_eq!(dsc.files[0].hash, "hash1");
        assert_eq!(dsc.files[0].size, 10);
        assert_eq!(dsc.files[0].filename, "file1");

        assert_eq!(dsc.files[1].hash, "hash2");
        assert_eq!(dsc.files[1].size, 27);
        assert_eq!(dsc.files[1].filename, "file2");
    }
}
