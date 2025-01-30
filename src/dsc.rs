use serde::{
    Deserialize, Deserializer,
    de::{IntoDeserializer, Visitor},
};
use std::marker::PhantomData;

#[derive(Debug)]
pub struct FileEntry {
    pub hash: String,
    #[allow(dead_code)]
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

pub fn discard_pgp(dsc: &str) -> &str {
    const PGP_HEADER_START: &str = "-----BEGIN PGP SIGNED MESSAGE-----\n";
    const PGP_HEADER_END: &str = "\n\n";
    const PGP_FOOTER_START: &str = "-----BEGIN PGP SIGNATURE-----\n";

    if dsc.starts_with(PGP_HEADER_START) {
        if let Some((_gpg_header, payload_and_sig)) = dsc.split_once(PGP_HEADER_END) {
            if let Some((payload, _sig)) = payload_and_sig.split_once(PGP_FOOTER_START) {
                return payload;
            }
        }
    }

    dsc
}

#[cfg(test)]
mod tests {
    use claims::*;

    use super::*;

    const TEST_GPG_DSC: &str = "-----BEGIN PGP SIGNED MESSAGE-----
Hash: SHA512

Source: abc
SomeUnknownAttribute: 10
Files:
 hash1 10 file1
 hash2 27 file2
-----BEGIN PGP SIGNATURE-----

iQEzBAEBCgAdFiEEQxKqmcM1tb5xMnn0axtijDBFQh4FAmebtGQACgkQaxtijDBF
Qh6iAwf+NfOEM4+DbA8PPZnVz12bBqBNgMdaOx8CisQtd9xTmOMECaF3u2Vpfcha
zWRVtVh7Js2UidlHEwdzVJuNwrkneBoIHJEyOd/X2EXI8hOlU71OJGCyx1fayDNp
zf9Fe9kmlF9PJZRpB33YcTDSf5fYMNG2b4osa0ICOKssXoIbNVjaEPDdx3h/gsVm
x/JPxsxWjuM98ALa3ncn4UUPrn4QQfbu73qFEKyOLqhjCZxb52LG5/w96bXQodPS
Zhy+ZtJTpPJA9kuz9vZimQMPVimxUhYQQlBTl+3Bcg2Afw1X57H4MpkS+UPi16id
+DdRlEyxB4frFnYXK84u3VYR3Ml+8A==
=wcHv
-----END PGP SIGNATURE-----
";

    const TEST_DSC: &str = "Source: abc
SomeUnknownAttribute: 10
Files:
 hash1 10 file1
 hash2 27 file2
";

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

    #[test]
    fn test_gpg_de() {
        assert_eq!(discard_pgp(TEST_GPG_DSC), TEST_DSC);
    }
}
