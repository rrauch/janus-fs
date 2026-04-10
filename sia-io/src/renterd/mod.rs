pub mod client;
pub mod download;
pub mod object;

use crate::tagged::{TaggedValue, TryFromInner, WithFromStr, WithSerde};
use ct_codecs::{Decoder, Encoder, Hex};
use serde::de::Visitor;
use serde::ser::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::fmt;
use std::str::FromStr;
use thiserror::Error;
use zeroize::Zeroize;

#[derive(Error, Debug)]
pub enum BucketError {
    #[error("Bucket Name is invalid")]
    InvalidBucketName,
}

pub struct BucketKind;
pub type BucketName = TaggedValue<BucketKind, String>;
impl WithFromStr for BucketName {}
impl WithSerde for BucketName {}

impl Default for BucketName {
    fn default() -> Self {
        BucketName::from_str("default").unwrap()
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum BucketNameError {
    #[error("bucket name must be between 3 and 63 characters long, got {0}")]
    InvalidLength(usize),

    #[error("bucket name must start with a lowercase letter or number")]
    InvalidStart,

    #[error("bucket name must end with a lowercase letter or number")]
    InvalidEnd,

    #[error("bucket name contains invalid character '{0}' at position {1}")]
    InvalidCharacter(char, usize),

    #[error("bucket name must not contain consecutive periods")]
    ConsecutivePeriods,

    #[error("bucket name must not contain uppercase letters")]
    UppercaseLetter,
}

fn validate_bucket_name(name: &str) -> Result<(), BucketNameError> {
    let len = name.len();

    // Check length
    if !(3..=63).contains(&len) {
        return Err(BucketNameError::InvalidLength(len));
    }

    // Check for uppercase early (common mistake, give clear error)
    if name.chars().any(|c| c.is_ascii_uppercase()) {
        return Err(BucketNameError::UppercaseLetter);
    }

    // Check start character
    let first = name.chars().next().unwrap();
    if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
        return Err(BucketNameError::InvalidStart);
    }

    // Check end character
    let last = name.chars().last().unwrap();
    if !last.is_ascii_lowercase() && !last.is_ascii_digit() {
        return Err(BucketNameError::InvalidEnd);
    }

    // Check each character and consecutive periods
    let mut prev_was_period = false;
    for (i, c) in name.chars().enumerate() {
        match c {
            'a'..='z' | '0'..='9' | '-' => {
                prev_was_period = false;
            }
            '.' => {
                if prev_was_period {
                    return Err(BucketNameError::ConsecutivePeriods);
                }
                prev_was_period = true;
            }
            _ => {
                return Err(BucketNameError::InvalidCharacter(c, i));
            }
        }
    }

    Ok(())
}

impl TryFromInner<String> for BucketName {
    type Err = BucketNameError;

    fn try_from_inner(inner: String) -> Result<Self, Self::Err>
    where
        Self: Sized,
    {
        validate_bucket_name(inner.as_str())?;
        Ok(Self::new_from_inner(inner))
    }
}

#[derive(Clone, Debug, PartialEq, Zeroize)]
pub enum EncryptionKey {
    Unsalted(Vec<u8>),
    Salted(Vec<u8>),
}

impl Serialize for EncryptionKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let (prefix, bytes) = match self {
            Self::Salted(b) => ("skey", b.as_slice()),
            Self::Unsalted(b) => ("key", b.as_slice()),
        };

        let hex = Hex::encode_to_string(bytes).map_err(S::Error::custom)?;
        let s = format!("{}:{}", prefix, hex);

        serializer.serialize_str(s.as_str())
    }
}

impl<'de> Deserialize<'de> for EncryptionKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct EncryptionKeyVisitor;

        impl<'de> Visitor<'de> for EncryptionKeyVisitor {
            type Value = EncryptionKey;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string in the format 'prefix:hex'")
            }

            fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let (prefix, hex) = s
                    .split_once(':')
                    .ok_or_else(|| E::custom("expected format 'prefix:hex'"))?;
                let bytes = Hex::decode_to_vec(hex, None).map_err(E::custom)?;
                match prefix {
                    "skey" => Ok(EncryptionKey::Salted(bytes)),
                    "key" => Ok(EncryptionKey::Unsalted(bytes)),
                    _ => Err(E::custom(format!("unknown prefix '{}'", prefix))),
                }
            }
        }

        deserializer.deserialize_str(EncryptionKeyVisitor)
    }
}

fn encode_object_path<S: AsRef<str>>(path: S, prefix: &str) -> String {
    format!("{}/{}", prefix, path.as_ref().trim_start_matches('/'))
}

#[cfg(test)]
mod tests {
    use crate::MimeType;
    use crate::renterd::BucketName;
    use crate::renterd::client::{ApiPassword, Client};
    use crate::tagged::TryFromInner;
    use anyhow::bail;
    use futures_util::io::Cursor;
    use futures_util::{AsyncReadExt, TryStreamExt};
    use reqwest::Url;
    use std::str::FromStr;

    static ONE_MB: &[u8] = include_bytes!("../../testdata/1mb.bin");

    async fn is_empty(client: &Client) -> Result<bool, anyhow::Error> {
        let count = count_entries(client, "").await?;
        Ok(count == 0)
    }

    async fn count_entries(
        client: &Client,
        prefix: impl AsRef<str>,
    ) -> Result<usize, anyhow::Error> {
        let stream = client.list_objects(prefix)?;
        let count = stream
            .try_fold(0usize, |acc, _| async move { Ok(acc + 1) })
            .await?;
        Ok(count)
    }

    #[ignore]
    #[tokio::test]
    async fn integration_test1() -> Result<(), anyhow::Error> {
        dotenv::dotenv().ok();
        let renterd_api_endpoint =
            Url::parse(std::env::var("RENTERD_API_ENDPOINT").unwrap().as_str())?;
        let renterd_api_password = std::env::var("RENTERD_API_PASSWORD")
            .ok()
            .map(ApiPassword::from);
        let renterd_bucket_name =
            BucketName::try_from_inner(std::env::var("RENTERD_INTEGRATION_TEST_BUCKET")?)?;
        let renterd_root = std::env::var("RENTERD_INTEGRATION_TEST1_ROOT")?;

        let client = Client::builder()
            .api_endpoint(renterd_api_endpoint)
            .maybe_api_password(renterd_api_password)
            .bucket(renterd_bucket_name)
            .root(renterd_root.clone())
            .build()?;

        if !is_empty(&client).await? {
            bail!("bucket/directory not empty");
        }

        let root = client.object_id("/")?;

        let dir1 = client.create_directory(&root, "dir1").await?;
        assert_eq!(dir1.key().as_str(), format!("{}dir1/", renterd_root));
        let subdir1 = client.create_directory(&dir1, "subdir1").await?;
        assert_eq!(
            subdir1.key().as_str(),
            format!("{}dir1/subdir1/", renterd_root)
        );

        let dir2 = client.create_directory(&root, "dir2").await?;
        assert_eq!(dir2.key().as_str(), format!("{}dir2/", renterd_root));
        let subdir2 = client.create_directory(&dir2, "subdir2").await?;
        assert_eq!(
            subdir2.key().as_str(),
            format!("{}dir2/subdir2/", renterd_root)
        );

        assert_eq!(count_entries(&client, "").await?, 4);

        let file1 = client.object_id("/dir1/subdir1/file1")?;
        client
            .upload(
                &file1,
                Some(&MimeType::from_str("foo/bar")?),
                Some(vec![("Foo", "bar")]),
                Cursor::new(ONE_MB),
            )
            .await?;

        let dl1 = client.download(&file1).await?;
        assert!(!dl1.object().is_folder());
        assert_eq!(dl1.object().name(), "file1");
        assert_eq!(dl1.object().size(), ONE_MB.len() as u64);
        assert_eq!(dl1.object().mime_type().as_str(), "foo/bar");
        assert_eq!(dl1.object().metadata().len(), 1);
        assert_eq!(
            dl1.object().metadata().get("Foo").map(|s| s.as_str()),
            Some("bar")
        );

        let mut buf = Vec::with_capacity(ONE_MB.len());
        let mut reader = dl1.open(None).await?;
        let read = reader.read_to_end(&mut buf).await?;
        assert_eq!(read, ONE_MB.len());
        assert_eq!(&buf, ONE_MB);

        let file2 = client.object_id("/dir2/subdir2/file2")?;
        client.rename_object(&file1, file2.key()).await?;
        assert!(client.object(&file1).await.is_err());
        assert!(client.object(&file2).await.is_ok());

        client.delete_object(&file2).await?;
        assert!(client.object(&file2).await.is_err());

        client.delete_object(&dir1).await?;
        client.delete_object(&dir2).await?;
        assert!(is_empty(&client).await?);

        Ok(())
    }
}
