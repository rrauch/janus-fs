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
