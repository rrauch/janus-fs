use crate::confidential::{Confidential, NewSecretExt};
use crate::tagged::{TaggedValue, TryFromInner, WithFromStr, WithSerde};
use mime::Mime;
use serde::{Deserialize, Deserializer};
use std::str::FromStr;
use thiserror::Error;

pub mod confidential;
#[cfg(feature = "renterd")]
pub mod renterd;
pub(crate) mod tagged;

pub struct MimeTypeKind;
pub type MimeType = TaggedValue<MimeTypeKind, String>;
impl WithFromStr for MimeType {}
impl WithSerde for MimeType {}
impl TryFromInner<String> for MimeType {
    type Err = MimeTypeError;

    fn try_from_inner(inner: String) -> Result<Self, Self::Err>
    where
        Self: Sized,
    {
        // check if mime type is well-formed
        let _ = Mime::from_str(inner.as_str())?;
        Ok(Self::new_from_inner(inner))
    }
}

#[derive(Error, Debug)]
#[error(transparent)]
#[repr(transparent)]
pub struct MimeTypeError(#[from] mime::FromStrError);

pub struct ETagKind;
pub type ETag = TaggedValue<ETagKind, String>;
impl WithFromStr for ETag {}
impl WithSerde for ETag {}

#[derive(Debug, Error)]
pub enum ETagError {
    #[error("empty ETag value")]
    Empty,
    #[error("missing opening quote")]
    MissingOpeningQuote,
    #[error("missing closing quote")]
    MissingClosingQuote,
    #[error("invalid weak prefix")]
    InvalidWeakPrefix,
    #[error("invalid character at position {0}")]
    InvalidCharacter(usize),
}

fn validate_etag(etag: &str) -> Result<(), ETagError> {
    if etag.is_empty() {
        return Err(ETagError::Empty);
    }

    let rest = if let Some(stripped) = etag.strip_prefix("W/") {
        stripped
    } else if etag.starts_with('W') && !etag.starts_with('"') {
        return Err(ETagError::InvalidWeakPrefix);
    } else {
        etag
    };

    if !rest.starts_with('"') {
        return Err(ETagError::MissingOpeningQuote);
    }

    if rest.len() < 2 || !rest.ends_with('"') {
        return Err(ETagError::MissingClosingQuote);
    }

    let opaque = &rest[1..rest.len() - 1];
    for (i, c) in opaque.chars().enumerate() {
        let b = c as u32;
        if !(b == 0x21 || (0x23..=0x7E).contains(&b) || (0x80..=0xFF).contains(&b)) {
            return Err(ETagError::InvalidCharacter(i));
        }
    }

    Ok(())
}

impl TryFromInner<String> for ETag {
    type Err = ETagError;

    fn try_from_inner(inner: String) -> Result<Self, Self::Err>
    where
        Self: Sized,
    {
        validate_etag(inner.as_str())?;
        Ok(Self::new_from_inner(inner))
    }
}

pub type Password<Tag> = Confidential<TaggedValue<Tag, String>>;

impl<Tag> From<String> for Password<Tag> {
    fn from(value: String) -> Self {
        TaggedValue::<Tag, String>::new_from_inner(value).confidential()
    }
}

fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    T: Default + Deserialize<'de>,
    D: Deserializer<'de>,
{
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

pub struct FileKind;
pub struct FolderKind;
