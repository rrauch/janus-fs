use derive_where::derive_where;
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::str::FromStr;
use thiserror::Error;

#[derive_where(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Zeroize; Value)]
#[serde(transparent)]
#[repr(transparent)]
pub struct TaggedValue<Tag, Value> {
    inner: Value,
    _tag: PhantomData<Tag>,
}

// SAFETY: TaggedValue is a transparent wrapper around Value. PhantomData<Tag> doesn't affect thread safety.
unsafe impl<Tag, Value: Send> Send for TaggedValue<Tag, Value> {}
unsafe impl<Tag, Value: Sync> Sync for TaggedValue<Tag, Value> {}

impl<Tag, Value> TaggedValue<Tag, Value> {
    pub(crate) const fn new_from_inner(inner: Value) -> Self {
        Self {
            inner,
            _tag: PhantomData,
        }
    }

    pub(crate) fn into_inner(self) -> Value {
        self.inner
    }
}

pub trait WithSerde {}

impl<'de, Value: Deserialize<'de>, Tag> Deserialize<'de> for TaggedValue<Tag, Value>
where
    Self: WithSerde,
    Self: TryFromInner<Value>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Value::deserialize(deserializer)
            .map(|d| Self::try_from_inner(d))?
            .map_err(D::Error::custom)
    }
}

impl<Value: Serialize, Tag> Serialize for TaggedValue<Tag, Value>
where
    Self: WithSerde,
    Value: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Serialize::serialize(&self.inner, serializer)
    }
}

pub trait WithFromStr {}

#[derive(Error)]
#[derive_where(Debug)]
pub enum FromStrError<Tag, Value: FromStr>
where
    TaggedValue<Tag, Value>: TryFromInner<Value>,
    <Value as FromStr>::Err: Debug,
    <TaggedValue<Tag, Value> as TryFromInner<Value>>::Err: Debug,
{
    #[error(transparent)]
    StrError(<Value as FromStr>::Err),
    #[error(transparent)]
    InnerError(<TaggedValue<Tag, Value> as TryFromInner<Value>>::Err),
}

impl<Tag, Value> FromStr for TaggedValue<Tag, Value>
where
    Value: FromStr,
    <Value as FromStr>::Err: Debug,
    Self: WithFromStr,
    Self: TryFromInner<Value>,
    <Self as TryFromInner<Value>>::Err: Debug,
{
    type Err = FromStrError<Tag, Value>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Value::from_str(s)
            .map(|value| Self::try_from_inner(value).map_err(FromStrError::InnerError))
            .map_err(FromStrError::StrError)?
    }
}

impl<Tag, Value> Display for TaggedValue<Tag, Value>
where
    Value: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.as_ref(), f)
    }
}

impl<Tag, Value> Deref for TaggedValue<Tag, Value> {
    type Target = Value;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<Tag, Value> DerefMut for TaggedValue<Tag, Value> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<Tag, Value> AsRef<Value> for TaggedValue<Tag, Value> {
    fn as_ref(&self) -> &Value {
        &self.inner
    }
}

impl<Tag, Value> AsMut<Value> for TaggedValue<Tag, Value> {
    fn as_mut(&mut self) -> &mut Value {
        &mut self.inner
    }
}

pub trait TryFromInner<Value> {
    type Err: Display;

    fn try_from_inner(inner: Value) -> Result<Self, Self::Err>
    where
        Self: Sized;
}
