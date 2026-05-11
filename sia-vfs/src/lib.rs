use derive_where::derive_where;
use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

pub mod blob;
pub mod chunk;

#[derive_where(Debug, Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct ContentId<T>(Arc<blake3::Hash>, PhantomData<T>);

impl<T> Deref for ContentId<T> {
    type Target = blake3::Hash;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> AsRef<[u8]> for ContentId<T> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl<T> Display for ContentId<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl<T> ContentId<T> {
    pub(crate) fn new_internal(hash: blake3::Hash) -> Self {
        Self(Arc::new(hash), PhantomData)
    }
}
