use crate::gen_flatbuffers::object::metadata::{
    Entry, EntryArgs, Metadata as FlatMetadata, MetadataArgs,
};
use crate::object::{METADATA_MAGIC_NUMBER, METADATA_VFS_OBJECT_TYPE};
use crate::sync::{METADATA_VFS_ID, METADATA_VFS_VERSION};
use crate::vfs::VfsId;
use flatbuffers::{FlatBufferBuilder, InvalidFlatbuffer};
use sia_io::{Metadata as IoMetadata, MetadataSource};
use std::borrow::Cow;
use std::collections::HashMap;
use std::iter;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use thiserror::Error;
use yoke::{Yoke, Yokeable};

#[derive(Debug, Error)]
pub enum MetadataError {
    #[error(transparent)]
    FlatbufferError(#[from] InvalidFlatbuffer),
    #[error("magic number invalid")]
    InvalidMagicNumber,
    #[error("metadata bytes too short")]
    TooShort,
}

#[derive(Debug, Clone)]
pub struct Metadata<'a>(Inner<'a>);

impl<'a> TryFrom<Cow<'a, [u8]>> for Metadata<'a> {
    type Error = MetadataError;

    fn try_from(value: Cow<'a, [u8]>) -> Result<Self, Self::Error> {
        match value {
            Cow::Owned(owned) => Metadata::<'static>::try_from(Arc::from(owned)),
            Cow::Borrowed(borrowed) => Self::try_from(borrowed),
        }
    }
}

impl<'a> TryFrom<&'a [u8]> for Metadata<'a> {
    type Error = MetadataError;

    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        Ok(Metadata(Inner::Flatbuffer(Flatbuffer::Borrowed(
            try_from_flatbuffer(value)?,
        ))))
    }
}

impl TryFrom<Arc<[u8]>> for Metadata<'static> {
    type Error = MetadataError;

    fn try_from(value: Arc<[u8]>) -> Result<Self, Self::Error> {
        let yoke = Yoke::try_attach_to_cart::<Self::Error, _>(value, |data| {
            Ok(Wrapper(try_from_flatbuffer(data)?))
        })?;
        Ok(Metadata(Inner::Flatbuffer(Flatbuffer::Owned(yoke))))
    }
}

fn try_from_flatbuffer(value: &[u8]) -> Result<FlatMetadata<'_>, MetadataError> {
    if value.len() < METADATA_MAGIC_NUMBER.len() {
        return Err(MetadataError::TooShort);
    }

    let (header, body) = value.split_at(METADATA_MAGIC_NUMBER.len());
    if header != &METADATA_MAGIC_NUMBER {
        return Err(MetadataError::InvalidMagicNumber);
    }

    Ok(flatbuffers::root::<FlatMetadata>(body)?)
}

impl<'a> From<Cow<'a, HashMap<String, String>>> for Metadata<'a> {
    fn from(value: Cow<'a, HashMap<String, String>>) -> Self {
        match value {
            Cow::Owned(owned) => Metadata::<'static>::from(owned),
            Cow::Borrowed(borrowed) => Self::from(borrowed),
        }
    }
}

impl<'a> From<&'a HashMap<String, String>> for Metadata<'a> {
    fn from(value: &'a HashMap<String, String>) -> Self {
        Metadata(Inner::HashMap(Cow::Borrowed(value)))
    }
}

impl From<HashMap<String, String>> for Metadata<'static> {
    fn from(value: HashMap<String, String>) -> Self {
        Metadata(Inner::HashMap(Cow::Owned(value)))
    }
}

impl From<MetadataMut> for Metadata<'static> {
    fn from(value: MetadataMut) -> Self {
        value.0.into()
    }
}

impl<'a> TryFrom<IoMetadata<'a>> for Metadata<'a> {
    type Error = MetadataError;

    fn try_from(value: IoMetadata<'a>) -> Result<Self, Self::Error> {
        match value {
            #[cfg(feature = "indexd")]
            IoMetadata::Indexd(bytes) => Self::try_from(bytes),
            #[cfg(feature = "renterd")]
            IoMetadata::Renterd(map) => Ok(Self::from(map)),
            #[cfg(test)]
            IoMetadata::Mock(map) => Ok(Self::from(map)),
        }
    }
}

#[derive(Debug, Clone)]
enum Inner<'a> {
    Flatbuffer(Flatbuffer<'a>),
    HashMap(Cow<'a, HashMap<String, String>>),
}

#[derive(Debug, Yokeable, Clone)]
#[repr(transparent)]
struct Wrapper<'a>(FlatMetadata<'a>);

#[derive(Debug, Clone)]
enum Flatbuffer<'a> {
    Owned(Yoke<Wrapper<'static>, Arc<[u8]>>),
    Borrowed(FlatMetadata<'a>),
}

impl Flatbuffer<'_> {
    fn get(&self) -> &FlatMetadata<'_> {
        match self {
            Self::Borrowed(fb) => fb,
            Self::Owned(yoke) => &yoke.get().0,
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (&str, &str)> + '_> {
        if let Some(entries) = self.get().entries() {
            Box::new(
                entries
                    .iter()
                    .map(|e| (e.key(), e.value().unwrap_or_default())),
            )
        } else {
            Box::new(iter::empty())
        }
    }
}

impl Metadata<'_> {
    pub fn len(&self) -> usize {
        match &self.0 {
            Inner::Flatbuffer(fb) => fb.get().entries().map(|e| e.len()).unwrap_or(0),
            Inner::HashMap(map) => map.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn contains_key<K: AsRef<str> + ?Sized>(&self, key: &K) -> bool {
        match &self.0 {
            Inner::Flatbuffer(_) => self.get(key).is_some(),
            Inner::HashMap(map) => map.contains_key(key.as_ref()),
        }
    }

    pub fn get<K: AsRef<str> + ?Sized>(&self, key: &K) -> Option<&str> {
        match &self.0 {
            Inner::Flatbuffer(fb) => fb
                .get()
                .entries()
                .map(|e| {
                    e.lookup_by_key(key.as_ref(), |e, k| e.key_compare_with_value(k))
                        .map(|v| v.value())
                })
                .flatten()
                .flatten(),
            Inner::HashMap(map) => map.get(key.as_ref()).map(|s| s.as_str()),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        let iter: Box<dyn Iterator<Item = (&str, &str)>> = match &self.0 {
            Inner::HashMap(map) => Box::new(map.iter().map(|(k, v)| (k.as_str(), v.as_str()))),
            Inner::Flatbuffer(fb) => Box::new(fb.iter()),
        };

        iter
    }

    pub fn into_mut(self) -> MetadataMut {
        self.into()
    }
}

impl<'a> MetadataSource for Metadata<'a> {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        match &self.0 {
            Inner::Flatbuffer(Flatbuffer::Owned(yoke)) => {
                Cow::Borrowed(yoke.backing_cart().as_ref())
            }
            _ => {
                let mm = self.clone().into_mut();
                Cow::Owned(mm.into())
            }
        }
    }

    fn to_map(&self) -> Cow<'_, HashMap<String, String>> {
        match &self.0 {
            Inner::HashMap(map) => Cow::Borrowed(map.as_ref()),
            Inner::Flatbuffer(_) => {
                let mm = self.clone().into_mut();
                Cow::Owned(mm.0)
            }
        }
    }
}

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct MetadataMut(HashMap<String, String>);

impl MetadataMut {
    pub fn empty() -> Self {
        Self(HashMap::new())
    }
    pub fn with_vfs_template(vfs_id: &VfsId, object_type: &str) -> Self {
        let mut map = HashMap::new();
        map.insert(METADATA_VFS_VERSION.to_string(), "1".to_string());
        map.insert(METADATA_VFS_ID.to_string(), vfs_id.to_string());
        map.insert(
            METADATA_VFS_OBJECT_TYPE.to_string(),
            object_type.to_string(),
        );
        Self(map)
    }
    pub fn freeze(self) -> Metadata<'static> {
        self.into()
    }
}

impl Deref for MetadataMut {
    type Target = HashMap<String, String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for MetadataMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'a> From<Metadata<'a>> for MetadataMut {
    fn from(value: Metadata<'a>) -> Self {
        match value.0 {
            Inner::HashMap(map) => Self(map.into_owned()),
            Inner::Flatbuffer(fb) => Self(
                fb.iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            ),
        }
    }
}

impl From<MetadataMut> for Vec<u8> {
    fn from(value: MetadataMut) -> Self {
        let mut fbb = FlatBufferBuilder::new();

        // entries **have** to be sorted
        let mut pairs: Vec<(&String, &String)> = value.0.iter().collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));

        let entries: Vec<_> = pairs
            .into_iter()
            .map(|(k, v)| {
                let key = fbb.create_string(k.as_str());
                let value = fbb.create_string(v.as_str());
                Entry::create(
                    &mut fbb,
                    &EntryArgs {
                        key: Some(key),
                        value: Some(value),
                    },
                )
            })
            .collect();

        let entries_vec = fbb.create_vector(&entries);

        let metadata = FlatMetadata::create(
            &mut fbb,
            &MetadataArgs {
                entries: Some(entries_vec),
            },
        );

        fbb.finish(metadata, None);
        let data = fbb.finished_data();

        let mut buf = Vec::with_capacity(data.len() + METADATA_MAGIC_NUMBER.len());
        buf.extend_from_slice(&METADATA_MAGIC_NUMBER);
        buf.extend_from_slice(data);
        buf
    }
}

impl From<MetadataMut> for HashMap<String, String> {
    fn from(value: MetadataMut) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::{Metadata, MetadataMut};
    use sia_io::MetadataSource;
    use std::sync::Arc;

    #[test]
    fn roundtrip() -> anyhow::Result<()> {
        let mut metadata_mut = MetadataMut::empty();
        assert!(metadata_mut.is_empty());
        metadata_mut.insert("key1".to_string(), "value1".to_string());
        metadata_mut.insert("key2".to_string(), "value2".to_string());

        let fb = Vec::<u8>::from(metadata_mut.clone());

        // borrowed
        let metadata = Metadata::try_from(fb.as_slice())?;
        assert_eq!(metadata_mut.len(), metadata.len());
        assert_eq!(
            metadata_mut.get("key1").map(String::as_str),
            metadata.get("key1")
        );
        assert_eq!(
            metadata_mut.get("key2").map(String::as_str),
            metadata.get("key2")
        );
        drop(metadata);

        //owned
        let metadata = Metadata::try_from(Arc::from(fb.clone()))?;
        assert_eq!(metadata_mut.len(), metadata.len());
        assert_eq!(
            metadata_mut.get("key1").map(String::as_str),
            metadata.get("key1")
        );
        assert_eq!(
            metadata_mut.get("key2").map(String::as_str),
            metadata.get("key2")
        );

        let map = <Metadata as MetadataSource>::to_map(&metadata);
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("key1").map(String::as_str), Some("value1"));
        assert_eq!(map.get("key2").map(String::as_str), Some("value2"));
        assert_eq!(map.get("key3").map(String::as_str), None);

        let metadata = Metadata::from(map.into_owned());
        let bytes = <Metadata as MetadataSource>::to_bytes(&metadata);
        assert_eq!(bytes, fb);

        Ok(())
    }
}
