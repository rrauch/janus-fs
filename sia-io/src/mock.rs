use crate::{ETag, MimeType};
use anyhow::anyhow;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::{StreamExt, TryStream, stream};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

pub type MockError = anyhow::Error;

#[derive(Debug, Clone)]
pub struct MockClient {
    pub objects: Arc<Mutex<HashMap<MockObjectId, MockObject>>>,
}

impl Default for MockClient {
    fn default() -> Self {
        Self {
            objects: Arc::new(Mutex::new(HashMap::default())),
        }
    }
}

impl MockClient {
    pub fn object(&self, id: &MockObjectId) -> Result<MockObject, MockError> {
        self.objects
            .lock()
            .unwrap()
            .get(id)
            .map(|o| o.clone())
            .ok_or_else(|| anyhow!("object not found"))
    }

    pub fn insert_object(&self, object: MockObject) -> Result<(), MockError> {
        let id = object.id.clone();
        self.objects.lock().unwrap().insert(id, object);
        Ok(())
    }

    pub fn delete_object(&self, id: &MockObjectId) -> Result<(), MockError> {
        let _ = self
            .objects
            .lock()
            .unwrap()
            .remove(id)
            .ok_or_else(|| anyhow!("object not found"))?;
        Ok(())
    }

    pub fn list_objects(
        &self,
    ) -> impl TryStream<Ok = MockObject, Error = MockError> + Send + Unpin {
        let objects = self
            .objects
            .lock()
            .unwrap()
            .values()
            .map(|v| v.clone())
            .collect::<Vec<_>>();
        stream::iter(objects.into_iter().map(Ok)).boxed()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct MockObjectId(String);

impl Display for MockObjectId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl AsRef<str> for MockObjectId {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl TryFrom<String> for MockObjectId {
    type Error = MockError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.starts_with("mock:") && value.len() > 5 {
            Ok(Self(value))
        } else {
            Err(anyhow!("id needs to start with 'mock:'"))
        }
    }
}

impl FromStr for MockObjectId {
    type Err = MockError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("mock:") && s.len() > 5 {
            Ok(Self(s.to_string()))
        } else {
            Err(anyhow!("id needs to start with 'mock:'"))
        }
    }
}

#[derive(Debug, Clone)]
pub struct MockObject {
    pub id: MockObjectId,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub mime_type: Option<MimeType>,
    pub etag: Option<ETag>,
    pub metadata: HashMap<String, String>,
    pub content: Bytes,
}

impl MockObject {
    pub fn size(&self) -> u64 {
        self.content.len() as u64
    }

    pub(super) fn hash(&self, hasher: &mut impl Hasher) {
        hasher.write(b"ID:");
        self.id.hash(hasher);
        hasher.write(b"\nCREATED_AT:");
        self.created_at.hash(hasher);
        hasher.write(b"\nUPDATED_AT:");
        self.updated_at.hash(hasher);
        hasher.write(b"\nMIME_TYPE:");
        self.mime_type.hash(hasher);
        hasher.write(b"\nETAG:");
        self.etag.hash(hasher);
        hasher.write(b"\nSIZE:");
        hasher.write_u64(self.content.len() as u64);

        hasher.write(b"\nMETADATA:");
        let metadata = &self.metadata;
        for (k, v) in metadata.iter() {
            hasher.write(b"\nKEY:");
            k.hash(hasher);
            hasher.write(b"\nVALUE:");
            v.hash(hasher);
        }
        hasher.write(b"\nMETADATA_LEN:");
        hasher.write_usize(metadata.len());
    }
}
