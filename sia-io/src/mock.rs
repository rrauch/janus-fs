use crate::{ETag, MimeType};
use anyhow::anyhow;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::{StreamExt, TryStream, stream};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

pub type MockError = anyhow::Error;

#[derive(Debug, Clone)]
pub struct MockClient {
    pub objects: Arc<Mutex<HashMap<String, MockObject>>>,
}

impl Default for MockClient {
    fn default() -> Self {
        Self {
            objects: Arc::new(Mutex::new(HashMap::default())),
        }
    }
}

impl MockClient {
    pub fn object(&self, id: impl AsRef<str>) -> Result<MockObject, MockError> {
        let id = id.as_ref();
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

    pub fn delete_object(&self, id: impl AsRef<str>) -> Result<(), MockError> {
        let id = id.as_ref();
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

#[derive(Debug, Clone)]
pub struct MockObject {
    pub id: String,
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
