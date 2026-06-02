pub(crate) mod metadata;

use crate::db::{Error as DbError, Read, Transaction, TxScope, Write};
use crate::object::metadata::MetadataError;
use crate::vfs::Timestamp;
use std::fmt::{Display, Formatter};
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::str::FromStr;
use thiserror::Error;

const METADATA_MAGIC_NUMBER: [u8; 4] = [0xA8, 0x19, 0xCD, 0x28];

pub(crate) const METADATA_VFS_OBJECT_TYPE: &'static str = "VFS-OBJECT-TYPE";

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    MetadataError(#[from] MetadataError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub(crate) struct ObjectId(u64);
impl From<i64> for ObjectId {
    fn from(value: i64) -> Self {
        Self(value as u64)
    }
}

impl From<u64> for ObjectId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl Deref for ObjectId {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for ObjectId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Object {
    id: ObjectId,
    remote_location: String,
}

impl Object {
    pub fn id(&self) -> ObjectId {
        self.id
    }

    pub fn remote_location(&self) -> &str {
        self.remote_location.as_str()
    }

    pub fn try_to_backend_object_id(&self) -> Option<sia_io::object::ObjectId> {
        sia_io::object::ObjectId::from_str(self.remote_location.as_str()).ok()
    }
}

impl<C: TxScope> Transaction<C>
where
    Self: Read,
{
    pub async fn object_by_id(&mut self, id: ObjectId) -> Result<Option<Object>, DbError> {
        let id = *id.deref() as i64;
        Ok(
            sqlx::query!("SELECT id, remote_location FROM object WHERE id = ?", id)
                .map(|r| Object {
                    id: r.id.into(),
                    remote_location: r.remote_location,
                })
                .fetch_optional(self.conn())
                .await?,
        )
    }

    pub async fn object_by_remote_location(
        &mut self,
        remote_location: &str,
    ) -> Result<Option<Object>, DbError> {
        Ok(sqlx::query!(
            "SELECT id, remote_location FROM object WHERE remote_location = ?",
            remote_location
        )
        .map(|r| Object {
            id: r.id.into(),
            remote_location: r.remote_location,
        })
        .fetch_optional(self.conn())
        .await?)
    }

    pub async fn list_objects(&mut self) -> Result<Vec<Object>, DbError> {
        Ok(sqlx::query!("SELECT id, remote_location FROM object")
            .map(|r| Object {
                id: r.id.into(),
                remote_location: r.remote_location,
            })
            .fetch_all(self.conn())
            .await?)
    }
}

pub enum ObjectCreateResult {
    New(ObjectId),
    Existing(Object),
}

impl<C: TxScope> Transaction<C>
where
    Self: Write,
{
    pub async fn create_or_mark_object(
        &mut self,
        remote_location: &str,
        seen: Timestamp,
    ) -> Result<ObjectCreateResult, DbError> {
        if let Some(existing) = self.object_by_remote_location(remote_location).await? {
            // object already exists, mark as seen & reset error count
            self.mark_object_seen(existing.id(), seen).await?;
            return Ok(ObjectCreateResult::Existing(existing));
        }

        let first_seen = seen.to_millis();

        let id = sqlx::query!(
            "INSERT INTO object (remote_location, first_seen, last_seen) VALUES (?, ?, ?)",
            remote_location,
            first_seen,
            first_seen,
        )
        .execute(self.conn())
        .await?
        .last_insert_rowid();

        Ok(ObjectCreateResult::New(id.into()))
    }

    async fn mark_object_seen(
        &mut self,
        object_id: ObjectId,
        timestamp: Timestamp,
    ) -> Result<(), DbError> {
        let timestamp = timestamp.to_millis();
        let object_id = *object_id.deref() as i64;

        let _ = sqlx::query!(
            "UPDATE object SET last_seen = ?, error_count = 0 WHERE id = ?",
            timestamp,
            object_id,
        )
        .execute(self.conn())
        .await?;
        Ok(())
    }

    pub async fn mark_object_error(
        &mut self,
        object_ids: impl Iterator<Item = ObjectId>,
    ) -> Result<(), DbError> {
        for object_id in object_ids {
            let object_id = *object_id.deref() as i64;

            let _ = sqlx::query!(
                "UPDATE object SET error_count = error_count + 1 WHERE id = ?",
                object_id,
            )
            .execute(self.conn())
            .await?;
        }
        Ok(())
    }

    pub async fn delete_gone_objects(
        &mut self,
        error_threshold: NonZeroUsize,
    ) -> Result<usize, DbError> {
        let error_threshold = error_threshold.get() as i64;

        Ok(sqlx::query!(
            "DELETE FROM object WHERE ref_count = 0 AND error_count >= ?",
            error_threshold
        )
        .execute(self.conn())
        .await?
        .rows_affected() as usize)
    }
}
