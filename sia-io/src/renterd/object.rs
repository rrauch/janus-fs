use crate::confidential::Protected;
use crate::renterd::client::{ApiRequest, ApiRequestBuilder, Client, ClientError, RequestContent};
use crate::renterd::{BucketName, EncryptionKey, encode_object_path};
use crate::tagged::{FromStrError, TaggedValue, TryFromInner, WithFromStr, WithSerde};
use crate::{ETag, MimeType};
use chrono::{DateTime, Utc};
use futures_io::AsyncRead;
use futures_util::{StreamExt, TryStream};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use thiserror::Error;
use typed_path::{Utf8UnixPath, Utf8UnixPathBuf};

const MAX_OBJECT_KEY_LENGTH: usize = 1024;

pub struct ObjectKeyKind;
pub type ObjectKey = TaggedValue<ObjectKeyKind, String>;

impl ObjectKey {
    pub(crate) fn new_root(path: impl AsRef<str>) -> Result<Self, ObjectKeyError> {
        let owned_path;
        let path = path.as_ref();
        let end_with_slash = path.ends_with("/");
        let path = Utf8UnixPathBuf::from_str(path)
            .expect("path conversion to be infallible")
            .normalize();
        let path = if end_with_slash && !path.as_str().ends_with("/") {
            owned_path = format!("{}/", path.as_str());
            owned_path.as_str()
        } else {
            path.as_str()
        };
        Ok(ObjectKey::from_str(path).map_err(|e| match e {
            FromStrError::InnerError(e) => e,
            FromStrError::StrError(_) => unreachable!("infallible str conversion"),
        })?)
    }

    pub(crate) fn new(root: &ObjectKey, path: impl AsRef<str>) -> Result<Self, ObjectKeyError> {
        let mut owned_path;
        let mut path = path.as_ref();
        let end_with_slash = path.ends_with("/");
        if path.starts_with("/") {
            // make relative to root
            owned_path = format!(".{}", path);
            path = owned_path.as_str();
        }
        let path = root
            .as_unix_path()
            .join_checked(path)
            .map_err(|e| ObjectKeyError::Other(e.to_string()))?
            .normalize();

        let path = if end_with_slash && !path.as_str().ends_with("/") {
            owned_path = format!("{}/", path.as_str());
            owned_path.as_str()
        } else {
            path.as_str()
        };

        let this = ObjectKey::from_str(path).map_err(|e| match e {
            FromStrError::InnerError(e) => e,
            FromStrError::StrError(_) => unreachable!("infallible str conversion"),
        })?;

        this.check_root(root)?;
        Ok(this)
    }

    pub fn prefix(&self) -> Option<&str> {
        let mut s = self.as_str();
        if s.ends_with("/") {
            s = &s[..s.len() - 1];
        }
        s.rfind('/')
            .map(|idx| &s[..idx])
            .and_then(|s| if s.is_empty() { None } else { Some(s) })
    }

    pub fn filename(&self) -> &str {
        let mut s = self.as_str();
        if s.ends_with("/") {
            s = &s[..s.len() - 1];
        }
        s.rfind('/').map(|idx| &s[idx + 1..]).unwrap_or(s)
    }

    pub fn is_folder(&self) -> bool {
        self.as_str().ends_with("/")
    }

    pub(crate) fn check_root(&self, expected_root: &ObjectKey) -> Result<(), ObjectKeyError> {
        let path = self.as_unix_path();
        let root = expected_root.as_unix_path();
        if !path.starts_with(root) {
            Err(ObjectKeyError::PathOutsideRoot {
                root: root.to_path_buf(),
                path: path.to_path_buf(),
            })?
        }
        Ok(())
    }

    pub(crate) fn as_unix_path(&self) -> &Utf8UnixPath {
        Utf8UnixPath::new(self.as_str())
    }

    pub(crate) fn as_relative_path(&self) -> &str {
        let s = self.as_str();
        s.strip_prefix("/").unwrap_or(s)
    }
}

impl WithFromStr for ObjectKey {}
impl WithSerde for ObjectKey {}

#[derive(Debug, Error)]
pub enum ObjectKeyError {
    #[error("key must not be empty")]
    Empty,

    #[error("key exceeds maximum length of {MAX_OBJECT_KEY_LENGTH} bytes (got {0} bytes)")]
    TooLong(usize),

    #[error("key contains consecutive slashes at byte offset {0}")]
    ConsecutiveSlashes(usize),

    #[error("key component {component:?} is a relative path reference")]
    RelativePathComponent { component: String },

    #[error("path '{path}' not inside root '{root}'")]
    PathOutsideRoot {
        root: Utf8UnixPathBuf,
        path: Utf8UnixPathBuf,
    },

    #[error("other object key error: {0}")]
    Other(String),
}

fn validate_object_key(key: &str) -> Result<(), ObjectKeyError> {
    // Check empty
    if key.is_empty() {
        return Err(ObjectKeyError::Empty);
    }

    // Check length
    let byte_len = key.len();
    if byte_len > MAX_OBJECT_KEY_LENGTH {
        return Err(ObjectKeyError::TooLong(byte_len));
    }

    // Check for invalid characters and consecutive slashes
    let mut prev_was_slash = false;
    for (offset, c) in key.char_indices() {
        if c == '/' {
            if prev_was_slash {
                return Err(ObjectKeyError::ConsecutiveSlashes(offset));
            }
            prev_was_slash = true;
        } else {
            prev_was_slash = false;
        }
    }

    // Check for relative path components
    for component in key.split('/') {
        if component == "." || component == ".." {
            return Err(ObjectKeyError::RelativePathComponent {
                component: component.to_string(),
            });
        }
    }

    Ok(())
}

impl TryFromInner<String> for ObjectKey {
    type Err = ObjectKeyError;

    fn try_from_inner(inner: String) -> Result<Self, Self::Err>
    where
        Self: Sized,
    {
        validate_object_key(inner.as_str())?;
        Ok(Self::new_from_inner(inner))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObjectId {
    bucket: BucketName,
    key: ObjectKey,
}

impl Display for ObjectId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]{}", self.bucket, self.key)
    }
}

impl ObjectId {
    pub(super) fn new_root(
        bucket: BucketName,
        prefix: impl AsRef<str>,
    ) -> Result<Self, ObjectKeyError> {
        let key = ObjectKey::new_root(prefix)?;
        Ok(Self { bucket, key })
    }

    pub(super) fn new(
        bucket: BucketName,
        root: &ObjectKey,
        key: impl AsRef<str>,
    ) -> Result<Self, ObjectKeyError> {
        Ok(ObjectId {
            bucket,
            key: ObjectKey::new(root, key)?,
        })
    }

    #[inline]
    pub fn bucket(&self) -> &BucketName {
        &self.bucket
    }

    #[inline]
    pub fn key(&self) -> &ObjectKey {
        &self.key
    }
}

pub struct ObjectEncryptionKeyKind;
pub type ObjectEncryptionKey = Protected<TaggedValue<ObjectEncryptionKeyKind, EncryptionKey>>;
impl WithSerde for TaggedValue<ObjectEncryptionKeyKind, EncryptionKey> {}
impl TryFromInner<EncryptionKey> for TaggedValue<ObjectEncryptionKeyKind, EncryptionKey> {
    type Err = Infallible;

    fn try_from_inner(inner: EncryptionKey) -> Result<Self, Self::Err>
    where
        Self: Sized,
    {
        Ok(Self::new_from_inner(inner))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Object {
    #[serde(flatten)]
    id: ObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    etag: Option<ETag>,
    health: f64,
    mod_time: DateTime<Utc>,
    size: u64,
    mime_type: MimeType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    encryption_key: Option<ObjectEncryptionKey>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    slabs: Vec<SlabSlice>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    metadata: HashMap<String, String>,
}

impl Object {
    #[inline]
    pub fn id(&self) -> &ObjectId {
        &self.id
    }

    #[inline]
    pub fn name(&self) -> &str {
        self.id.key.filename()
    }

    #[inline]
    pub fn mime_type(&self) -> &MimeType {
        &self.mime_type
    }

    #[inline]
    pub fn etag(&self) -> Option<&ETag> {
        self.etag.as_ref()
    }

    #[inline]
    pub fn mod_time(&self) -> &DateTime<Utc> {
        &self.mod_time
    }

    #[inline]
    pub fn size(&self) -> u64 {
        self.size
    }

    #[inline]
    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    #[inline]
    pub fn is_folder(&self) -> bool {
        self.id.key.is_folder()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlabSlice {
    slab: Slab,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    offset: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    limit: Option<u64>,
}

pub struct SlabEncryptionKeyKind;
pub type SlabEncryptionKey = Protected<TaggedValue<SlabEncryptionKeyKind, EncryptionKey>>;
impl WithSerde for TaggedValue<SlabEncryptionKeyKind, EncryptionKey> {}
impl TryFromInner<EncryptionKey> for TaggedValue<SlabEncryptionKeyKind, EncryptionKey> {
    type Err = Infallible;

    fn try_from_inner(inner: EncryptionKey) -> Result<Self, Self::Err>
    where
        Self: Sized,
    {
        Ok(Self::new_from_inner(inner))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Slab {
    health: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    encryption_key: Option<SlabEncryptionKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    min_shards: Option<u64>,
}

#[derive(Debug, Error)]
pub enum ListError {
    #[error(transparent)]
    ObjectKeyError(#[from] ObjectKeyError),
    #[error(transparent)]
    ClientError(#[from] ClientError),
}

#[derive(Debug, Error)]
pub enum RenameError {
    #[error("type differs: expected_file: {expected_file}, is_file: {is_file}")]
    TypeDiffers { expected_file: bool, is_file: bool },
    #[error(transparent)]
    ClientError(#[from] ClientError),
}

#[derive(Debug, Error)]
pub enum CreateDirectoryError {
    #[error("'{0}' is not a directory")]
    NotDirectory(ObjectId),
    #[error(transparent)]
    ObjectKeyError(#[from] ObjectKeyError),
    #[error(transparent)]
    ClientError(#[from] ClientError),
}

impl Client {
    pub fn list_objects(
        &self,
        prefix: impl AsRef<str>,
    ) -> Result<impl TryStream<Ok = Object, Error = ClientError> + Send + Unpin, ListError> {
        let prefix = ObjectKey::new(self.root().key(), prefix)?;
        let this = self.clone();
        let initial_state = (VecDeque::new(), true, None);
        Ok(
            futures_util::stream::try_unfold(initial_state, move |state| {
                let this = this.clone();
                let prefix = prefix.clone();
                async move {
                    let mut objects: VecDeque<Object> = state.0;
                    let mut has_more = state.1;
                    let mut next_marker: Option<String> = state.2;

                    loop {
                        if let Some(object) = objects.pop_front() {
                            // filter out root object
                            if object.id.key != this.root().key {
                                return Ok(Some((object, (objects, has_more, next_marker))));
                            }
                            continue;
                        }

                        if !has_more {
                            return Ok(None);
                        }

                        let resp: ListResponse = this
                            .send_api_request(list_req(
                                prefix.as_relative_path(),
                                this.bucket(),
                                next_marker.as_deref(),
                            ))
                            .await?
                            .json()
                            .await?;

                        resp.objects.into_iter().for_each(|o| objects.push_back(o));

                        if resp.has_more {
                            has_more = true;
                            next_marker = objects
                                .back()
                                .and_then(|o| o.id.key.prefix().map(|s| s.to_string()));
                        } else {
                            has_more = false;
                            next_marker = None;
                        }
                    }
                }
            })
            .boxed(),
        )
    }

    pub async fn object(&self, object_id: &ObjectId) -> Result<Object, ClientError> {
        self.check_object_id(object_id)?;
        Ok(self
            .send_api_request(get_req(
                object_id.key().as_relative_path(),
                object_id.bucket(),
            ))
            .await?
            .json()
            .await?)
    }

    pub async fn rename_object(&self, from: &ObjectId, to: &ObjectKey) -> Result<(), RenameError> {
        self.check_object_id(from)?;
        to.check_root(self.root().key())
            .map_err(ClientError::ObjectKeyError)?;

        let is_folder = from.key.is_folder();

        if is_folder != to.is_folder() {
            Err(RenameError::TypeDiffers {
                expected_file: !is_folder,
                is_file: !to.is_folder(),
            })?
        }

        let _ = self
            .send_api_request(rename_req(from.key(), to, from.bucket(), is_folder))
            .await?;

        Ok(())
    }

    pub async fn delete_object(&self, object_id: &ObjectId) -> Result<(), ClientError> {
        self.check_object_id(object_id)?;
        let _ = self
            .send_api_request(delete_req(
                object_id.key(),
                object_id.bucket(),
                object_id.key().is_folder(),
            ))
            .await?;
        Ok(())
    }

    pub fn object_id(
        &self,
        parent: &ObjectId,
        name: impl AsRef<str>,
        is_folder: bool,
    ) -> Result<ObjectId, ObjectKeyError> {
        self.check_object_id(parent)
            .map_err(|e| ObjectKeyError::Other(e.to_string()))?;
        if !parent.key.is_folder() {
            Err(ObjectKeyError::Other(
                "parent needs to be a folder".to_string(),
            ))?;
        }

        let owned_name;
        let mut name = name.as_ref();
        if is_folder && !name.ends_with("/") {
            owned_name = format!("{}/", name);
            name = owned_name.as_str();
        }

        let object_id = ObjectId::new(self.bucket().clone(), parent.key(), name)?;
        self.check_object_id(&object_id)
            .map_err(|e| ObjectKeyError::Other(e.to_string()))?;
        Ok(object_id)
    }

    pub async fn upload<'a, U: AsyncRead + Send + Unpin + 'static>(
        &self,
        object_id: &'a ObjectId,
        mime_type: Option<&'a MimeType>,
        metadata: Option<impl IntoIterator<Item = (&'a str, &'a str)>>,
        content: U,
    ) -> Result<(), ClientError> {
        self.check_object_id(object_id)?;

        let _ = self
            .send_api_request(upload_req(
                object_id.key(),
                mime_type.map(|m| m.as_str()),
                metadata,
                object_id.bucket(),
                content,
            ))
            .await?;

        Ok(())
    }

    pub async fn create_directory(&self, new_dir: &ObjectId) -> Result<(), CreateDirectoryError> {
        self.check_object_id(new_dir)?;
        if !new_dir.key.is_folder() {
            Err(CreateDirectoryError::NotDirectory(new_dir.clone()))?;
        }

        let _ = self
            .send_api_request(mkdir_req(new_dir.key(), new_dir.bucket()))
            .await?;

        Ok(())
    }
}

fn mkdir_req<'a>(key: &str, bucket: &'a str) -> ApiRequest<'a> {
    let path = encode_object_path(key, "./worker/object");
    let params = vec![("bucket", bucket)];

    ApiRequestBuilder::put(path).params(Some(params)).build()
}

fn upload_req<'a, U: AsyncRead + Send + Unpin + 'static>(
    key: &str,
    mime_type: Option<&'a str>,
    metadata: Option<impl IntoIterator<Item = (&'a str, &'a str)>>,
    bucket: &'a str,
    stream: U,
) -> ApiRequest<'a> {
    let path = encode_object_path(key, "./worker/object");
    let mut params = vec![("bucket", bucket)];
    if let Some(mime_type) = mime_type {
        params.push(("mimetype", mime_type));
    }

    ApiRequestBuilder::put(path)
        .params(Some(params))
        .content(Some(RequestContent::Stream(
            Box::new(stream),
            mime_type.map(|m| m.into()),
            metadata.map(|m| m.into_iter().map(|(k, v)| (k.into(), v.into())).collect()),
        )))
        .build()
}

fn rename_req<'a>(
    old_key: &'a str,
    new_key: &'a str,
    bucket: &'a str,
    is_folder: bool,
) -> ApiRequest<'a> {
    let mode = if is_folder { "multi" } else { "single" };

    let content = RequestContent::Json(
        serde_json::to_value(RenameReq {
            bucket,
            force: false,
            from: old_key,
            mode,
            to: new_key,
        })
        .expect("RenameReq serialization to never fail"),
    );

    ApiRequestBuilder::post("./bus/objects/rename")
        .content(Some(content))
        .build()
}

#[derive(Serialize)]
struct RenameReq<'a> {
    bucket: &'a str,
    force: bool,
    from: &'a str,
    mode: &'a str,
    to: &'a str,
}

fn delete_req<'a>(key: &'a str, bucket: &'a str, is_folder: bool) -> ApiRequest<'a> {
    if is_folder {
        let content = RequestContent::Json(
            serde_json::to_value(BatchDeleteReq {
                bucket,
                prefix: key,
            })
            .expect("BatchDeleteReq serialization to never fail"),
        );

        ApiRequestBuilder::post("./worker/objects/remove")
            .content(Some(content))
            .build()
    } else {
        let path = encode_object_path(key, "./worker/object");
        let params = vec![("bucket", bucket)];
        ApiRequestBuilder::delete(path).params(Some(params)).build()
    }
}

#[derive(Serialize)]
struct BatchDeleteReq<'a> {
    bucket: &'a str,
    prefix: &'a str,
}

fn get_req<'a>(key: &str, bucket: &'a str) -> ApiRequest<'a> {
    let path = encode_object_path(key, "./bus/object");
    let params = vec![("bucket", bucket)];
    ApiRequestBuilder::get(path).params(Some(params)).build()
}

fn list_req<'a>(prefix: &str, bucket: &'a str, marker: Option<&'a str>) -> ApiRequest<'a> {
    let path = encode_object_path(prefix, "./bus/objects");
    let mut params = vec![("bucket", bucket)];
    if let Some(marker) = marker {
        params.push(("marker", marker));
    }

    ApiRequestBuilder::get(path).params(Some(params)).build()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListResponse {
    has_more: bool,
    #[serde(deserialize_with = "crate::deserialize_null_default")]
    objects: Vec<Object>,
}

#[cfg(test)]
mod tests {
    use crate::confidential::RevealExt;
    use crate::renterd::EncryptionKey;
    use crate::renterd::object::Object;
    use chrono::{DateTime, Utc};

    #[test]
    fn object_serde() -> anyhow::Result<()> {
        let object: Object = serde_json::from_str(
            r#"
        {
  "metadata": {
    "additionalProperty": "add_prop"
  },
  "bucket": "bucket-name",
  "etag": null,
  "health": 1,
  "modTime": "2026-04-03T13:51:40.623Z",
  "key": "key_value",
  "size": 2,
  "mimeType": "mime/value",
  "encryptionKey": "skey:8c9796286918fd38a62b2d4ef0418889ffd20ab01b2e9c7091963e043669bc07",
  "slabs": [
    {
      "slab": {
        "health": 3,
        "encryptionKey": null,
        "minShards": 2
      },
      "offset": 4,
      "limit": 5
    }
  ]
}
        "#,
        )?;

        assert_eq!(object.id.bucket, "bucket-name".parse()?);
        assert_eq!(object.id.key.as_str(), "key_value");
        assert_eq!(object.id.key.prefix(), None);
        assert_eq!(object.id.key.filename(), "key_value");
        assert_eq!(object.etag, None);
        assert_eq!(object.health, 1f64);
        assert_eq!(
            object.mod_time,
            "2026-04-03T13:51:40.623Z".parse::<DateTime<Utc>>()?
        );
        assert_eq!(object.size, 2);
        assert_eq!(object.mime_type.as_str(), "mime/value");
        assert_eq!(
            object.encryption_key.as_ref().unwrap().reveal().as_ref(),
            &EncryptionKey::Salted(vec![
                0x8c, 0x97, 0x96, 0x28, 0x69, 0x18, 0xfd, 0x38, 0xa6, 0x2b, 0x2d, 0x4e, 0xf0, 0x41,
                0x88, 0x89, 0xff, 0xd2, 0x0a, 0xb0, 0x1b, 0x2e, 0x9c, 0x70, 0x91, 0x96, 0x3e, 0x04,
                0x36, 0x69, 0xbc, 0x07
            ])
        );
        assert_eq!(object.metadata.len(), 1);
        assert_eq!(
            object
                .metadata
                .get("additionalProperty")
                .map(|s| s.as_str()),
            Some("add_prop")
        );
        assert_eq!(object.slabs.len(), 1);
        assert_eq!(object.slabs.first().unwrap().offset, Some(4));
        assert_eq!(object.slabs.first().unwrap().limit, Some(5));
        assert_eq!(object.slabs.first().unwrap().slab.health, 3f64);
        assert_eq!(object.slabs.first().unwrap().slab.encryption_key, None);
        assert_eq!(object.slabs.first().unwrap().slab.min_shards, Some(2));

        Ok(())
    }
}
