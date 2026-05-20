use crate::vfs::{InodeId, Name, NameError, Read, Vfs, VfsResult, check_valid_filename};
use std::str::FromStr;
use std::sync::{Arc, LazyLock};
use thiserror::Error;
use typed_path::{Utf8Component, Utf8UnixEncoding, Utf8UnixPath, Utf8UnixPathBuf};

pub(crate) static ROOT_PATH: LazyLock<VfsPath> =
    LazyLock::new(|| VfsPath::try_from("/").expect("/ to be valid Root path"));

#[derive(Error, Debug)]
pub enum VfsPathError {
    #[error("path contains invalid elements")]
    Invalid,
    #[error(transparent)]
    NameError(#[from] NameError),
    #[error("path is not absolute")]
    NotAbsolute,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
#[repr(transparent)]
pub struct VfsPath(Arc<Utf8UnixPathBuf>);

impl VfsPath {
    pub fn try_from<S: AsRef<str> + ?Sized>(value: &S) -> Result<Self, VfsPathError> {
        value.as_ref().parse()
    }

    pub fn join(&self, name: &Name) -> VfsPath {
        Self(Arc::new(self.0.as_path().join(name.as_str())))
    }

    pub fn is_root(&self) -> bool {
        let str = self.0.as_str();
        str == "/" || str.is_empty()
    }

    pub fn split(&self) -> (VfsPath, Option<Name>) {
        (
            self.0
                .parent()
                .map(|p| {
                    if p.as_str().is_empty() || p.as_str() == "/" {
                        ROOT_PATH.clone()
                    } else {
                        Self(Arc::new(p.to_path_buf()))
                    }
                })
                .unwrap_or_else(|| ROOT_PATH.clone()),
            self.0
                .file_name()
                .map(|n| Name::from_str(n).expect("name to be valid")),
        )
    }

    pub fn parts(&self) -> impl Iterator<Item = Self> {
        let root = ROOT_PATH.clone();
        let components = self.0.components();
        components
            .into_iter()
            .enumerate()
            .scan(root, |path, (n, name)| {
                if n > 0 {
                    let name = Name::from_str(name.as_str()).expect("name to be valid");
                    *path = path.join(&name);
                }
                Some(path.clone())
            })
    }
}

impl FromStr for VfsPath {
    type Err = VfsPathError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let path = Utf8UnixPath::new(s);
        Ok(Self(Arc::new(sanitize_path(path)?)))
    }
}

impl TryFrom<String> for VfsPath {
    type Error = VfsPathError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let path = Utf8UnixPathBuf::from(value);
        Ok(Self(Arc::new(sanitize_path(&path)?)))
    }
}

impl<Mode: Read> Vfs<Mode> {
    pub async fn inode_id_by_path(&self, path: &VfsPath) -> VfsResult<Option<InodeId>> {
        if path.is_root() {
            return Ok(Some(self.root().inode_id()));
        }
        todo!()
    }
}

fn sanitize_path(path: &Utf8UnixPath) -> Result<Utf8UnixPathBuf, VfsPathError> {
    let normalized = path.normalize();
    if !normalized.is_absolute() {
        return Err(VfsPathError::NotAbsolute);
    }
    if !normalized.is_valid() {
        return Err(VfsPathError::Invalid);
    }
    // extended validity test
    normalized.components().try_for_each(|c| {
        if let Some(file_name) = c.as_path::<Utf8UnixEncoding>().file_name() {
            check_valid_filename(file_name)
        } else {
            Ok(())
        }
    })?;

    Ok(normalized)
}
