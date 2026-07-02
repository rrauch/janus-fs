use crate::gen_flatbuffers::vfs::common::ContentId as FlatContentId;
use crate::gen_flatbuffers::vfs::common::Uuid as FlatUuid;
use bytemuck::TransparentWrapper;
pub use bytesize::ByteSize;
use derive_where::derive_where;
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use std::ops::Deref;
use std::str::FromStr;

pub mod blob;
pub mod chunk;
pub(crate) mod db;
pub(crate) mod object;
pub mod sync;
pub mod vfs;

#[derive_where(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct TypedUuid<T>(uuid::Uuid, PhantomData<T>);

// SAFETY: `TypedUuid<T>` is `#[repr(transparent)]` over `uuid::Uuid`, which is
// `Pod` (and `Zeroable`). The `PhantomData<T>` field is zero-sized
// with no alignment requirements, so the layout is identical to `uuid::Uuid`.
unsafe impl<T> TransparentWrapper<uuid::Uuid> for TypedUuid<T> {}
unsafe impl<T: 'static> bytemuck::Zeroable for TypedUuid<T> {}
unsafe impl<T: 'static> bytemuck::Pod for TypedUuid<T> {}

impl<T> TypedUuid<T> {
    pub(crate) fn try_from_str(input: &str) -> Option<Self> {
        uuid::Uuid::from_str(input)
            .ok()
            .map(|id| Self(id, PhantomData))
    }

    pub(crate) fn try_from_bytes(input: Vec<u8>) -> Option<Self> {
        let bytes = match input.try_into() {
            Ok(bytes) => bytes,
            Err(_) => return None,
        };
        Some(Self(uuid::Uuid::from_bytes(bytes), PhantomData))
    }

    pub(crate) fn from_byte_ref(input: &[u8; 16]) -> &Self {
        Self::wrap_ref(uuid::Uuid::wrap_ref(input))
    }

    pub(crate) fn as_flatbuffer(&self) -> &FlatUuid {
        const {
            assert!(size_of::<FlatUuid>() == size_of::<[u8; 16]>());
            assert!(align_of::<FlatUuid>() == align_of::<[u8; 16]>());
            assert!(size_of::<TypedUuid<T>>() == size_of::<[u8; 16]>());
            assert!(align_of::<TypedUuid<T>>() == align_of::<[u8; 16]>());
        }
        // SAFETY: `TypedUuid<T>` is `#[repr(transparent)]` over `[u8; 16]`.
        // The const assertions above verify both `TypedUuid<T>` and
        // `FlatUuid` have identical size and alignment to `[u8; 16]`.
        unsafe { &*(self.0.as_bytes().as_ptr() as *const FlatUuid) }
    }

    pub(crate) fn _generate() -> Self {
        Self(uuid::Uuid::now_v7(), PhantomData)
    }

    pub fn as_slice(&self) -> &[u8] {
        self.0.as_bytes().as_slice()
    }
}

impl<T> Deref for TypedUuid<T> {
    type Target = uuid::Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> AsRef<[u8]> for TypedUuid<T> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<T> Display for TypedUuid<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

#[derive_where(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct ContentId<T>([u8; 32], PhantomData<T>);

// SAFETY: `ContentId<T>` is `#[repr(transparent)]` over `[u8; 32]`, which is
// `Pod` (and `Zeroable`). The `PhantomData<T>` field is zero-sized
// with no alignment requirements, so the layout is identical to `[u8; 32]`.
unsafe impl<T> TransparentWrapper<[u8; 32]> for ContentId<T> {}
unsafe impl<T: 'static> bytemuck::Zeroable for ContentId<T> {}
unsafe impl<T: 'static> bytemuck::Pod for ContentId<T> {}

impl<T> ContentId<T> {
    pub(crate) fn try_from_str(input: &str) -> Option<Self> {
        if input.len() != 64 {
            return None;
        }
        let mut bytes = [0u8; 32];
        for (i, chunk) in input.as_bytes().chunks_exact(2).enumerate() {
            let hi = (chunk[0] as char).to_digit(16)?;
            let lo = (chunk[1] as char).to_digit(16)?;
            bytes[i] = ((hi << 4) | lo) as u8;
        }
        Some(Self(bytes, PhantomData))
    }

    pub(crate) fn try_from_bytes(input: Vec<u8>) -> Option<Self> {
        let bytes = match input.try_into() {
            Ok(bytes) => bytes,
            Err(_) => return None,
        };
        Some(Self(bytes, PhantomData))
    }

    pub(crate) fn as_flatbuffer(&self) -> &FlatContentId {
        const {
            assert!(size_of::<FlatContentId>() == size_of::<[u8; 32]>());
            assert!(align_of::<FlatContentId>() == align_of::<[u8; 32]>());
            assert!(size_of::<ContentId<T>>() == size_of::<[u8; 32]>());
            assert!(align_of::<ContentId<T>>() == align_of::<[u8; 32]>());
        }
        // SAFETY: `ContentId<T>` is `#[repr(transparent)]` over `[u8; 32]`.
        // The const assertions above verify both `ContentId<T>` and
        // `FlatContentId` have identical size and alignment to `[u8; 32]`.
        unsafe { &*(self.0.as_ptr() as *const FlatContentId) }
    }

    pub(crate) fn from_byte_ref(input: &[u8; 32]) -> &Self {
        Self::wrap_ref(input)
    }

    pub(crate) fn zeroed() -> Self {
        Self([0u8; 32], PhantomData)
    }
}

impl<T> Deref for ContentId<T> {
    type Target = [u8; 32];

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
        for b in self.0 {
            write!(f, "{:02x}", b)?;
        }
        Ok(())
    }
}

impl<T> PartialOrd for ContentId<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for ContentId<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.as_slice().cmp(other.0.as_slice())
    }
}

impl<T> ContentId<T> {
    pub(crate) fn new_internal(hash: blake3::Hash) -> Self {
        Self(hash.into(), PhantomData)
    }
}

#[allow(warnings)]
#[rustfmt::skip]
mod gen_flatbuffers {
    pub mod object {
        include!(concat!(env!("OUT_DIR"), "/flatbuffers/object/mod.rs"));
    }
    pub mod vfs {
        include!(concat!(env!("OUT_DIR"), "/flatbuffers/vfs/mod.rs"));
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::vfs::file::File;
    use crate::vfs::path::VfsPath;
    use crate::vfs::{Inode, Vfs};
    use futures_util::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
    use sia_io::Client as Sia;
    use std::io::SeekFrom;
    use std::num::NonZero;
    use std::time::Duration;
    use tempfile::tempdir;

    #[tokio::test]
    async fn full_tree_test() -> anyhow::Result<()> {
        let temp_dir = tempdir()?;
        let path = temp_dir.path().join("vfs.sqlite");
        let sia_client = Sia::mock().await;

        // create a new vfs & upload to network
        let vfs_id = Vfs::create_new(None, &sia_client).await?;

        // instantiate vfs + initial sync from network
        let vfs = Vfs::builder()
            .vfs_id(vfs_id.clone())
            .sia_client(sia_client.clone())
            .db_file(path.clone())
            .max_sync_concurrency(NonZero::new(1).unwrap())
            .build()
            .await?;

        assert!(vfs.current_commit().await?.is_synced());

        // make multiple local changes
        let dir1 = vfs
            .create_dir(&vfs.root().await?, "dir1".try_into()?)
            .await?;
        assert!(!dir1.is_synced());
        let file1 = vfs.create_file(&dir1, "file1".try_into()?).await?;
        assert!(!file1.is_synced());
        assert_eq!(file1.len(), 0);
        let mut fh = vfs.open_rw(&file1).await?;
        fh.write_all("some bytes".as_bytes()).await?;
        fh.close().await?;
        let file1 = fh.commit().await?;
        assert_eq!(file1.len(), 10);
        let mut fh = vfs.open_rw(&file1).await?;
        fh.seek(SeekFrom::End(0)).await?;
        fh.write_all("more".as_bytes()).await?;
        let file1 = fh.commit().await?;
        assert_eq!(file1.len(), 14);

        let commit = vfs.current_commit().await?;
        assert!(!commit.is_synced());

        read_file(&vfs, b"some bytesmore", false).await?;

        // sync changes back to network
        vfs.sync().await?;

        let current_commit = vfs.current_commit().await?;
        assert!(current_commit.is_synced());
        assert_eq!(current_commit.id(), commit.id());

        read_file(&vfs, b"some bytesmore", true).await?;

        drop(vfs);
        tokio::time::sleep(Duration::from_millis(1500)).await;

        // start completely from scratch & sync from the network
        let path = temp_dir.path().join("vfs2.sqlite");

        let vfs = Vfs::builder()
            .vfs_id(vfs_id)
            .sia_client(sia_client)
            .db_file(path)
            .max_sync_concurrency(NonZero::new(1).unwrap())
            .build()
            .await?;

        let current_commit = vfs.current_commit().await?;
        assert!(current_commit.is_synced());
        assert_eq!(current_commit.id(), commit.id());

        read_file(&vfs, b"some bytesmore", true).await?;

        // make additional changes

        let file = get_file(&vfs).await?;
        let mut fh = vfs.open_rw(&file).await?;
        fh.seek(SeekFrom::End(0)).await?;
        fh.write_all(b"even more").await?;
        fh.commit().await?;

        read_file(&vfs, b"some bytesmoreeven more", false).await?;

        // sync changes back to network
        vfs.sync().await?;

        read_file(&vfs, b"some bytesmoreeven more", true).await?;

        Ok(())
    }

    async fn read_file(
        vfs: &Vfs,
        expected_content: &[u8],
        expect_synced: bool,
    ) -> anyhow::Result<()> {
        let file1 = get_file(vfs).await?;
        assert_eq!(file1.len(), expected_content.len() as u64);
        assert_eq!(file1.is_synced(), expect_synced);
        let mut fh = vfs.open(&file1).await?;
        let mut buffer = Vec::with_capacity(fh.len() as usize);
        fh.read_to_end(&mut buffer).await?;
        assert_eq!(buffer.as_slice(), expected_content);

        Ok(())
    }

    async fn get_file(vfs: &Vfs) -> anyhow::Result<File> {
        Ok(
            match vfs
                .inode_by_path(&VfsPath::try_from("/dir1/file1")?)
                .await?
                .unwrap()
            {
                Inode::File(file) => file,
                _ => unreachable!("should be a file"),
            },
        )
    }
}
