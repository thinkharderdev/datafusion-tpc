use std::ops::Range;

use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::Result as ObjectStoreResult;
use object_store::{GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore};

use async_trait::async_trait;
use tokio::io::AsyncWrite;

#[derive(Debug)]
pub struct S3ObjectStore {}

impl std::fmt::Display for S3ObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "S3ObjectStore")
    }
}

#[async_trait]
impl ObjectStore for S3ObjectStore {
    async fn put_multipart(
        &self,
        location: &Path,
    ) -> ObjectStoreResult<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        todo!()
    }

    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> ObjectStoreResult<()> {
        todo!()
    }

    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        todo!()
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> ObjectStoreResult<Bytes> {
        todo!()
    }

    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> ObjectStoreResult<BoxStream<'_, ObjectStoreResult<ObjectMeta>>> {
        todo!()
    }

    // async fn get_ranges(
    //     &self,
    //     location: &Path,
    //     ranges: &[Range<usize>],
    // ) -> ObjectStoreResult<Vec<Bytes>> {
    //     self.inner.get_ranges(location, ranges).await
    // }

    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        todo!()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        todo!()
    }

    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        todo!()
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        todo!()
    }

    async fn put(&self, location: &Path, bytes: Bytes) -> ObjectStoreResult<()> {
        todo!()
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        todo!()
    }
}
