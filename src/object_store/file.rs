use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::channel::oneshot;
use futures::stream::{BoxStream, FuturesUnordered};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::Result as ObjectStoreResult;
use object_store::{GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore};
use std::fmt::Display;
use std::io;
use std::ops::Range;
use std::rc::Rc;

use tokio::io::AsyncWrite;
use tracing::{debug, info};

const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024;

async fn read_glommio(path: String, range: Option<Range<usize>>) -> io::Result<Bytes> {
    //    let file = glommio::io::BufferedFile::open(&path).await?;

    let file = glommio::io::ImmutableFileBuilder::new(&path)
        .build_existing()
        .await?;

    if let Some(range) = range {
        debug!("reading {path} at {range:?}");
        let pos = range.start as u64;
        let length = range.end - range.start;

        //        let mut reader = glommio::io::DmaStreamReaderBuilder::new(file)
        //            .build();
        //        reader.skip(pos);

        match file.read_at(pos, length).await {
            Ok(result) => {
                debug!("finished reading {path} {range:?}");

                return Ok(Bytes::copy_from_slice(&*result));
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    } else {
        debug!("reading {path}");

        let length = file.file_size();

        //        let mut reader = glommio::io::DmaStreamReaderBuilder::new(file).build();

        match file.read_at(0, length as usize).await {
            Ok(result) => {
                debug!("finished reading {path} {range:?}");

                return Ok(Bytes::copy_from_slice(&*result));
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }
}

async fn read_tokio(path: String, range: Option<Range<usize>>) -> io::Result<Bytes> {
    if let Some(range) = range {
        debug!("reading {path} at {range:?}");
        let pos = range.start as u64;
        let length = range.end - range.start;

        let std_file = std::fs::File::open(&path)?;

        let file = tokio_uring::fs::File::from_std(std_file);

        let mut buf = BytesMut::with_capacity(length);
        unsafe {
            buf.set_len(length);
        }

        match file.read_at(buf, pos).await {
            (Ok(n), mut buf) => {
                debug!("finished reading {path} {range:?}");

                buf.truncate(n);
                return Ok(buf.into());
            }
            (Err(e), _) => {
                return Err(e);
            }
        }
    } else {
        debug!("reading {path}");

        let file = tokio_uring::fs::File::open(&path).await?;

        let mut buf = BytesMut::with_capacity(DEFAULT_BUFFER_SIZE);

        unsafe {
            buf.set_len(usize::MAX);
        }

        match file.read_at(buf, 0).await {
            (Ok(n), mut buf) => {
                debug!("finished reading {path}");

                buf.truncate(n);
                return Ok(buf.into());
            }
            (Err(e), _) => {
                return Err(e);
            }
        }
    }
}

async fn read_ranges_tokio(path: String, ranges: Vec<Range<usize>>) -> io::Result<Vec<Bytes>> {
    let std_file = std::fs::File::open(&path)?;

    let file = Rc::new(tokio_uring::fs::File::from_std(std_file));

    let mut results: Vec<Option<Bytes>> = vec![None; ranges.len()];

    let mut tasks: FuturesUnordered<_> = ranges.into_iter().enumerate().map(|(idx,range)| {
        let file = file.clone();
        Box::pin(async move {
            let pos = range.start as u64;
            let length = range.end - range.start;

            let mut buf = BytesMut::with_capacity(length);
            unsafe {
                buf.set_len(length);
            }

            match file.read_at(buf, pos).await {
                (Ok(n), mut buf) => {
                    buf.truncate(n);
                    return Ok::<(usize,Bytes), io::Error>((idx,buf.into()));
                }
                (Err(e), _) => {
                    return Err(e);
                }
            }
        })
    }).collect();

    while let Some(task_res) = tasks.next().await {
        let (idx,buf) = task_res?;
        results[idx] = Some(buf);
    }

    Ok(results.into_iter().flatten().collect())

}

fn read_sendable(
    path: String,
    range: Option<Range<usize>>,
) -> oneshot::Receiver<io::Result<Bytes>> {
    let (sender, receiver) = oneshot::channel();

    tokio_uring::spawn(async move {
        debug!("spawning local future");

        if let Err(_) = sender.send(read_tokio(path, range).await) {
            info!("receiver dropped!")
        } else {
            debug!("sent response to channel");
        }
    });

    receiver
}

fn read_ranges_sendable(
        path: String,
        ranges: Vec<Range<usize>>,
        ) -> oneshot::Receiver<io::Result<Vec<Bytes>>> {
    let (sender, receiver) = oneshot::channel();

    tokio_uring::spawn(async move {
        debug!("spawning local future");

        if let Err(_) = sender.send(read_ranges_tokio(path, ranges).await) {
            info!("receiver dropped!")
        } else {
            debug!("sent response to channel");
        }
    });

    receiver
}

#[derive(Debug)]
pub struct AsyncFileStore {
    inner: LocalFileSystem,
}

impl AsyncFileStore {
    pub fn new() -> Self {
        Self {
            inner: LocalFileSystem::new(),
        }
    }
}

impl Display for AsyncFileStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AsyncFileStore")
    }
}

#[async_trait]
impl ObjectStore for AsyncFileStore {
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
        let path = format!("/{}", location.to_string());
        debug!("reading {path}");

        let bytes = read_sendable(path, None).await.unwrap().unwrap();

        let stream = Box::pin(futures::stream::once(async { Ok(bytes) }));

        Ok(GetResult::Stream(stream))
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> ObjectStoreResult<Bytes> {
        let path = format!("/{}", location.to_string());
        debug!("reading {path} {range:?}");

        let bytes = read_sendable(path, Some(range))
            .await
            .map_err(|err| object_store::Error::Generic {
                store: "AsyncFileStore",
                source: Box::new(err),
            })?
            .map_err(|err| object_store::Error::Generic {
                store: "AsyncFileStore",
                source: Box::new(err),
            })?;

        Ok(bytes)
    }

    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> ObjectStoreResult<BoxStream<'_, ObjectStoreResult<ObjectMeta>>> {
        self.inner.list(prefix).await
    }

     async fn get_ranges(
         &self,
         location: &Path,
         ranges: &[Range<usize>],
     ) -> ObjectStoreResult<Vec<Bytes>> {
        let path = format!("/{}", location.to_string());

        let bytes = read_ranges_sendable(path, ranges.to_vec())
            .await
            .map_err(|err| object_store::Error::Generic {
                store: "AsyncFileStore",
                source: Box::new(err),
            })?
            .map_err(|err| object_store::Error::Generic {
                store: "AsyncFileStore",
                source: Box::new(err),
            })?;

        Ok(bytes)
     }

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

#[derive(Clone, Debug)]
struct InternalError(String);

impl Display for InternalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InternalError({})", self.0)
    }
}

impl std::error::Error for InternalError {}
