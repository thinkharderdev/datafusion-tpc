use datafusion::common::DataFusionError;
use datafusion::datasource::object_store::ObjectStoreProvider;
use object_store::aws::AmazonS3Builder;
use object_store::{ClientOptions, ObjectStore};
use std::sync::Arc;
use std::time::Duration;
use url::Url;

pub struct S3ObjectStoreProvider {
    region: String,
}

impl S3ObjectStoreProvider {
    pub fn new(region: impl Into<String>) -> Self {
        Self {
            region: region.into(),
        }
    }
}

impl ObjectStoreProvider for S3ObjectStoreProvider {
    fn get_by_url(&self, url: &Url) -> datafusion::common::Result<Arc<dyn ObjectStore>> {
        if url.scheme() == "s3" {
            if let Some(bucket) = url.host_str() {
                // See https://github.com/hyperium/hyper/issues/2136
                let client_options = ClientOptions::default()
                    .with_http2_keep_alive_while_idle()
                    .with_pool_idle_timeout(Duration::from_secs(20));

                Ok(Arc::new(
                    AmazonS3Builder::from_env()
                        .with_bucket_name(bucket)
                        .with_region(self.region.clone())
                        .with_client_options(client_options)
                        .build()?,
                ))
            } else {
                Err(DataFusionError::Internal(
                    "Cannot resolve S3 object store, missing bucket".to_owned(),
                ))
            }
        } else {
            Err(DataFusionError::Internal(
                "Cannot resolve S3 object store, invalid URL scheme".to_owned(),
            ))
        }
    }
}
