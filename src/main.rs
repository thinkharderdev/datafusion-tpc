use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use datafusion::common::DataFusionError;

use datafusion::datasource::object_store::{ObjectStoreProvider, ObjectStoreRegistry};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::*;
use datafusion::scheduler::Scheduler;

use futures_lite::StreamExt;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::{ClientOptions, ObjectStore};
use tracing::info;
use url::Url;

const AWS_REGION: &str = "eu-west-1";
const AWS_ACCESS_KEY: &str = "";
const AWS_SECRET: &str = "";
const AWS_TOKEN: &str = "";
const TABLE_PATH: &str = "s3://cgx-query-engine-bench-data/access-logs/";

const QUERY: &[(&str,&str); 4] = &[
    ("full_scan", "SELECT * FROM logs"),
    ("filter", "SELECT service, pod FROM logs WHERE request_method = 'DELETE'"),
    ("aggregation", "SELECT container, pod, AVG(response_bytes) AS avg_response_size FROM logs GROUP BY container, pod"),
    ("limit", "SELECT container FROM logs LIMIT 1"),
];

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .pretty()
        .with_thread_names(true)
        .with_max_level(tracing::Level::INFO)
        .init();

    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let object_store_registry = ObjectStoreRegistry::new_with_provider(Some(Arc::new(
            S3ObjectStoreProvider::new()
        )));

        let runtime_config =
            RuntimeConfig::default().with_object_store_registry(Arc::new(object_store_registry));

        let runtime_env = Arc::new(RuntimeEnv::new(runtime_config)?);

        let config = SessionConfig::new().with_target_partitions(6);
        let ctx = SessionContext::with_config_rt(config, runtime_env);

        let scheduler = Scheduler::new_thread_per_core();

        ctx.register_parquet("logs", TABLE_PATH, ParquetReadOptions::default()).await?;

        for (name, sql) in QUERY.iter() {
            let plan = ctx.sql(*sql).await?.create_physical_plan().await?;

            let mut stream = execute_stream(plan.clone(), ctx.task_ctx())?;

            let start = Instant::now();

            while let Some(batch) = stream.next().await {
                let _ = batch?;
            }

            let elapsed = start.elapsed().as_secs_f64();
            info!(elapsed, name, "standard");

            let mut stream = scheduler.schedule(plan.clone(), ctx.task_ctx())?.stream();

            let start = Instant::now();

            while let Some(batch) = stream.next().await {
                let _ = batch?;
            }

            let elapsed = start.elapsed().as_secs_f64();
            info!(elapsed, name, "scheduled");
        }

        Ok::<(), anyhow::Error>(())
    })?;


    Ok(())
}

struct S3ObjectStoreProvider {
    region: String,
}

impl S3ObjectStoreProvider {
    fn new() -> Self {
        Self {
            region: AWS_REGION.to_string(),
        }
    }
}

impl ObjectStoreProvider for S3ObjectStoreProvider {
    fn get_by_url(&self, url: &Url) -> datafusion::common::Result<Arc<dyn ObjectStore>> {
        if url.scheme() == "s3" {
            if let Some(bucket) = url.host_str() {
                // See https://github.com/hyperium/hyper/issues/2136
                let client_options =
                    ClientOptions::default().with_pool_idle_timeout(Duration::from_secs(15));

                Ok(Arc::new(AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_region(self.region.clone())
                    .with_access_key_id(AWS_ACCESS_KEY)
                    .with_secret_access_key(AWS_SECRET)
                    .with_token(AWS_TOKEN)
                    .with_client_options(client_options)
                    .build()?))
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
