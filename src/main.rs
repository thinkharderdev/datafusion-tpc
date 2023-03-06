use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;

use datafusion::datasource::object_store::ObjectStoreRegistry;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::*;
use datafusion::scheduler::Scheduler;

use futures_lite::StreamExt;
use tracing::info;

use datafusion_tpc::S3ObjectStoreProvider;

const AWS_REGION: &str = "eu-west-1";

const TABLE_PATH: &str = "s3://cgx-query-engine-bench-data/access-logs/";

const QUERY: &[(&str,&str); 3] = &[
    // ("full_scan", "SELECT * FROM logs"),
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
            S3ObjectStoreProvider::new(AWS_REGION),
        )));

        let runtime_config =
            RuntimeConfig::default().with_object_store_registry(Arc::new(object_store_registry));

        let runtime_env = Arc::new(RuntimeEnv::new(runtime_config)?);

        let config = SessionConfig::new().with_target_partitions(6);
        let ctx = SessionContext::with_config_rt(config, runtime_env);

        let scheduler = Scheduler::new_thread_per_core();

        ctx.register_parquet("logs", TABLE_PATH, ParquetReadOptions::default())
            .await?;

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
