use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;

use datafusion::datasource::object_store::ObjectStoreRegistry;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use datafusion::scheduler::Scheduler;
use datafusion_tpc::driver::IOUringDriver;
use datafusion_tpc::object_store::file::AsyncFileStore;
use futures::StreamExt;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .pretty()
        .with_thread_names(true)
        .with_max_level(tracing::Level::INFO)
        .init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tpc_plan = create_plan("/home/ubuntu/data/tpc-1").await?;
        let normal_plan = create_plan("/home/ubuntu/data/normal-1").await?;

        execute_tpc_query(&tpc_plan).await?;
        execute_query(&normal_plan).await?;

        Ok::<(), anyhow::Error>(())
    })?;

    Ok(())
}

async fn create_plan(path: &str) -> Result<Arc<dyn ExecutionPlan>> {
    let config = SessionConfig::new().with_target_partitions(6);
    let ctx = SessionContext::with_config(config);

    let plan = ctx
        .read_parquet(path, ParquetReadOptions::default().parquet_pruning(true))
        .await?;
    // .unwrap()
    // .aggregate(
    //     vec![col("service"), col("host")],
    //     vec![avg(col("request_bytes"))],
    // )

    Ok(ctx
        .state()
        .create_physical_plan(plan.logical_plan())
        .await?)
}

async fn execute_query(plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
    let config = SessionConfig::new().with_target_partitions(6);
    let ctx = SessionContext::with_config(config);

    let start = Instant::now();

    for i in 0..30 {
        let plan = plan.clone();
        let start = Instant::now();

        let mut batches = datafusion::physical_plan::execute_stream(plan, ctx.task_ctx())?;

        let mut rows = 0;

        while let Some(batch) = batches.next().await {
            rows += batch?.num_rows();
        }

        let elapsed = start.elapsed().as_secs_f64();

        println!("Normal iteration {i}: Read {rows} rows in {elapsed}s");
    }

    let elapsed = start.elapsed().as_secs_f64();

    println!("Normal: 10 concurrent executions in {elapsed}s");

    Ok(())
}

async fn execute_tpc_query(plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
    let registry = ObjectStoreRegistry::new();

    registry.register_store("file", "", Arc::new(AsyncFileStore::new()));

    let runtime_config = RuntimeConfig::new().with_object_store_registry(Arc::new(registry));
    let runtime = Arc::new(RuntimeEnv::new(runtime_config)?);

    let config = SessionConfig::new().with_target_partitions(6);
    let ctx = SessionContext::with_config_rt(config, runtime);

    let scheduler = Arc::new(Scheduler::new_with_driver(IOUringDriver::default(), 6));

    let start = Instant::now();
    for i in 0..30 {
        let start = Instant::now();

        let mut batches = scheduler.schedule(plan.clone(), ctx.task_ctx())?.stream();

        let mut rows = 0;

        while let Some(batch) = batches.next().await {
            rows += batch?.num_rows();
        }

        let elapsed = start.elapsed().as_secs_f64();

        println!("TPC iteration {i}: Read {rows} rows in {elapsed}s");
    }
    let elapsed = start.elapsed().as_secs_f64();

    println!("Normal: 10 concurrent executions in {elapsed}s");

    Ok(())
}
