use std::sync::Arc;

use criterion::Criterion;
use criterion::SamplingMode;
use criterion::{criterion_group, criterion_main};

use datafusion::datasource::object_store::ObjectStoreRegistry;
use datafusion::execution::runtime_env::RuntimeConfig;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::ParquetReadOptions;
use datafusion::prelude::SessionConfig;
use datafusion::prelude::SessionContext;
use datafusion::scheduler::Scheduler;
use datafusion_tpc::driver::IOUringDriver;
use datafusion_tpc::object_store::file::AsyncFileStore;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::runtime::Builder;

use anyhow::Result;

const QUERY: &[(&str,&str); 3] = &[
    ("full_scan", "SELECT * FROM logs"),
    ("filter", "SELECT service, pod FROM logs WHERE request_method = 'DELETE'"),
    ("aggregation", "SELECT container, pod, AVG(response_bytes) AS avg_response_size FROM logs GROUP BY container, pod"),
];

async fn setup(
    table_path: &str,
    ctx: &SessionContext,
) -> Result<Vec<(String, Arc<dyn ExecutionPlan>)>> {
    let mut plans = vec![];

    ctx.register_parquet(
        "logs",
        table_path,
        ParquetReadOptions::default().parquet_pruning(true),
    )
    .await?;

    for (name, sql) in QUERY {
        let plan = ctx.sql(sql).await?.create_physical_plan().await?;
        plans.push((name.to_string(), plan));
    }

    Ok(plans)
}

async fn run(ctx: &SessionContext, plan: &Arc<dyn ExecutionPlan>) {
    let mut batches =
        datafusion::physical_plan::execute_stream(plan.clone(), ctx.task_ctx()).unwrap();

    let mut rows = 0;
    while let Some(batch) = batches.next().await {
        rows += batch.unwrap().num_rows();
    }
}

async fn run_concurrent(ctx: &SessionContext, plan: &Arc<dyn ExecutionPlan>, concurrency: usize) {
    let mut tasks = vec![];

    for _ in 0..concurrency {
        tasks.push(async {
            let mut batches =
                datafusion::physical_plan::execute_stream(plan.clone(), ctx.task_ctx()).unwrap();

            let mut rows = 0;
            while let Some(batch) = batches.next().await {
                rows += batch.unwrap().num_rows();
            }
        });
    }

    let _ = futures::future::join_all(tasks).await;
}

async fn run_scheduled(ctx: &SessionContext, scheduler: &Scheduler, plan: &Arc<dyn ExecutionPlan>) {
    let mut batches = scheduler
        .schedule(plan.clone(), ctx.task_ctx())
        .unwrap()
        .stream();

    let mut rows = 0;

    while let Some(batch) = batches.next().await {
        rows += batch.unwrap().num_rows();
    }
}

async fn run_scheduled_concurrenct(
    ctx: &SessionContext,
    scheduler: &Scheduler,
    plan: &Arc<dyn ExecutionPlan>,
    concurrency: usize,
) {
    let mut tasks = vec![];

    for _ in 0..concurrency {
        tasks.push(async {
            let mut batches = scheduler
                .schedule(plan.clone(), ctx.task_ctx())
                .unwrap()
                .stream();

            let mut rows = 0;

            while let Some(batch) = batches.next().await {
                rows += batch.unwrap().num_rows();
            }
        });
    }

    let _ = futures::future::join_all(tasks).await;
}

fn bench_standard(c: &mut Criterion) {
    let rt = Builder::new_multi_thread().enable_all().build().unwrap();

    let config = SessionConfig::new().with_target_partitions(6);
    let ctx = Arc::new(SessionContext::with_config(config));

    let mut group = c.benchmark_group("standard");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);

    let plans = rt
        .block_on(setup("/home/ubuntu/data/normal-1", ctx.as_ref()))
        .unwrap();

    for (name, plan) in plans {
        group.bench_function(&name, |b| {
            b.to_async(&rt).iter(|| run(ctx.as_ref(), &plan));
        });
    }
}

fn bench_concurrency_standard(c: &mut Criterion) {
    let rt = Builder::new_multi_thread().enable_all().build().unwrap();

    let config = SessionConfig::new().with_target_partitions(6);
    let ctx = Arc::new(SessionContext::with_config(config));

    let mut group = c.benchmark_group("standard_concurrent");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);

    let plans = rt
        .block_on(setup("/home/ubuntu/data/normal-1", ctx.as_ref()))
        .unwrap();

    for (name, plan) in plans {
        group.bench_function(&name, |b| {
            b.to_async(&rt)
                .iter(|| run_concurrent(ctx.as_ref(), &plan, 12));
        });
    }
}

fn bench_scheduled(c: &mut Criterion) {
    let rt = Builder::new_current_thread().enable_all().build().unwrap();

    let registry = Arc::new(ObjectStoreRegistry::new());

    let runtime_config = RuntimeConfig::new().with_object_store_registry(registry.clone());
    let runtime = Arc::new(RuntimeEnv::new(runtime_config).unwrap());

    let config = SessionConfig::new().with_target_partitions(6);
    let ctx = Arc::new(SessionContext::with_config_rt(config, runtime));

    let mut group = c.benchmark_group("scheduled");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);

    let plans = rt
        .block_on(setup("/home/ubuntu/data/tpc-1", ctx.as_ref()))
        .unwrap();

    // Wait to register AsyncFileStore until after planning so we don't need the tokio-uring
    // context while doing registration/planning
    registry.register_store("file", "", Arc::new(AsyncFileStore::new()));

    let scheduler = Arc::new(Scheduler::new_with_driver(IOUringDriver::default(), 6));

    for (name, plan) in plans {
        group.bench_function(&name, |b| {
            b.to_async(&rt)
                .iter(|| run_scheduled(ctx.as_ref(), scheduler.as_ref(), &plan));
        });
    }
}

fn bench_concurrency_scheduled(c: &mut Criterion) {
    let rt = Builder::new_current_thread().enable_all().build().unwrap();

    let registry = Arc::new(ObjectStoreRegistry::new());

    let runtime_config = RuntimeConfig::new().with_object_store_registry(registry.clone());
    let runtime = Arc::new(RuntimeEnv::new(runtime_config).unwrap());

    let config = SessionConfig::new().with_target_partitions(6);
    let ctx = Arc::new(SessionContext::with_config_rt(config, runtime));

    let mut group = c.benchmark_group("scheduled_concurrent");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);

    let plans = rt
        .block_on(setup("/home/ubuntu/data/tpc-1", ctx.as_ref()))
        .unwrap();

    // Wait to register AsyncFileStore until after planning so we don't need the tokio-uring
    // context while doing registration/planning
    registry.register_store("file", "", Arc::new(AsyncFileStore::new()));

    let scheduler = Arc::new(Scheduler::new_with_driver(IOUringDriver::default(), 6));

    for (name, plan) in plans {
        group.bench_function(&name, |b| {
            b.to_async(&rt)
                .iter(|| run_scheduled_concurrenct(ctx.as_ref(), scheduler.as_ref(), &plan, 12));
        });
    }
}

criterion_group!(
    benches,
    bench_standard,
    //        bench_concurrency_standard,
    bench_scheduled,
    //        bench_concurrency_scheduled
);
criterion_main!(benches);
