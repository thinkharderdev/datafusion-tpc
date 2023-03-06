use std::sync::Arc;

use criterion::Criterion;
use criterion::SamplingMode;
use criterion::{criterion_group, criterion_main};

use datafusion::datasource::object_store::ObjectStoreRegistry;
use datafusion::execution::runtime_env::RuntimeConfig;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::ParquetReadOptions;
use datafusion::prelude::SessionConfig;
use datafusion::prelude::SessionContext;
use datafusion::scheduler::Scheduler;
use tokio::runtime::Builder;

use anyhow::Result;
use datafusion_tpc::S3ObjectStoreProvider;
use futures_lite::StreamExt;

const AWS_REGION: &str = "eu-west-1";

const TABLE_PATH: &str = "s3://cgx-query-engine-bench-data/access-logs/";

const QUERY: &[(&str,&str); 3] = &[
    ("full_scan", "SELECT * FROM logs"),
    ("filter", "SELECT service, pod FROM logs WHERE request_method = 'DELETE'"),
    ("aggregation", "SELECT container, pod, AVG(response_bytes) AS avg_response_size FROM logs GROUP BY container, pod"),
];

struct DatafusionEnv {
    ctx: Arc<SessionContext>,
    scheduler: Arc<Scheduler>,
}

impl DatafusionEnv {
    pub fn new() -> Result<Self> {
        let object_store_registry = ObjectStoreRegistry::new_with_provider(Some(Arc::new(
            S3ObjectStoreProvider::new(AWS_REGION),
        )));

        let runtime_config =
            RuntimeConfig::default().with_object_store_registry(Arc::new(object_store_registry));

        let runtime_env = Arc::new(RuntimeEnv::new(runtime_config)?);

        let config = SessionConfig::new().with_target_partitions(8);

        Ok(Self {
            ctx: Arc::new(SessionContext::with_config_rt(config, runtime_env)),
            scheduler: Arc::new(Scheduler::new_thread_per_core()),
        })
    }

    async fn run(&self, plan: Arc<dyn ExecutionPlan>) {
        let mut stream = execute_stream(plan, self.ctx.task_ctx()).unwrap();

        while let Some(batch) = stream.next().await {
            let _ = batch.unwrap();
        }
    }

    async fn run_scheduled(&self, plan: Arc<dyn ExecutionPlan>) {
        let mut stream = self
            .scheduler
            .schedule(plan, self.ctx.task_ctx())
            .unwrap()
            .stream();

        while let Some(batch) = stream.next().await {
            let _ = batch.unwrap();
        }
    }
}

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

fn bench_standard(c: &mut Criterion) {
    let rt = Builder::new_multi_thread().enable_all().build().unwrap();

    let env = DatafusionEnv::new().unwrap();

    let mut group = c.benchmark_group("standard");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);

    let plans = rt.block_on(setup(TABLE_PATH, env.ctx.as_ref())).unwrap();

    for (name, plan) in plans {
        group.bench_function(&name, |b| {
            b.to_async(&rt).iter(|| env.run(plan.clone()));
        });
    }
}

fn bench_scheduled(c: &mut Criterion) {
    let rt = Builder::new_current_thread().enable_all().build().unwrap();

    let env = DatafusionEnv::new().unwrap();

    let mut group = c.benchmark_group("scheduled");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);

    let plans = rt.block_on(setup(TABLE_PATH, env.ctx.as_ref())).unwrap();

    for (name, plan) in plans {
        group.bench_function(&name, |b| {
            b.to_async(&rt).iter(|| env.run_scheduled(plan.clone()));
        });
    }
}

criterion_group!(benches, bench_standard, bench_scheduled,);
criterion_main!(benches);
