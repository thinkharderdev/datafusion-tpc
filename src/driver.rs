use datafusion::scheduler::worker::*;
use futures::Future;
use glommio::{Latency, LocalExecutor, Shares, TaskQueueHandle};
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tracing::{error, info, trace, warn};

use std::cell::Cell;
use std::thread_local;

thread_local! {
    static IO_TASK_QUEUE: Cell<*const TaskQueueHandle> = Cell::new(std::ptr::null());
}

#[derive(Default, Clone, Debug)]
pub struct IOUringDriver {}

impl IOUringDriver {
    pub fn io_task_queue_handle() -> TaskQueueHandle {
        let handle = unsafe { &*IO_TASK_QUEUE.with(Cell::get) };
        handle.clone()
    }
}

const TICKS_UNTIL_YIELD: usize = 32;

struct SchedulerTick(&'static WorkerContext);

impl Future for SchedulerTick {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        while !self.0.is_terminated() {
            if let Some(task) = self.0.next_task() {
                trace!("executing task {:#?} on {}", task, self.0.name());
                match std::panic::catch_unwind(AssertUnwindSafe(|| task.do_work())) {
                    Ok(()) => {}
                    Err(e) => error!("{}", format_worker_panic(self.0.name(), e)),
                }
                trace!("finished executing task");
            } else {
                return Poll::Pending;
//                break;
            }
        }

        trace!("complete tick");
        Poll::Ready(())
    }
}

impl Driver for IOUringDriver {
    fn run(
        &mut self,
        ctx: &'static WorkerContext,
        _panic_handler: Box<dyn Fn(Box<dyn std::any::Any + Send>) + Send + Sync + 'static>,
    ) {
        info!(
            "[io-uring-driver] starting run loop on worker {}",
            ctx.name()
        );

        tokio_uring::start(async {
            info!("[io-uring-driver] starting run loop");

            while !ctx.is_terminated() {
                if let Err(e) = tokio_uring::spawn(futures_lite::future::or(
                    SchedulerTick(ctx),
                    tokio::task::yield_now(),
                ))
                .await {
                    warn!("[io-uring-driver] error joining driver task: {e}");
                }
            }
        });

        info!(
            "[io-uring-driver] terminating run loop on worker {}",
            ctx.name()
        );
    }
}

fn format_worker_panic(thread_name: &str, panic: Box<dyn std::any::Any + Send>) -> String {
    let message = if let Some(msg) = panic.downcast_ref::<&str>() {
        *msg
    } else if let Some(msg) = panic.downcast_ref::<String>() {
        msg.as_str()
    } else {
        "UNKNOWN"
    };

    format!("worker {thread_name} panicked with: {message}")
}
