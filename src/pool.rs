use std::cell::Cell;
use std::ptr;
use std::thread_local;
use std::time::Duration;

use core_affinity::CoreId;
use crossbeam_deque::Injector;
use crossbeam_deque::Worker;
use futures::{channel::oneshot, Future};
use glommio::{LocalExecutor, LocalExecutorBuilder};
use rayon::{ThreadPool, ThreadPoolBuilder};
use tracing::{debug, warn};

use std::sync::Arc;

thread_local! {
    static IO_EXECUTOR: Cell<*const LocalExecutor> = Cell::new(ptr::null());
}

fn is_io_worker() -> bool {
    !IO_EXECUTOR.with(Cell::get).is_null()
}

fn local_executor() -> &'static LocalExecutor {
    assert!(is_io_worker(), "not on IO worker!");
    unsafe { &*IO_EXECUTOR.with(Cell::get) }
}

pub fn spawn_detached<F: Future + 'static>(fut: F) {
    //    glommio::spawn_local(fut).detach();
    local_executor().run(async move {
        glommio::spawn_local(fut).detach();
    });
}

pub fn spawn_local_io<F: Future + 'static>(fut: F) -> oneshot::Receiver<F::Output> {
    let (sender, receiver) = oneshot::channel();

    // glommio::spawn_local(async move {
    //     let r = glommio::spawn_local(fut).await;

    //     if let Err(_) = sender.send(r) {
    //         warn!("receiver dropped");
    //     }
    // }).detach();

    local_executor().run(async move {
        glommio::spawn_local(async move {
            let r = glommio::spawn_local(fut).await;

            if let Err(_) = sender.send(r) {
                warn!("receiver dropped");
            }
        })
        .detach();
    });

    // local_executor().run(async move {
    //     glommio::spawn_local(async move {
    //         let r = fut.await;

    //         if let Err(_) = sender.send(r) {
    //             warn!("receiver dropped");
    //         }
    //     }).detach();

    // });

    receiver
}

pub fn init_tokio_worker_pool() -> Arc<ThreadPool> {
    let mut cores = Vec::from_iter(0..num_cpus::get());

    let builder = ThreadPoolBuilder::new()
        .num_threads(cores.len())
        .spawn_handler(|thread| {
            let core_id = cores.pop().unwrap();
            let b = std::thread::Builder::new().name(format!("df-worker-{core_id}"));

            b.spawn(move || {
                debug!(
                    "Starting worker thread {:?} on core {core_id}",
                    std::thread::current().name()
                );
                core_affinity::set_for_current(CoreId { id: core_id });

                let local_set = tokio::task::LocalSet::new();

                let _guard = local_set.enter();

                debug!("Initialized tokio IO worker on core {core_id}");

                thread.run();
            })?;
            Ok(())
        })
        .panic_handler(|p| println!("{}", format_worker_panic(p)))
        .thread_name(|idx| format!("df-worker-{idx}"));

    Arc::new(builder.build().unwrap())
}

pub fn init_worker_pool(injector: Arc<Injector<String>>) -> Arc<ThreadPool> {
    let mut cores = Vec::from_iter(0..num_cpus::get());

    let builder = ThreadPoolBuilder::new()
        .num_threads(cores.len())
        .spawn_handler(|_thread| {
            let core_id = cores.pop().unwrap();
            let b = std::thread::Builder::new().name(format!("df-worker-{core_id}"));

            let thread_name = format!("df-worker-{core_id}");
            let injector = injector.clone();
            let handle = LocalExecutorBuilder::default()
                .name(&thread_name)
                .spawn(|| async move {
                    let local = Worker::new_fifo();

                    loop {
                        if let Some(path) = local.pop().or_else(|| {
                            std::iter::repeat_with(|| injector.steal_batch_and_pop(&local))
                                .find(|s| !s.is_retry())
                                .and_then(|s| s.success())
                        }) {
                        } else {
                            glommio::yield_if_needed().await;
                        }
                    }
                })
                .unwrap();

            handle.join().unwrap();

            Ok(())
            // b.spawn(move || {
            //     debug!(
            //         "Starting worker thread {:?} on core {core_id}",
            //         std::thread::current().name()
            //     );
            //     core_affinity::set_for_current(CoreId { id: core_id });

            //     let executor = &LocalExecutorBuilder::default()
            //         .ring_depth(2048)
            //         .make()
            //         .unwrap();

            //     // debug!("Initialized IO worker on core {core_id}");

            //     // IO_EXECUTOR.with(|exec| {
            //     //     assert!(exec.get().is_null());
            //     //     exec.set(executor);
            //     // });

            //     // thread.run();
            // })?;
            // Ok(())
        })
        .panic_handler(|p| println!("{}", format_worker_panic(p)))
        .thread_name(|idx| format!("df-worker-{idx}"));

    Arc::new(builder.build().unwrap())
}

fn format_worker_panic(panic: Box<dyn std::any::Any + Send>) -> String {
    let maybe_idx = rayon::current_thread_index();
    let worker: &dyn std::fmt::Display = match &maybe_idx {
        Some(idx) => idx,
        None => &"UNKNOWN",
    };

    let message = if let Some(msg) = panic.downcast_ref::<&str>() {
        *msg
    } else if let Some(msg) = panic.downcast_ref::<String>() {
        msg.as_str()
    } else {
        "UNKNOWN"
    };

    format!("worker {worker} panicked with: {message}")
}
