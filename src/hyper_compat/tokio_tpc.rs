use futures::Future;
use hyper::Client;
use rustls::RootCertStore;
use tracing::debug;

struct HyperExecutor;

impl<F> hyper::rt::Executor<F> for HyperExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        debug!("spawing task from HyperExecutor");
        tokio::task::spawn_local(fut);
    }
}

pub fn hyper_client() -> Client<hyper_rustls::HttpsConnector<hyper::client::HttpConnector>> {
    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_only()
        .enable_http2()
        .build();

    Client::builder()
        .http2_only(true)
        .http2_adaptive_window(true)
        .executor(HyperExecutor)
        .build(https)
}
