use futures::{channel::oneshot, Future};
use futures::{AsyncRead, AsyncWrite};
use hyper::{
    client::{
        connect::{Connected, Connection},
        HttpConnector,
    },
    service::Service,
    Client, Uri,
};
use rustls::{ClientConnection, OwnedTrustAnchor, RootCertStore, ServerName};
use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::io::ReadBuf;
use tokio_rustls::TlsStream;

use glommio::net::TcpStream;

use crate::pool::{spawn_detached, spawn_local_io};
use tracing::debug;

pub mod tokio_tpc;

#[derive(Clone)]
struct HyperExecutor;

impl<F> hyper::rt::Executor<F> for HyperExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        debug!("spawing task from HyperExecutor");
        tokio_uring::spawn(fut);
    }
}

fn connect_tls(req: Uri) -> oneshot::Receiver<io::Result<HyperTlsStream>> {
    spawn_local_io(async move {
        let host = req.host().unwrap();
        let port = req.port_u16().unwrap_or(443);
        let address = format!("{host}:{port}");

        let server_name = rustls::ServerName::try_from(host).unwrap();

        let mut root_store = RootCertStore::empty();
        root_store.add_server_trust_anchors(webpki_roots::TLS_SERVER_ROOTS.0.iter().map(|ta| {
            rustls::OwnedTrustAnchor::from_subject_spki_name_constraints(
                ta.subject,
                ta.spki,
                ta.name_constraints,
            )
        }));

        let mut config = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(root_store)
            .with_no_client_auth(); // i guess this was previously the default?

        config.alpn_protocols = vec![b"h2".to_vec()];
        config.enable_early_data = true;

        let connector = tokio_rustls::TlsConnector::from(Arc::new(config));

        let mut conn = glommio::net::TcpStream::connect(address).await?;
        conn.set_nodelay(true)?;

        debug!("creating HyperTlsStream");
        Ok(HyperTlsStream(
            connector.connect(server_name, HyperStream(conn)).await?,
        ))
    })
}

fn connect_tcp(req: Uri) -> oneshot::Receiver<io::Result<HyperStream>> {
    spawn_local_io(async move {
        let host = req.host().unwrap();
        let port = req.port_u16().unwrap_or(443);
        let address = format!("{host}:{port}");

        let mut conn = glommio::net::TcpStream::connect(address).await?;

        conn.set_nodelay(true)?;

        Ok(HyperStream(conn))
    })
}

#[derive(Clone)]
pub struct HyperConnector;

impl Service<Uri> for HyperConnector {
    type Response = HyperStream;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Uri) -> Self::Future {
        let recv = connect_tcp(req);

        Box::pin(async move {
            match recv.await {
                Ok(result) => result,
                Err(error) => Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("{error}"),
                )),
            }
        })
    }
}

#[derive(Clone)]
pub struct HyperHttpsConnector;

impl Service<Uri> for HyperHttpsConnector {
    type Response = HyperTlsStream;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Uri) -> Self::Future {
        let recv = connect_tls(req);

        Box::pin(async move {
            match recv.await {
                Ok(result) => result,
                Err(error) => Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("{error}"),
                )),
            }
        })
    }
}

unsafe impl Send for HyperHttpsConnector {}

pub struct HyperTlsStream(pub tokio_rustls::client::TlsStream<HyperStream>);

impl HyperTlsStream {
    pub fn client_connection(&self) -> &ClientConnection {
        self.0.get_ref().1
    }
}

impl tokio::io::AsyncRead for HyperTlsStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for HyperTlsStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let s = String::from_utf8_lossy(buf);
        debug!("writing {}: {s}", buf.len());
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl Connection for HyperTlsStream {
    fn connected(&self) -> Connected {
        if let Some(b"h2") = self.client_connection().alpn_protocol() {
            Connected::new().negotiated_h2()
        } else {
            Connected::new()
        }
    }
}

pub struct HyperStream(pub TcpStream);

impl tokio::io::AsyncRead for HyperStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        debug!("trying to read {} bytes", buf.remaining());
        Pin::new(&mut self.0)
            .poll_read(cx, buf.initialize_unfilled())
            .map(|n| {
                if let Ok(n) = n {
                    debug!("read {n} bytes");
                    buf.advance(n);
                }
                Ok(())
            })
    }
}

unsafe impl Send for HyperStream {}

impl Connection for HyperStream {
    fn connected(&self) -> Connected {
        Connected::new()
    }
}

impl tokio::io::AsyncWrite for HyperStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        debug!("writing {} bytes", buf.len());
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_close(cx)
    }
}

pub fn hyper_client() -> Client<HyperHttpsConnector> {
    Client::builder()
        .http2_only(true)
        .http2_adaptive_window(true)
        .executor(HyperExecutor)
        .build(HyperHttpsConnector)
}
