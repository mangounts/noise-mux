pub mod adapter;
pub mod handshake;

use adapter::EncryptedAdapter;
use std::io;
use std::task::Poll;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};

pub use tokio_kcp::{KcpConfig, KcpNoDelayConfig};
pub use yamux::StreamId;

pub type SubStream = tokio_util::compat::Compat<yamux::Stream>;

enum ControlMsg {
    OpenStream(tokio::sync::oneshot::Sender<io::Result<SubStream>>),
}

/// A handle to open new outbound multiplexed sub-streams.
#[derive(Clone)]
pub struct Control {
    sender: tokio::sync::mpsc::Sender<ControlMsg>,
}

impl Control {
    pub async fn open_stream(&self) -> io::Result<SubStream> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(ControlMsg::OpenStream(tx))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::ConnectionAborted, "connection closed"))?;
        rx.await
            .map_err(|_| io::Error::new(io::ErrorKind::ConnectionAborted, "connection closed"))?
    }
}

pub struct NoiseMuxSession {
    control: Control,
}

impl NoiseMuxSession {
    /// Connect to a remote server over UDP using fast reliable ARQ (QUIC-like over UDP) without TLS,
    /// perform the Noise handshake, and establish a multiplexed session.
    pub async fn connect_udp<A: tokio::net::ToSocketAddrs>(addr: A, pattern: &str) -> io::Result<Self> {
        let config = tokio_kcp::KcpConfig {
            nodelay: tokio_kcp::KcpNoDelayConfig::fastest(),
            ..Default::default()
        };
        Self::connect_udp_with_config(addr, pattern, config).await
    }

    /// Connect to a remote server over UDP with custom KCP configuration.
    pub async fn connect_udp_with_config<A: tokio::net::ToSocketAddrs>(
        addr: A,
        pattern: &str,
        config: tokio_kcp::KcpConfig,
    ) -> io::Result<Self> {
        let socket_addr = tokio::net::lookup_host(addr)
            .await?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "could not resolve address"))?;
        let stream = tokio_kcp::KcpStream::connect(&config, socket_addr)
            .await
            .map_err(io::Error::other)?;
        Self::client(stream, pattern).await
    }

    /// Establish a multiplexed Noise session over any custom asynchronous stream.
    pub async fn client<T>(transport: T, pattern: &str) -> io::Result<Self>
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (framed, snow_transport) = handshake::run_initiator_nn(transport, pattern).await?;
        let adapter = EncryptedAdapter::new(framed, snow_transport);
        let compat = adapter.compat();

        let mut conn = yamux::Connection::new(compat, yamux::Config::default(), yamux::Mode::Client);
        let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::channel::<ControlMsg>(32);

        tokio::spawn(async move {
            let mut pending_open: Option<tokio::sync::oneshot::Sender<io::Result<SubStream>>> = None;
            let mut is_cmd_closed = false;

            futures::future::poll_fn(|cx| {
                while pending_open.is_none() && !is_cmd_closed {
                    match cmd_rx.poll_recv(cx) {
                        Poll::Ready(Some(ControlMsg::OpenStream(tx))) => {
                            match conn.poll_new_outbound(cx) {
                                Poll::Ready(Ok(stream)) => {
                                    let _ = tx.send(Ok(stream.compat()));
                                }
                                Poll::Ready(Err(e)) => {
                                    let _ = tx.send(Err(io::Error::new(io::ErrorKind::ConnectionAborted, e)));
                                }
                                Poll::Pending => {
                                    pending_open = Some(tx);
                                    break;
                                }
                            }
                        }
                        Poll::Ready(None) => {
                            is_cmd_closed = true;
                            break;
                        }
                        Poll::Pending => break,
                    }
                }

                if let Some(tx) = pending_open.take() {
                    match conn.poll_new_outbound(cx) {
                        Poll::Ready(Ok(stream)) => {
                            let _ = tx.send(Ok(stream.compat()));
                        }
                        Poll::Ready(Err(e)) => {
                            let _ = tx.send(Err(io::Error::new(io::ErrorKind::ConnectionAborted, e)));
                        }
                        Poll::Pending => {
                            pending_open = Some(tx);
                        }
                    }
                }

                loop {
                    match conn.poll_next_inbound(cx) {
                        Poll::Ready(Some(Ok(_stream))) => {
                            continue;
                        }
                        Poll::Ready(Some(Err(e))) => {
                            if let Some(tx) = pending_open.take() {
                                let _ = tx.send(Err(io::Error::new(io::ErrorKind::ConnectionAborted, e)));
                            }
                            return Poll::Ready(());
                        }
                        Poll::Ready(None) => {
                            if let Some(tx) = pending_open.take() {
                                let _ = tx.send(Err(io::Error::new(io::ErrorKind::ConnectionReset, "connection closed")));
                            }
                            return Poll::Ready(());
                        }
                        Poll::Pending => break,
                    }
                }

                if is_cmd_closed && pending_open.is_none() {
                    match conn.poll_close(cx) {
                        Poll::Ready(_) => return Poll::Ready(()),
                        Poll::Pending => return Poll::Pending,
                    }
                }

                Poll::Pending
            }).await;
        });

        Ok(Self {
            control: Control { sender: cmd_tx },
        })
    }

    pub fn control(&self) -> Control {
        self.control.clone()
    }

    pub async fn open_stream(&mut self) -> io::Result<SubStream> {
        self.control.open_stream().await
    }
}

pub trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncStream for T {}

pub struct NoiseMuxServer {
    stream_rx: tokio::sync::mpsc::Receiver<io::Result<SubStream>>,
}

impl NoiseMuxServer {
    /// Accept a multiplexed Noise session over any custom asynchronous stream.
    pub async fn accept<T>(transport: T, pattern: &str) -> io::Result<Self>
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (framed, snow_transport) = handshake::run_responder_nn(Box::new(transport) as Box<dyn AsyncStream>, pattern).await?;
        let adapter = EncryptedAdapter::new(framed, snow_transport);
        let compat = adapter.compat();

        let mut conn = yamux::Connection::new(compat, yamux::Config::default(), yamux::Mode::Server);
        let (stream_tx, stream_rx) = tokio::sync::mpsc::channel::<io::Result<SubStream>>(128);

        tokio::spawn(async move {
            let mut pending_stream: Option<SubStream> = None;

            futures::future::poll_fn(|cx| {
                if let Some(stream) = pending_stream.take() {
                    match stream_tx.try_send(Ok(stream)) {
                        Ok(()) => {}
                        Err(tokio::sync::mpsc::error::TrySendError::Full(s)) => {
                            if let Ok(s) = s {
                                pending_stream = Some(s);
                            }
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                            return conn.poll_close(cx).map(|_| ());
                        }
                    }
                }

                loop {
                    if pending_stream.is_some() {
                        break;
                    }

                    match conn.poll_next_inbound(cx) {
                        Poll::Ready(Some(Ok(stream))) => {
                            let substream = stream.compat();
                            match stream_tx.try_send(Ok(substream)) {
                                Ok(()) => continue,
                                Err(tokio::sync::mpsc::error::TrySendError::Full(s)) => {
                                    if let Ok(s) = s {
                                        pending_stream = Some(s);
                                    }
                                    break;
                                }
                                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                    return conn.poll_close(cx).map(|_| ());
                                }
                            }
                        }
                        Poll::Ready(Some(Err(e))) => {
                            let _ = stream_tx.try_send(Err(io::Error::new(io::ErrorKind::ConnectionReset, e)));
                            return Poll::Ready(());
                        }
                        Poll::Ready(None) => {
                            return Poll::Ready(());
                        }
                        Poll::Pending => break,
                    }
                }

                if stream_tx.is_closed() && pending_stream.is_none() {
                    return conn.poll_close(cx).map(|_| ());
                }

                Poll::Pending
            }).await;
        });

        Ok(Self { stream_rx })
    }

    pub async fn accept_stream(&mut self) -> io::Result<Option<SubStream>> {
        match self.stream_rx.recv().await {
            Some(Ok(stream)) => Ok(Some(stream)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }
}

/// A UDP listener for accepting encrypted, multiplexed Noise sessions without TLS.
pub struct NoiseMuxListener {
    listener: tokio_kcp::KcpListener,
    pattern: String,
}

impl NoiseMuxListener {
    /// Bind a UDP socket with fast reliable ARQ (QUIC-like over UDP) without TLS.
    pub async fn bind<A: tokio::net::ToSocketAddrs>(addr: A, pattern: &str) -> io::Result<Self> {
        let config = tokio_kcp::KcpConfig {
            nodelay: tokio_kcp::KcpNoDelayConfig::fastest(),
            ..Default::default()
        };
        Self::bind_with_config(addr, pattern, config).await
    }

    /// Bind a UDP socket with custom KCP configuration.
    pub async fn bind_with_config<A: tokio::net::ToSocketAddrs>(
        addr: A,
        pattern: &str,
        config: tokio_kcp::KcpConfig,
    ) -> io::Result<Self> {
        let listener = tokio_kcp::KcpListener::bind(config, addr)
            .await
            .map_err(io::Error::other)?;
        Ok(Self {
            listener,
            pattern: pattern.to_string(),
        })
    }

    /// Returns the local socket address of the underlying UDP socket.
    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    /// Accept the next incoming UDP session and perform the Noise handshake.
    pub async fn accept(&mut self) -> io::Result<(NoiseMuxServer, std::net::SocketAddr)> {
        let (stream, peer_addr) = self.listener.accept().await.map_err(io::Error::other)?;
        let server = NoiseMuxServer::accept(stream, &self.pattern).await?;
        Ok((server, peer_addr))
    }
}
