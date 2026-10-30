use bytes::{Buf, BytesMut};
use futures::{Sink, Stream};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

const MAX_NOISE_FRAME: usize = 65535;
const NOISE_TAG_LEN: usize = 16;
const MAX_PAYLOAD_LEN: usize = MAX_NOISE_FRAME - NOISE_TAG_LEN;

pub struct EncryptedAdapter<S> {
    framed: S,
    transport: snow::TransportState,
    decrypted_buf: BytesMut,
    encrypt_scratch: Vec<u8>,
}

impl<S> EncryptedAdapter<S> {
    pub fn new(framed: S, transport: snow::TransportState) -> Self {
        Self {
            framed,
            transport,
            decrypted_buf: BytesMut::with_capacity(MAX_NOISE_FRAME),
            encrypt_scratch: vec![0u8; MAX_NOISE_FRAME],
        }
    }
}

impl<S> AsyncRead for EncryptedAdapter<S>
where
    S: Stream<Item = Result<BytesMut, io::Error>> + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.as_mut().get_mut();

        if !this.decrypted_buf.is_empty() {
            let to_read = std::cmp::min(buf.remaining(), this.decrypted_buf.len());
            buf.put_slice(&this.decrypted_buf[..to_read]);
            this.decrypted_buf.advance(to_read);
            return Poll::Ready(Ok(()));
        }

        match Pin::new(&mut this.framed).poll_next(cx) {
            Poll::Ready(Some(Ok(ciphertext))) => {
                let mut temp_buf = vec![0u8; MAX_NOISE_FRAME];
                match this.transport.read_message(&ciphertext, &mut temp_buf) {
                    Ok(len) => {
                        this.decrypted_buf.extend_from_slice(&temp_buf[..len]);
                        let to_read = std::cmp::min(buf.remaining(), this.decrypted_buf.len());
                        buf.put_slice(&this.decrypted_buf[..to_read]);
                        this.decrypted_buf.advance(to_read);
                        Poll::Ready(Ok(()))
                    }
                    Err(_) => Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Noise decryption failed (authentication tag mismatch)",
                    ))),
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Err(e)),
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S> AsyncWrite for EncryptedAdapter<S>
where
    S: Sink<bytes::Bytes, Error = io::Error> + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.as_mut().get_mut();

        // Check if the sink is ready before encrypting to avoid incrementing the Noise cipher nonce on Poll::Pending
        match Pin::new(&mut this.framed).poll_ready(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => return Poll::Pending,
        }

        let chunk_size = std::cmp::min(buf.len(), MAX_PAYLOAD_LEN);
        let enc_len = match this
            .transport
            .write_message(&buf[..chunk_size], &mut this.encrypt_scratch)
        {
            Ok(len) => len,
            Err(_) => {
                return Poll::Ready(Err(io::Error::other(
                    "Noise encryption failure",
                )))
            }
        };

        let payload = bytes::Bytes::copy_from_slice(&this.encrypt_scratch[..enc_len]);
        match Pin::new(&mut this.framed).start_send(payload) {
            Ok(()) => Poll::Ready(Ok(chunk_size)),
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().framed).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().framed).poll_close(cx)
    }
}
