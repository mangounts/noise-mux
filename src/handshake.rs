use futures::{SinkExt, StreamExt};
use std::io;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub async fn run_initiator_nn<T>(io: T, pattern: &str) -> io::Result<(Framed<T, LengthDelimitedCodec>, snow::TransportState)>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut codec = LengthDelimitedCodec::builder();
    codec.length_field_length(2); // 2 bytes for frame length (up to 65535)
    let mut framed = Framed::new(io, codec.new_codec());

    let mut initiator = snow::Builder::new(pattern.parse().map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?)
        .build_initiator()
        .map_err(io::Error::other)?;

    let mut buf = vec![0u8; 65535];

    // Step 1: -> e
    let len = initiator.write_message(&[], &mut buf)
        .map_err(io::Error::other)?;
    framed.send(bytes::Bytes::copy_from_slice(&buf[..len])).await?;

    // Step 2: <- e, ee
    let resp = framed.next().await
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "Handshake EOF"))??;
    initiator.read_message(&resp, &mut buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let transport = initiator.into_transport_mode()
        .map_err(io::Error::other)?;

    Ok((framed, transport))
}

pub async fn run_responder_nn<T>(io: T, pattern: &str) -> io::Result<(Framed<T, LengthDelimitedCodec>, snow::TransportState)>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut codec = LengthDelimitedCodec::builder();
    codec.length_field_length(2);
    let mut framed = Framed::new(io, codec.new_codec());

    let mut responder = snow::Builder::new(pattern.parse().map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?)
        .build_responder()
        .map_err(io::Error::other)?;

    let mut buf = vec![0u8; 65535];

    let msg1 = framed.next().await
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "Handshake EOF"))??;
    responder.read_message(&msg1, &mut buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let len = responder.write_message(&[], &mut buf)
        .map_err(io::Error::other)?;
    framed.send(bytes::Bytes::copy_from_slice(&buf[..len])).await?;

    let transport = responder.into_transport_mode()
        .map_err(io::Error::other)?;

    Ok((framed, transport))
}
