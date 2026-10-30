use noise_mux::{NoiseMuxListener, NoiseMuxSession};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PATTERN: &str = "Noise_NN_25519_ChaChaPoly_BLAKE2s";

#[tokio::test]
async fn test_udp_echo_multiple_streams() -> Result<(), Box<dyn std::error::Error>> {
    let mut listener = NoiseMuxListener::bind("127.0.0.1:0", PATTERN).await?;
    let server_addr = listener.local_addr()?;

    let server_task = tokio::spawn(async move {
        let (mut server, _peer_addr) = listener.accept().await.unwrap();

        while let Ok(Some(mut stream)) = server.accept_stream().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let n = stream.read(&mut buf).await.unwrap();
                stream.write_all(&buf[..n]).await.unwrap();
                stream.shutdown().await.unwrap();
            });
        }
    });

    let mut session = NoiseMuxSession::connect_udp(server_addr, PATTERN).await?;

    let mut handles = Vec::new();
    for i in 1..=5 {
        let mut stream = session.open_stream().await?;
        handles.push(tokio::spawn(async move {
            let msg = format!("Hello UDP stream #{}", i);
            stream.write_all(msg.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();

            let mut reply = [0u8; 1024];
            let n = stream.read(&mut reply).await.unwrap();
            assert_eq!(&reply[..n], msg.as_bytes());
        }));
    }

    for h in handles {
        h.await?;
    }

    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn test_udp_multi_client_sessions() -> Result<(), Box<dyn std::error::Error>> {
    let mut listener = NoiseMuxListener::bind("127.0.0.1:0", PATTERN).await?;
    let server_addr = listener.local_addr()?;

    let server_task = tokio::spawn(async move {
        while let Ok((mut server, _peer_addr)) = listener.accept().await {
            tokio::spawn(async move {
                while let Ok(Some(mut stream)) = server.accept_stream().await {
                    tokio::spawn(async move {
                        let mut buf = [0u8; 128];
                        let n = stream.read(&mut buf).await.unwrap();
                        stream.write_all(&buf[..n]).await.unwrap();
                        stream.shutdown().await.unwrap();
                    });
                }
            });
        }
    });

    let mut client_handles = Vec::new();
    for client_idx in 0..3 {
        let handle = tokio::spawn(async move {
            let mut session = NoiseMuxSession::connect_udp(server_addr, PATTERN).await.unwrap();

            for stream_idx in 0..3 {
                let mut stream = session.open_stream().await.unwrap();
                let msg = format!("udp-client-{}-stream-{}", client_idx, stream_idx);
                stream.write_all(msg.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();

                let mut reply = [0u8; 128];
                let n = stream.read(&mut reply).await.unwrap();
                assert_eq!(String::from_utf8_lossy(&reply[..n]), msg);
            }
        });
        client_handles.push(handle);
    }

    for h in client_handles {
        h.await?;
    }

    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn test_udp_large_payload_transfer() -> Result<(), Box<dyn std::error::Error>> {
    let mut listener = NoiseMuxListener::bind("127.0.0.1:0", PATTERN).await?;
    let server_addr = listener.local_addr()?;

    let payload_size = 120 * 1024; // 120 KB over UDP ARQ
    let payload: Vec<u8> = (0..payload_size).map(|i| (i % 251) as u8).collect();
    let payload_clone = payload.clone();

    let server_task = tokio::spawn(async move {
        let (mut server, _) = listener.accept().await.unwrap();
        let mut stream = server.accept_stream().await.unwrap().expect("stream");

        let mut received = Vec::with_capacity(payload_size);
        let mut buf = [0u8; 16384];
        while received.len() < payload_size {
            let n = stream.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            received.extend_from_slice(&buf[..n]);
        }

        assert_eq!(received.len(), payload_size);
        assert_eq!(received, payload_clone);

        stream.write_all(b"OK").await.unwrap();
        stream.shutdown().await.unwrap();
    });

    let mut session = NoiseMuxSession::connect_udp(server_addr, PATTERN).await?;
    let mut stream = session.open_stream().await?;

    stream.write_all(&payload).await?;
    stream.shutdown().await?;

    let mut ack = [0u8; 2];
    stream.read_exact(&mut ack).await?;
    assert_eq!(&ack, b"OK");

    server_task.await?;
    Ok(())
}
