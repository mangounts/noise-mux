use noise_mux::{NoiseMuxServer, NoiseMuxSession};
use std::sync::Arc;
use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};

const PATTERN: &str = "Noise_NN_25519_ChaChaPoly_BLAKE2s";

#[tokio::test]
async fn test_in_memory_basic_transfer() -> Result<(), Box<dyn std::error::Error>> {
    let (client_io, server_io) = duplex(65536);

    let server_task = tokio::spawn(async move {
        let mut server = NoiseMuxServer::accept(server_io, PATTERN).await.unwrap();
        let mut stream = server.accept_stream().await.unwrap().expect("expected stream");

        let mut buf = vec![0u8; 12];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping message");

        stream.write_all(b"pong message").await.unwrap();
        stream.shutdown().await.unwrap();
    });

    let mut session = NoiseMuxSession::client(client_io, PATTERN).await?;
    let mut stream = session.open_stream().await?;

    stream.write_all(b"ping message").await?;
    stream.shutdown().await?;

    let mut response = vec![0u8; 12];
    stream.read_exact(&mut response).await?;
    assert_eq!(&response, b"pong message");

    server_task.await?;
    Ok(())
}

#[tokio::test]
async fn test_large_payload_chunking() -> Result<(), Box<dyn std::error::Error>> {
    // 250 KB payload: exceeds single Noise frame limit (65535 bytes) multiple times
    let payload_size = 250 * 1024;
    let payload: Vec<u8> = (0..payload_size).map(|i| (i % 251) as u8).collect();
    let payload = Arc::new(payload);

    let (client_io, server_io) = duplex(128 * 1024);

    let payload_clone = Arc::clone(&payload);
    let server_task = tokio::spawn(async move {
        let mut server = NoiseMuxServer::accept(server_io, PATTERN).await.unwrap();
        let mut stream = server.accept_stream().await.unwrap().expect("expected stream");

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
        assert_eq!(&received, &*payload_clone);

        // Echo back the first 100 bytes as acknowledgment
        stream.write_all(&received[..100]).await.unwrap();
        stream.shutdown().await.unwrap();
    });

    let mut session = NoiseMuxSession::client(client_io, PATTERN).await?;
    let mut stream = session.open_stream().await?;

    stream.write_all(&payload).await?;
    stream.shutdown().await?;

    let mut ack = vec![0u8; 100];
    stream.read_exact(&mut ack).await?;
    assert_eq!(&ack, &payload[..100]);

    server_task.await?;
    Ok(())
}

#[tokio::test]
async fn test_exact_noise_boundary_sizes() -> Result<(), Box<dyn std::error::Error>> {
    // Max single frame payload in Noise is 65535 - 16 = 65519
    let sizes = [65518, 65519, 65520];

    for size in sizes {
        let (client_io, server_io) = duplex(128 * 1024);
        let test_data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        let test_data_clone = test_data.clone();

        let server_task = tokio::spawn(async move {
            let mut server = NoiseMuxServer::accept(server_io, PATTERN).await.unwrap();
            let mut stream = server.accept_stream().await.unwrap().expect("expected stream");

            let mut received = Vec::with_capacity(size);
            let mut buf = [0u8; 8192];
            while received.len() < size {
                let n = stream.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                received.extend_from_slice(&buf[..n]);
            }
            assert_eq!(received, test_data_clone);
        });

        let mut session = NoiseMuxSession::client(client_io, PATTERN).await?;
        let mut stream = session.open_stream().await?;

        stream.write_all(&test_data).await?;
        stream.shutdown().await?;

        server_task.await?;
    }

    Ok(())
}

#[tokio::test]
async fn test_concurrent_many_streams() -> Result<(), Box<dyn std::error::Error>> {
    const NUM_STREAMS: usize = 30;
    let (client_io, server_io) = duplex(128 * 1024);

    let server_task = tokio::spawn(async move {
        let mut server = NoiseMuxServer::accept(server_io, PATTERN).await.unwrap();

        let mut workers = Vec::new();
        for _ in 0..NUM_STREAMS {
            if let Ok(Some(mut stream)) = server.accept_stream().await {
                workers.push(tokio::spawn(async move {
                    let mut buf = [0u8; 64];
                    let n = stream.read(&mut buf).await.unwrap();
                    let msg = String::from_utf8_lossy(&buf[..n]);
                    let reply = format!("ack: {}", msg);
                    stream.write_all(reply.as_bytes()).await.unwrap();
                    stream.shutdown().await.unwrap();
                }));
            }
        }

        for w in workers {
            w.await.unwrap();
        }
    });

    let mut session = NoiseMuxSession::client(client_io, PATTERN).await?;
    let mut client_tasks = Vec::new();

    for i in 0..NUM_STREAMS {
        let mut stream = session.open_stream().await?;
        client_tasks.push(tokio::spawn(async move {
            let request = format!("req-{}", i);
            stream.write_all(request.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();

            let mut reply = [0u8; 64];
            let n = stream.read(&mut reply).await.unwrap();
            assert_eq!(String::from_utf8_lossy(&reply[..n]), format!("ack: req-{}", i));
        }));
    }

    for t in client_tasks {
        t.await?;
    }

    server_task.await?;
    Ok(())
}

#[tokio::test]
async fn test_cloned_control_handles_across_tasks() -> Result<(), Box<dyn std::error::Error>> {
    let (client_io, server_io) = duplex(128 * 1024);

    let server_task = tokio::spawn(async move {
        let mut server = NoiseMuxServer::accept(server_io, PATTERN).await.unwrap();
        let mut workers = Vec::new();
        while workers.len() < 4 {
            if let Ok(Some(mut stream)) = server.accept_stream().await {
                workers.push(tokio::spawn(async move {
                    let mut b = [0u8; 16];
                    let n = stream.read(&mut b).await.unwrap();
                    stream.write_all(&b[..n]).await.unwrap();
                    stream.shutdown().await.unwrap();
                }));
            }
        }
        for w in workers {
            w.await.unwrap();
        }
    });

    let session = NoiseMuxSession::client(client_io, PATTERN).await?;
    let control = session.control();

    let mut tasks = Vec::new();
    for i in 0..4 {
        let ctrl = control.clone();
        tasks.push(tokio::spawn(async move {
            let mut stream = ctrl.open_stream().await.unwrap();
            let msg = format!("worker-{}", i);
            stream.write_all(msg.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();

            let mut resp = [0u8; 16];
            let n = stream.read(&mut resp).await.unwrap();
            assert_eq!(&resp[..n], msg.as_bytes());
        }));
    }

    for t in tasks {
        t.await?;
    }

    server_task.await?;
    Ok(())
}

#[tokio::test]
async fn test_bidirectional_ping_pong_rounds() -> Result<(), Box<dyn std::error::Error>> {
    const ROUNDS: usize = 10;
    let (client_io, server_io) = duplex(65536);

    let server_task = tokio::spawn(async move {
        let mut server = NoiseMuxServer::accept(server_io, PATTERN).await.unwrap();
        let mut stream = server.accept_stream().await.unwrap().expect("stream");

        for round in 0..ROUNDS {
            let mut buf = [0u8; 32];
            let n = stream.read(&mut buf).await.unwrap();
            assert_eq!(String::from_utf8_lossy(&buf[..n]), format!("ping-{}", round));

            let reply = format!("pong-{}", round);
            stream.write_all(reply.as_bytes()).await.unwrap();
        }
        stream.shutdown().await.unwrap();
    });

    let mut session = NoiseMuxSession::client(client_io, PATTERN).await?;
    let mut stream = session.open_stream().await?;

    for round in 0..ROUNDS {
        let ping = format!("ping-{}", round);
        stream.write_all(ping.as_bytes()).await?;

        let mut buf = [0u8; 32];
        let n = stream.read(&mut buf).await?;
        assert_eq!(String::from_utf8_lossy(&buf[..n]), format!("pong-{}", round));
    }

    server_task.await?;
    Ok(())
}

#[tokio::test]
async fn test_stream_eof_after_shutdown() -> Result<(), Box<dyn std::error::Error>> {
    let (client_io, server_io) = duplex(65536);

    let server_task = tokio::spawn(async move {
        let mut server = NoiseMuxServer::accept(server_io, PATTERN).await.unwrap();
        let mut stream = server.accept_stream().await.unwrap().expect("stream");

        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        assert_eq!(&buf, b"send until shutdown");

        // Write final response and shutdown
        stream.write_all(b"completed").await.unwrap();
        stream.shutdown().await.unwrap();
    });

    let mut session = NoiseMuxSession::client(client_io, PATTERN).await?;
    let mut stream = session.open_stream().await?;

    stream.write_all(b"send until shutdown").await?;
    // Half-close write side
    stream.shutdown().await?;

    // Read server's full response until server's EOF
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    assert_eq!(&response, b"completed");

    server_task.await?;
    Ok(())
}
