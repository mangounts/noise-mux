use noise_mux::{NoiseMuxServer, NoiseMuxSession};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const PATTERN: &str = "Noise_NN_25519_ChaChaPoly_BLAKE2s";

#[tokio::test]
async fn test_tcp_multi_client_sessions() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let server_task = tokio::spawn(async move {
        let mut session_count = 0;
        while session_count < 3 {
            let (socket, _) = listener.accept().await.unwrap();
            session_count += 1;

            tokio::spawn(async move {
                let mut server = NoiseMuxServer::accept(socket, PATTERN).await.unwrap();

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
            let socket = TcpStream::connect(addr).await.unwrap();
            let mut session = NoiseMuxSession::client(socket, PATTERN).await.unwrap();

            for stream_idx in 0..3 {
                let mut stream = session.open_stream().await.unwrap();
                let msg = format!("client-{}-stream-{}", client_idx, stream_idx);
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
async fn test_tcp_clean_client_disconnect() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let (tx_done, rx_done) = tokio::sync::oneshot::channel();

    let server_task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut server = NoiseMuxServer::accept(socket, PATTERN).await.unwrap();

        // Accept first stream
        let mut stream = server.accept_stream().await.unwrap().expect("stream");
        let mut buf = [0u8; 5];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");

        // Notify client to drop connection
        tx_done.send(()).unwrap();

        // The next accept_stream should observe EOF or None from closed connection
        let next = server.accept_stream().await;
        assert!(matches!(next, Ok(None) | Err(_)));
    });

    let socket = TcpStream::connect(addr).await?;
    let mut session = NoiseMuxSession::client(socket, PATTERN).await?;

    let mut stream = session.open_stream().await?;
    stream.write_all(b"hello").await?;
    stream.flush().await?;

    // Wait until server read the stream
    rx_done.await?;

    // Explicitly drop session and stream
    drop(stream);
    drop(session);

    server_task.await?;
    Ok(())
}
