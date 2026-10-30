use noise_mux::{NoiseMuxServer, NoiseMuxSession};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn test_echo_and_parallel_streams() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let server_handle = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut server = NoiseMuxServer::accept(socket, "Noise_NN_25519_ChaChaPoly_BLAKE2s")
            .await
            .unwrap();

        while let Ok(Some(mut stream)) = server.accept_stream().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let n = stream.read(&mut buf).await.unwrap();
                stream.write_all(&buf[..n]).await.unwrap();
                stream.shutdown().await.unwrap();
            });
        }
    });

    let client_socket = TcpStream::connect(addr).await?;
    let mut session = NoiseMuxSession::client(client_socket, "Noise_NN_25519_ChaChaPoly_BLAKE2s").await?;

    let mut handles = Vec::new();
    for i in 1..=5 {
        let mut stream = session.open_stream().await?;
        handles.push(tokio::spawn(async move {
            let msg = format!("Hello stream #{}", i);
            stream.write_all(msg.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();

            let mut reply = [0u8; 1024];
            let n = stream.read(&mut reply).await.unwrap();
            assert_eq!(&reply[..n], msg.as_bytes());
        }));
    }

    for handle in handles {
        handle.await?;
    }

    server_handle.abort();
    Ok(())
}
