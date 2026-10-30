use noise_mux::{NoiseMuxListener, NoiseMuxSession};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn test_echo_and_parallel_streams() -> Result<(), Box<dyn std::error::Error>> {
    let mut listener = NoiseMuxListener::bind("127.0.0.1:0", "Noise_NN_25519_ChaChaPoly_BLAKE2s").await?;
    let addr = listener.local_addr()?;

    let server_handle = tokio::spawn(async move {
        let (mut server, _) = listener.accept().await.unwrap();

        while let Ok(Some(mut stream)) = server.accept_stream().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let n = stream.read(&mut buf).await.unwrap();
                stream.write_all(&buf[..n]).await.unwrap();
                stream.shutdown().await.unwrap();
            });
        }
    });

    let mut session = NoiseMuxSession::connect_udp(addr, "Noise_NN_25519_ChaChaPoly_BLAKE2s").await?;

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
