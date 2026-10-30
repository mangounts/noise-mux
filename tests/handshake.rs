use noise_mux::{NoiseMuxServer, NoiseMuxSession};
use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};

const PATTERN: &str = "Noise_NN_25519_ChaChaPoly_BLAKE2s";

#[tokio::test]
async fn test_handshake_invalid_pattern() {
    let (client_io, _server_io) = duplex(1024);
    let result = NoiseMuxSession::client(client_io, "Noise_Invalid_Pattern_XYZ").await;
    assert!(result.is_err(), "Expected handshake initialization to fail on invalid pattern");
}

#[tokio::test]
async fn test_handshake_client_write_to_closed_server() {
    let (client_io, server_io) = duplex(1024);

    // Drop the server half immediately so client send fails with BrokenPipe
    drop(server_io);

    let result = NoiseMuxSession::client(client_io, PATTERN).await;
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);
}

#[tokio::test]
async fn test_handshake_server_drops_before_response() {
    let (client_io, mut server_io) = duplex(4096);

    let server_task = tokio::spawn(async move {
        // Read client's handshake message
        let mut buf = [0u8; 100];
        let n = server_io.read(&mut buf).await.unwrap();
        assert!(n > 0);
        // Explicitly drop server_io so client experiences UnexpectedEof when waiting for response
        drop(server_io);
    });

    let result = NoiseMuxSession::client(client_io, PATTERN).await;
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);

    server_task.await.unwrap();
}

#[tokio::test]
async fn test_handshake_server_unexpected_eof() {
    let (client_io, server_io) = duplex(1024);
    // Drop client immediately without sending initial handshake message
    drop(client_io);

    let result = NoiseMuxServer::accept(server_io, PATTERN).await;
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
}

#[tokio::test]
async fn test_handshake_corrupted_response() {
    let (client_io, mut server_io) = duplex(4096);

    let server_task = tokio::spawn(async move {
        let mut buf = [0u8; 100];
        let n = server_io.read(&mut buf).await.unwrap();
        assert!(n > 0);

        // Send a frame with garbage payload (not valid Noise handshake message)
        let garbage = [0u8, 10, 0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06];
        server_io.write_all(&garbage).await.unwrap();
    });

    let result = NoiseMuxSession::client(client_io, PATTERN).await;
    assert!(result.is_err(), "Expected client handshake to fail on corrupted response");

    server_task.await.unwrap();
}
