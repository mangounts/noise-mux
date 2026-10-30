# noise-mux

[![Crates.io](https://img.shields.io/crates/v/noise-mux.svg)](https://crates.io/crates/noise-mux)
[![Docs.rs](https://docs.rs/noise-mux/badge.svg)](https://docs.rs/noise-mux)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)

A lightweight, TLS-free, QUIC-like multiplexed streaming library for Rust and Tokio.

`noise-mux` bridges the gap between state-of-the-art authenticated encryption (via [Snow](https://crates.io/crates/snow) and the [Noise Protocol Framework](http://noiseprotocol.org/)) and concurrent stream multiplexing (via [Yamux](https://crates.io/crates/yamux)).

---

## Why noise-mux?

* **No X.509 Certificate Burden:** Authenticate peers directly with public keys (Curve25519) instead of complex CA infrastructures or ASN.1 parsing.
* **QUIC/HTTP-2 Architecture without TLS:** Open virtually unlimited concurrent bidirectional sub-streams over a single encrypted connection with zero head-of-line blocking across streams.
* **0-RTT Sub-stream Creation:** Handshake happens once per session. Opening a new stream only takes a compact 12-byte Yamux frame.
* **Transport Agnostic:** Runs over TCP, KCP, raw UDP with ARQ, Unix domain sockets, or WebSockets.

---

## Architecture

```text
       ┌────────────────────────────────────────────────────────┐
       │   Application Layer (SubStream 1, SubStream 2, ...)    │
       └───────────────────────────▲────────────────────────────┘
                                   │
                   [ Stream Multiplexer: Yamux ]
                                   │
                                   ▼
             [ Authenticated Encryption: Snow (Noise) ]
                                   │
                                   ▼
          [ Length-Delimited Framing (2-byte frame prefix) ]
                                   │
                                   ▼
             [ Underlying Transport: TCP / KCP / Unix ]
```

---

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
noise-mux = "0.1"
tokio = { version = "1.38", features = ["full"] }
```

---

## Quickstart

### Echo Server

Accept incoming encrypted sessions, listen for multiple concurrent sub-streams, and echo received payloads back.

```rust
use noise_mux::NoiseMuxServer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:8989").await?;
    println!("Listening on 127.0.0.1:8989...");

    while let Ok((socket, addr)) = listener.accept().await {
        println!("Accepted connection from {}", addr);

        tokio::spawn(async move {
            // 1. Perform Noise handshake and set up the Yamux session
            let mut server = NoiseMuxServer::accept(
                socket, 
                "Noise_NN_25519_ChaChaPoly_BLAKE2s"
            )
            .await
            .expect("Handshake failed");

            // 2. Accept concurrent sub-streams
            while let Ok(Some(mut stream)) = server.accept_stream().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    let n = stream.read(&mut buf).await.unwrap();

                    // Echo back
                    stream.write_all(&buf[..n]).await.unwrap();
                    stream.shutdown().await.unwrap();
                });
            }
        });
    }

    Ok(())
}
```

---

### Client with Parallel Streams

Connect once, complete the Noise handshake, and open multiple parallel streams over the same connection:

```rust
use noise_mux::NoiseMuxSession;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tcp = TcpStream::connect("127.0.0.1:8989").await?;

    // 1. Establish the encrypted session
    let mut session = NoiseMuxSession::client(
        tcp, 
        "Noise_NN_25519_ChaChaPoly_BLAKE2s"
    ).await?;

    // 2. Open parallel streams concurrently over a single socket
    let mut handles = Vec::new();

    for i in 1..=3 {
        let mut stream = session.open_stream().await?;

        handles.push(tokio::spawn(async move {
            let msg = format!("Hello from stream #{}", i);
            stream.write_all(msg.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();

            let mut reply = [0u8; 1024];
            let n = stream.read(&mut reply).await.unwrap();
            println!("[Stream #{}] Server replied: {}", i, String::from_utf8_lossy(&reply[..n]));
        }));
    }

    for handle in handles {
        handle.await?;
    }

    Ok(())
}
```

---

## Building an HTTP-like Protocol

Each `SubStream` implements standard `tokio::io::AsyncRead + AsyncWrite`. You can use formats like JSON, CBOR, or [Postcard](https://crates.io/crates/postcard) directly on top:

```rust
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Request {
    pub path: String,
    pub payload: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub struct Response {
    pub status: u16,
    pub body: Vec<u8>,
}
```

---

## Handshake Patterns Supported

`noise-mux` supports any standard Noise handshake string supported by `snow`:

| Pattern | Mutual Auth | Use Case |
| :--- | :--- | :--- |
| `Noise_NN` | None (Ephemeral only) | Anonymous opportunistic encryption (testing, internal proxies). |
| `Noise_NK` | Server Authenticated | Client verifies server public key (similar to traditional HTTPS). |
| `Noise_XX` | Mutual Authentication | Both peers exchange and verify static keys (zero-trust, P2P mesh). |

---

## License

Licensed under either of:

* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
* MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.