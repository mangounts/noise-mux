# noise-mux

[![Crates.io](https://img.shields.io/crates/v/noise-mux.svg)](https://crates.io/crates/noise-mux)
[![Docs.rs](https://docs.rs/noise-mux/badge.svg)](https://docs.rs/noise-mux)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)

A lightweight, TLS-free, QUIC-like multiplexed streaming library over **UDP** for Rust and Tokio.

`noise-mux` delivers modern QUIC-style multiplexed streaming without the heavy burden or mandatory TLS 1.3 / X.509 certificate requirements. It bridges the gap between fast, reliable UDP transport (via ARQ), authenticated encryption with raw public keys (via [Snow](https://crates.io/crates/snow) and the [Noise Protocol Framework](http://noiseprotocol.org/)), and concurrent stream multiplexing (via [Yamux](https://crates.io/crates/yamux)).

---

## Why noise-mux?

* **Strictly No TLS / No X.509:** Authenticate peers directly with public keys (Curve25519) instead of complex CA infrastructures, ASN.1 parsing, or TLS 1.3 certificate overhead.
* **QUIC-style UDP Architecture:** Runs directly over UDP (`tokio::net::UdpSocket`) with fast reliable ARQ, packet loss recovery, and low-latency congestion control.
* **Concurrent Sub-stream Multiplexing:** Open virtually unlimited concurrent bidirectional sub-streams over a single UDP connection without head-of-line blocking across sessions.
* **0-RTT Sub-stream Creation:** Handshake happens once per UDP session. Opening a new stream only takes a compact 12-byte Yamux frame.
* **Transport Agnostic:** Native high-level UDP support via `NoiseMuxListener` / `connect_udp`, with support for custom `AsyncRead + AsyncWrite` transports.

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
       [ QUIC-like Reliable ARQ over UDP without TLS: KCP ]
                                   │
                                   ▼
                 [ Underlying Transport: UDP Socket ]
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

### UDP Echo Server

Bind a UDP listener, accept incoming encrypted sessions without TLS, listen for multiple concurrent sub-streams, and echo received payloads back.

```rust
use noise_mux::NoiseMuxListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut listener = NoiseMuxListener::bind("127.0.0.1:8989", "Noise_NN_25519_ChaChaPoly_BLAKE2s").await?;
    println!("UDP server listening on 127.0.0.1:8989 (No TLS)...");

    while let Ok((mut server, peer_addr)) = listener.accept().await {
        println!("Accepted encrypted UDP session from {}", peer_addr);

        tokio::spawn(async move {
            // Accept concurrent sub-streams inside this UDP session
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

### UDP Client with Parallel Streams

Connect to the server over UDP, complete the Noise handshake, and open multiple parallel streams over the same UDP session:

```rust
use noise_mux::NoiseMuxSession;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Establish the encrypted session over UDP without TLS
    let mut session = NoiseMuxSession::connect_udp(
        "127.0.0.1:8989", 
        "Noise_NN_25519_ChaChaPoly_BLAKE2s"
    ).await?;

    // 2. Open parallel streams concurrently over a single UDP socket
    let mut handles = Vec::new();

    for i in 1..=3 {
        let mut stream = session.open_stream().await?;

        handles.push(tokio::spawn(async move {
            let msg = format!("Hello from UDP stream #{}", i);
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

## Building an Application Protocol

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
