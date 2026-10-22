use std::time::Duration;

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt},
    net::TcpListener,
    spawn,
};
use worterbuch_common::socket::{TcpSocketConfig, create_tcp_server_socket};

#[tokio::main]
async fn main() {
    let config = TcpSocketConfig {
        keepalive_time: Some(Duration::from_secs(5)),
        keepalive_interval: Some(Duration::from_secs(5)),
        keepalive_retries: Some(3),
        send_timeout: Duration::from_secs(5),
    };

    let socket = create_tcp_server_socket("0.0.0.0".parse().unwrap(), 1337, config)
        .expect("socket creation failed");

    let socket = TcpListener::from_std(socket).expect("socket conversion failed");

    while let Ok((stream, addr)) = socket.accept().await {
        spawn(serve(stream, addr));
    }
}

async fn serve(stream: tokio::net::TcpStream, addr: std::net::SocketAddr) {
    eprintln!("Accepted connection from {}", addr);

    let (read_half, write_half) = stream.into_split();

    let mut reader = tokio::io::BufReader::new(read_half).lines();
    let mut writer = tokio::io::BufWriter::new(write_half);

    loop {
        match reader.next_line().await {
            Ok(Some(line)) => {
                eprintln!("Received line from {}: {}", addr, line);
                writer
                    .write_all((line + "\n").as_bytes())
                    .await
                    .expect("write failed");
                writer.flush().await.expect("flush failed");
            }
            Ok(None) => {
                eprintln!("Connection from {} closed by peer.", addr);
                break;
            }
            Err(e) => {
                eprintln!("Error reading from {}: {}", addr, e);
                break;
            }
        }
    }

    eprintln!("Connection from {} closed.", addr);
}
