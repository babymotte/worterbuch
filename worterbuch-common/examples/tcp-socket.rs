/*
 *  Copyright (C) 2024 Michael Bachmann
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

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

    let socket =
        create_tcp_server_socket("0.0.0.0".parse().expect("valid IPv4 address"), 1337, config)
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
