use crate::error::ConnectionResult;
use crate::{
    client::Connection,
    codec::{
        encode_message, read_server_message, ClientMessage as CM, Export, Get, Import, PGet,
        PSubscribe, ServerMessage as SM, Set, Subscribe,
    },
};
use futures_util::{SinkExt, StreamExt};
use std::sync::{Arc, Mutex};
use tokio::{
    spawn,
    sync::broadcast,
    sync::mpsc::{self, UnboundedSender},
};
use tokio_tungstenite::{connect_async, tungstenite};

#[derive(Clone)]
pub struct WsConnection {
    cmd_tx: UnboundedSender<CM>,
    result_tx: broadcast::Sender<SM>,
    counter: Arc<Mutex<u64>>,
}

impl WsConnection {
    fn inc_counter(&mut self) -> u64 {
        let mut counter = self.counter.lock().expect("poisoned counter mutex");
        let i = *counter;
        *counter += 1;
        i
    }
}

impl Connection for WsConnection {
    fn set(&mut self, key: &str, value: &str) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx.send(CM::Set(Set {
            transaction_id: i,
            key: key.to_owned(),
            value: value.to_owned(),
        }))?;
        Ok(i)
    }

    fn get(&mut self, key: &str) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx.send(CM::Get(Get {
            transaction_id: i,
            key: key.to_owned(),
        }))?;
        Ok(i)
    }

    fn pget(&mut self, key: &str) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx.send(CM::PGet(PGet {
            transaction_id: i,
            request_pattern: key.to_owned(),
        }))?;
        Ok(i)
    }

    fn subscribe(&mut self, key: &str) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx.send(CM::Subscribe(Subscribe {
            transaction_id: i,
            key: key.to_owned(),
        }))?;
        Ok(i)
    }

    fn psubscribe(&mut self, key: &str) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx.send(CM::PSubscribe(PSubscribe {
            transaction_id: i,
            request_pattern: key.to_owned(),
        }))?;
        Ok(i)
    }

    fn export(&mut self, path: &str) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx.send(CM::Export(Export {
            transaction_id: i,
            path: path.to_owned(),
        }))?;
        Ok(i)
    }

    fn import(&mut self, path: &str) -> ConnectionResult<u64> {
        let i = self.inc_counter();
        self.cmd_tx.send(CM::Import(Import {
            transaction_id: i,
            path: path.to_owned(),
        }))?;
        Ok(i)
    }

    fn responses(&mut self) -> broadcast::Receiver<SM> {
        self.result_tx.subscribe()
    }
}

pub async fn connect(proto: &str, addr: &str, port: u16) -> ConnectionResult<WsConnection> {
    let (server, _) = connect_async(format!("{proto}://{addr}:{port}")).await?;
    let (mut ws_tx, mut ws_rx) = server.split();

    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
    let (result_tx, result_rx) = broadcast::channel(1_000);
    let result_tx_recv = result_tx.clone();

    spawn(async move {
        while let Some(msg) = cmd_rx.recv().await {
            if let Ok(Some(data)) = encode_message(&msg).map(Some) {
                let msg = tungstenite::Message::Binary(data);
                if let Err(e) = ws_tx.send(msg).await {
                    eprintln!("failed to send tcp message: {e}");
                    break;
                }
            } else {
                break;
            }
        }
        // make sure initial rx is not dropped as long as stdin is read
        drop(result_rx);
    });

    spawn(async move {
        loop {
            if let Some(Ok(incoming_msg)) = ws_rx.next().await {
                if incoming_msg.is_binary() {
                    let data = incoming_msg.into_data();
                    match read_server_message(&*data).await {
                        Ok(Some(msg)) => {
                            if let Err(e) = result_tx_recv.send(msg) {
                                eprintln!("Error forwarding server message: {e}");
                            }
                        }
                        Ok(None) => {
                            eprintln!("Connection to server lost.");
                            break;
                        }
                        Err(e) => {
                            eprintln!("Error decoding message: {e}");
                        }
                    }
                }
            }
        }
    });

    let con = WsConnection {
        cmd_tx,
        result_tx,
        counter: Arc::default(),
    };

    Ok(con)
}
