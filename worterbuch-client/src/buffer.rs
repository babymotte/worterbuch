use crate::Command;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    spawn,
    sync::{mpsc, oneshot},
    time::sleep,
};
use worterbuch_common::{error::ConnectionResult, Key, KeyValuePair, Value};

type Buffer = Arc<Mutex<HashMap<Key, Value>>>;

const LOCK_MSG: &str = "the lock scope must not contain code that can panic!";

#[derive(Clone)]
pub struct SendBuffer {
    delay: Duration,
    set_tx: mpsc::Sender<KeyValuePair>,
    publish_tx: mpsc::Sender<KeyValuePair>,
    set_buffer: Buffer,
    publish_buffer: Buffer,
    commands: mpsc::Sender<Command>,
}

impl SendBuffer {
    pub(crate) async fn new(commands: mpsc::Sender<Command>, delay: Duration) -> Self {
        let (set_tx, set_rx) = mpsc::channel(1);
        let (publish_tx, publish_rx) = mpsc::channel(1);

        let set_buffer = Buffer::default();
        let publish_buffer = Buffer::default();

        let buf = Self {
            delay,
            set_tx,
            publish_tx,
            set_buffer,
            publish_buffer,
            commands,
        };

        spawn(buf.clone().buffer_set_messages(set_rx));
        spawn(buf.clone().buffer_publish_messages(publish_rx));

        buf
    }

    pub async fn set_later(&self, key: Key, value: Value) -> ConnectionResult<()> {
        self.set_tx.send(KeyValuePair { key, value }).await?;
        Ok(())
    }

    pub async fn publish_later(&self, key: Key, value: Value) -> ConnectionResult<()> {
        self.publish_tx.send(KeyValuePair { key, value }).await?;
        Ok(())
    }

    async fn buffer_set_messages(self, mut rx: mpsc::Receiver<KeyValuePair>) {
        while let Some(KeyValuePair { key, value }) = rx.recv().await {
            let previous = self
                .set_buffer
                .lock()
                .expect(LOCK_MSG)
                .insert(key.clone(), value);
            if previous.is_none() {
                spawn(self.clone().set_value(key));
            }
        }
    }

    async fn buffer_publish_messages(self, mut rx: mpsc::Receiver<KeyValuePair>) {
        while let Some(KeyValuePair { key, value }) = rx.recv().await {
            let previous = self
                .set_buffer
                .lock()
                .expect(LOCK_MSG)
                .insert(key.clone(), value);
            if previous.is_none() {
                spawn(self.clone().publish_value(key));
            }
        }
    }

    async fn set_value(self, key: Key) {
        sleep(self.delay).await;
        let value = self.set_buffer.lock().expect(LOCK_MSG).remove(&key);
        if let Some(value) = value {
            if let Err(e) = self.do_set_value(key, value).await {
                log::error!("Error sending set message: {e}");
            }
        }
    }

    async fn do_set_value(&self, key: Key, value: Value) -> ConnectionResult<()> {
        let (tx, rx) = oneshot::channel();
        self.commands.send(Command::Set(key, value, tx)).await?;
        rx.await.ok();
        Ok(())
    }

    async fn publish_value(self, key: Key) {
        sleep(self.delay).await;
        let value = self.publish_buffer.lock().expect(LOCK_MSG).remove(&key);
        if let Some(value) = value {
            if let Err(e) = self.do_publish_value(key, value).await {
                log::error!("Error sending publish message: {e}");
            }
        }
    }

    async fn do_publish_value(&self, key: Key, value: Value) -> ConnectionResult<()> {
        let (tx, rx) = oneshot::channel();
        self.commands.send(Command::Publish(key, value, tx)).await?;
        rx.await.ok();
        Ok(())
    }
}
