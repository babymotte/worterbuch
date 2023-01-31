use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    spawn,
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
    time::sleep,
};
use worterbuch_common::{error::ConnectionResult, Key, KeyValuePair, Value};

use crate::Connection;

type Buffer = Arc<Mutex<HashMap<Key, Value>>>;

const LOCK_MSG: &str = "the lock scope must not contain code that can panic!";

#[derive(Clone)]
pub struct SendBuffer {
    delay: Duration,
    set_tx: UnboundedSender<KeyValuePair>,
    _publish_tx: UnboundedSender<KeyValuePair>,
    set_buffer: Buffer,
    publish_buffer: Buffer,
    conn: Connection,
}

impl SendBuffer {
    pub async fn new(conn: Connection, delay: Duration) -> Self {
        let (set_tx, set_rx) = mpsc::unbounded_channel();
        let (_publish_tx, publish_rx) = mpsc::unbounded_channel();

        let set_buffer = Buffer::default();
        let publish_buffer = Buffer::default();

        let buf = Self {
            delay,
            set_tx,
            _publish_tx,
            set_buffer,
            publish_buffer,
            conn,
        };

        spawn(buf.clone().buffer_set_messages(set_rx));
        spawn(buf.clone().buffer_publish_messages(publish_rx));

        buf
    }

    pub fn set_later(&self, key: Key, value: Value) -> ConnectionResult<()> {
        self.set_tx.send(KeyValuePair { key, value })?;
        Ok(())
    }

    pub fn publish_later(&self, _key: Key, _value: Value) -> ConnectionResult<()> {
        todo!("publish isn't implemented on worterbuch yet");
        // self.publish_tx.send(KeyValuePair { key, value })?;
        // Ok(())
    }

    pub fn conn(&mut self) -> &mut Connection {
        &mut self.conn
    }

    async fn buffer_set_messages(self, mut rx: UnboundedReceiver<KeyValuePair>) {
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

    async fn buffer_publish_messages(self, mut rx: UnboundedReceiver<KeyValuePair>) {
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

    async fn set_value(mut self, key: Key) {
        sleep(self.delay).await;
        let value = self.set_buffer.lock().expect(LOCK_MSG).remove(&key);
        if let Some(value) = value {
            if let Err(e) = self.conn.set(key, value) {
                log::error!("Error sending set message: {e}");
            }
        }
    }

    async fn publish_value(self, key: Key) {
        sleep(self.delay).await;
        let value = self.publish_buffer.lock().expect(LOCK_MSG).remove(&key);
        if let Some(_value) = value {
            // TODO uncomment once pusblish is implemented
            // if let Err(e) = self.conn.publish(&key, &value) {
            //     log::error!("Error setting key {key} to value {value}: {e}");
            // }
        }
    }
}
