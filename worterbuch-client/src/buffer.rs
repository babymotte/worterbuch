/*
 *  Worterbuch client send buffer module
 *
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
use tracing::error;
use worterbuch_common::{Key, KeyValuePair, Value, error::ConnectionResult};

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
        if let Some(value) = value
            && let Err(e) = self.do_set_value(key, value).await {
                error!("Error sending set message: {e}");
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
        if let Some(value) = value
            && let Err(e) = self.do_publish_value(key, value).await {
                error!("Error sending publish message: {e}");
            }
    }

    async fn do_publish_value(&self, key: Key, value: Value) -> ConnectionResult<()> {
        let (tx, rx) = oneshot::channel();
        self.commands.send(Command::Publish(key, value, tx)).await?;
        rx.await.ok();
        Ok(())
    }
}
