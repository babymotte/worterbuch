use super::v0::V0;
use crate::auth::JwtClaims;
use tokio::spawn;
use tracing::{Level, debug, instrument, trace};
use worterbuch_common::{
    Ack, CSet, CState, CStateEvent, ClientMessage as CM, Err, ErrorCode, Get, Lock, Privilege,
    ServerMessage, WbApi,
    error::{Context, WorterbuchResult},
};

#[derive(Clone)]
pub struct V1 {
    pub v0: V0,
}

impl V1 {
    pub fn new(v0: V0) -> Self {
        Self { v0 }
    }

    #[instrument(level=Level::TRACE, skip(self), fields(protocol = "v1", client_id=%self.v0.client_id))]
    pub async fn process_incoming_message(
        &self,
        msg: CM,
        authorized: &mut Option<JwtClaims>,
    ) -> WorterbuchResult<()> {
        match msg {
            CM::CGet(msg) => {
                if self
                    .v0
                    .check_auth(Privilege::Read, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Getting CAS value for client {} …", self.v0.client_id);
                    self.cget(msg).await?;
                    trace!("Getting CAS value for client {} done.", self.v0.client_id);
                }
            }
            CM::CSet(msg) => {
                if self
                    .v0
                    .check_auth(Privilege::Write, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Setting cas value for client {} …", self.v0.client_id);
                    self.cset(msg).await?;
                    trace!("Setting cas value for client {} done.", self.v0.client_id);
                }
            }
            CM::Lock(msg) => {
                if self
                    .v0
                    .check_auth(Privilege::Write, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Locking key for client {} …", self.v0.client_id);
                    self.lock(msg).await?;
                    trace!("Locking key for client {} done.", self.v0.client_id);
                }
            }
            CM::AcquireLock(msg) => {
                if self
                    .v0
                    .check_auth(Privilege::Write, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Locking key for client {} …", self.v0.client_id);
                    self.acquire_lock(msg).await?;
                    trace!("Locking key for client {} done.", self.v0.client_id);
                }
            }
            CM::ReleaseLock(msg) => {
                if self
                    .v0
                    .check_auth(Privilege::Write, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Unlocking key for client {} …", self.v0.client_id);
                    self.release_lock(msg).await?;
                    trace!("Unlocking key for client {} done.", self.v0.client_id);
                }
            }
            msg => {
                self.v0.process_incoming_message(msg, authorized).await?;
            }
        }
        Ok(())
    }

    pub async fn cget(&self, msg: Get) -> WorterbuchResult<()> {
        let (value, version) = match self.v0.worterbuch.cget(msg.key).await {
            Ok(it) => it,
            Err(e) => {
                self.v0.handle_store_error(e, msg.transaction_id).await?;
                return Ok(());
            }
        };

        let response = CState {
            transaction_id: msg.transaction_id,
            event: CStateEvent { value, version },
        };

        self.v0
            .tx
            .send(ServerMessage::CState(response))
            .await
            .context(|| {
                format!(
                    "Error sending CSTATE message for transaction ID {}",
                    msg.transaction_id
                )
            })?;

        Ok(())
    }

    pub async fn cset(&self, msg: CSet) -> WorterbuchResult<()> {
        if let Err(e) = self
            .v0
            .worterbuch
            .cset(msg.key, msg.value, msg.version, self.v0.client_id)
            .await
        {
            self.v0.handle_store_error(e, msg.transaction_id).await?;
            return Ok(());
        }

        let response = Ack {
            transaction_id: msg.transaction_id,
        };

        trace!("Value set, queuing Ack …");
        let res = self.v0.tx.send(ServerMessage::Ack(response)).await;
        trace!("Value set, queuing Ack done.");
        res.context(|| {
            format!(
                "Error sending ACK message for transaction ID {}",
                msg.transaction_id
            )
        })?;

        Ok(())
    }

    pub async fn lock(&self, msg: Lock) -> WorterbuchResult<()> {
        if let Err(e) = self.v0.worterbuch.lock(msg.key, self.v0.client_id).await {
            self.v0.handle_store_error(e, msg.transaction_id).await?;
            return Ok(());
        }

        let response = Ack {
            transaction_id: msg.transaction_id,
        };

        trace!("Key locked, queuing Ack …");
        let res = self.v0.tx.send(ServerMessage::Ack(response)).await;
        trace!("Key locked, queuing Ack done.");
        res.context(|| {
            format!(
                "Error sending ACK message for transaction ID {}",
                msg.transaction_id
            )
        })?;

        Ok(())
    }

    pub async fn acquire_lock(&self, msg: Lock) -> WorterbuchResult<()> {
        let rx = match self
            .v0
            .worterbuch
            .acquire_lock(msg.key, self.v0.client_id)
            .await
        {
            Err(e) => {
                self.v0.handle_store_error(e, msg.transaction_id).await?;
                return Ok(());
            }
            Ok(rx) => rx,
        };

        let client = self.v0.tx.clone();
        let transaction_id = msg.transaction_id;
        spawn(async move {
            debug!("Receiving lock confirmation for transaction {transaction_id:?} …");

            match rx.await {
                Ok(_) => {
                    client
                        .send(ServerMessage::Ack(Ack { transaction_id }))
                        .await
                        .ok();
                }
                Err(_) => {
                    client
                        .send(ServerMessage::Err(Err {
                            transaction_id,
                            error_code: ErrorCode::LockAcquisitionCancelled,
                            metadata: "lock acquisition was cancelled".to_owned(),
                        }))
                        .await
                        .ok();
                }
            }
        });

        Ok(())
    }

    pub async fn release_lock(&self, msg: Lock) -> WorterbuchResult<()> {
        if let Err(e) = self
            .v0
            .worterbuch
            .release_lock(msg.key, self.v0.client_id)
            .await
        {
            self.v0.handle_store_error(e, msg.transaction_id).await?;
            return Ok(());
        }

        let response = Ack {
            transaction_id: msg.transaction_id,
        };

        trace!("Key unlocked, queuing Ack …");
        let res = self.v0.tx.send(ServerMessage::Ack(response)).await;
        trace!("Key unlocked, queuing Ack done.");
        res.context(|| {
            format!(
                "Error sending ACK message for transaction ID {}",
                msg.transaction_id
            )
        })?;

        Ok(())
    }
}
