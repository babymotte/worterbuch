use super::v0::V0;
use crate::{
    auth::JwtClaims,
    server::common::{
        protocol::v0::{check_auth, handle_store_error},
        CloneableWbApi,
    },
    Config,
};
use tokio::sync::mpsc;
use uuid::Uuid;
use worterbuch_common::{
    error::{Context, WorterbuchResult},
    Ack, CSet, CState, CStateEvent, ClientMessage as CM, Get, Privilege, ServerMessage,
};

#[derive(Debug, Clone, Default)]
pub struct V1 {
    v0: V0,
}

impl V1 {
    pub async fn process_incoming_message(
        &self,
        client_id: Uuid,
        msg: CM,
        worterbuch: &CloneableWbApi,
        tx: &mpsc::Sender<ServerMessage>,
        auth_required: bool,
        authorized: &mut Option<JwtClaims>,
        config: &Config,
    ) -> WorterbuchResult<()> {
        match msg {
            CM::CGet(msg) => {
                if check_auth(
                    auth_required,
                    Privilege::Read,
                    &msg.key,
                    &authorized,
                    tx,
                    msg.transaction_id,
                )
                .await?
                {
                    log::trace!("Getting CAS value for client {} …", client_id);
                    cget(msg, worterbuch, tx).await?;
                    log::trace!("Getting CAS value for client {} done.", client_id);
                }
            }
            CM::CSet(msg) => {
                if check_auth(
                    auth_required,
                    Privilege::Write,
                    &msg.key,
                    &authorized,
                    tx,
                    msg.transaction_id,
                )
                .await?
                {
                    log::trace!("Setting cas value for client {} …", client_id);
                    cset(msg, worterbuch, tx, client_id).await?;
                    log::trace!("Setting cas value for client {} done.", client_id);
                }
            }
            msg => {
                self.v0
                    .process_incoming_message(
                        client_id,
                        msg,
                        worterbuch,
                        tx,
                        auth_required,
                        authorized,
                        config,
                    )
                    .await?;
            }
        }
        Ok(())
    }
}

pub async fn cget(
    msg: Get,
    worterbuch: &CloneableWbApi,
    client: &mpsc::Sender<ServerMessage>,
) -> WorterbuchResult<()> {
    let (value, version) = match worterbuch.cget(msg.key).await {
        Ok(it) => it,
        Err(e) => {
            handle_store_error(e, client, msg.transaction_id).await?;
            return Ok(());
        }
    };

    let response = CState {
        transaction_id: msg.transaction_id,
        event: CStateEvent { value, version },
    };

    client
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

pub async fn cset(
    msg: CSet,
    worterbuch: &CloneableWbApi,
    client: &mpsc::Sender<ServerMessage>,
    client_id: Uuid,
) -> WorterbuchResult<()> {
    if let Err(e) = worterbuch
        .cset(msg.key, msg.value, msg.version, client_id)
        .await
    {
        handle_store_error(e, client, msg.transaction_id).await?;
        return Ok(());
    }

    let response = Ack {
        transaction_id: msg.transaction_id,
    };

    log::trace!("Value set, queuing Ack …");
    let res = client.send(ServerMessage::Ack(response)).await;
    log::trace!("Value set, queuing Ack done.");
    res.context(|| {
        format!(
            "Error sending ACK message for transaction ID {}",
            msg.transaction_id
        )
    })?;

    Ok(())
}
