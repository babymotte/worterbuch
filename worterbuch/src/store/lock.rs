use std::collections::VecDeque;

use tokio::sync::oneshot;
use uuid::Uuid;

pub struct Lock {
    pub holder: Uuid,
    pub candidates: VecDeque<(Uuid, Vec<oneshot::Sender<()>>)>,
}

impl Lock {
    pub fn new(client_id: Uuid) -> Self {
        Lock {
            holder: client_id,
            candidates: VecDeque::new(),
        }
    }

    pub async fn release(&mut self, client_id: Uuid) -> (bool, bool) {
        if client_id == self.holder {
            if let Some((id, txs)) = self.candidates.pop_front() {
                self.holder = id;
                for tx in txs {
                    tx.send(()).ok();
                }
                (true, false)
            } else {
                (true, true)
            }
        } else {
            self.candidates.retain(|(c, _)| c != &client_id);
            (false, false)
        }
    }

    pub async fn queue(&mut self, client_id: Uuid, tx: oneshot::Sender<()>) {
        if let Some((_, txs)) = self.candidates.iter_mut().find(|(id, _)| id == &client_id) {
            txs.push(tx);
        } else {
            self.candidates.push_back((client_id, vec![tx]));
        }
    }
}
