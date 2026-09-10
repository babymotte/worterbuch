use crate::controller::UserAction;
use tokio::sync::mpsc;

pub struct Tui {}

impl Tui {
    pub fn new(user_action_tx: mpsc::Sender<UserAction>) -> Self {
        // TODO create TUI interface
        Self {}
    }
}
