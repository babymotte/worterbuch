use crate::{
    controller::{ClientAddress, Protocol, TuiMessage, UserAction},
    tui::input::Input,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use worterbuch_common::{
    ClientId,
    protocol::v1::{TransactionId, Value},
};

const MAX_LOG_LINES: usize = 500;
const TOAST_TTL: Duration = Duration::from_secs(5);

/// What the user is currently interacting with.
pub enum Mode {
    Normal,
    Connect(ConnectDialog),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ConnectField {
    Protocol,
    Address,
}

pub struct ConnectDialog {
    pub protocol: Protocol,
    pub address: Input,
    pub field: ConnectField,
    pub error: Option<String>,
}

impl Default for ConnectDialog {
    fn default() -> Self {
        Self {
            protocol: Protocol::Tcp,
            address: Input::default(),
            field: ConnectField::Address,
            error: None,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Focus {
    Key,
    Value,
    Action,
    Subs,
}

impl Focus {
    fn next(self) -> Self {
        match self {
            Focus::Key => Focus::Value,
            Focus::Value => Focus::Action,
            Focus::Action => Focus::Subs,
            Focus::Subs => Focus::Key,
        }
    }

    fn prev(self) -> Self {
        match self {
            Focus::Key => Focus::Subs,
            Focus::Value => Focus::Key,
            Focus::Action => Focus::Value,
            Focus::Subs => Focus::Action,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ActionKind {
    Get,
    Set,
    Subscribe,
}

impl ActionKind {
    pub const ALL: [ActionKind; 3] = [ActionKind::Get, ActionKind::Set, ActionKind::Subscribe];

    pub fn label(self) -> &'static str {
        match self {
            ActionKind::Get => "Get",
            ActionKind::Set => "Set",
            ActionKind::Subscribe => "Subscribe",
        }
    }

    fn next(self) -> Self {
        match self {
            ActionKind::Get => ActionKind::Set,
            ActionKind::Set => ActionKind::Subscribe,
            ActionKind::Subscribe => ActionKind::Get,
        }
    }

    fn prev(self) -> Self {
        match self {
            ActionKind::Get => ActionKind::Subscribe,
            ActionKind::Set => ActionKind::Get,
            ActionKind::Subscribe => ActionKind::Set,
        }
    }
}

#[derive(Clone, Copy)]
pub enum LogKind {
    Info,
    Ok,
    Result,
    Event,
    Error,
}

pub struct LogEntry {
    pub kind: LogKind,
    pub text: String,
}

pub struct Subscription {
    pub id: TransactionId,
    pub key: String,
}

pub struct ClientTab {
    pub client_id: ClientId,
    pub address: ClientAddress,
    pub connected: bool,
    pub focus: Focus,
    pub key: Input,
    pub value: Input,
    pub action: ActionKind,
    pub log: VecDeque<LogEntry>,
    pub subscriptions: Vec<Subscription>,
    pub sub_selected: usize,
}

impl ClientTab {
    fn new(client_id: ClientId, address: ClientAddress) -> Self {
        Self {
            client_id,
            address,
            connected: true,
            focus: Focus::Key,
            key: Input::default(),
            value: Input::default(),
            action: ActionKind::Get,
            log: VecDeque::new(),
            subscriptions: Vec::new(),
            sub_selected: 0,
        }
    }

    pub fn title(&self) -> String {
        format!("{}://{}", self.address.protocol, self.address.socket)
    }

    fn push_log(&mut self, kind: LogKind, text: impl Into<String>) {
        self.log.push_back(LogEntry {
            kind,
            text: text.into(),
        });
        while self.log.len() > MAX_LOG_LINES {
            self.log.pop_front();
        }
    }
}

pub enum ToastKind {
    Info,
    Error,
}

pub struct Toast {
    pub kind: ToastKind,
    pub text: String,
    created: Instant,
}

pub struct App {
    pub mode: Mode,
    pub tabs: Vec<ClientTab>,
    pub selected: usize,
    pub toasts: Vec<Toast>,
    pub should_quit: bool,
    actions: mpsc::Sender<UserAction>,
}

impl App {
    pub fn new(actions: mpsc::Sender<UserAction>) -> Self {
        Self {
            mode: Mode::Normal,
            tabs: Vec::new(),
            selected: 0,
            toasts: Vec::new(),
            should_quit: false,
            actions,
        }
    }

    fn send(&self, action: UserAction) {
        self.actions.try_send(action).ok();
    }

    fn toast(&mut self, kind: ToastKind, text: impl Into<String>) {
        self.toasts.push(Toast {
            kind,
            text: text.into(),
            created: Instant::now(),
        });
    }

    /// Drop expired toasts. Called periodically from the event loop.
    pub fn tick(&mut self) {
        let now = Instant::now();
        self.toasts
            .retain(|t| now.duration_since(t.created) < TOAST_TTL);
    }

    pub fn selected_tab(&self) -> Option<&ClientTab> {
        self.tabs.get(self.selected)
    }

    fn tab_mut(&mut self, client_id: ClientId) -> Option<&mut ClientTab> {
        self.tabs.iter_mut().find(|t| t.client_id == client_id)
    }

    // ----------------------------------------------------------------------
    // Backend messages
    // ----------------------------------------------------------------------

    pub fn on_message(&mut self, msg: TuiMessage) {
        match msg {
            TuiMessage::ClientAdded { address, client_id } => {
                self.tabs.push(ClientTab::new(client_id, address));
                self.selected = self.tabs.len() - 1;
                self.mode = Mode::Normal;
                self.toast(ToastKind::Info, format!("Connected to {address}"));
            }
            TuiMessage::ConnectionFailed {
                protocol,
                address,
                error,
            } => {
                if let Mode::Connect(dialog) = &mut self.mode {
                    dialog.error = Some(error.clone());
                }
                self.toast(
                    ToastKind::Error,
                    format!("Connection to {protocol}://{address} failed: {error}"),
                );
            }
            TuiMessage::ClientDisconnected { client_id } => {
                if let Some(tab) = self.tab_mut(client_id) {
                    tab.connected = false;
                    tab.push_log(
                        LogKind::Error,
                        "connection lost — press Ctrl+R to reconnect",
                    );
                    let title = tab.title();
                    self.toast(ToastKind::Error, format!("{title} disconnected"));
                }
            }
            TuiMessage::ClientReconnected {
                old_client_id,
                client_id,
                address,
            } => {
                if let Some(tab) = self.tab_mut(old_client_id) {
                    tab.client_id = client_id;
                    tab.address = address;
                    tab.connected = true;
                    tab.subscriptions.clear();
                    tab.sub_selected = 0;
                    tab.push_log(LogKind::Ok, "reconnected (subscriptions cleared)");
                    let title = tab.title();
                    self.toast(ToastKind::Info, format!("{title} reconnected"));
                }
            }
            TuiMessage::ActionFailed { client_id, error } => {
                if let Some(tab) = self.tab_mut(client_id) {
                    tab.push_log(LogKind::Error, error);
                }
            }
            TuiMessage::GetResult {
                client_id,
                key,
                value,
            } => {
                if let Some(tab) = self.tab_mut(client_id) {
                    tab.push_log(LogKind::Result, format!("{key} = {}", render_value(&value)));
                }
            }
            TuiMessage::SetOk { client_id, key } => {
                if let Some(tab) = self.tab_mut(client_id) {
                    tab.push_log(LogKind::Ok, format!("set {key}"));
                }
            }
            TuiMessage::SubscriptionStarted {
                client_id,
                sub,
                key,
            } => {
                if let Some(tab) = self.tab_mut(client_id) {
                    tab.subscriptions.push(Subscription {
                        id: sub,
                        key: key.clone(),
                    });
                    tab.push_log(LogKind::Info, format!("subscribed to {key} (#{sub})"));
                }
            }
            TuiMessage::SubscriptionEvent {
                client_id,
                sub,
                value,
            } => {
                if let Some(tab) = self.tab_mut(client_id) {
                    let key = tab
                        .subscriptions
                        .iter()
                        .find(|s| s.id == sub)
                        .map(|s| s.key.clone())
                        .unwrap_or_else(|| format!("#{sub}"));
                    tab.push_log(LogKind::Event, format!("{key} -> {}", render_value(&value)));
                }
            }
            TuiMessage::SubscriptionStopped { client_id, sub } => {
                if let Some(tab) = self.tab_mut(client_id) {
                    tab.subscriptions.retain(|s| s.id != sub);
                    if tab.sub_selected >= tab.subscriptions.len() {
                        tab.sub_selected = tab.subscriptions.len().saturating_sub(1);
                    }
                    tab.push_log(LogKind::Info, format!("unsubscribed #{sub}"));
                }
            }
        }
    }

    // ----------------------------------------------------------------------
    // Key events
    // ----------------------------------------------------------------------

    pub fn on_key(&mut self, key: KeyEvent) {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        let alt = key.modifiers.contains(KeyModifiers::ALT);

        // Global bindings, available in every mode.
        match key.code {
            KeyCode::Char('q') if ctrl => {
                self.should_quit = true;
                return;
            }
            KeyCode::Char('n') if ctrl => {
                self.mode = Mode::Connect(ConnectDialog::default());
                return;
            }
            KeyCode::Char('w') if ctrl && matches!(self.mode, Mode::Normal) => {
                self.close_current_tab();
                return;
            }
            KeyCode::Char('r') if ctrl && matches!(self.mode, Mode::Normal) => {
                self.reconnect_current_tab();
                return;
            }
            KeyCode::Left if alt => {
                self.prev_tab();
                return;
            }
            KeyCode::Right if alt => {
                self.next_tab();
                return;
            }
            _ => {}
        }

        match &mut self.mode {
            Mode::Connect(_) => self.on_key_connect(key),
            Mode::Normal => self.on_key_tab(key),
        }
    }

    fn on_key_connect(&mut self, key: KeyEvent) {
        let Mode::Connect(dialog) = &mut self.mode else {
            return;
        };

        match key.code {
            KeyCode::Esc => {
                self.mode = Mode::Normal;
            }
            KeyCode::Tab | KeyCode::BackTab => {
                dialog.field = match dialog.field {
                    ConnectField::Protocol => ConnectField::Address,
                    ConnectField::Address => ConnectField::Protocol,
                };
            }
            KeyCode::Enter => {
                let address = dialog.address.value().trim().to_owned();
                if address.is_empty() {
                    dialog.error = Some("address must not be empty".to_owned());
                    return;
                }
                let protocol = dialog.protocol;
                self.send(UserAction::CreateClient { protocol, address });
            }
            KeyCode::Left if dialog.field == ConnectField::Protocol => {
                dialog.protocol = dialog.protocol.prev();
            }
            KeyCode::Right if dialog.field == ConnectField::Protocol => {
                dialog.protocol = dialog.protocol.next();
            }
            _ if dialog.field == ConnectField::Address => {
                dialog.address.handle_key(key);
            }
            _ => {}
        }
    }

    fn on_key_tab(&mut self, key: KeyEvent) {
        let Some(tab) = self.tabs.get_mut(self.selected) else {
            return;
        };
        let focus = tab.focus;

        // Text editing takes precedence when a text field is focused.
        match focus {
            Focus::Key if tab.key.handle_key(key) => return,
            Focus::Value if tab.value.handle_key(key) => return,
            _ => {}
        }

        // Navigation that only touches the current tab.
        match key.code {
            KeyCode::Tab => {
                tab.focus = focus.next();
                return;
            }
            KeyCode::BackTab => {
                tab.focus = focus.prev();
                return;
            }
            KeyCode::Left if focus == Focus::Action => {
                tab.action = tab.action.prev();
                return;
            }
            KeyCode::Right if focus == Focus::Action => {
                tab.action = tab.action.next();
                return;
            }
            KeyCode::Up if focus == Focus::Subs => {
                tab.sub_selected = tab.sub_selected.saturating_sub(1);
                return;
            }
            KeyCode::Down if focus == Focus::Subs => {
                if tab.sub_selected + 1 < tab.subscriptions.len() {
                    tab.sub_selected += 1;
                }
                return;
            }
            _ => {}
        }

        // Actions that need access to the backend channel.
        match key.code {
            KeyCode::Char('d') if focus == Focus::Subs => self.unsubscribe_selected(),
            KeyCode::Enter => self.run_action(),
            _ => {}
        }
    }

    fn run_action(&mut self) {
        let Some(tab) = self.tabs.get(self.selected) else {
            return;
        };
        if !tab.connected {
            return;
        }

        let client = tab.client_id;
        let action = tab.action;
        let key = tab.key.value().trim().to_owned();
        let value = tab.value.value().to_owned();

        if key.is_empty() {
            if let Some(tab) = self.tabs.get_mut(self.selected) {
                tab.push_log(LogKind::Error, "key must not be empty");
            }
            return;
        }

        match action {
            ActionKind::Get => self.send(UserAction::Get { client, key }),
            ActionKind::Set => self.send(UserAction::Set { client, key, value }),
            ActionKind::Subscribe => self.send(UserAction::Subscribe {
                client,
                key,
                unique: false,
                live_only: false,
            }),
        }
    }

    fn unsubscribe_selected(&mut self) {
        let Some(tab) = self.tabs.get(self.selected) else {
            return;
        };
        let client = tab.client_id;
        let Some(sub) = tab.subscriptions.get(tab.sub_selected).map(|s| s.id) else {
            return;
        };
        self.send(UserAction::Unsubscribe { client, sub });
    }

    fn close_current_tab(&mut self) {
        if self.tabs.is_empty() {
            return;
        }
        let tab = self.tabs.remove(self.selected);
        // Always tell the backend to drop the client; the request is a no-op if
        // it is already gone (e.g. the connection was lost).
        self.send(UserAction::CloseClient(tab.client_id));
        if self.selected >= self.tabs.len() {
            self.selected = self.tabs.len().saturating_sub(1);
        }
    }

    fn reconnect_current_tab(&mut self) {
        let Some(tab) = self.tabs.get(self.selected) else {
            return;
        };
        if tab.connected {
            return;
        }
        let client = tab.client_id;
        self.send(UserAction::Reconnect { client });
        if let Some(tab) = self.tabs.get_mut(self.selected) {
            tab.push_log(LogKind::Info, "reconnecting…");
        }
    }

    fn next_tab(&mut self) {
        if !self.tabs.is_empty() {
            self.selected = (self.selected + 1) % self.tabs.len();
        }
    }

    fn prev_tab(&mut self) {
        if !self.tabs.is_empty() {
            self.selected = (self.selected + self.tabs.len() - 1) % self.tabs.len();
        }
    }
}

fn render_value(value: &Option<Value>) -> String {
    match value {
        None => "<none>".to_owned(),
        Some(Value::String(s)) => s.clone(),
        Some(v) => v.to_string(),
    }
}
