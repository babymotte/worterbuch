use crate::{
    controller::{ClientAddress, Protocol, TuiMessage, UserAction},
    tui::input::Input,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
use ratatui::layout::{Position, Rect};
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
    Name,
}

pub struct ConnectDialog {
    pub protocol: Protocol,
    pub address: Input,
    pub name: Input,
    pub field: ConnectField,
    pub error: Option<String>,
}

impl Default for ConnectDialog {
    fn default() -> Self {
        Self {
            protocol: Protocol::Tcp,
            address: Input::default(),
            name: Input::default(),
            field: ConnectField::Address,
            error: None,
        }
    }
}

/// A focusable element in a client view. `Get` / `Subscribe` / `Set` are the
/// inline action buttons; pressing Enter on one fires it.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Focus {
    Key,
    Get,
    Subscribe,
    Value,
    Set,
    Subs,
}

impl Focus {
    fn next(self) -> Self {
        match self {
            Focus::Key => Focus::Get,
            Focus::Get => Focus::Subscribe,
            Focus::Subscribe => Focus::Value,
            Focus::Value => Focus::Set,
            Focus::Set => Focus::Subs,
            Focus::Subs => Focus::Key,
        }
    }

    fn prev(self) -> Self {
        match self {
            Focus::Key => Focus::Subs,
            Focus::Get => Focus::Key,
            Focus::Subscribe => Focus::Get,
            Focus::Value => Focus::Subscribe,
            Focus::Set => Focus::Value,
            Focus::Subs => Focus::Set,
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
    pub name: Option<String>,
    pub connected: bool,
    pub focus: Focus,
    pub key: Input,
    pub value: Input,
    pub log: VecDeque<LogEntry>,
    pub subscriptions: Vec<Subscription>,
    pub sub_selected: usize,
}

impl ClientTab {
    fn new(client_id: ClientId, address: ClientAddress, name: Option<String>) -> Self {
        Self {
            client_id,
            address,
            name,
            connected: true,
            focus: Focus::Key,
            key: Input::default(),
            value: Input::default(),
            log: VecDeque::new(),
            subscriptions: Vec::new(),
            sub_selected: 0,
        }
    }

    pub fn title(&self) -> String {
        if let Some(name) = self.name.clone() {
            name
        } else {
            self.address.to_string()
        }
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

/// Screen rectangles recorded while rendering, so that mouse clicks can be
/// hit-tested against UI elements.
#[derive(Default)]
pub struct Regions {
    pub tabs: Vec<(usize, Rect)>,
    pub new_tab: Option<Rect>,
    pub address: Option<Rect>,
    pub key: Option<Rect>,
    pub value: Option<Rect>,
    pub get: Option<Rect>,
    pub subscribe: Option<Rect>,
    pub set: Option<Rect>,
    pub subs: Option<Rect>,
    pub sub_rows: Vec<(usize, Rect)>,
    pub dialog_protocol: Option<Rect>,
    pub dialog_protocol_items: Vec<(Protocol, Rect)>,
    pub dialog_address: Option<Rect>,
    pub dialog_name: Option<Rect>,
}

pub struct App {
    pub mode: Mode,
    pub tabs: Vec<ClientTab>,
    pub selected: usize,
    pub toasts: Vec<Toast>,
    pub should_quit: bool,
    pub regions: Regions,
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
            regions: Regions::default(),
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
            TuiMessage::ClientAdded {
                address,
                client_id,
                name,
            } => {
                self.tabs
                    .push(ClientTab::new(client_id, address.clone(), name));
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
                    ConnectField::Address => ConnectField::Name,
                    ConnectField::Name => ConnectField::Protocol,
                };
            }
            KeyCode::Enter => {
                let address = dialog.address.value().trim().to_owned();
                if address.is_empty() {
                    dialog.error = Some("address must not be empty".to_owned());
                    return;
                }
                let protocol = dialog.protocol;

                let name = dialog.name.value().trim().to_owned();
                let name = if name.is_empty() { None } else { Some(name) };
                self.send(UserAction::CreateClient {
                    protocol,
                    address,
                    name,
                });
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
            _ if dialog.field == ConnectField::Name => {
                dialog.name.handle_key(key);
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
        let focus = tab.focus;
        let key = tab.key.value().trim().to_owned();
        let value = tab.value.value().to_owned();

        if matches!(focus, Focus::Subs) {
            return;
        }

        if key.is_empty() {
            if let Some(tab) = self.tabs.get_mut(self.selected) {
                tab.push_log(LogKind::Error, "key must not be empty");
            }
            return;
        }

        // Enter in the Key field runs Get, Enter in the Value field runs Set;
        // otherwise the focused button decides.
        match focus {
            Focus::Key | Focus::Get => self.send(UserAction::Get { client, key }),
            Focus::Value | Focus::Set => self.send(UserAction::Set { client, key, value }),
            Focus::Subscribe => self.send(UserAction::Subscribe {
                client,
                key,
                unique: false,
                live_only: false,
            }),
            Focus::Subs => {}
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

    // ----------------------------------------------------------------------
    // Mouse events
    // ----------------------------------------------------------------------

    pub fn on_mouse(&mut self, ev: MouseEvent) {
        if !matches!(ev.kind, MouseEventKind::Down(MouseButton::Left)) {
            return;
        }
        let pos = Position::new(ev.column, ev.row);
        if matches!(self.mode, Mode::Connect(_)) {
            self.on_mouse_connect(pos);
        } else {
            self.on_mouse_normal(pos);
        }
    }

    fn on_mouse_normal(&mut self, pos: Position) {
        let r = &self.regions;

        if r.new_tab.is_some_and(|rect| rect.contains(pos)) {
            self.mode = Mode::Connect(ConnectDialog::default());
            return;
        }

        if let Some((idx, _)) = r.tabs.iter().find(|(_, rect)| rect.contains(pos)) {
            let idx = *idx;
            if idx < self.tabs.len() {
                self.selected = idx;
            }
            return;
        }

        let hit_key = r.key.is_some_and(|rect| rect.contains(pos));
        let hit_value = r.value.is_some_and(|rect| rect.contains(pos));
        let hit_get = r.get.is_some_and(|rect| rect.contains(pos));
        let hit_subscribe = r.subscribe.is_some_and(|rect| rect.contains(pos));
        let hit_set = r.set.is_some_and(|rect| rect.contains(pos));
        let hit_sub_row = r
            .sub_rows
            .iter()
            .find(|(_, rect)| rect.contains(pos))
            .map(|(row, _)| *row);
        let hit_subs = r.subs.is_some_and(|rect| rect.contains(pos));

        let Some(tab) = self.tabs.get_mut(self.selected) else {
            return;
        };

        // Clicking an action button focuses it *and* fires it right away;
        // focusing it via Tab does not fire.
        let mut fire = false;
        if hit_key {
            tab.focus = Focus::Key;
        } else if hit_get {
            tab.focus = Focus::Get;
            fire = true;
        } else if hit_subscribe {
            tab.focus = Focus::Subscribe;
            fire = true;
        } else if hit_value {
            tab.focus = Focus::Value;
        } else if hit_set {
            tab.focus = Focus::Set;
            fire = true;
        } else if let Some(row) = hit_sub_row {
            tab.focus = Focus::Subs;
            if row < tab.subscriptions.len() {
                tab.sub_selected = row;
            }
        } else if hit_subs {
            tab.focus = Focus::Subs;
        }

        if fire {
            self.run_action();
        }
    }

    fn on_mouse_connect(&mut self, pos: Position) {
        let r = &self.regions;
        let proto = r
            .dialog_protocol_items
            .iter()
            .find(|(_, rect)| rect.contains(pos))
            .map(|(proto, _)| *proto);
        let hit_protocol = r.dialog_protocol.is_some_and(|rect| rect.contains(pos));
        let hit_address = r.dialog_address.is_some_and(|rect| rect.contains(pos));
        let hit_name = r.dialog_name.is_some_and(|rect| rect.contains(pos));

        let Mode::Connect(dialog) = &mut self.mode else {
            return;
        };
        if let Some(proto) = proto {
            // Picking a protocol is done; move on to typing the address.
            dialog.protocol = proto;
            dialog.field = ConnectField::Address;
        } else if hit_protocol {
            dialog.field = ConnectField::Protocol;
        } else if hit_address {
            dialog.field = ConnectField::Address;
        } else if hit_name {
            dialog.field = ConnectField::Name;
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
