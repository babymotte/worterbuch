mod app;
mod input;
mod ui;

use crate::controller::{TuiMessage, UserAction};
use app::App;
use crossterm::event::{Event, EventStream, KeyEventKind};
use futures::StreamExt;
use miette::IntoDiagnostic;
use std::time::Duration;
use tokio::{sync::mpsc, time::interval};
use tosub::SubsystemHandle;

/// Restores the terminal to a sane state when the TUI subsystem exits, however
/// it exits.
struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        ratatui::restore();
    }
}

pub async fn run(
    subsys: SubsystemHandle,
    mut messages: mpsc::Receiver<TuiMessage>,
    actions: mpsc::Sender<UserAction>,
) -> miette::Result<()> {
    let mut terminal = ratatui::init();
    let _guard = TerminalGuard;

    let mut app = App::new(actions);
    let mut events = EventStream::new();
    let mut ticker = interval(Duration::from_millis(500));

    let result = loop {
        if let Err(e) = terminal.draw(|frame| ui::render(frame, &app)) {
            break Err(e).into_diagnostic();
        }

        if app.should_quit {
            break Ok(());
        }

        tokio::select! {
            biased;
            _ = subsys.shutdown_requested() => break Ok(()),
            event = events.next() => match event {
                Some(Ok(Event::Key(key))) if key.kind == KeyEventKind::Press => app.on_key(key),
                Some(Ok(_)) => {}
                Some(Err(e)) => break Err(e).into_diagnostic(),
                None => break Ok(()),
            },
            message = messages.recv() => match message {
                Some(message) => app.on_message(message),
                None => break Ok(()),
            },
            _ = ticker.tick() => app.tick(),
        }
    };

    // However the TUI ends, the whole application should shut down with it.
    subsys.request_global_shutdown();

    result
}
