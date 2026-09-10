mod app;
mod input;
mod ui;

use crate::controller::{TuiMessage, UserAction};
use app::App;
use crossterm::{
    event::{
        DisableMouseCapture, EnableMouseCapture, Event, EventStream, KeyEventKind, MouseEventKind,
    },
    execute,
};
use futures::StreamExt;
use miette::IntoDiagnostic;
use std::{io::stdout, time::Duration};
use tokio::{sync::mpsc, time::interval};
use tosub::SubsystemHandle;

/// Restores the terminal to a sane state when the TUI subsystem exits, however
/// it exits.
struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        execute!(stdout(), DisableMouseCapture).ok();
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
    if let Err(e) = execute!(stdout(), EnableMouseCapture) {
        return Err(e).into_diagnostic();
    }

    let mut app = App::new(actions);
    let mut events = EventStream::new();
    let mut ticker = interval(Duration::from_millis(500));
    let mut dirty = true;

    let result = loop {
        if dirty && let Err(e) = terminal.draw(|frame| ui::render(frame, &mut app)) {
            break Err(e).into_diagnostic();
        }

        if app.should_quit {
            break Ok(());
        }
        dirty = true;

        tokio::select! {
            biased;
            _ = subsys.shutdown_requested() => break Ok(()),
            event = events.next() => match event {
                Some(Ok(Event::Key(key))) if key.kind == KeyEventKind::Press => app.on_key(key),
                Some(Ok(Event::Mouse(mouse))) => {
                    if matches!(mouse.kind, MouseEventKind::Down(_)) {
                        app.on_mouse(mouse);
                    } else {
                        // Ignore moves/drags/scroll without forcing a redraw.
                        dirty = false;
                    }
                }
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
