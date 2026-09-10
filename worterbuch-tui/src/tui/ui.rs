use crate::tui::app::{
    ActionKind, App, ClientTab, ConnectField, Focus, LogKind, Mode, ToastKind,
};
use crate::controller::Protocol;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Position, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Clear, List, ListItem, ListState, Paragraph, Tabs, Wrap},
};

const ACCENT: Color = Color::Cyan;

pub fn render(frame: &mut Frame, app: &App) {
    let area = frame.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(0),
            Constraint::Length(1),
        ])
        .split(area);

    render_tab_bar(frame, app, chunks[0]);

    if app.tabs.is_empty() {
        render_welcome(frame, chunks[1]);
    } else if let Some(tab) = app.selected_tab() {
        render_client(frame, tab, chunks[1]);
    }

    render_footer(frame, app, chunks[2]);

    if let Mode::Connect(_) = &app.mode {
        render_connect_dialog(frame, app, area);
    }

    render_toasts(frame, app, chunks[1]);
}

fn render_tab_bar(frame: &mut Frame, app: &App, area: Rect) {
    if app.tabs.is_empty() {
        let hint = Line::from(vec![
            Span::styled(" worterbuch ", Style::new().fg(Color::Black).bg(ACCENT)),
            Span::raw("  no connections"),
        ]);
        frame.render_widget(Paragraph::new(hint), area);
        return;
    }

    let titles: Vec<Line> = app
        .tabs
        .iter()
        .map(|tab| {
            let marker = if tab.connected { "" } else { " (offline)" };
            Line::from(format!("{}{marker}", tab.title()))
        })
        .collect();

    let tabs = Tabs::new(titles)
        .select(app.selected)
        .highlight_style(Style::new().fg(Color::Black).bg(ACCENT).bold())
        .divider("│");
    frame.render_widget(tabs, area);
}

fn render_welcome(frame: &mut Frame, area: Rect) {
    let text = vec![
        Line::from(""),
        Line::from("Wörterbuch TUI").centered().bold(),
        Line::from(""),
        Line::from("Press Ctrl+N to open a connection.").centered(),
        Line::from("Press Ctrl+Q to quit.").centered(),
    ];
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(Color::DarkGray));
    frame.render_widget(Paragraph::new(text).block(block), centered(area, 50, 9));
}

fn render_client(frame: &mut Frame, tab: &ClientTab, area: Rect) {
    let area = if tab.connected {
        area
    } else {
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Min(0)])
            .split(area);
        frame.render_widget(
            Paragraph::new(
                Line::from(" connection lost — press Ctrl+R to reconnect ")
                    .fg(Color::White)
                    .bg(Color::Red),
            ),
            rows[0],
        );
        rows[1]
    };

    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(42), Constraint::Percentage(58)])
        .split(area);

    render_form(frame, tab, cols[0]);
    render_log(frame, tab, cols[1]);
}

fn render_form(frame: &mut Frame, tab: &ClientTab, area: Rect) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(3),
        ])
        .split(area);

    render_input(
        frame,
        rows[0],
        "Key",
        tab.key.value(),
        tab.focus == Focus::Key,
        Some(tab.key.cursor()),
    );
    render_input(
        frame,
        rows[1],
        "Value",
        tab.value.value(),
        tab.focus == Focus::Value,
        Some(tab.value.cursor()),
    );
    render_action_row(frame, tab, rows[2]);
    render_subs(frame, tab, rows[3]);
}

fn render_input(
    frame: &mut Frame,
    area: Rect,
    label: &str,
    value: &str,
    focused: bool,
    cursor: Option<usize>,
) {
    let block = bordered(label, focused);
    let inner = block.inner(area);
    frame.render_widget(Paragraph::new(value).block(block), area);

    if focused && let Some(cursor) = cursor {
        let x = inner.x + (cursor as u16).min(inner.width.saturating_sub(1));
        frame.set_cursor_position(Position::new(x, inner.y));
    }
}

fn render_action_row(frame: &mut Frame, tab: &ClientTab, area: Rect) {
    let focused = tab.focus == Focus::Action;
    let mut spans = vec![Span::raw(" ")];
    for action in ActionKind::ALL {
        let selected = action == tab.action;
        let style = match (focused, selected) {
            (true, true) => Style::new().fg(Color::Black).bg(ACCENT).bold(),
            (false, true) => Style::new().fg(ACCENT).bold(),
            _ => Style::new().fg(Color::DarkGray),
        };
        spans.push(Span::styled(format!(" {} ", action.label()), style));
        spans.push(Span::raw(" "));
    }
    let block = bordered("Action  (Enter to run)", focused);
    frame.render_widget(Paragraph::new(Line::from(spans)).block(block), area);
}

fn render_subs(frame: &mut Frame, tab: &ClientTab, area: Rect) {
    let focused = tab.focus == Focus::Subs;
    let block = bordered("Subscriptions  (d to cancel)", focused);

    if tab.subscriptions.is_empty() {
        let p = Paragraph::new(Line::from("none").fg(Color::DarkGray)).block(block);
        frame.render_widget(p, area);
        return;
    }

    let items: Vec<ListItem> = tab
        .subscriptions
        .iter()
        .map(|s| ListItem::new(format!("#{} {}", s.id, s.key)))
        .collect();
    let list = List::new(items).block(block).highlight_style(
        Style::new()
            .fg(Color::Black)
            .bg(if focused { ACCENT } else { Color::Gray }),
    );
    let mut state = ListState::default();
    if focused {
        state.select(Some(tab.sub_selected));
    }
    frame.render_stateful_widget(list, area, &mut state);
}

fn render_log(frame: &mut Frame, tab: &ClientTab, area: Rect) {
    let block = bordered("Log", false);
    let inner = block.inner(area);
    let capacity = inner.height as usize;

    let start = tab.log.len().saturating_sub(capacity);
    let items: Vec<ListItem> = tab
        .log
        .iter()
        .skip(start)
        .map(|entry| {
            let style = match entry.kind {
                LogKind::Info => Style::new().fg(Color::Gray),
                LogKind::Ok => Style::new().fg(Color::Green),
                LogKind::Result => Style::new().fg(Color::White),
                LogKind::Event => Style::new().fg(ACCENT),
                LogKind::Error => Style::new().fg(Color::Red),
            };
            ListItem::new(Line::styled(entry.text.clone(), style))
        })
        .collect();

    frame.render_widget(List::new(items).block(block), area);
}

fn render_footer(frame: &mut Frame, app: &App, area: Rect) {
    let hint = match &app.mode {
        Mode::Connect(_) => "Tab: field   ←/→: protocol   Enter: connect   Esc: cancel",
        Mode::Normal if app.tabs.is_empty() => "Ctrl+N: connect   Ctrl+Q: quit",
        Mode::Normal if app.selected_tab().is_some_and(|t| !t.connected) => {
            "Ctrl+R: reconnect   Ctrl+W: close tab   Ctrl+N: connect   Alt+←/→: switch   Ctrl+Q: quit"
        }
        Mode::Normal => {
            "Tab: focus   Enter: run   d: unsubscribe   Ctrl+N: connect   Ctrl+W: close   Alt+←/→: switch   Ctrl+Q: quit"
        }
    };
    frame.render_widget(
        Paragraph::new(Line::from(hint).fg(Color::DarkGray)),
        area,
    );
}

fn render_connect_dialog(frame: &mut Frame, app: &App, area: Rect) {
    let Mode::Connect(dialog) = &app.mode else {
        return;
    };

    let rect = centered(area, 60, 12);
    frame.render_widget(Clear, rect);

    let block = Block::default()
        .title(" New connection ")
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(ACCENT));
    let inner = block.inner(rect);
    frame.render_widget(block, rect);

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Length(3),
            Constraint::Length(1),
            Constraint::Min(1),
        ])
        .margin(1)
        .split(inner);

    // Protocol selector
    let proto_focused = dialog.field == ConnectField::Protocol;
    let mut proto_spans = vec![Span::styled(
        "Protocol  ",
        Style::new().fg(if proto_focused { ACCENT } else { Color::Gray }),
    )];
    for proto in Protocol::ALL {
        let selected = proto == dialog.protocol;
        let style = if selected {
            Style::new().fg(Color::Black).bg(ACCENT).bold()
        } else {
            Style::new().fg(Color::DarkGray)
        };
        proto_spans.push(Span::styled(format!(" {proto} "), style));
        proto_spans.push(Span::raw(" "));
    }
    frame.render_widget(Paragraph::new(Line::from(proto_spans)), rows[0]);

    // Address input
    let addr_focused = dialog.field == ConnectField::Address;
    let addr_block = bordered("Address  (host:port)", addr_focused);
    let addr_inner = addr_block.inner(rows[2]);
    frame.render_widget(
        Paragraph::new(dialog.address.value()).block(addr_block),
        rows[2],
    );
    if addr_focused {
        let x = addr_inner.x
            + (dialog.address.cursor() as u16).min(addr_inner.width.saturating_sub(1));
        frame.set_cursor_position(Position::new(x, addr_inner.y));
    }

    if let Some(err) = &dialog.error {
        frame.render_widget(
            Paragraph::new(Line::from(err.as_str()).fg(Color::Red)).wrap(Wrap { trim: true }),
            rows[4],
        );
    }
}

fn render_toasts(frame: &mut Frame, app: &App, area: Rect) {
    if app.toasts.is_empty() {
        return;
    }

    let width = area.width.saturating_sub(4).min(60);
    let visible = app.toasts.iter().rev().take(4).collect::<Vec<_>>();
    let height = visible.len() as u16 + 2;
    if area.height < height + 1 || width < 10 {
        return;
    }

    let rect = Rect {
        x: area.x + area.width - width - 2,
        y: area.y + area.height - height - 1,
        width,
        height,
    };
    frame.render_widget(Clear, rect);

    let lines: Vec<Line> = visible
        .iter()
        .rev()
        .map(|t| {
            let color = match t.kind {
                ToastKind::Info => Color::Green,
                ToastKind::Error => Color::Red,
            };
            Line::from(t.text.as_str()).fg(color)
        })
        .collect();

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(Color::DarkGray));
    frame.render_widget(
        Paragraph::new(lines).block(block).wrap(Wrap { trim: true }),
        rect,
    );
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn bordered(title: &str, focused: bool) -> Block<'_> {
    let border_style = if focused {
        Style::new().fg(ACCENT)
    } else {
        Style::new().fg(Color::DarkGray)
    };
    Block::default()
        .title(Span::raw(title))
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(border_style)
        .title_style(if focused {
            Style::new().fg(ACCENT).add_modifier(Modifier::BOLD)
        } else {
            Style::new().fg(Color::Gray)
        })
}

fn centered(area: Rect, width: u16, height: u16) -> Rect {
    let width = width.min(area.width);
    let height = height.min(area.height);
    Rect {
        x: area.x + (area.width - width) / 2,
        y: area.y + (area.height - height) / 2,
        width,
        height,
    }
}
