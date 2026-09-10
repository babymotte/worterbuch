use crate::controller::Protocol;
use crate::tui::app::{App, ClientTab, ConnectField, Focus, LogKind, Mode, Regions, ToastKind};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Position, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap},
};

const ACCENT: Color = Color::Cyan;

pub fn render(frame: &mut Frame, app: &mut App) {
    let mut regions = Regions::default();
    draw(frame, app, &mut regions);
    app.regions = regions;
}

fn draw(frame: &mut Frame, app: &App, regions: &mut Regions) {
    let area = frame.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(0),
            Constraint::Length(2),
            Constraint::Length(1),
        ])
        .split(area);

    let main = chunks[0];

    let selected_tab = app.selected_tab();
    if app.tabs.is_empty() {
        render_welcome(frame, main);
    } else if let Some(tab) = selected_tab {
        render_client(frame, tab, main, regions);
    }

    render_tab_bar(frame, app, chunks[1], regions, selected_tab);
    render_footer(frame, app, chunks[2]);

    if let Mode::Connect(_) = &app.mode {
        render_connect_dialog(frame, app, area, regions);
    }

    render_toasts(frame, app, main);
}

fn render_tab_bar(
    frame: &mut Frame,
    app: &App,
    area: Rect,
    regions: &mut Regions,
    selected_tab: Option<&ClientTab>,
) {
    let end = area.x.saturating_add(area.width);

    // Right-aligned client id of the selected tab. Only shown when the tab bar
    // is wide enough that it doesn't crowd out the tabs themselves.
    let id_text = selected_tab
        .map(|t| t.client_id.to_string())
        .unwrap_or_default();
    let id_w = id_text.chars().count() as u16;
    let show_id = id_w > 0 && area.width > id_w + 16;
    let reserve = if show_id { id_w + 2 } else { 0 };
    let tabs_end = end.saturating_sub(reserve).max(area.x);

    let connecting = matches!(app.mode, Mode::Connect(_));
    let divider = Style::new().fg(Color::DarkGray);
    let mut spans = Vec::new();
    let mut x = area.x;

    for (i, tab) in app.tabs.iter().enumerate() {
        if x >= tabs_end {
            break;
        }
        let marker = if tab.connected { "" } else { " (offline)" };
        let label = format!(" {}{marker} ", tab.title());
        let w = label.chars().count() as u16;
        regions.tabs.push((
            i,
            Rect {
                x,
                y: area.y,
                width: w.min(tabs_end - x),
                height: 1,
            },
        ));

        let style = if i == app.selected && !connecting {
            Style::new().fg(Color::Black).bg(ACCENT).bold()
        } else {
            Style::new().fg(Color::Gray)
        };
        spans.push(Span::styled(label, style));
        x = x.saturating_add(w);

        spans.push(Span::styled("│", divider));
        x = x.saturating_add(1);
    }

    // "+" tab: opens the new-connection dialog when clicked.
    if x < tabs_end {
        let label = " + ";
        let w = label.chars().count() as u16;
        regions.new_tab = Some(Rect {
            x,
            y: area.y,
            width: w.min(tabs_end - x),
            height: 1,
        });
        let style = if connecting {
            Style::new().fg(Color::Black).bg(ACCENT).bold()
        } else {
            Style::new().fg(Color::Green).bold()
        };
        spans.push(Span::styled(label, style));
    }

    if app.tabs.is_empty() {
        spans.push(Span::styled(
            "  no connections",
            Style::new().fg(Color::DarkGray),
        ));
    }

    // Tab strip, clipped so it never overlaps the right-aligned client id.
    let strip = Rect {
        x: area.x,
        y: area.y,
        width: tabs_end.saturating_sub(area.x),
        height: 1,
    };
    frame.render_widget(Paragraph::new(Line::from(spans)), strip);

    if show_id {
        let id_area = Rect {
            x: tabs_end,
            y: area.y,
            width: end - tabs_end,
            height: 1,
        };
        frame.render_widget(
            Paragraph::new(Line::from(id_text).right_aligned().fg(Color::Gray)),
            id_area,
        );
    }
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

fn render_client(frame: &mut Frame, tab: &ClientTab, area: Rect, regions: &mut Regions) {
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

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(3),
        ])
        .split(area);

    // Key row: [ Key input ][ Get ][ Subscribe ]
    let key_row = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Min(10),
            Constraint::Length(9),
            Constraint::Length(15),
        ])
        .split(rows[0]);
    regions.key = Some(key_row[0]);
    regions.get = Some(key_row[1]);
    regions.subscribe = Some(key_row[2]);
    render_input(
        frame,
        key_row[0],
        "Key",
        tab.key.value(),
        tab.focus == Focus::Key,
        Some(tab.key.cursor()),
    );
    render_button(frame, key_row[1], "Get", tab.focus == Focus::Get);
    render_button(
        frame,
        key_row[2],
        "Subscribe",
        tab.focus == Focus::Subscribe,
    );

    // Value row: [ Value input ][ Set ]
    let value_row = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(10), Constraint::Length(9)])
        .split(rows[1]);
    regions.value = Some(value_row[0]);
    regions.set = Some(value_row[1]);
    render_input(
        frame,
        value_row[0],
        "Value",
        tab.value.value(),
        tab.focus == Focus::Value,
        Some(tab.value.cursor()),
    );
    render_button(frame, value_row[1], "Set", tab.focus == Focus::Set);

    let bottom = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(rows[2]);

    regions.subs = Some(bottom[0]);
    render_subs(frame, tab, bottom[0], regions);
    render_log(frame, tab, bottom[1]);
}

/// A 3-row bordered action button. Enter fires it while focused.
fn render_button(frame: &mut Frame, area: Rect, label: &str, focused: bool) {
    let border_style = if focused {
        Style::new().fg(ACCENT)
    } else {
        Style::new().fg(Color::DarkGray)
    };
    let label_style = if focused {
        Style::new().fg(Color::Black).bg(ACCENT).bold()
    } else {
        Style::new().fg(Color::Gray)
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(border_style);
    frame.render_widget(
        Paragraph::new(Line::from(label).centered().style(label_style)).block(block),
        area,
    );
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

fn render_subs(frame: &mut Frame, tab: &ClientTab, area: Rect, regions: &mut Regions) {
    let focused = tab.focus == Focus::Subs;
    let block = bordered("Subscriptions  (d to cancel)", focused);
    let inner = block.inner(area);

    for i in 0..tab.subscriptions.len() {
        if i as u16 >= inner.height {
            break;
        }
        regions.sub_rows.push((
            i,
            Rect {
                x: inner.x,
                y: inner.y + i as u16,
                width: inner.width,
                height: 1,
            },
        ));
    }

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
            "Tab/click: focus   Enter: run   d: unsubscribe   Ctrl+N: connect   Ctrl+W: close   Alt+←/→: switch   Ctrl+Q: quit"
        }
    };
    frame.render_widget(Paragraph::new(Line::from(hint).fg(Color::DarkGray)), area);
}

fn render_connect_dialog(frame: &mut Frame, app: &App, area: Rect, regions: &mut Regions) {
    let Mode::Connect(dialog) = &app.mode else {
        return;
    };

    let rect = centered(area, 60, 15);
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
            Constraint::Length(3),
            Constraint::Length(1),
            Constraint::Length(1),
        ])
        .margin(1)
        .split(inner);

    // Protocol selector
    let proto_focused = dialog.field == ConnectField::Protocol;
    regions.dialog_protocol = Some(rows[0]);
    let mut proto_spans = vec![Span::styled(
        "Protocol  ",
        Style::new().fg(if proto_focused { ACCENT } else { Color::Gray }),
    )];
    let mut x = rows[0].x.saturating_add(10); // width of "Protocol  "
    for proto in Protocol::ALL {
        let selected = proto == dialog.protocol;
        let style = if selected {
            Style::new().fg(Color::Black).bg(ACCENT).bold()
        } else {
            Style::new().fg(Color::DarkGray)
        };
        let text = format!(" {proto} ");
        let w = text.chars().count() as u16;
        regions.dialog_protocol_items.push((
            proto,
            Rect {
                x,
                y: rows[0].y,
                width: w,
                height: 1,
            },
        ));
        x = x.saturating_add(w + 1);
        proto_spans.push(Span::styled(text, style));
        proto_spans.push(Span::raw(" "));
    }
    frame.render_widget(Paragraph::new(Line::from(proto_spans)), rows[0]);

    // Address input
    let addr_focused = dialog.field == ConnectField::Address;
    regions.dialog_address = Some(rows[2]);
    let addr_label = if dialog.protocol.is_unix() {
        "Address (/path/to/socket)"
    } else {
        "Address (host:port)"
    };
    let addr_block = bordered(addr_label, addr_focused);
    let addr_inner = addr_block.inner(rows[2]);
    frame.render_widget(
        Paragraph::new(dialog.address.value()).block(addr_block),
        rows[2],
    );
    if addr_focused {
        let x =
            addr_inner.x + (dialog.address.cursor() as u16).min(addr_inner.width.saturating_sub(1));
        frame.set_cursor_position(Position::new(x, addr_inner.y));
    }

    // Name input
    let name_focused = dialog.field == ConnectField::Name;
    regions.dialog_name = Some(rows[4]);
    let name_label = "Client Name (optional)";
    let name_block = bordered(name_label, name_focused);
    let name_inner = name_block.inner(rows[4]);
    frame.render_widget(
        Paragraph::new(dialog.name.value()).block(name_block),
        rows[4],
    );
    if name_focused {
        let x =
            name_inner.x + (dialog.name.cursor() as u16).min(name_inner.width.saturating_sub(1));
        frame.set_cursor_position(Position::new(x, name_inner.y));
    }

    if let Some(err) = &dialog.error {
        frame.render_widget(
            Paragraph::new(Line::from(err.as_str()).fg(Color::Red)).wrap(Wrap { trim: true }),
            rows[6],
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
