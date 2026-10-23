use std::io::{self, stdout};
use std::time::{Duration, Instant};

use anyhow::Result;
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph, Row, Table, Wrap},
    Terminal,
};

use crate::config::{validate_room_id, RoomConfig, RoomStore};

enum Mode {
    List,
    DiscordChannel { room_id: String, value: String },
    Create { field: usize, values: [String; 3] },
    ConfirmDelete,
    ShowKey { title: String, key: String },
}

pub fn run(db_path: &str) -> Result<()> {
    let store = RoomStore::open(db_path)?;
    let mut terminal = setup_terminal()?;
    let result = run_loop(&store, &mut terminal);
    let restore_result = restore_terminal(&mut terminal);
    result.and(restore_result)
}

fn setup_terminal() -> Result<Terminal<CrosstermBackend<io::Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;
    Ok(terminal)
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

fn run_loop(
    store: &RoomStore,
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
) -> Result<()> {
    let mut selected = 0usize;
    let mut mode = Mode::List;
    let mut message: Option<(String, Instant)> = None;

    loop {
        let rooms = store.list()?;
        selected = selected.min(rooms.len().saturating_sub(1));
        if message
            .as_ref()
            .is_some_and(|(_, expires)| Instant::now() >= *expires)
        {
            message = None;
        }

        terminal.draw(|frame| {
            draw(
                frame,
                &rooms,
                selected,
                &mode,
                message.as_ref().map(|(text, _)| text.as_str()),
            )
        })?;

        if !event::poll(Duration::from_millis(100))? {
            continue;
        }
        let Event::Key(key) = event::read()? else {
            continue;
        };
        if key.kind != KeyEventKind::Press {
            continue;
        }

        match &mut mode {
            Mode::List => match key.code {
                KeyCode::Char('q') => return Ok(()),
                KeyCode::Char('n') => {
                    mode = Mode::Create {
                        field: 0,
                        values: [String::new(), String::new(), String::new()],
                    }
                }
                KeyCode::Char('d') if !rooms.is_empty() => mode = Mode::ConfirmDelete,
                KeyCode::Char('k') => {
                    if let Some(room) = rooms.get(selected) {
                        mode = Mode::ShowKey {
                            title: format!("Socket key: {}", room.id),
                            key: room.socket_key.clone(),
                        };
                    }
                }
                KeyCode::Char('r') => {
                    if let Some(room) = rooms.get(selected) {
                        if let Some(key) = store.regenerate_key(&room.id)? {
                            mode = Mode::ShowKey {
                                title: format!("New socket key: {}", room.id),
                                key,
                            };
                        }
                    }
                }
                KeyCode::Char('p') => {
                    if let Some(room) = rooms.get(selected) {
                        mode = Mode::Create {
                            field: 2,
                            values: [
                                room.id.clone(),
                                room.name.clone(),
                                room.podium_key.clone().unwrap_or_default(),
                            ],
                        };
                    }
                }
                KeyCode::Char('v') => {
                    if let Some(room) = rooms.get(selected) {
                        mode = Mode::DiscordChannel {
                            room_id: room.id.clone(),
                            value: room.discord_channel_id.clone().unwrap_or_default(),
                        };
                    }
                }
                KeyCode::Up => selected = selected.saturating_sub(1),
                KeyCode::Down => selected = (selected + 1).min(rooms.len().saturating_sub(1)),
                _ => {}
            },
            Mode::ConfirmDelete => match key.code {
                KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => {
                    if let Some(room) = rooms.get(selected) {
                        let text = match store.delete(&room.id) {
                            Ok(true) => format!("Deleted {}", room.id),
                            Ok(false) => "Room not found".into(),
                            Err(error) => error.to_string(),
                        };
                        message = Some((text, Instant::now() + Duration::from_secs(3)));
                    }
                    mode = Mode::List;
                }
                _ => mode = Mode::List,
            },
            Mode::ShowKey { .. } => {
                if matches!(key.code, KeyCode::Enter | KeyCode::Esc) {
                    mode = Mode::List;
                }
            }
            Mode::DiscordChannel { room_id, value } => match key.code {
                KeyCode::Esc => mode = Mode::List,
                KeyCode::Backspace => {
                    value.pop();
                }
                KeyCode::Enter => {
                    let selected_id = room_id.clone();
                    let channel = (!value.is_empty()).then_some(value.as_str());
                    let result = store.set_discord_channel(&selected_id, channel);
                    message = Some((
                        match result {
                            Ok(true) => format!("Discord VC updated for {selected_id}"),
                            Ok(false) => "Room not found".into(),
                            Err(error) => error.to_string(),
                        },
                        Instant::now() + Duration::from_secs(3),
                    ));
                    mode = Mode::List;
                }
                KeyCode::Char(ch) if ch.is_ascii_digit() => value.push(ch),
                _ => {}
            },
            Mode::Create { field, values } => match key.code {
                KeyCode::Esc => mode = Mode::List,
                KeyCode::Tab => *field = (*field + 1) % 3,
                KeyCode::Backspace => {
                    values[*field].pop();
                }
                KeyCode::Enter => {
                    if values[0].is_empty() {
                        message = Some((
                            "Room ID is required".into(),
                            Instant::now() + Duration::from_secs(3),
                        ));
                        *field = 0;
                    } else if validate_room_id(&values[0]).is_err() {
                        message = Some((
                            "Room ID: 1..64 ASCII letters, digits, _ or -".into(),
                            Instant::now() + Duration::from_secs(3),
                        ));
                        *field = 0;
                    } else if values[1].trim().is_empty() {
                        message = Some((
                            "Name is required".into(),
                            Instant::now() + Duration::from_secs(3),
                        ));
                        *field = 1;
                    } else if store.exists(&values[0])? {
                        store.set_podium_key(
                            &values[0],
                            Some(values[2].as_str()).filter(|v| !v.is_empty()),
                        )?;
                        message = Some((
                            format!("Updated Podium key for {}", values[0]),
                            Instant::now() + Duration::from_secs(3),
                        ));
                        mode = Mode::List;
                    } else {
                        let room = store.create(
                            &values[0],
                            &values[1],
                            Some(values[2].as_str()).filter(|v| !v.is_empty()),
                        )?;
                        mode = Mode::ShowKey {
                            title: format!("Created {} — save this socket key", room.id),
                            key: room.socket_key,
                        };
                    }
                }
                KeyCode::Char(value)
                    if key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT =>
                {
                    if !value.is_control() {
                        values[*field].push(value);
                    }
                }
                _ => {}
            },
        }
    }
}

fn draw(
    frame: &mut ratatui::Frame<'_>,
    rooms: &[RoomConfig],
    selected: usize,
    mode: &Mode,
    message: Option<&str>,
) {
    let area = frame.area();
    let regions = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(5),
            Constraint::Length(3),
        ])
        .split(area);
    let header = regions[0];
    let body = regions[1];
    let footer = regions[2];

    draw_header(frame, header);
    match mode {
        Mode::List | Mode::ConfirmDelete => draw_list(
            frame,
            body,
            rooms,
            selected,
            matches!(mode, Mode::ConfirmDelete),
        ),
        Mode::Create { field, values } => draw_form(frame, body, *field, values),
        Mode::DiscordChannel { room_id, value } => {
            let text = format!("Discord voice channel ID for {room_id}\n\n{value}▌\n\nEnter: save | Empty: disable | Esc: cancel");
            frame.render_widget(
                Paragraph::new(text)
                    .block(Block::default().borders(Borders::ALL).title(" Discord VC ")),
                body,
            );
        }
        Mode::ShowKey { title, key } => draw_key(frame, body, title, key),
    }
    draw_footer(frame, footer, mode, message);
}

fn draw_header(frame: &mut ratatui::Frame<'_>, area: Rect) {
    let title = Paragraph::new(Line::from(vec![
        Span::styled(
            " EcoRun Logger ",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw("· Room Manager"),
    ]))
    .block(Block::default().borders(Borders::ALL).title(" Admin "));
    frame.render_widget(title, area);
}

fn draw_list(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    rooms: &[RoomConfig],
    selected: usize,
    confirming: bool,
) {
    let header = Row::new(["", "ID", "NAME", "SOCKET KEY", "PODIUM", "DISCORD VC"])
        .style(Style::default().add_modifier(Modifier::BOLD));
    let rows = rooms.iter().enumerate().map(|(index, room)| {
        let marker = if index == selected { "›" } else { " " };
        let podium = if room.podium_key.is_some() {
            "configured"
        } else {
            "—"
        };
        Row::new([
            marker.to_string(),
            room.id.clone(),
            room.name.clone(),
            mask(&room.socket_key),
            podium.into(),
            room.discord_channel_id
                .clone()
                .unwrap_or_else(|| "off".into()),
        ])
    });
    let table = Table::new(
        rows,
        [
            Constraint::Length(3),
            Constraint::Length(18),
            Constraint::Min(20),
            Constraint::Length(18),
            Constraint::Length(14),
            Constraint::Length(21),
        ],
    )
    .header(header)
    .block(Block::default().borders(Borders::ALL).title(" Rooms "))
    .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED | Modifier::BOLD));

    let mut state = ratatui::widgets::TableState::default();
    state.select((!rooms.is_empty()).then_some(selected));
    frame.render_stateful_widget(table, area, &mut state);

    if rooms.is_empty() {
        let hint = Paragraph::new("No rooms yet. Press n to create one.");
        let hint_area = Rect::new(area.x + 2, area.y + 2, area.width.saturating_sub(4), 1);
        frame.render_widget(hint, hint_area);
    }

    if confirming {
        let popup = centered_rect(70, 30, area);
        let text =
            Paragraph::new("Delete the selected room?  [y / Enter] delete   [any key] cancel")
                .wrap(Wrap { trim: true })
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(" Confirm delete "),
                );
        frame.render_widget(Clear, popup);
        frame.render_widget(text, popup);
    }
}

fn draw_form(frame: &mut ratatui::Frame<'_>, area: Rect, field: usize, values: &[String; 3]) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(2),
            Constraint::Length(2),
            Constraint::Length(2),
            Constraint::Min(1),
        ])
        .margin(2)
        .split(area);
    frame.render_widget(
        Paragraph::new("Create / Edit Room").style(Style::default().add_modifier(Modifier::BOLD)),
        rows[0],
    );

    for (index, (label, value)) in [
        ("Room ID", &values[0]),
        ("Name", &values[1]),
        ("Podium key", &values[2]),
    ]
    .iter()
    .enumerate()
    {
        let marker = if index == field { "›" } else { " " };
        let cursor = if index == field { "▌" } else { "" };
        let line = Line::from(vec![
            Span::raw(format!("{marker} ")),
            Span::styled(
                format!("{label:<12}"),
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!(" {value}{cursor}")),
        ]);
        frame.render_widget(Paragraph::new(line), rows[index + 1]);
    }
}

fn draw_key(frame: &mut ratatui::Frame<'_>, area: Rect, title: &str, key: &str) {
    let popup = centered_rect(80, 40, area);
    let text = Paragraph::new(vec![
        Line::from(Span::styled(
            title,
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from("Save this key securely. It will not be shown again here."),
        Line::from(""),
        Line::from(Span::styled(
            key,
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from("Press Enter or Esc to return."),
    ])
    .wrap(Wrap { trim: true })
    .block(Block::default().borders(Borders::ALL).title(" Socket key "));
    frame.render_widget(Clear, popup);
    frame.render_widget(text, popup);
}

fn draw_footer(frame: &mut ratatui::Frame<'_>, area: Rect, mode: &Mode, message: Option<&str>) {
    let help = match mode {
        Mode::List => "↑/↓ select · n create · k key · r regenerate · p podium · v Discord VC · d delete · q quit",
        Mode::DiscordChannel { .. } => "Digits: channel ID · Enter save · Empty: disable · Esc cancel",
        Mode::ConfirmDelete => "y / Enter delete  ·  any other key cancel",
        Mode::Create { .. } => "Tab next field  ·  Enter save  ·  Esc cancel",
        Mode::ShowKey { .. } => "Enter / Esc return",
    };
    let lines = if let Some(message) = message {
        vec![
            Line::from(help),
            Line::from(Span::styled(
                format!("Status  {message}"),
                Style::default().add_modifier(Modifier::BOLD),
            )),
        ]
    } else {
        vec![Line::from(help)]
    };
    let footer = Paragraph::new(lines)
        .block(Block::default().borders(Borders::TOP))
        .wrap(Wrap { trim: true });
    frame.render_widget(footer, area);
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vertical[1])[1]
}

fn mask(value: &str) -> String {
    if value.len() <= 8 {
        return "••••••••".into();
    }
    format!("{}…", &value[..8])
}
