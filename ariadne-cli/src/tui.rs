use std::io;
use std::sync::Arc;
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Terminal;
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

use crate::error::CliResult;
use crate::llm::Translator;
use ariadne_core::graph_backend::GraphBackend;

struct AppState {
    question: String,
    cypher: String,
    results: String,
    status: String,
    error: Option<String>,
}

impl AppState {
    fn new() -> Self {
        Self {
            question: String::new(),
            cypher: String::new(),
            results: String::new(),
            status: "Ready".to_string(),
            error: None,
        }
    }
}

pub fn run_tui(
    runtime: &Runtime,
    backend: Arc<dyn GraphBackend>,
    translator: Arc<dyn Translator>,
    token: CancellationToken,
) -> CliResult<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let terminal_backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(terminal_backend)?;

    let mut app = AppState::new();

    let res = run_loop(&mut terminal, runtime, backend, translator, &mut app);
    token.cancel();

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    res
}

fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    runtime: &Runtime,
    backend: Arc<dyn GraphBackend>,
    translator: Arc<dyn Translator>,
    app: &mut AppState,
) -> CliResult<()> {
    loop {
        terminal.draw(|frame| render(frame, app))?;

        if event::poll(Duration::from_millis(200))? {
            match event::read()? {
                Event::Key(key) => {
                    if handle_key(key, runtime, backend.clone(), translator.clone(), app)? {
                        return Ok(());
                    }
                }
                Event::Resize(_, _) => {}
                _ => {}
            }
        }
    }
}

fn handle_key(
    key: KeyEvent,
    runtime: &Runtime,
    backend: Arc<dyn GraphBackend>,
    translator: Arc<dyn Translator>,
    app: &mut AppState,
) -> CliResult<bool> {
    match key.code {
        KeyCode::Char('q') if key.modifiers.contains(KeyModifiers::CONTROL) => return Ok(true),
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => return Ok(true),
        KeyCode::Char(ch) => {
            app.question.push(ch);
        }
        KeyCode::Backspace => {
            app.question.pop();
        }
        KeyCode::Enter => {
            let question = app.question.trim().to_string();
            if question.is_empty() {
                return Ok(false);
            }
            app.status = "Translating...".to_string();
            app.error = None;
            let translation = runtime.block_on(translator.translate(&question));
            match translation {
                Ok(cypher) => {
                    app.cypher = cypher.clone();
                    app.status = "Running query...".to_string();
                    let result = runtime.block_on(backend.execute_query(cypher));
                    match result {
                        Ok(records) => {
                            app.results = serde_json::to_string_pretty(&records)
                                .unwrap_or_else(|_| "Failed to format results".to_string());
                            app.status = "Ready".to_string();
                        }
                        Err(err) => {
                            app.error = Some(err.to_string());
                            app.status = "Query failed".to_string();
                        }
                    }
                }
                Err(err) => {
                    app.error = Some(err.to_string());
                    app.status = "Translation failed".to_string();
                }
            }
        }
        KeyCode::Esc => {
            app.question.clear();
        }
        _ => {}
    }
    Ok(false)
}

fn render(frame: &mut ratatui::Frame, app: &AppState) {
    let size = frame.size();
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(5),
            Constraint::Length(7),
            Constraint::Min(3),
        ])
        .split(size);

    let status = if let Some(err) = &app.error {
        format!("Status: {} | Error: {}", app.status, err)
    } else {
        format!("Status: {}", app.status)
    };

    let status_block = Paragraph::new(status)
        .style(Style::default().add_modifier(Modifier::BOLD))
        .block(Block::default().borders(Borders::ALL).title("Status"));
    frame.render_widget(status_block, layout[0]);

    let question_block = Paragraph::new(app.question.as_str())
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Question (Enter to run, Ctrl+C/Ctrl+Q to quit)"),
        )
        .wrap(Wrap { trim: false });
    frame.render_widget(question_block, layout[1]);

    let cypher_block = Paragraph::new(app.cypher.as_str())
        .block(Block::default().borders(Borders::ALL).title("Cypher"))
        .wrap(Wrap { trim: false });
    frame.render_widget(cypher_block, layout[2]);

    let results_block = Paragraph::new(app.results.as_str())
        .block(Block::default().borders(Borders::ALL).title("Results"))
        .wrap(Wrap { trim: false });
    frame.render_widget(results_block, layout[3]);
}
