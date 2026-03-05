use clap::Parser;
use coding_agent::models::OllamaModel;
use coding_agent::agents::{Agent, MainAgent};
use coding_agent::tools::ReadTool;
use coding_agent::logging::Logger;
use ratatui::{
    backend::CrosstermBackend,
    widgets::{Block, Borders, Paragraph, Wrap},
    layout::{Layout, Constraint, Direction},
    Terminal,
};
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use std::io;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "ollama:llama3")]
    model: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let model_name = if args.model.starts_with("ollama:") {
        &args.model[7..]
    } else {
        &args.model
    };

    let logger = Logger::new("main")?;
    logger.log(&format!("Starting agent with model: {}", model_name))?;

    let model = Box::new(OllamaModel::new(model_name));
    let tools = vec![Box::new(ReadTool) as Box<dyn coding_agent::tools::Tool>];
    let agent = MainAgent::new(model, tools);

    // TUI setup
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut input = String::new();
    let mut output = String::from("Agent output will appear here...");

    loop {
        terminal.draw(|f| {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(1)
                .constraints(
                    [
                        Constraint::Min(1),
                        Constraint::Length(5),
                    ]
                    .as_ref(),
                )
                .split(f.size());

            let output_block = Paragraph::new(output.as_str())
                .block(Block::default().title("Agent Output").borders(Borders::ALL))
                .wrap(Wrap { trim: true });
            f.render_widget(output_block, chunks[0]);

            let input_block = Paragraph::new(input.as_str())
                .block(Block::default().title("Input").borders(Borders::ALL));
            f.render_widget(input_block, chunks[1]);
        })?;

        if let Event::Key(key) = event::read()? {
            match key.code {
                KeyCode::Enter => {
                    let user_input = input.drain(..).collect::<String>();
                    logger.log(&format!("User input: {}", user_input))?;

                    output = String::from("Thinking...");
                    terminal.draw(|f| {
                        // Redraw to show "Thinking..."
                        let chunks = Layout::default()
                            .direction(Direction::Vertical)
                            .margin(1)
                            .constraints([Constraint::Min(1), Constraint::Length(5)].as_ref())
                            .split(f.size());
                        let output_block = Paragraph::new(output.as_str())
                            .block(Block::default().title("Agent Output").borders(Borders::ALL))
                            .wrap(Wrap { trim: true });
                        f.render_widget(output_block, chunks[0]);
                        let input_block = Paragraph::new("")
                            .block(Block::default().title("Input").borders(Borders::ALL));
                        f.render_widget(input_block, chunks[1]);
                    })?;

                    match agent.process(&user_input).await {
                        Ok(res) => {
                            output = res.clone();
                            logger.log(&format!("Agent output: {}", res))?;
                        }
                        Err(e) => {
                            output = format!("Error: {}", e);
                            logger.log(&format!("Error: {}", e))?;
                        }
                    }
                }
                KeyCode::Char(c) => {
                    if c == 'd' && key.modifiers.contains(event::KeyModifiers::CONTROL) {
                        break;
                    }
                    input.push(c);
                }
                KeyCode::Backspace => {
                    input.pop();
                }
                KeyCode::Esc => {
                    break;
                }
                _ => {}
            }
        }
    }

    // Cleanup
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
    )?;
    terminal.show_cursor()?;

    Ok(())
}
