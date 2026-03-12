use clap::Parser;
use coding_agent::models::OllamaModel;
use coding_agent::agents::{Agent, MainAgent};
use coding_agent::tools::{ReadTool, Tool};
use coding_agent::mcp::{McpConfig, McpServerConfig, McpClient, McpTool, merge_mcp_configs};
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
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "ollama:llama3")]
    model: String,

    #[arg(short, long)]
    json: Option<PathBuf>,

    #[arg(long, help = "MCP server configuration in the format name:\"command args\"")]
    mcp: Option<String>,
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
    logger.log("SYSTEM", &format!("Starting agent with model: {}", model_name))?;

    let file_config = if let Some(path) = args.json {
        let content = std::fs::read_to_string(path)?;
        Some(serde_json::from_str::<McpConfig>(&content)?)
    } else {
        None
    };

    let cli_mcp = if let Some(mcp_str) = args.mcp {
        let parts: Vec<&str> = mcp_str.splitn(2, ':').collect();
        if parts.len() == 2 {
            let name = parts[0].to_string();
            let cmd_parts: Vec<String> = parts[1].split_whitespace().map(|s| s.to_string()).collect();
            if !cmd_parts.is_empty() {
                Some((name, McpServerConfig {
                    command: cmd_parts[0].clone(),
                    args: cmd_parts[1..].to_vec(),
                    env: std::collections::HashMap::new(),
                }))
            } else {
                return Err(anyhow::anyhow!("Invalid MCP argument: missing command after colon"));
            }
        } else {
            return Err(anyhow::anyhow!("Invalid MCP argument: expected format name:\"command args\""));
        }
    } else {
        None
    };

    let mcp_config = merge_mcp_configs(file_config, cli_mcp);

    let mut tools: Vec<Box<dyn Tool>> = vec![Box::new(ReadTool)];

    for (name, server_config) in mcp_config.mcp_servers {
        logger.log("SYSTEM", &format!("Connecting to MCP server: {}", name))?;
        let client = McpClient::new(&server_config, logger.clone()).await?;
        let mcp_tools = client.list_tools().await?;
        for tool_info in mcp_tools {
            logger.log("SYSTEM", &format!("Registering tool: {}", tool_info.name))?;
            tools.push(Box::new(McpTool::new(client.clone(), tool_info)));
        }
    }

    let model = Box::new(OllamaModel::new(model_name));
    let agent = MainAgent::new(model, tools, logger.clone());

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
                .block(Block::default().title("Input").borders(Borders::ALL))
                .wrap(Wrap { trim: true });
            f.render_widget(input_block, chunks[1]);
        })?;

        if let Event::Key(key) = event::read()? {
            match key.code {
                KeyCode::Enter => {
                    let user_input = input.drain(..).collect::<String>();
                    logger.log("USER", &format!("{}", user_input))?;

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
                            .block(Block::default().title("Input").borders(Borders::ALL))
                            .wrap(Wrap { trim: true });
                        f.render_widget(input_block, chunks[1]);
                    })?;

                    match agent.process(&user_input).await {
                        Ok(res) => {
                            output = res.clone();
                            logger.log("ASSISTANT", &format!("{}", res))?;
                        }
                        Err(e) => {
                            output = format!("Error: {}", e);
                            logger.log("ERROR", &format!("{}", e))?;
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
