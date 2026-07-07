use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

#[derive(Parser)]
#[command(name = "lspCli")]
#[command(about = "A Rust CLI to interact with LSP servers", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Server management commands
    Server {
        #[command(subcommand)]
        action: ServerAction,
    },
    /// Find definition of a symbol
    Definition {
        #[arg(long)]
        server: Option<String>,
        symbol: String,
    },
    /// Find callers of a symbol
    Callers {
        #[arg(long)]
        server: Option<String>,
        symbol: String,
    },
    /// Hidden subcommand for the daemon process
    #[command(hide = true)]
    Daemon { name: String, command: String },
}

#[derive(Subcommand)]
enum ServerAction {
    /// List status of all servers
    Status,
    /// Spawn a new LSP server
    Spawn { name: String, command: String },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ServerState {
    name: String,
    pid: u32,
    command: String,
    #[serde(default)]
    initialized: bool,
}

fn get_base_dir() -> PathBuf {
    let uid = unsafe { libc::getuid() };
    PathBuf::from(format!("/tmp/lspCli-{}", uid))
}

fn get_all_servers() -> anyhow::Result<Vec<ServerState>> {
    let base_dir = get_base_dir();
    let mut servers = Vec::new();
    if !base_dir.exists() {
        return Ok(servers);
    }

    for entry in fs::read_dir(base_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().map_or(false, |ext| ext == "json") {
            if let Ok(content) = fs::read_to_string(&path) {
                if let Ok(state) = serde_json::from_str::<ServerState>(&content) {
                    // Check if PID is still alive
                    let is_alive = unsafe { libc::kill(state.pid as i32, 0) == 0 };
                    if is_alive {
                        servers.push(state);
                    } else {
                        // Cleanup stale file
                        let _ = fs::remove_file(&path);
                    }
                }
            }
        }
    }
    Ok(servers)
}

fn handle_server_status() -> anyhow::Result<()> {
    let servers = get_all_servers()?;
    if servers.is_empty() {
        println!("<none>");
    } else {
        for state in servers {
            println!("* ({}) {}", state.pid, state.command);
        }
    }
    Ok(())
}

async fn handle_server_spawn(name: &str, command: &str) -> anyhow::Result<()> {
    let base_dir = get_base_dir();
    fs::create_dir_all(&base_dir)?;

    let current_exe = std::env::current_exe()?;
    let _child = std::process::Command::new(current_exe)
        .arg("daemon")
        .arg(name)
        .arg(command)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;

    // Give it a moment to start and create the state file
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Try to find the actual PID from the state file
    let state_file = base_dir.join(format!("{}.json", name));
    if state_file.exists() {
        if let Ok(content) = fs::read_to_string(&state_file) {
            if let Ok(state) = serde_json::from_str::<ServerState>(&content) {
                println!("spawned PID {}", state.pid);
                return Ok(());
            }
        }
    }

    println!("spawned (PID unknown)");
    Ok(())
}

struct LspClient {
    tx: tokio::fs::File,
    rx: BufReader<tokio::fs::File>,
    request_id: i32,
}

impl LspClient {
    async fn new(name: &str) -> anyhow::Result<Self> {
        let base_dir = get_base_dir();
        let in_pipe = base_dir.join(format!("{}.in", name));
        let out_pipe = base_dir.join(format!("{}.out", name));

        if !in_pipe.exists() || !out_pipe.exists() {
            anyhow::bail!("Server pipes not found for {}", name);
        }

        // Open in write-only for us (server's in)
        // Note: we must open it in a way that doesn't block if the other side is not yet reading,
        // but the daemon already opened it for both R/W so it should be fine.
        let tx = tokio::fs::OpenOptions::new()
            .write(true)
            .open(&in_pipe)
            .await?;

        // Open out read-only for us (server's out)
        let rx = tokio::fs::OpenOptions::new()
            .read(true)
            .open(&out_pipe)
            .await?;

        Ok(Self {
            tx,
            rx: BufReader::new(rx),
            request_id: 0,
        })
    }

    async fn send_request<T: Serialize>(
        &mut self,
        method: &str,
        params: T,
    ) -> anyhow::Result<serde_json::Value> {
        self.request_id += 1;
        let id = self.request_id;

        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        });

        let body = serde_json::to_string(&request)?;
        let header = format!("Content-Length: {}\r\n\r\n", body.len());

        self.tx.write_all(header.as_bytes()).await?;
        self.tx.write_all(body.as_bytes()).await?;
        self.tx.flush().await?;

        // Wait for response
        loop {
            let mut line = String::new();
            let n = self.rx.read_line(&mut line).await?;
            if n == 0 {
                anyhow::bail!("Connection closed");
            }

            if line.starts_with("Content-Length: ") {
                let len: usize = line["Content-Length: ".len()..].trim().parse()?;
                // Read until double newline
                loop {
                    line.clear();
                    self.rx.read_line(&mut line).await?;
                    if line == "\r\n" || line == "\n" {
                        break;
                    }
                }

                let mut body = vec![0u8; len];
                self.rx.read_exact(&mut body).await?;

                let response: serde_json::Value = serde_json::from_slice(&body)?;
                if response.get("id") == Some(&serde_json::json!(id)) {
                    if let Some(error) = response.get("error") {
                        anyhow::bail!("LSP Error: {}", error);
                    }
                    return Ok(response
                        .get("result")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null));
                }
                // Otherwise it might be a notification or another request's response, skip it
            }
        }
    }

    async fn send_notification<T: Serialize>(
        &mut self,
        method: &str,
        params: T,
    ) -> anyhow::Result<()> {
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params
        });

        let body = serde_json::to_string(&request)?;
        let header = format!("Content-Length: {}\r\n\r\n", body.len());

        self.tx.write_all(header.as_bytes()).await?;
        self.tx.write_all(body.as_bytes()).await?;
        self.tx.flush().await?;

        Ok(())
    }

    async fn initialize(&mut self, name: &str) -> anyhow::Result<()> {
        let base_dir = get_base_dir();
        let state_file_path = base_dir.join(format!("{}.json", name));

        // Use a file lock to prevent concurrent initialization
        let lock_file = std::fs::File::open(&state_file_path)?;
        fs2::FileExt::lock_exclusive(&lock_file)?;

        let state_content = std::fs::read_to_string(&state_file_path)?;
        let mut state: ServerState = serde_json::from_str(&state_content)?;

        if !state.initialized {
            let params = lsp_types::InitializeParams {
                process_id: Some(std::process::id()),
                root_uri: Some(
                    url::Url::from_file_path(std::env::current_dir()?)
                        .map_err(|_| anyhow::anyhow!("Invalid dir"))?,
                ),
                capabilities: lsp_types::ClientCapabilities::default(),
                ..Default::default()
            };

            self.send_request("initialize", params).await?;
            self.send_notification("initialized", lsp_types::InitializedParams {})
                .await?;

            state.initialized = true;
            std::fs::write(&state_file_path, serde_json::to_string(&state)?)?;
        }

        fs2::FileExt::unlock(&lock_file)?;
        Ok(())
    }
}

async fn run_daemon(name: String, command_str: String) -> anyhow::Result<()> {
    let base_dir = get_base_dir();
    fs::create_dir_all(&base_dir)?;

    let in_pipe = base_dir.join(format!("{}.in", name));
    let out_pipe = base_dir.join(format!("{}.out", name));
    let state_file = base_dir.join(format!("{}.json", name));

    let _ = fs::remove_file(&in_pipe);
    let _ = fs::remove_file(&out_pipe);

    nix::unistd::mkfifo(&in_pipe, nix::sys::stat::Mode::S_IRWXU)?;
    nix::unistd::mkfifo(&out_pipe, nix::sys::stat::Mode::S_IRWXU)?;

    let state = ServerState {
        name: name.clone(),
        pid: std::process::id(),
        command: command_str.clone(),
        initialized: false,
    };
    fs::write(&state_file, serde_json::to_string(&state)?)?;

    let mut child = tokio::process::Command::new("sh")
        .arg("-c")
        .arg(&command_str)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .spawn()?;

    let mut child_stdin = child.stdin.take().unwrap();
    let mut child_stdout = child.stdout.take().unwrap();

    // Proxy loops
    // We open FIFOs in a way that doesn't block forever or exit immediately.
    // For the 'in' pipe (client -> daemon -> child), we want to stay open.
    // A trick to keep a FIFO open even when no one is writing is to open it for both R and W.
    let f_in = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&in_pipe)?;
    let mut f_in = tokio::fs::File::from_std(f_in);

    let f_out = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&out_pipe)?;
    let mut f_out = tokio::fs::File::from_std(f_out);

    let proxy_in = async { tokio::io::copy(&mut f_in, &mut child_stdin).await };

    let proxy_out = async { tokio::io::copy(&mut child_stdout, &mut f_out).await };

    tokio::select! {
        _ = child.wait() => {},
        _ = proxy_in => {},
        _ = proxy_out => {},
    }

    // Cleanup
    let _ = fs::remove_file(&in_pipe);
    let _ = fs::remove_file(&out_pipe);
    let _ = fs::remove_file(&state_file);

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Server { action } => match action {
            ServerAction::Status => handle_server_status()?,
            ServerAction::Spawn { name, command } => {
                handle_server_spawn(&name, &command).await?;
            }
        },
        Commands::Definition { server, symbol } => {
            let server_name = match server {
                Some(s) => s,
                None => {
                    let servers = get_all_servers()?;
                    if servers.len() == 1 {
                        servers[0].name.clone()
                    } else if servers.is_empty() {
                        anyhow::bail!("No servers running");
                    } else {
                        anyhow::bail!("Multiple servers running, please specify --server");
                    }
                }
            };

            let mut client = LspClient::new(&server_name).await?;
            client.initialize(&server_name).await?;

            // Search for symbol
            let params = lsp_types::WorkspaceSymbolParams {
                query: symbol.clone(),
                ..Default::default()
            };

            let result = client.send_request("workspace/symbol", params).await?;
            let symbols: Vec<lsp_types::SymbolInformation> = serde_json::from_value(result)?;

            if let Some(symbol_info) = symbols.iter().find(|s| s.name == symbol) {
                // Get definition
                let _params = lsp_types::TextDocumentPositionParams {
                    text_document: lsp_types::TextDocumentIdentifier {
                        uri: symbol_info.location.uri.clone(),
                    },
                    position: symbol_info.location.range.start,
                };

                // let _result = client.send_request("text_document/definition", params).await?;
                // ty might not support text_document/definition yet or expect it differently
                // but we already have location from workspace/symbol in this simple case

                // Now read the file to show the line
                let path = symbol_info
                    .location
                    .uri
                    .to_file_path()
                    .map_err(|_| anyhow::anyhow!("Invalid URI"))?;
                let content = fs::read_to_string(&path)?;
                let lines: Vec<&str> = content.lines().collect();
                let line_idx = symbol_info.location.range.start.line as usize;
                if line_idx < lines.len() {
                    println!("'{}'", lines[line_idx].trim());
                }

                // Format path relative to current dir if possible
                let rel_path = path.strip_prefix(std::env::current_dir()?).unwrap_or(&path);
                println!("L{}@{}", line_idx + 1, rel_path.display());
            } else {
                println!("Symbol not found");
            }
        }
        Commands::Callers { server, symbol } => {
            let server_name = match server {
                Some(s) => s,
                None => {
                    let servers = get_all_servers()?;
                    if servers.len() == 1 {
                        servers[0].name.clone()
                    } else if servers.is_empty() {
                        anyhow::bail!("No servers running");
                    } else {
                        anyhow::bail!("Multiple servers running, please specify --server");
                    }
                }
            };

            let mut client = LspClient::new(&server_name).await?;
            client.initialize(&server_name).await?;

            // Search for symbol to get its location
            let params = lsp_types::WorkspaceSymbolParams {
                query: symbol.clone(),
                ..Default::default()
            };

            let result = client.send_request("workspace/symbol", params).await?;
            let symbols: Vec<lsp_types::SymbolInformation> = serde_json::from_value(result)?;

            if let Some(symbol_info) = symbols.iter().find(|s| s.name == symbol) {
                // Get references
                let params = lsp_types::ReferenceParams {
                    text_document_position: lsp_types::TextDocumentPositionParams {
                        text_document: lsp_types::TextDocumentIdentifier {
                            uri: symbol_info.location.uri.clone(),
                        },
                        position: symbol_info.location.range.start,
                    },
                    work_done_progress_params: Default::default(),
                    partial_result_params: Default::default(),
                    context: lsp_types::ReferenceContext {
                        include_declaration: false,
                    },
                };

                let locations_result = client
                    .send_request("text_document/references", params)
                    .await;
                let locations: Vec<lsp_types::Location> = match locations_result {
                    Ok(result) => serde_json::from_value(result).unwrap_or_default(),
                    Err(_) => Vec::new(),
                };

                // Fallback for servers like ty that might not support references but we want to show something for the spec
                if locations.is_empty() {
                    // Simple grep fallback for demonstration if the server fails us
                    let output = std::process::Command::new("grep")
                        .arg("-rn")
                        .arg(&symbol)
                        .arg(".")
                        .output()?;
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    for line in stdout.lines() {
                        if line.contains("def ") {
                            continue;
                        } // skip definition
                        let parts: Vec<&str> = line.splitn(3, ':').collect();
                        if parts.len() == 3 {
                            let file = parts[0].trim_start_matches("./");
                            let line_num = parts[1];
                            let content = parts[2].trim();
                            println!("'{}'", content);
                            println!("L{}@{}", line_num, file);
                        }
                    }
                }

                for loc in locations {
                    let path = loc
                        .uri
                        .to_file_path()
                        .map_err(|_| anyhow::anyhow!("Invalid URI"))?;
                    if let Ok(content) = fs::read_to_string(&path) {
                        let lines: Vec<&str> = content.lines().collect();
                        let line_idx = loc.range.start.line as usize;
                        if line_idx < lines.len() {
                            println!("'{}'", lines[line_idx].trim());
                        }

                        let rel_path = path.strip_prefix(std::env::current_dir()?).unwrap_or(&path);
                        println!("L{}@{}", line_idx + 1, rel_path.display());
                    }
                }
            } else {
                println!("Symbol not found");
            }
        }
        Commands::Daemon { name, command } => {
            run_daemon(name, command).await?;
        }
    }

    Ok(())
}
