use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot, Mutex};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use crate::tools::Tool;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct McpConfig {
    #[serde(rename = "mcpServers")]
    pub mcp_servers: HashMap<String, McpServerConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpServerConfig {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
}

pub fn merge_mcp_configs(file_config: Option<McpConfig>, cli_mcp: Option<(String, McpServerConfig)>) -> McpConfig {
    let mut merged = file_config.unwrap_or_default();
    if let Some((name, config)) = cli_mcp {
        merged.mcp_servers.insert(name, config);
    }
    merged
}

#[derive(Debug, Serialize, Deserialize)]
struct JsonRpcRequest {
    jsonrpc: String,
    id: Value,
    method: String,
    params: Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    id: Option<Value>,
    result: Option<Value>,
    error: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct JsonRpcNotification {
    jsonrpc: String,
    method: String,
    #[serde(default)]
    params: Value,
}

enum Message {
    Request(JsonRpcRequest, oneshot::Sender<Result<Value>>),
    Notification(JsonRpcNotification),
}

pub struct McpClient {
    #[allow(dead_code)]
    child: Child,
    message_tx: mpsc::Sender<Message>,
    logger: crate::logging::Logger,
}

#[derive(Debug, Deserialize)]
pub struct McpToolInfo {
    pub name: String,
    pub description: Option<String>,
    #[serde(rename = "inputSchema")]
    pub input_schema: Value,
}

#[derive(Debug, Deserialize)]
struct ListToolsResult {
    tools: Vec<McpToolInfo>,
}

#[derive(Debug, Deserialize)]
struct CallToolResult {
    content: Vec<CallToolContent>,
    #[serde(rename = "isError")]
    pub is_error: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct CallToolContent {
    #[serde(rename = "type")]
    pub content_type: String,
    pub text: Option<String>,
}

impl McpClient {
    pub async fn new(config: &McpServerConfig, logger: crate::logging::Logger) -> Result<Arc<Self>> {
        let mut child = Command::new(&config.command)
            .args(&config.args)
            .envs(&config.env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;

        let stdin = child.stdin.take().ok_or_else(|| anyhow!("Failed to open stdin"))?;
        let stdout = child.stdout.take().ok_or_else(|| anyhow!("Failed to open stdout"))?;

        let (message_tx, mut message_rx) = mpsc::channel::<Message>(32);
        let pending_requests: Arc<Mutex<HashMap<String, oneshot::Sender<Result<Value>>>>> = Arc::new(Mutex::new(HashMap::new()));

        let pending_requests_clone = pending_requests.clone();
        tokio::spawn(async move {
            let mut reader = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                if let Ok(response) = serde_json::from_str::<JsonRpcResponse>(&line) {
                    if let Some(id) = response.id {
                        let id_str = match id {
                            Value::String(s) => s,
                            Value::Number(n) => n.to_string(),
                            _ => id.to_string(),
                        };
                        let mut pending = pending_requests_clone.lock().await;
                        if let Some(tx) = pending.remove(&id_str) {
                            if let Some(error) = response.error {
                                let _ = tx.send(Err(anyhow!("MCP Error: {}", error)));
                            } else {
                                let _ = tx.send(Ok(response.result.unwrap_or(Value::Null)));
                            }
                        }
                    }
                }
            }
        });

        let pending_requests_clone = pending_requests.clone();
        let mut writer = stdin;
        tokio::spawn(async move {
            while let Some(msg) = message_rx.recv().await {
                let line = match msg {
                    Message::Request(req, tx) => {
                        let id_str = match &req.id {
                            Value::String(s) => s.clone(),
                            Value::Number(n) => n.to_string(),
                            _ => req.id.to_string(),
                        };
                        pending_requests_clone.lock().await.insert(id_str, tx);
                        serde_json::to_string(&req).unwrap() + "\n"
                    }
                    Message::Notification(notif) => {
                        serde_json::to_string(&notif).unwrap() + "\n"
                    }
                };
                if writer.write_all(line.as_bytes()).await.is_err() {
                    break;
                }
                if writer.flush().await.is_err() {
                    break;
                }
            }
        });

        let client = Arc::new(Self { child, message_tx, logger });

        // Initialize handshake
        let params = json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {
                "name": "coding-agent",
                "version": "0.1.0"
            }
        });
        client.send_request("initialize", params).await?;
        client.send_notification("notifications/initialized", json!({})).await?;

        Ok(client)
    }

    pub async fn list_tools(&self) -> Result<Vec<McpToolInfo>> {
        let result = self.send_request("tools/list", json!({})).await?;
        let list_tools_result: ListToolsResult = serde_json::from_value(result)?;
        Ok(list_tools_result.tools)
    }

    pub async fn call_tool(&self, name: &str, arguments: Value) -> Result<String> {
        let params = json!({
            "name": name,
            "arguments": arguments
        });
        let result = self.send_request("tools/call", params).await?;
        let call_tool_result: CallToolResult = serde_json::from_value(result)?;

        if let Some(true) = call_tool_result.is_error {
            let error_msg = call_tool_result.content.iter()
                .filter_map(|c| if c.content_type == "text" { c.text.clone() } else { None })
                .collect::<Vec<_>>()
                .join("\n");
            return Err(anyhow!("MCP Tool Error: {}", error_msg));
        }

        let text = call_tool_result.content.iter()
            .filter_map(|c| if c.content_type == "text" { c.text.clone() } else { None })
            .collect::<Vec<_>>()
            .join("\n");
        Ok(text)
    }

    async fn send_request(&self, method: &str, params: Value) -> Result<Value> {
        let (tx, rx) = oneshot::channel();
        let id = uuid::Uuid::new_v4().to_string();
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Value::String(id.clone()),
            method: method.to_string(),
            params: params.clone(),
        };

        self.logger.log("MCP", &format!("Request {}: {} {}", id, method, params))?;
        self.message_tx.send(Message::Request(request, tx)).await.map_err(|_| anyhow!("Failed to send request"))?;
        let result = rx.await.map_err(|_| anyhow!("Failed to receive response"))?;
        match &result {
            Ok(res) => self.logger.log("MCP", &format!("Response {}: {}", id, res))?,
            Err(e) => self.logger.log("MCP", &format!("Response {}: Error: {}", id, e))?,
        }
        result
    }

    async fn send_notification(&self, method: &str, params: Value) -> Result<()> {
        let notification = JsonRpcNotification {
            jsonrpc: "2.0".to_string(),
            method: method.to_string(),
            params: params.clone(),
        };
        self.logger.log("MCP", &format!("Notification: {} {}", method, params))?;
        self.message_tx.send(Message::Notification(notification)).await.map_err(|_| anyhow!("Failed to send notification"))?;
        Ok(())
    }
}

pub struct McpTool {
    client: Arc<McpClient>,
    info: McpToolInfo,
}

impl McpTool {
    pub fn new(client: Arc<McpClient>, info: McpToolInfo) -> Self {
        Self { client, info }
    }
}

#[async_trait]
impl Tool for McpTool {
    fn name(&self) -> &str {
        &self.info.name
    }

    fn description(&self) -> &str {
        self.info.description.as_deref().unwrap_or("")
    }

    fn definition(&self) -> Value {
        json!({
            "type": "function",
            "function": {
                "name": self.name(),
                "description": self.description(),
                "parameters": self.info.input_schema
            }
        })
    }

    async fn run(&self, args: &Value) -> Result<String> {
        self.client.call_tool(self.name(), args.clone()).await
    }
}
