use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use reqwest::{Client, StatusCode};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: Option<String>,
    #[serde(rename = "type")]
    #[serde(default = "default_tool_call_type")]
    pub call_type: String,
    pub function: FunctionCall,
}

fn default_tool_call_type() -> String {
    "function".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionCall {
    pub name: String,
    pub arguments: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub role: Role,
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

#[async_trait]
pub trait Model: Send + Sync {
    async fn chat(&self, messages: &[Message], tools: &[serde_json::Value]) -> anyhow::Result<Message>;
    // Keep generate for backward compatibility or simple use cases
    async fn generate(&self, prompt: &str) -> anyhow::Result<String>;
}

pub struct OllamaModel {
    model_name: String,
    client: Client,
}

impl OllamaModel {
    pub fn new(model_name: &str) -> Self {
        Self {
            model_name: model_name.to_string(),
            client: Client::new(),
        }
    }
}

#[derive(Serialize)]
struct OllamaChatRequest {
    model: String,
    messages: Vec<Message>,
    tools: Vec<serde_json::Value>,
    stream: bool,
}

#[derive(Deserialize)]
struct OllamaChatResponse {
    message: Message,
}

#[derive(Serialize)]
struct OllamaGenerateRequest {
    model: String,
    prompt: String,
    stream: bool,
}

#[derive(Deserialize)]
struct OllamaGenerateResponse {
    response: Option<String>,
    error: Option<String>,
}

#[async_trait]
impl Model for OllamaModel {
    async fn chat(&self, messages: &[Message], tools: &[serde_json::Value]) -> anyhow::Result<Message> {
        let url = "http://localhost:11434/api/chat";
        let request = OllamaChatRequest {
            model: self.model_name.clone(),
            messages: messages.to_vec(),
            tools: tools.to_vec(),
            stream: false,
        };

        let response = self.client.post(url)
            .json(&request)
            .send()
            .await?;

        let status = response.status();
        let body = response.text().await?;

        if status != StatusCode::OK {
            return Err(anyhow::anyhow!("Ollama chat error ({}): {}", status, body));
        }

        let res_data: OllamaChatResponse = serde_json::from_str(&body)
            .map_err(|e| anyhow::anyhow!("Failed to parse Ollama chat response: {}. Body: {}", e, body))?;

        Ok(res_data.message)
    }

    async fn generate(&self, prompt: &str) -> anyhow::Result<String> {
        let url = "http://localhost:11434/api/generate";
        let request = OllamaGenerateRequest {
            model: self.model_name.clone(),
            prompt: prompt.to_string(),
            stream: false,
        };

        let response = self.client.post(url)
            .json(&request)
            .send()
            .await?;

        let status = response.status();
        let body = response.text().await?;

        if status != StatusCode::OK {
            if let Ok(err_res) = serde_json::from_str::<OllamaGenerateResponse>(&body) {
                if let Some(err) = err_res.error {
                    return Err(anyhow::anyhow!("Ollama error: {}", err));
                }
            }
            return Err(anyhow::anyhow!("Ollama error ({}): {}", status, body));
        }

        let res_data: OllamaGenerateResponse = serde_json::from_str(&body)
            .map_err(|e| anyhow::anyhow!("Failed to parse Ollama response: {}. Body: {}", e, body))?;

        res_data.response.ok_or_else(|| anyhow::anyhow!("Ollama response missing 'response' field. Body: {}", body))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tool_call_deserialization_no_type() {
        let body = r#"{"function":{"name":"read","arguments":{"filename":"summary.md"}}}"#;
        let res: ToolCall = serde_json::from_str(body).unwrap();
        assert_eq!(res.call_type, "function");
        assert_eq!(res.function.name, "read");
    }

    #[test]
    fn test_ollama_response_deserialization() {
        let body = r#"{"response": "hello"}"#;
        let res: OllamaGenerateResponse = serde_json::from_str(body).unwrap();
        assert_eq!(res.response.unwrap(), "hello");

        let err_body = r#"{"error": "model not found"}"#;
        let res: OllamaGenerateResponse = serde_json::from_str(err_body).unwrap();
        assert_eq!(res.error.unwrap(), "model not found");
    }
}
