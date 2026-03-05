use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use reqwest::{Client, StatusCode};

#[async_trait]
pub trait Model: Send + Sync {
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
struct OllamaRequest {
    model: String,
    prompt: String,
    stream: bool,
}

#[derive(Deserialize)]
struct OllamaResponse {
    response: Option<String>,
    error: Option<String>,
}

#[async_trait]
impl Model for OllamaModel {
    async fn generate(&self, prompt: &str) -> anyhow::Result<String> {
        let url = "http://localhost:11434/api/generate";
        let request = OllamaRequest {
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
            // Try to parse error from body if it's JSON
            if let Ok(err_res) = serde_json::from_str::<OllamaResponse>(&body) {
                if let Some(err) = err_res.error {
                    return Err(anyhow::anyhow!("Ollama error: {}", err));
                }
            }
            return Err(anyhow::anyhow!("Ollama error ({}): {}", status, body));
        }

        let res_data: OllamaResponse = serde_json::from_str(&body)
            .map_err(|e| anyhow::anyhow!("Failed to parse Ollama response: {}. Body: {}", e, body))?;

        res_data.response.ok_or_else(|| anyhow::anyhow!("Ollama response missing 'response' field. Body: {}", body))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ollama_response_deserialization() {
        let body = r#"{"response": "hello"}"#;
        let res: OllamaResponse = serde_json::from_str(body).unwrap();
        assert_eq!(res.response.unwrap(), "hello");

        let err_body = r#"{"error": "model not found"}"#;
        let res: OllamaResponse = serde_json::from_str(err_body).unwrap();
        assert_eq!(res.error.unwrap(), "model not found");
    }
}
