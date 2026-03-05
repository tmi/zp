use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use reqwest::Client;

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
    response: String,
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
            .await?
            .json::<OllamaResponse>()
            .await?;

        Ok(response.response)
    }
}
