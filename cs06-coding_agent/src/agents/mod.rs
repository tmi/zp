use crate::models::Model;
use crate::tools::Tool;
use async_trait::async_trait;

#[async_trait]
pub trait Agent {
    async fn process(&self, input: &str) -> anyhow::Result<String>;
}

pub struct MainAgent {
    model: Box<dyn Model>,
    #[allow(dead_code)]
    tools: Vec<Box<dyn Tool>>,
    system_prompt: String,
}

impl MainAgent {
    pub fn new(model: Box<dyn Model>, tools: Vec<Box<dyn Tool>>) -> Self {
        Self {
            model,
            tools,
            system_prompt: "You are a smart assistant. You have tools at your disposal. Be very brief in general".to_string(),
        }
    }
}

#[async_trait]
impl Agent for MainAgent {
    async fn process(&self, input: &str) -> anyhow::Result<String> {
        let prompt = format!("System: {}\nUser: {}", self.system_prompt, input);
        self.model.generate(&prompt).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Model;

    struct MockModel;
    #[async_trait]
    impl Model for MockModel {
        async fn generate(&self, _prompt: &str) -> anyhow::Result<String> {
            Ok("Mock response".to_string())
        }
    }

    #[tokio::test]
    async fn test_main_agent_process() {
        let agent = MainAgent::new(Box::new(MockModel), vec![]);
        let result = agent.process("Hello").await.unwrap();
        assert_eq!(result, "Mock response");
    }
}
