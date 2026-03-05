use crate::models::Model;
use crate::tools::Tool;
use async_trait::async_trait;

#[async_trait]
pub trait Agent {
    async fn process(&self, input: &str) -> anyhow::Result<String>;
}

pub struct MainAgent {
    model: Box<dyn Model>,
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

    fn build_prompt(&self, input: &str) -> String {
        let mut tool_info = String::new();
        if !self.tools.is_empty() {
            tool_info.push_str("\n\nYou have the following tools available:\n");
            for tool in &self.tools {
                tool_info.push_str(&format!("- {}: {}\n", tool.name(), tool.description()));
            }
        }

        format!("System: {}{}\nUser: {}", self.system_prompt, tool_info, input)
    }
}

#[async_trait]
impl Agent for MainAgent {
    async fn process(&self, input: &str) -> anyhow::Result<String> {
        let prompt = self.build_prompt(input);
        self.model.generate(&prompt).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Model;

    struct MockModel {
        last_prompt: std::sync::Arc<std::sync::Mutex<String>>,
    }

    #[async_trait]
    impl Model for MockModel {
        async fn generate(&self, prompt: &str) -> anyhow::Result<String> {
            let mut last_prompt = self.last_prompt.lock().unwrap();
            *last_prompt = prompt.to_string();
            Ok("Mock response".to_string())
        }
    }

    struct MockTool;
    impl Tool for MockTool {
        fn name(&self) -> &str { "mock_tool" }
        fn description(&self) -> &str { "A mock tool" }
        fn run(&self, _input: &str) -> anyhow::Result<String> { Ok("".to_string()) }
    }

    #[tokio::test]
    async fn test_main_agent_process() {
        let last_prompt = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
        let model = MockModel { last_prompt: last_prompt.clone() };
        let agent = MainAgent::new(Box::new(model), vec![Box::new(MockTool)]);

        let result = agent.process("Hello").await.unwrap();
        assert_eq!(result, "Mock response");

        let prompt = last_prompt.lock().unwrap();
        assert!(prompt.contains("mock_tool: A mock tool"));
    }
}
