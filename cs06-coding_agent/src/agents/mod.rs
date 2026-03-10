use crate::models::{Model, Message, Role};
use crate::tools::Tool;
use async_trait::async_trait;

#[async_trait]
pub trait Agent {
    async fn process(&self, input: &str) -> anyhow::Result<String>;
}

pub struct MainAgent {
    model: Box<dyn Model>,
    tools: Vec<Box<dyn Tool>>,
    history: std::sync::Arc<tokio::sync::Mutex<Vec<Message>>>,
}

impl MainAgent {
    pub fn new(model: Box<dyn Model>, tools: Vec<Box<dyn Tool>>) -> Self {
        let system_prompt = "You are a smart assistant. You have tools at your disposal. Be very brief in general".to_string();
        let history = vec![Message {
            role: Role::System,
            content: system_prompt,
            tool_calls: None,
            tool_call_id: None,
        }];

        Self {
            model,
            tools,
            history: std::sync::Arc::new(tokio::sync::Mutex::new(history)),
        }
    }
}

#[async_trait]
impl Agent for MainAgent {
    async fn process(&self, input: &str) -> anyhow::Result<String> {
        let mut history = self.history.lock().await;
        history.push(Message {
            role: Role::User,
            content: input.to_string(),
            tool_calls: None,
            tool_call_id: None,
        });

        let tool_definitions: Vec<serde_json::Value> = self.tools.iter().map(|t| t.definition()).collect();

        loop {
            let response = self.model.chat(&history, &tool_definitions).await?;
            history.push(response.clone());

            if let Some(tool_calls) = &response.tool_calls {
                for tool_call in tool_calls {
                    let tool = self.tools.iter().find(|t| t.name() == tool_call.function.name)
                        .ok_or_else(|| anyhow::anyhow!("Tool not found: {}", tool_call.function.name))?;

                    let result = tool.run(&tool_call.function.arguments).await?;

                    history.push(Message {
                        role: Role::Tool,
                        content: result,
                        tool_calls: None,
                        tool_call_id: tool_call.id.clone(),
                    });
                }
                // Continue the loop to let the model see the tool results
            } else {
                // No more tool calls, return the final response
                return Ok(response.content);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Model, Message, ToolCall, FunctionCall};
    use serde_json::json;

    struct MockModel {
        responses: std::sync::Arc<tokio::sync::Mutex<Vec<Message>>>,
    }

    #[async_trait]
    impl Model for MockModel {
        async fn chat(&self, _messages: &[Message], _tools: &[serde_json::Value]) -> anyhow::Result<Message> {
            let mut responses = self.responses.lock().await;
            if !responses.is_empty() {
                Ok(responses.remove(0))
            } else {
                Ok(Message {
                    role: Role::Assistant,
                    content: "Final answer".to_string(),
                    tool_calls: None,
                    tool_call_id: None,
                })
            }
        }
        async fn generate(&self, _prompt: &str) -> anyhow::Result<String> {
            Ok("Mock response".to_string())
        }
    }

    struct MockTool;
    #[async_trait]
    impl Tool for MockTool {
        fn name(&self) -> &str { "mock_tool" }
        fn description(&self) -> &str { "A mock tool" }
        fn definition(&self) -> serde_json::Value { json!({}) }
        async fn run(&self, _args: &serde_json::Value) -> anyhow::Result<String> { Ok("tool result".to_string()) }
    }

    #[tokio::test]
    async fn test_main_agent_tool_loop() {
        let responses = vec![
            Message {
                role: Role::Assistant,
                content: "".to_string(),
                tool_calls: Some(vec![ToolCall {
                    id: Some("1".to_string()),
                    call_type: "function".to_string(),
                    function: FunctionCall {
                        name: "mock_tool".to_string(),
                        arguments: json!({}),
                    }
                }]),
                tool_call_id: None,
            }
        ];
        let model = MockModel { responses: std::sync::Arc::new(tokio::sync::Mutex::new(responses)) };
        let agent = MainAgent::new(Box::new(model), vec![Box::new(MockTool)]);

        let result = agent.process("Call the tool").await.unwrap();
        assert_eq!(result, "Final answer");
    }
}
