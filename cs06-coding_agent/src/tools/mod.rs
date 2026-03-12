use std::fs;
use serde_json::json;
use async_trait::async_trait;

#[async_trait]
pub trait Tool: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn definition(&self) -> serde_json::Value;
    async fn run(&self, args: &serde_json::Value) -> anyhow::Result<String>;
}

pub struct ReadTool;

#[async_trait]
impl Tool for ReadTool {
    fn name(&self) -> &str {
        "read"
    }

    fn description(&self) -> &str {
        "Reads the complete contents of a file given its name."
    }

    fn definition(&self) -> serde_json::Value {
        json!({
            "type": "function",
            "function": {
                "name": self.name(),
                "description": self.description(),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "filename": {
                            "type": "string",
                            "description": "The name of the file to read."
                        }
                    },
                    "required": ["filename"]
                }
            }
        })
    }

    async fn run(&self, args: &serde_json::Value) -> anyhow::Result<String> {
        let filename = args["filename"].as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing 'filename' argument"))?;
        let content = fs::read_to_string(filename)?;
        Ok(content)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_read_tool() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "hello world").unwrap();

        let tool = ReadTool;
        let args = json!({"filename": temp_file.path().to_str().unwrap()});
        let result = tool.run(&args).await.unwrap();

        assert_eq!(result.trim(), "hello world");
    }
}
