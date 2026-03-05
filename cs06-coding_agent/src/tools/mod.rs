use std::fs;

pub trait Tool: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn run(&self, input: &str) -> anyhow::Result<String>;
}

pub struct ReadTool;

impl Tool for ReadTool {
    fn name(&self) -> &str {
        "read"
    }

    fn description(&self) -> &str {
        "Reads the complete contents of a file given its name."
    }

    fn run(&self, input: &str) -> anyhow::Result<String> {
        let content = fs::read_to_string(input.trim())?;
        Ok(content)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_read_tool() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "hello world").unwrap();

        let tool = ReadTool;
        let path = temp_file.path().to_str().unwrap();
        let result = tool.run(path).unwrap();

        assert_eq!(result.trim(), "hello world");
    }
}
