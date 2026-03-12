use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use uuid::Uuid;
use chrono::Local;

#[derive(Clone)]
pub struct Logger {
    session_id: String,
    #[allow(dead_code)]
    agent_id: String,
    log_path: PathBuf,
}

impl Logger {
    pub fn new(agent_id: &str) -> anyhow::Result<Self> {
        let session_id = Uuid::new_v4().to_string();
        let log_dir = PathBuf::from("/tmp/agenticSessions");
        fs::create_dir_all(&log_dir)?;

        let timestamp = Local::now().format("%Y-%m-%dT%H:%M").to_string();
        let log_file_name = format!("{}.{}.{}.log", timestamp, session_id, agent_id);
        let log_path = log_dir.join(log_file_name);

        Ok(Self {
            session_id,
            agent_id: agent_id.to_string(),
            log_path,
        })
    }

    pub fn log(&self, category: &str, message: &str) -> anyhow::Result<()> {
        let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S");
        let log_entry = format!("[{}] [{}] {}\n", timestamp, category, message);

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)?;

        file.write_all(log_entry.as_bytes())?;
        Ok(())
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_logger_creation_and_logging() {
        let logger = Logger::new("test_agent").unwrap();
        logger.log("TEST", "test message").unwrap();

        let content = fs::read_to_string(&logger.log_path).unwrap();
        assert!(content.contains("[TEST] test message"));

        // Clean up
        let _ = fs::remove_file(logger.log_path);
    }
}
