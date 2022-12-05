use anyhow::Result;
use std::io::Write;

#[cfg(test)]
mod config_from_file_tests {

    use tempfile::NamedTempFile;

    use crate::manager::Config;

    use super::*;

    #[tokio::test]
    async fn reads_config_file() -> Result<()> {
        let mut file = NamedTempFile::new()?;
        write!(
            &mut file,
            r#"
worker:
  heartbeat_timeout: 15
"#
        )?;

        assert!(Config::from_file(file.path()).await.is_ok());

        Ok(())
    }
}
