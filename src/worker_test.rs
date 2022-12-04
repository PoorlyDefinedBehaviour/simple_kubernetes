use anyhow::Result;
use std::io::Write;

#[cfg(test)]
mod config_from_file_tests {

    use tempfile::NamedTempFile;

    use crate::worker::Config;

    use super::*;

    #[tokio::test]
    async fn reads_config_file() -> Result<()> {
        let mut file = NamedTempFile::new()?;
        write!(
            &mut file,
            r#"
id: "63483d5c-ac50-411c-9771-b5689578e2bf"
heartbeat:
  interval: 5
manager:
  addr: "::1"
"#
        )?;

        assert!(Config::from_file(file.path()).await.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn manager_addr_is_required() -> Result<()> {
        let mut file = NamedTempFile::new()?;
        write!(
            &mut file,
            r#"
id: "63483d5c-ac50-411c-9771-b5689578e2bf"
heartbeat:
  interval: 5
manager:
  addr: ""
"#
        )?;

        let result = Config::from_file(file.path()).await.unwrap_err();

        assert!(result
            .to_string()
            .contains("manager.addr: invalid IPv6 address syntax"));

        Ok(())
    }
}
