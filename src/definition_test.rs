use anyhow::Result;
use std::io::Write;

#[cfg(test)]
mod definition_from_file_tests {
    use tempfile::NamedTempFile;

    use crate::definition::{Definition, DefinitionError};

    use super::*;

    #[tokio::test]
    async fn reads_task_definition() -> Result<()> {
        let mut file = NamedTempFile::new()?;
        write!(
            &mut file,
            r#"
apiVersion: v1
kind: Task
metadata:
  name: task-1
spec:
  containers:
    - image: poorlydefinedbehaviour/kubia
      name: kubia
      ports:
        - containerPort: 8080
          protocol: TCP
"#
        )?;

        assert!(Definition::from_file(file.path()).await.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn metadata_name_is_required() -> Result<()> {
        let mut file = NamedTempFile::new()?;
        write!(
            &mut file,
            r#"
apiVersion: v1
kind: Task
metadata:
spec:
  containers:
    - image: poorlydefinedbehaviour/kubia
      name: kubia
      ports:
        - containerPort: 8080
          protocol: TCP
"#
        )?;

        let error = Definition::from_file(file.path()).await.unwrap_err();

        assert_eq!(
            DefinitionError::MissingField("metadata.name".to_owned()),
            error.downcast()?
        );

        Ok(())
    }
}
