use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::Path};

/// The resource definition used in apply -f <FILE>
#[derive(Debug, Serialize, Deserialize)]
pub struct Definition {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub metadata: HashMap<String, String>,
    pub spec: Spec,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Spec {
    pub containers: Vec<Container>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Container {
    pub image: String,
    pub name: String,
    pub ports: Vec<Port>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Port {
    #[serde(rename = "containerPort")]
    pub container_port: u16,
    pub protocol: String,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DefinitionError {
    #[error("field {0} is required")]
    MissingField(String),
}

impl Definition {
    pub fn metadata_name(&self) -> &str {
        self.metadata
            .get("name")
            .expect("should exist because it was validated when the definition was created")
    }

    pub async fn from_file(file_path: impl AsRef<Path>) -> Result<Self> {
        let file_contents = tokio::fs::read_to_string(file_path.as_ref()).await?;
        let definition: Definition = serde_yaml::from_str(&file_contents)?;

        if !definition.metadata.contains_key("name") {
            return Err(DefinitionError::MissingField("metadata.name".to_owned()).into());
        }

        Ok(definition)
    }
}
