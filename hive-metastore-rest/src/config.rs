use std::net::SocketAddr;
use std::path::Path;

use anyhow::Context;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub rest: Rest,
    pub metastore: Metastore,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Rest {
    pub addr: SocketAddr,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Metastore {
    pub addr: SocketAddr,
}

pub fn parse_config<P: AsRef<Path>>(p: P) -> anyhow::Result<Config> {
    let p = p.as_ref();
    let s = std::fs::read(p).with_context(|| format!("failed to read: {}", p.display()))?;
    let s = toml::from_slice(&s).with_context(|| format!("failed to parse to toml"))?;
    Ok(s)
}
