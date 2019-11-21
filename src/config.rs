use std::env;

const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");
const GIT_HASH: Option<&'static str> = option_env!("GIT_HASH");

#[derive(Debug)]
pub struct Config {
    pub bootstrap_servers: String,
    pub topic: String,
    pub group_id: String,
    pub db_dir: String,
}

impl Config {
    pub fn from_env() -> Self {
        Config {
            bootstrap_servers: env::var("BOOTSTRAP_SERVERS")
                .unwrap_or_else(|_| "localhost:9092".to_string()),
            topic: "crux_topic".to_string(),
            group_id: "crux-group".to_string(),
            db_dir: "data".to_string(),
        }
    }
}

pub fn print_banner(config: &Config) {
    log::info!(
        "crux.rs version: {} revision: {}",
        VERSION.unwrap_or("unknown"),
        GIT_HASH.unwrap_or("unknown")
    );
    log::debug!("config = {:#?}", config);
}
