use std::env;
use std::ffi::CString;
use std::os::raw::c_char;

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

pub fn version_string() -> String {
    format!(
        "crux.rs version: {} revision: {}",
        VERSION.unwrap_or("unknown"),
        GIT_HASH.unwrap_or("unknown")
    )
}

#[no_mangle]
pub extern "C" fn c_version_string() -> *const c_char {
    CString::new(version_string())
        .expect("Unexpected NULL")
        .into_raw()
}

#[allow(clippy::missing_safety_doc)]
#[no_mangle]
pub unsafe extern "C" fn c_string_free(c_string: *mut c_char) {
    let _ = CString::from_raw(c_string);
}

pub fn log_banner(config: &Config) {
    log::info!("{}", version_string());
    log::debug!("config = {:#?}", config);
}
