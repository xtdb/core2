pub mod lmdb;
pub mod rocksdb;

use hex;
use log;

pub fn log_key_access<K: AsRef<[u8]>, V: AsRef<[u8]>>(key: K, value: Option<V>) {
    let key_hex = hex::encode(key);

    match value {
        Some(value) => log::info!(
            "Read key {:?} from KV: {:?}",
            key_hex,
            String::from_utf8_lossy(value.as_ref())
        ),
        None => log::warn!("Key not found in KV: {:?}", key_hex),
    }
}
