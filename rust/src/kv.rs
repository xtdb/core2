pub mod lmdb;
pub mod rocksdb;

pub fn log_key_access<K: AsRef<[u8]>, V: AsRef<[u8]>>(key: K, value: Option<V>) {
    match value {
        Some(value) => log::info!(
            "Read key {:?} from KV: {:?}",
            hex::encode(key),
            String::from_utf8_lossy(value.as_ref())
        ),
        None => log::warn!("Key not found in KV: {:?}", hex::encode(key)),
    }
}
