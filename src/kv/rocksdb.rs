use rocksdb::{Snapshot, DB};

pub fn get<K: AsRef<[u8]>>(snapshot: &Snapshot, key: &K) -> Option<impl AsRef<[u8]>> {
    match snapshot.get(key) {
        Ok(value) => value,
        Err(e) => {
            log::error!("RocksDB error: {:?}", e);
            None
        }
    }
}

pub fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
    rocksdb: &DB,
    key: &K,
    value: &V,
) -> Result<(), rocksdb::Error> {
    rocksdb.put(key, value)
}
