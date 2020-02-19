use rocksdb::{Error, Snapshot, DB};
use std::path::Path;

pub fn open(path: &Path) -> Result<DB, Error> {
    DB::open_default(path)
}

pub fn snapshot(rocksdb: &DB) -> Result<Snapshot, Error> {
    Ok(rocksdb.snapshot())
}

pub fn get<K: AsRef<[u8]>>(snapshot: &Snapshot, key: &K) -> Option<impl AsRef<[u8]>> {
    match snapshot.get(key) {
        Ok(value) => value,
        Err(e) => {
            log::error!("RocksDB error: {:?}", e);
            None
        }
    }
}

pub fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(rocksdb: &DB, key: &K, value: &V) -> Result<(), Error> {
    rocksdb.put(key, value)
}
