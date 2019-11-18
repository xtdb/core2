use std::fs;
use std::path::Path;

use lmdb::{Database, DatabaseFlags, Environment, RoTransaction, Transaction, WriteFlags};

type LMDB = (Database, Environment);

pub fn open(path: &Path) -> Result<LMDB, lmdb::Error> {
    if let Err(e) = fs::create_dir_all(path) {
        log::error!("{}", e);
    }
    let lmdb_env = Environment::new().open(path)?;
    let lmdb = lmdb_env.create_db(None, DatabaseFlags::empty())?;
    Ok((lmdb, lmdb_env))
}

pub fn get<'txn, K: AsRef<[u8]>>(
    tx: &'txn RoTransaction,
    lmdb: Database,
    key: &K,
) -> Option<&'txn [u8]> {
    match tx.get(lmdb, key) {
        Ok(value) => Some(value),
        Err(lmdb::Error::NotFound) => None,
        Err(e) => {
            log::error!("LMDB error: {:?}", e);
            None
        }
    }
}

pub fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
    lmdb_env: &Environment,
    lmdb: Database,
    key: &K,
    value: &V,
) -> Result<(), lmdb::Error> {
    let mut tx = lmdb_env.begin_rw_txn()?;
    tx.put(lmdb, key, value, WriteFlags::empty())?;
    tx.commit()
}
