use lmdb::{Database, Environment, RoTransaction, Transaction, WriteFlags};

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
