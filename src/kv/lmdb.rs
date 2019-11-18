use std::fs;
use std::path::Path;

use lmdb::{Database, DatabaseFlags, Environment, RoTransaction, Transaction, WriteFlags};

type LMDB = (Database, Environment);
type LMDBSnapshot<'database> = (Database, RoTransaction<'database>);

pub fn open(path: &Path) -> Result<LMDB, lmdb::Error> {
    if let Err(e) = fs::create_dir_all(path) {
        log::error!("{}", e);
    }
    let lmdb_env = Environment::new().open(path)?;
    let lmdb = lmdb_env.create_db(None, DatabaseFlags::empty())?;
    Ok((lmdb, lmdb_env))
}

pub fn snapshot<'lmdb>((lmdb, lmdb_env): &'lmdb LMDB) -> Result<LMDBSnapshot<'lmdb>, lmdb::Error> {
    Ok((*lmdb, lmdb_env.begin_ro_txn()?))
}

pub fn get<'snapshot, K: AsRef<[u8]>>(
    (lmdb, tx): &'snapshot LMDBSnapshot,
    key: &K,
) -> Option<&'snapshot [u8]> {
    match tx.get(*lmdb, key) {
        Ok(value) => Some(value),
        Err(lmdb::Error::NotFound) => None,
        Err(e) => {
            log::error!("LMDB error: {:?}", e);
            None
        }
    }
}

pub fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
    (lmdb, lmdb_env): &LMDB,
    key: &K,
    value: &V,
) -> Result<(), lmdb::Error> {
    let mut tx = lmdb_env.begin_rw_txn()?;
    tx.put(*lmdb, key, value, WriteFlags::empty())?;
    tx.commit()
}
