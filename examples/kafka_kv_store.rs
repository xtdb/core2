use std::env;
use std::error::Error;
use std::fs;
use std::path::Path;

use env_logger::Env;
use log;

use chrono::offset::TimeZone;
use chrono::Utc;

use hex;
use sha1::{Digest, Sha1};

use futures::future::Future;
use futures::stream::Stream;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::{Message, ToBytes};
use rdkafka::producer::{FutureProducer, FutureRecord};

use lmdb::{Database, DatabaseFlags, Environment, RoTransaction, Transaction, WriteFlags};
use rocksdb::{Snapshot, DB};

const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");
const GIT_HASH: Option<&'static str> = option_env!("GIT_HASH");

fn send_record<K, P>(
    producer: FutureProducer,
    record: FutureRecord<K, P>,
) -> Result<(), rdkafka::error::KafkaError>
where
    K: ToBytes + ?Sized,
    P: ToBytes + ?Sized,
{
    match producer.send(record, 1000).wait() {
        Err(e) => log::error!("Future cancelled: {:?}", e),
        Ok(Err((e, _))) => return Err(e),
        Ok(Ok((partition, offset))) => {
            log::debug!(
                "Producer response, partition: {:?} offset: {:?}",
                partition,
                offset
            );
        }
    }
    Ok(())
}

fn rocksdb_get<K: AsRef<[u8]>>(snapshot: &Snapshot, key: &K) -> Option<impl AsRef<[u8]>> {
    match snapshot.get(key) {
        Ok(value) => value,
        Err(e) => {
            log::error!("RocksDB error: {:?}", e);
            None
        }
    }
}

fn rocksdb_put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
    rocksdb: &DB,
    key: &K,
    value: &V,
) -> Result<(), rocksdb::Error> {
    rocksdb.put(key, value)
}

fn lmdb_get<'txn, K: AsRef<[u8]>>(
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

fn lmdb_put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
    lmdb_env: &Environment,
    lmdb: Database,
    key: &K,
    value: &V,
) -> Result<(), lmdb::Error> {
    let mut tx = lmdb_env.begin_rw_txn()?;
    tx.put(lmdb, key, value, WriteFlags::empty())?;
    tx.commit()
}

#[derive(Debug)]
struct Config {
    bootstrap_servers: String,
    topic: String,
    group_id: String,
    db_dir: String,
}

fn init_logging() {
    env_logger::from_env(Env::default().default_filter_or("info")).init();
}

fn print_banner(config: &Config) {
    log::info!(
        "crux.rs version: {} revision: {}",
        VERSION.unwrap_or("unknown"),
        GIT_HASH.unwrap_or("unknown")
    );
    log::debug!("config = {:#?}", config);
}

fn init_config() -> Config {
    Config {
        bootstrap_servers: env::var("BOOTSTRAP_SERVERS")
            .unwrap_or_else(|_| "localhost:9092".to_string()),
        topic: "crux_topic".to_string(),
        group_id: "crux-group".to_string(),
        db_dir: "data".to_string(),
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    init_logging();

    let config = init_config();
    print_banner(&config);

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &config.bootstrap_servers)
        .create()?;

    let value = b"Hello World";
    let key: &[u8] = &Sha1::digest(value);
    let record = FutureRecord::to(&config.topic).key(key).payload(value);

    send_record(producer, record)?;

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &config.group_id)
        .set("bootstrap.servers", &config.bootstrap_servers)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer.subscribe(&[&config.topic])?;

    let rocksdb_path = Path::new(&config.db_dir).join("rocksdb");
    let rocksdb = DB::open_default(&rocksdb_path)?;

    let lmdb_path = Path::new(&config.db_dir).join("lmdb");
    fs::create_dir_all(&lmdb_path)?;
    let lmdb_env = Environment::new().open(&lmdb_path)?;

    let lmdb = lmdb_env.create_db(None, DatabaseFlags::empty())?;

    for message in consumer.start().wait() {
        let message = message.expect("Stream error")?;
        let key = &message.key().unwrap_or(&[]);
        let payload = &message.payload().unwrap_or(&[]);

        let key_hex = hex::encode(key);
        let payload_str = String::from_utf8_lossy(payload);
        let timestamp = Utc.timestamp_millis(message.timestamp().to_millis().unwrap_or_default());

        log::info!(
            "Consumed message: {:?} {:?} {:?} {:?} {:?} {:?}",
            message.topic(),
            message.partition(),
            message.offset(),
            timestamp,
            key_hex,
            payload_str
        );

        rocksdb_put(&rocksdb, key, payload)?;

        match rocksdb_get(&rocksdb.snapshot(), key) {
            Some(value) => log::info!(
                "Read key {:?} from RocksDB: {:?}",
                key_hex,
                String::from_utf8_lossy(value.as_ref())
            ),
            None => log::warn!("Key not found in RocksDB: {:?}", key_hex),
        }

        lmdb_put(&lmdb_env, lmdb, key, value)?;

        match lmdb_get(
            &lmdb_env
                .begin_ro_txn()
                .expect("Could not start LMDB transaction"),
            lmdb,
            key,
        ) {
            Some(value) => log::info!(
                "Read key {:?} from LMDB: {:?}",
                key_hex,
                String::from_utf8_lossy(&value)
            ),
            None => log::warn!("Key not found in RocksDB: {:?}", key_hex),
        }
    }
    Ok(())
}
