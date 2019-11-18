use std::env;
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

fn send_record<K, P>(producer: FutureProducer, record: FutureRecord<K, P>)
where
    K: ToBytes + ?Sized,
    P: ToBytes + ?Sized,
{
    let (partition, offset) = producer
        .send(record, 1000)
        .wait()
        .expect("Future cancelled")
        .expect("Delivery failed");
    log::debug!(
        "Producer response, partition: {:?} offset: {:?}",
        partition,
        offset
    );
}

fn rocksdb_get<K: AsRef<[u8]>>(snapshot: &Snapshot, key: &K) -> Option<impl AsRef<[u8]>> {
    snapshot.get(key).expect("RocksDB error")
}

fn lmdb_get<'txn, K: AsRef<[u8]>>(
    tx: &'txn RoTransaction,
    lmdb: Database,
    key: &K,
) -> Option<&'txn [u8]> {
    match tx.get(lmdb, key) {
        Ok(value) => Some(value),
        Err(lmdb::Error::NotFound) => None,
        Err(e) => panic!("LMDB error: {:?}", e),
    }
}

fn main() {
    env_logger::from_env(Env::default().default_filter_or("info")).init();
    log::info!(
        "crux.rs version: {} revision: {}",
        VERSION.unwrap_or("unknown"),
        GIT_HASH.unwrap_or("unknown")
    );

    let bootstrap_servers =
        &env::var("BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_string());
    log::debug!("bootstrap.servers = {}", bootstrap_servers);

    let topic = "my-topic";

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()
        .expect("Could not create producer");

    let value = b"Hello World";
    let key: &[u8] = &Sha1::digest(value);
    let record = FutureRecord::to(topic).key(key).payload(value);

    send_record(producer, record);

    let group_id = "crux-group";
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", bootstrap_servers)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Could not create consumer");

    consumer
        .subscribe(&[topic])
        .expect("Could not subscribe to topic");

    let rocksdb = DB::open_default("data/rocksdb").expect("Could not open RocksDB");

    let lmdb_path = Path::new("data/lmdb");
    fs::create_dir_all(lmdb_path).expect("Could not create LMDB directory");
    let lmdb_env = Environment::new()
        .open(lmdb_path)
        .expect("Could not open LMDB environment");

    let lmdb = lmdb_env
        .create_db(None, DatabaseFlags::empty())
        .expect("Could not create LMDB database");

    for message in consumer.start().wait() {
        match message.expect("Stream error") {
            Err(e) => log::error!("Consumer error: {:?}", e),
            Ok(m) => {
                let key = &m.key().unwrap_or(&[]);
                let payload = &m.payload().unwrap_or(&[]);

                let key_hex = hex::encode(key);
                let payload_str = String::from_utf8_lossy(payload);
                let timestamp = Utc.timestamp_millis(m.timestamp().to_millis().unwrap_or_default());

                log::info!(
                    "Consumed message: {:?} {:?} {:?} {:?} {:?} {:?}",
                    m.topic(),
                    m.partition(),
                    m.offset(),
                    timestamp,
                    key_hex,
                    payload_str
                );

                rocksdb
                    .put(key, payload)
                    .expect("Could not write to RocksDB");

                match rocksdb_get(&rocksdb.snapshot(), key) {
                    Some(value) => log::info!(
                        "Read key {:?} from RocksDB: {:?}",
                        key_hex,
                        String::from_utf8_lossy(value.as_ref())
                    ),
                    None => log::warn!("Key not found in RocksDB: {:?}", key_hex),
                }

                {
                    let mut tx = lmdb_env
                        .begin_rw_txn()
                        .expect("Could not start LMDB transaction");
                    tx.put(lmdb, key, payload, WriteFlags::empty())
                        .expect("Could not write to LMDB");
                    tx.commit().expect("Could not commit LMDB transaction");
                }

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
        }
    }
}
