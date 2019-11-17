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
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};

use lmdb::{DatabaseFlags, Environment, Transaction, WriteFlags};
use rocksdb::DB;

const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");
const GIT_HASH: Option<&'static str> = option_env!("GIT_HASH");

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

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()
        .expect("Could not create producer");

    let topic = "my-topic";

    let value = b"Hello World";
    let key: &[u8] = &Sha1::digest(value);
    let record = FutureRecord::to(topic).key(key).payload(value);

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
        match message {
            Err(e) => log::error!("Stream error: {:?}", e),
            Ok(Err(e)) => log::error!("Consumer error: {:?}", e),
            Ok(Ok(m)) => {
                let key = &m.key().unwrap_or(&[]);
                let payload = &m.payload().unwrap_or(&[]);

                let key_hex = hex::encode(key);
                let payload_str =
                    String::from_utf8(payload.to_vec()).unwrap_or_else(|_| "".to_string());
                let timestamp = Utc.timestamp_millis(m.timestamp().to_millis().unwrap_or(0));

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

                match rocksdb.snapshot().get(key) {
                    Ok(Some(value)) => match String::from_utf8(value.to_vec()) {
                        Ok(value) => log::info!("Read key {:?} from RocksDB: {:?}", key_hex, value),
                        Err(e) => log::warn!("Invalid RocksDB value: {:?}", e),
                    },
                    Ok(None) => log::warn!("Key not found in RocksDB: {:?}", key_hex),
                    Err(e) => log::error!("RocksDB error: {:?}", e),
                }

                {
                    let mut tx = lmdb_env
                        .begin_rw_txn()
                        .expect("Could not start LMDB transaction");
                    tx.put(lmdb, key, payload, WriteFlags::empty())
                        .expect("Could not write to LMDB");
                    tx.commit().expect("Could not commit LMDB transaction");
                }

                match lmdb_env.begin_ro_txn() {
                    Ok(tx) => match tx.get(lmdb, key) {
                        Ok(value) => match String::from_utf8(value.to_vec()) {
                            Ok(value) => {
                                log::info!("Read key {:?} from LMDB: {:?}", key_hex, value)
                            }
                            Err(e) => log::warn!("Invalid LMDB value: {:?}", e),
                        },
                        Err(lmdb::Error::NotFound) => {
                            log::warn!("Key not found in LMDB: {:?}", key_hex)
                        }
                        Err(e) => log::error!("LMDB error: {:?}", e),
                    },
                    Err(e) => log::error!("Could not start LMDB transaction: {:?}", e),
                }
            }
        }
    }
}
