use std::error::Error;
use std::fs;
use std::path::Path;

use env_logger::Env;
use sha1::{Digest, Sha1};

use futures::stream::Stream;

use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use rdkafka::producer::FutureRecord;

fn init_logging() {
    env_logger::from_env(Env::default().default_filter_or("info")).init();
}

fn main() -> Result<(), Box<dyn Error>> {
    init_logging();

    let config = crux::init_config();
    crux::print_banner(&config);

    let value = b"Hello World";
    let key: &[u8] = &Sha1::digest(value);

    let producer = crux::kafka::create_producer(&config)?;
    let record = FutureRecord::to(&config.topic).key(key).payload(value);
    crux::kafka::send_record(producer, record)?;

    let consumer = crux::kafka::create_consumer(&config)?;
    consumer.subscribe(&[&config.topic])?;

    let rocksdb_path = Path::new(&config.db_dir).join("rocksdb");
    let rocksdb = crux::kv::rocksdb::open(&rocksdb_path)?;

    let lmdb_path = Path::new(&config.db_dir).join("lmdb");
    fs::create_dir_all(&lmdb_path)?;
    let lmdb = crux::kv::lmdb::open(&lmdb_path)?;

    for message in consumer.start().wait() {
        let message = message.expect("Stream error")?;

        let key = &message.key().unwrap_or(&[]);
        let payload = &message.payload().unwrap_or(&[]);

        crux::kafka::log_message(&message);

        crux::kv::rocksdb::put(&rocksdb, key, payload)?;
        crux::kv::log_key_access(
            key,
            crux::kv::rocksdb::get(&crux::kv::rocksdb::snapshot(&rocksdb)?, key),
        );

        crux::kv::lmdb::put(&lmdb, key, value)?;
        crux::kv::log_key_access(
            key,
            crux::kv::lmdb::get(&crux::kv::lmdb::snapshot(&lmdb)?, key),
        );
    }
    Ok(())
}
