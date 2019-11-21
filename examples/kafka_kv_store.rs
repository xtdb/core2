use std::error::Error;
use std::path::Path;

use env_logger::Env;
use log::Level;
use sha1::{Digest, Sha1};

use futures::stream::Stream;

use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use rdkafka::producer::FutureRecord;

use crux::Config;

fn init_logging() {
    env_logger::from_env(Env::default().default_filter_or(Level::Info.to_string())).init();
}

fn main() -> Result<(), Box<dyn Error>> {
    init_logging();

    let config = Config::from_env();
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
    let lmdb = crux::kv::lmdb::open(&lmdb_path)?;

    for message in consumer.start().wait() {
        let message = message.expect("Stream error")?;
        crux::kafka::log_message(&message);

        let key = &message.key().unwrap_or(&[]);
        let value = &message.payload().unwrap_or(&[]);

        crux::kv::rocksdb::put(&rocksdb, key, value)?;
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
