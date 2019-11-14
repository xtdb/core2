use env_logger::Env;
use log;

use hex;
use sha1::Digest;

use futures::future::Future;
use futures::stream::Stream;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};

use rocksdb::DB;

const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");
const GIT_HASH: Option<&'static str> = option_env!("GIT_HASH");

fn main() {
    env_logger::from_env(Env::default().default_filter_or("info")).init();
    log::info!(
        "Crux.rs version: {} revision: {}",
        VERSION.unwrap_or("unknown"),
        GIT_HASH.unwrap_or("unknown")
    );

    let topic = "my-topic";
    let bootstrap_servers = "localhost:9092";

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()
        .expect("Could not create producer");

    let value = "Hello World";

    let send_future = producer.send(
        FutureRecord {
            topic: topic,
            key: Some(&hex::encode(
                sha1::Sha1::digest(value.as_bytes()).as_slice(),
            )),
            payload: Some(value),
            partition: None,
            headers: None,
            timestamp: None,
        },
        1000,
    );

    match send_future.wait() {
        Err(e) => log::error!("Could not deliver message: {:?}", e),
        Ok(result) => log::debug!("Producer response: {:?}", result),
    }

    let rocksdb = DB::open_default("data").expect("Could not open RocksDB");

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

    for message in consumer.start().wait() {
        match message {
            Err(e) => log::error!("Stream error: {:?}", e),
            Ok(Err(e)) => log::error!("Consumer error: {:?}", e),
            Ok(Ok(m)) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        log::error!("Deserializing error: {:?}", e);
                        ""
                    }
                };
                let key = match m.key_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        log::error!("Deserializing error: {:?}", e);
                        ""
                    }
                };
                log::info!(
                    "Consumed message: {:?} {:?} {:?} {:?} {:?} {:?}",
                    m.topic(),
                    m.partition(),
                    m.offset(),
                    m.timestamp(),
                    key,
                    payload
                );

                log::debug!("Storing key {:?} into RocksDB: {:?}", key, payload);
                rocksdb
                    .put(key, payload)
                    .expect("Could not write to RocksDB");

                match rocksdb.get(key) {
                    Ok(Some(value)) => match value.to_utf8() {
                        Some(value) => log::info!("Read key {:?} from RocksDB: {:?}", key, value),
                        None => log::warn!("Empty key: {}", key),
                    },
                    Ok(None) => log::warn!("Key not found: {:?}", key),
                    Err(e) => log::error!("RocksDB error: {}", e),
                }
            }
        }
    }
}
