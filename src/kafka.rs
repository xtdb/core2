use chrono::offset::TimeZone;
use chrono::Utc;

use futures::future::Future;

use hex;

use log;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::error::KafkaError;
use rdkafka::message::{Message, ToBytes};
use rdkafka::producer::{FutureProducer, FutureRecord};

pub fn create_producer(config: &super::Config) -> Result<FutureProducer, KafkaError> {
    ClientConfig::new()
        .set("bootstrap.servers", &config.bootstrap_servers)
        .create()
}

pub fn create_consumer(config: &super::Config) -> Result<StreamConsumer, KafkaError> {
    ClientConfig::new()
        .set("group.id", &config.group_id)
        .set("bootstrap.servers", &config.bootstrap_servers)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
}

pub fn send_record<K, P>(
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

pub fn log_message(message: &impl Message) {
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
}
