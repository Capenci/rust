use kafka::producer::{Producer, Record, RequiredAcks};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use kafka::error::Error as KafkaError;
use serde_json::json;
use serde;
use chrono::Utc;
#[derive(serde::Serialize,serde::Deserialize)]
pub struct MyMessage {
    data: String,
    id: i32,
}
pub fn publish_message(brokers: &str, topic: &str, message: &str) -> Result<(), KafkaError> {
    let mut producer = Producer::from_hosts(vec!(brokers.to_owned()))
        .with_ack_timeout(std::time::Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()?;

    let record = Record::from_value(topic, message);

    producer.send(&record)?;

    Ok(())
}

pub fn publish_json(brokers: &str, topic: &str, message: MyMessage) -> Result<(), KafkaError> {
    let mut producer = Producer::from_hosts(vec!(brokers.to_owned()))
        .with_ack_timeout(std::time::Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()?;
    let timestamp = Utc::now().to_rfc3339();
    let key_bytes = timestamp.as_bytes();
    let record = Record::from_key_value(topic,key_bytes, json!(message).to_string());

    producer.send(&record)?;

    Ok(())
}
pub fn consume_messages(brokers: &str, topic: &str) -> Result<Vec<String>, KafkaError> {
    let mut consumer = Consumer::from_hosts(vec!(brokers.to_owned()))
        .with_topic(topic.to_owned())
        .with_fallback_offset(FetchOffset::Earliest)
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()?;
    let mut message_count = 0;
    let mut data = vec![];
    for message_set in consumer.poll().unwrap().iter() {
        println!("{}", message_set.partition());
        for message in message_set.messages() {
            println!("Received message: {:?}", String::from_utf8_lossy(message.value));
            println!("{:?}", String::from_utf8_lossy(message.key));
            data.push(String::from_utf8_lossy(message.value).to_string());
            message_count += 1;
        }
    }
    println!("Total messages received: {}", message_count);

    Ok(data)
}

// pub fn modify_message() -> Result<Vec<String>, KafkaError>{
//     let mut consumer = Consumer::from_hosts(vec!(brokers.to_owned()))
//         .with_topic(topic.to_owned())
//         .with_fallback_offset(FetchOffset::Earliest)
//         .with_offset_storage(GroupOffsetStorage::Kafka)
//         .create()?;
//     let mut producer = Producer::from_hosts(vec!(brokers.to_owned()))
//         .with_ack_timeout(std::time::Duration::from_secs(1))
//         .with_required_acks(RequiredAcks::One)
//         .create()?;

// }
// fn main() {
//     let brokers = "192.168.56.131:9092";
//     let topic = "topic";
//     let message = "Hello, Kafka!";

//     // match publish_message(brokers, topic, message) {
//     //     Ok(_) => println!("Message published successfully!"),
//     //     Err(err) => eprintln!("Error publishing message: {:?}", err),
//     // }

//     match consume_messages(brokers, topic) {
//         Ok(_) => println!("Message consumption completed!"),
//         Err(err) => eprintln!("Error consuming messages: {:?}", err),
//     }
// }
