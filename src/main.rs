use std::thread;
use actix_web::{web, App, HttpServer};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer};
use rdkafka::consumer::{StreamConsumer, Consumer};
use std::sync::{Arc, Mutex};

mod handler;
mod models;
mod routes;
use models::{message, rate_limit};
use handler::kafka;

static  mut DETAIL : message::Detail = message::Detail {
    number_of_requests : 0,
    sum : 0,
    timestamp : String::new()
};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();
    let rate_limiter = Arc::new(Mutex::new(rate_limit::RateLimiter::new(60, 100))); // 5-second window, 10 max requests
    let consumer: StreamConsumer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("auto.offset.reset", "latest")
        .set("group.id", "console-consumer-50323")
        .create()
        .expect("Failed to create Kafka consumer");
    consumer.subscribe(&["topic_queue"]).expect("Failed to subscribe to Kafka topic");
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Failed to create Kafka producer");
    let  rate_limiter1 = rate_limiter.clone();
    // let  producer1 = producer.clone();
    thread::spawn(move||{
        let rt = tokio::runtime::Builder::new_current_thread()
            .build().expect("Failed to create Tokio runtime.");
        rt.block_on(async {
            // handler(rate_limiter1.clone(), producer1.clone()).await;
            kafka::kafka_handler(rate_limiter1.clone(), consumer).await;
        });
        
    });
    HttpServer::new(move || {
        App::new()
            .app_data(rate_limiter.clone())
            .app_data(web::Data::new(producer.clone()))
            .service(
                web::scope("/api")
                .configure(routes::route::config)
            )
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}

