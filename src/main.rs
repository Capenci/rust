use std::thread;
use std::time::{Duration, Instant};
use actix_web::{web, App, HttpResponse, HttpServer};
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::consumer::{StreamConsumer, Consumer};
use std::sync::{Arc, Mutex};
use actix_web::{error, delete, get, post, put};
use chrono::Local;


struct RateLimiter {
    window_size: usize,
    max_requests: usize,
    requests: Vec<Instant>,
    message_counter: usize,
}
#[derive(serde::Serialize,serde::Deserialize)]
struct Data {
    data : i32,
}
#[derive(serde::Serialize,serde::Deserialize)]
struct Detail {
    number_of_requests : i32,
    sum : i32,
    timestamp : String
}
static  mut detail :Detail = Detail {
    number_of_requests : 0,
    sum : 0,
    timestamp : String::new()
};
impl RateLimiter {
    fn new(window_size: usize, max_requests: usize) -> Self {
        RateLimiter {
            window_size,
            max_requests,
            requests: Vec::with_capacity(max_requests),
            message_counter: 0,
        }
    }

    fn allow_request(&mut self) -> bool {
        let now = Instant::now();
        self.requests.retain(|&t| now.duration_since(t) < Duration::from_secs(self.window_size as u64));
        if self.requests.len() >= self.max_requests || self.message_counter >= self.max_requests {
            return false;
        }
        self.requests.push(now);
        self.message_counter += 1;
        true
    }

    fn decrease_message_counter(&mut self) {
        self.message_counter -= 1;
    }
}

async fn kafka_handler(
    data: Arc<Mutex<RateLimiter>>,
    consumer: StreamConsumer,
) {
    loop {
        let allowed = data.lock().unwrap().allow_request();
        if allowed {
            match consumer.recv().await {
                Ok(result) => {            
                    match result.payload() {
                        Some(value) => {
                            let json_str = String::from_utf8_lossy(value).into_owned();
                            let json_data: Result<Data, serde_json::Error> = serde_json::from_str(&json_str);
                            match json_data {
                                Ok(dt) => {
                                    unsafe{
                                        detail.number_of_requests +=1;
                                        detail.sum+= dt.data;
                                    }
                                }
                                Err(e)=>{
                                    println!("{:?}",e)
                                }
                            }
                        }
                        None => {
                            println!("Not contain value");
                        }
                    }
                    data.lock().unwrap().decrease_message_counter();
                    // println!("Number of requests: {}, Sum: {}, at: {}",number_of_requests,sum,Local::now().to_rfc3339());
                }
                Err(err) => {
                    data.lock().unwrap().decrease_message_counter();
                    println!("{:?}",err);
                    continue;
                }
            }
        }
    }
}
#[post("")]
async fn publish_to_kafka(
    producer: web::Data<FutureProducer>,
    message :  web::Json<Data>
) -> HttpResponse {
            let topic = format!("topic_queue");
            let json_string = serde_json::to_string(&message.0).unwrap();
            let record = FutureRecord::to(&topic)
                .payload(&json_string)
                .key("key");
            match producer.send_result(record) {
                Ok(_) => {
                    println!("message_{:?}",message.0.data)
                }
                Err(_) => println!("Can not send message"),
            }

        HttpResponse::Ok().body("Send message to kafka")
        
    }
#[get("")]
async fn get_detail()-> HttpResponse{
    unsafe{
        detail.timestamp = Local::now().to_rfc3339();
        let temp = serde_json::to_string(&detail).unwrap();
        HttpResponse::Ok().body(temp)
    }
    
}
pub fn config(conf: &mut web::ServiceConfig) {
    let scope = web::scope("/kafka")
        .service(publish_to_kafka)
        .service(get_detail);
    conf.service(scope);
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();
    let rate_limiter = Arc::new(Mutex::new(RateLimiter::new(60, 100))); // 5-second window, 10 max requests
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
            kafka_handler(rate_limiter1.clone(), consumer).await;
        });
        
    });
    HttpServer::new(move || {
        App::new()
            .app_data(rate_limiter.clone())
            .app_data(web::Data::new(producer.clone()))
            .service(
                web::scope("/api")
                .configure(config)
            )
            // .route("/kafka", web::post().to(push_to_deque))
            // .route("/test", web::post().to(publish_to_kafka))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}

