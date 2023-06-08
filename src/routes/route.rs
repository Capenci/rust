use super::super::{message::Data,DETAIL};
use actix_web::{web, HttpResponse};
use rdkafka::producer::{FutureProducer, FutureRecord};
use actix_web::{get, post};
use chrono::Local;

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
        DETAIL.timestamp = Local::now().to_rfc3339();
        let temp = serde_json::to_string(&DETAIL).unwrap();
        HttpResponse::Ok().body(temp)
    }
    
}
pub fn config(conf: &mut web::ServiceConfig) {
    let scope = web::scope("/kafka")
        .service(publish_to_kafka)
        .service(get_detail);
    conf.service(scope);
}
