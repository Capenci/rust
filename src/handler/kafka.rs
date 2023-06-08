use rdkafka::consumer::{StreamConsumer};
use rdkafka::Message;
use std::sync::{Arc, Mutex};
use super::super::{message::Data,rate_limit::RateLimiter,DETAIL};
pub async fn kafka_handler(
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
                                        DETAIL.number_of_requests +=1;
                                        DETAIL.sum+= dt.data;
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

