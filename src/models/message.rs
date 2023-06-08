#[derive(serde::Serialize,serde::Deserialize)]
pub struct Data {
    pub data : i32,
}

#[derive(serde::Serialize,serde::Deserialize)]
pub struct Detail {
    pub number_of_requests : i32,
    pub sum : i32,
    pub timestamp : String
}