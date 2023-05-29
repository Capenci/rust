use actix_web::{web::{self}, App, HttpResponse, HttpServer, Responder};
use rusqlite::{params, Connection};
use serde;
use std::sync::Mutex;

mod handler;
use handler::kafka_handler;

// #[derive(Debug, serde::Serialize, serde::Deserialize)]
// struct Todo {
//     id: i32,
//     name: String,
//     description: String,
// }
// #[derive(serde::Deserialize)]
// struct QueryParams {
//     id: i32,
// }
#[derive(serde::Deserialize)]
struct QueryParams {
    message: String,
}
// #[derive(serde::Deserialize)]
struct AppState {
    conn: Mutex<Connection>,
}

// fn get_todos(db: web::Data<AppState>) -> Result<Vec<Todo>, rusqlite::Error> {
//     // let conn = Connection::open("todo.db")?;
//     let conn =db.conn.lock().unwrap();
//     let mut stmt = conn.prepare("SELECT * FROM list")?;
//     let rows = stmt.query_map(params![], |row| {
//         Ok(Todo {
//             id: row.get(0)?,
//             name: row.get(1)?,
//             description: row.get(2)?,
//         })
//     })?;
//     let mut todos = Vec::new();
//     for todo in rows {
//         todos.push(todo?);
//     }

//     Ok(todos)
// }

// fn get_todos_with_id(query_params: web::Query<QueryParams>,db: web::Data<AppState>) -> Result<Todo, rusqlite::Error> {
//     let id = query_params.id;
//     let conn =db.conn.lock().unwrap();
//     let mut stmt = conn.prepare("SELECT * FROM list WHERE id = ?1")?;
//     let todo = stmt.query_row(params![id], |row| {
//         Ok(Todo {
//             id: row.get(0)?,
//             name: row.get(1)?,
//             description: row.get(2)?,
//         })
//     })?;

//     Ok(todo)
    
// }



// fn add_todo(todo: web::Json<Todo>,db: web::Data<AppState>) -> Result<HttpResponse, rusqlite::Error> {
//     let conn =db.conn.lock().unwrap();
//     conn.execute(
//         "INSERT INTO list (name, description) VALUES (?1, ?2)",
//         params![todo.name, todo.description],
//     )?;
//     Ok(HttpResponse::Ok().body("Todo added successfully"))
// }

// fn delete_todo(query_params: web::Query<QueryParams>, db: web::Data<AppState>) -> Result<HttpResponse, rusqlite::Error> {
//     let conn =db.conn.lock().unwrap();
//     conn.execute(
//         "DELETE FROM list WHERE id = ?1",
//         params![query_params.id],
//     )?;
//     Ok(HttpResponse::Ok().body("Todo delete successfully"))
// }

// fn update_todo(query_params: web::Json<Todo>, db: web::Data<AppState>) -> Result<HttpResponse, rusqlite::Error> {
//     let conn = Connection::open("todo.db")?;
//     conn.execute(
//         "UPDATE list SET name = ?1, description = ?2 WHERE id = ?3",
//         params![query_params.name,query_params.description,query_params.id],
//     )?;
//     Ok(HttpResponse::Ok().body("Todo update successfully"))
// }

// async fn todos(conn: web::Data<AppState>) -> impl Responder {
//     match get_todos(conn) {
//         Ok(todos) => HttpResponse::Ok().json(todos),
//         Err(_) => HttpResponse::InternalServerError().body("Internal Server Error"),
//     }
// }

// async fn add_todo_handler(todo: web::Json<Todo>,conn: web::Data<AppState>) -> impl Responder {
//     match add_todo(todo,conn) {
//         Ok(response) => response,
//         Err(_) => HttpResponse::InternalServerError().body("Internal Server Error"),
//     }
// }
// async fn delete_todo_handler(querry_params: web::Query<QueryParams>,conn: web::Data<AppState>) -> impl Responder {
//     match delete_todo(querry_params,conn) {
//         Ok(response) => response,
//         Err(_) => HttpResponse::InternalServerError().body("Internal Server Error"),
//     }
// }

// async fn update_todo_handler(querry_params: web::Json<Todo>,conn: web::Data<AppState>) -> impl Responder {
//     match update_todo(querry_params,conn) {
//         Ok(response) => response,
//         Err(_) => HttpResponse::InternalServerError().body("Internal Server Error"),
//     }
// }
// async fn todo_id(querry_params: web::Query<QueryParams>,conn: web::Data<AppState>) -> impl Responder {
//     match get_todos_with_id(querry_params,conn){
//         Ok(todos) => HttpResponse::Ok().json(todos),
//         Err(_) => HttpResponse::InternalServerError().body("Internal Server Error"),
//     }
// }

async fn get_message() -> impl Responder{
    match kafka_handler::consume_messages("192.168.56.131:9092", "topic"){
        Ok(data) => HttpResponse::Ok().json(data),
        Err(error) => { HttpResponse::InternalServerError().body(error.to_string())},
    }

}

async fn create_message(querry_params: web::Json<QueryParams>) ->impl Responder{
    match kafka_handler::publish_message("192.168.56.131:9092", "topic",&querry_params.message){
        Ok(_) =>HttpResponse::Ok().body("Create message success"),
        Err(_) => HttpResponse::InternalServerError().body("Internal Server Error"),
    }
}

async fn create_json(querry_params: web::Json<kafka_handler::MyMessage>) ->impl Responder{
    match kafka_handler::publish_json("192.168.56.131:9092", "topic",querry_params.0){
        Ok(_) =>HttpResponse::Ok().body("Create message success"),
        Err(_) => HttpResponse::InternalServerError().body("Internal Server Error"),
    }
}


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let conn = Connection::open("todo.db").expect("Failed to create database connection");
    let shared_conn = web::Data::new(AppState {
        conn: Mutex::new(conn),
    });
    HttpServer::new(move || {
        App::new()
            .app_data(shared_conn.clone())
            .route("/message", web::get().to(get_message))
            .route("/message", web::post().to(create_message))
            .route("/message_json", web::post().to(create_json))
            // .route("/todos", web::get().to(todos))
            // .route("/todos", web::post().to(add_todo_handler))
            // .route("/todo", web::get().to(todo_id))
            // .route("/todo", web::delete().to(delete_todo_handler))
            // .route("/todo", web::patch().to(update_todo_handler))

    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}