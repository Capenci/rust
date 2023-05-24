use actix_web::{web::{self,to}, App, HttpResponse, HttpServer, Responder};
use rusqlite::{params, Connection};
use serde;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Todo {
    id: i32,
    name: String,
    description: String,
}
#[derive(serde::Deserialize)]
struct QueryParams {
    id: i32,
}
fn get_todos() -> Result<Vec<Todo>, rusqlite::Error> {
    let conn = Connection::open("todo.db")?;
    let mut stmt = conn.prepare("SELECT * FROM list")?;
    let rows = stmt.query_map(params![], |row| {
        Ok(Todo {
            id: row.get(0)?,
            name: row.get(1)?,
            description: row.get(2)?,
        })
    })?;
    let mut todos = Vec::new();
    for todo in rows {
        todos.push(todo?);
    }

    Ok(todos)
}

fn get_todos_with_id(query_params: web::Query<QueryParams>,) -> Result<Todo, rusqlite::Error> {
    let id = query_params.id;
    let conn = Connection::open("todo.db")?;
    let mut stmt = conn.prepare("SELECT * FROM list WHERE id = ?1")?;
    let todo = stmt.query_row(params![id], |row| {
        Ok(Todo {
            id: row.get(0)?,
            name: row.get(1)?,
            description: row.get(2)?,
        })
    })?;

    Ok(todo)
    
}



fn add_todo(todo: web::Json<Todo>) -> Result<HttpResponse, rusqlite::Error> {
    let conn = Connection::open("todo.db")?;
    conn.execute(
        "INSERT INTO list (name, description) VALUES (?1, ?2)",
        params![todo.name, todo.description],
    )?;
    Ok(HttpResponse::Ok().body("Todo added successfully"))
}

fn delete_todo(query_params: web::Query<QueryParams>) -> Result<HttpResponse, rusqlite::Error> {
    let conn = Connection::open("todo.db")?;
    conn.execute(
        "DELETE FROM list WHERE id = ?1",
        params![query_params.id],
    )?;
    Ok(HttpResponse::Ok().body("Todo delete successfully"))
}

fn update_todo(query_params: web::Json<Todo>) -> Result<HttpResponse, rusqlite::Error> {
    let conn = Connection::open("todo.db")?;
    conn.execute(
        "UPDATE list SET name = ?1, description = ?2 WHERE id = ?3",
        params![query_params.name,query_params.description,query_params.id],
    )?;
    Ok(HttpResponse::Ok().body("Todo update successfully"))
}

async fn todos() -> impl Responder {
    match get_todos() {
        Ok(todos) => HttpResponse::Ok().json(todos),
        Err(_) => HttpResponse::InternalServerError().body("Internal Server Error"),
    }
}

async fn add_todo_handler(todo: web::Json<Todo>) -> impl Responder {
    match add_todo(todo) {
        Ok(response) => response,
        Err(_) => HttpResponse::InternalServerError().body("Internal Server Error"),
    }
}
async fn delete_todo_handler(querry_params: web::Query<QueryParams>) -> impl Responder {
    match delete_todo(querry_params) {
        Ok(response) => response,
        Err(_) => HttpResponse::InternalServerError().body("Internal Server Error"),
    }
}

async fn update_todo_handler(querry_params: web::Json<Todo>) -> impl Responder {
    match update_todo(querry_params) {
        Ok(response) => response,
        Err(_) => HttpResponse::InternalServerError().body("Internal Server Error"),
    }
}
async fn todo_id(querry_params: web::Query<QueryParams>) -> impl Responder {
    match get_todos_with_id(querry_params){
        Ok(todos) => HttpResponse::Ok().json(todos),
        Err(_) => HttpResponse::InternalServerError().body("Internal Server Error"),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new()
            .route("/todos", web::get().to(todos))
            .route("/todos", web::post().to(add_todo_handler))
            .route("/todo", web::get().to(todo_id))
            .route("/todo", web::delete().to(delete_todo_handler))
            .route("/todo", web::patch().to(update_todo_handler))

    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}