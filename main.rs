use actix_web::{post, web, App, HttpServer, Responder};
use serde::{Deserialize, Serialize};
use chrono::{DateTime, ParseError};

#[derive(Deserialize)]
struct TaskInput {
    timestamps: Vec<String>,
}

#[derive(Serialize)]
struct TaskOutput {
    results: Vec<String>,
}

// Function to parse and compute time difference
fn compute_difference(t1: &str, t2: &str) -> Result<i64, ParseError> {
    let dt1 = DateTime::parse_from_str(t1, "%a %d %b %Y %H:%M:%S %z")?;
    let dt2 = DateTime::parse_from_str(t2, "%a %d %b %Y %H:%M:%S %z")?;
    Ok((dt1 - dt2).num_seconds().abs())
}

#[post("/process_task")]
async fn process_task(task_data: web::Json<TaskInput>) -> impl Responder {
    let mut results = Vec::new();
    for pair in task_data.timestamps.chunks(2) {
        if pair.len() == 2 {
            match compute_difference(&pair[0], &pair[1]) {
                Ok(diff) => results.push(diff.to_string()),
                Err(_) => results.push("Error parsing timestamps".to_string()),
            }
        }
    }
    web::Json(TaskOutput { results })
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Starting Rust Time Difference API...");
    HttpServer::new(|| App::new().service(process_task))
        .bind("127.0.0.1:8080")?
        .run()
        .await
}
