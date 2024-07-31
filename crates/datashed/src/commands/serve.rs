use std::fs::{File, OpenOptions};
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use actix_files::Files;
use actix_web::{
    get, guard, head, post, web, App, HttpResponse, HttpServer,
    Responder,
};
use csv::{Writer, WriterBuilder};
use serde::{Deserialize, Serialize};

use crate::error::DatashedResult;
use crate::prelude::Datashed;

#[derive(Debug, Default, clap::Parser)]
pub(crate) struct Serve {
    /// Run verbosely. Print additional progress information to the
    /// standard error stream. This option conflicts with the
    /// `--quiet` option.
    #[arg(short, long, conflicts_with = "quiet")]
    verbose: bool,

    /// Operate quietly; do not show progress. This option conflicts
    /// with the `--verbose` option.
    #[arg(short, long, conflicts_with = "verbose")]
    quiet: bool,

    #[arg(short, long)]
    port: Option<u16>,

    #[arg(long)]
    address: Option<IpAddr>,
}

struct AppState {
    datashed: Datashed,
    wtr: Mutex<Writer<File>>,
}

#[derive(Debug, Deserialize)]
struct RatingReq {
    path: PathBuf,
    rating: String,
    comment: Option<String>,
    user: String,
}

#[derive(Debug, Serialize)]
struct Record {
    rater: String,
    rating: String,
    comment: String,
}

#[post("/ratings")]
async fn f(
    state: web::Data<AppState>,
    req: web::Json<RatingReq>,
) -> HttpResponse {
    let dataset = &state.datashed;
    let base_dir = dataset.base_dir();
    let path = req.path.clone();
    let user = req.user.clone();
    let comment = req.comment.clone().unwrap_or_default();

    if !base_dir.join(&path).exists() {
        return HttpResponse::BadRequest()
            .body(format!("path {} does not exist!", path.display()));
    }

    let rating = match req.rating.as_str() {
        "C" | "C-" | "P" | "P-" | "P+" | "I" => req.rating.clone(),
        rating => {
            return HttpResponse::BadRequest()
                .body(format!("invalid rating '{rating}'!"))
        }
    };

    let path = path.to_str().unwrap_or_default();
    let created_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .to_string();

    let mut writer = state.wtr.lock().unwrap();
    let result = writer.write_record([
        path,
        &rating,
        &comment,
        &user,
        &created_at,
    ]);

    if result.is_err() {
        return HttpResponse::InternalServerError()
            .body("could not write record!");
    }

    let _ = writer.flush();

    HttpResponse::Ok().finish()
}

#[get("/")]
async fn index() -> impl Responder {
    HttpResponse::Ok().body("Hello, datashed!")
}

#[head("/health-check")]
async fn health_check() -> HttpResponse {
    HttpResponse::Ok().finish()
}

impl Serve {
    #[actix_web::main]
    pub(crate) async fn execute(self) -> DatashedResult<()> {
        let datashed = Datashed::discover()?;
        let config = datashed.config()?;
        let data_dir = datashed.data_dir();
        let temp_dir = datashed.temp_dir();

        let server_config = config.server.unwrap_or_default();
        let port = self.port.or(server_config.port).unwrap_or(9001);
        let addr = self
            .address
            .or(server_config.address)
            .or("0.0.0.0".parse().ok())
            .unwrap();

        let app_data = web::Data::new(AppState {
            datashed,
            wtr: Mutex::new(
                WriterBuilder::new().from_writer(
                    OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(temp_dir.join(Datashed::RATINGS))?,
                ),
            ),
        });

        let _ = HttpServer::new(move || {
            App::new()
                .app_data(app_data.clone())
                .service(health_check)
                .service(
                    Files::new("/data", data_dir.clone())
                        .method_guard(guard::Get()),
                )
                .service(f)
                .service(index)
        })
        .workers(2)
        .bind((addr, port))?
        .run()
        .await;

        Ok(())
    }
}
