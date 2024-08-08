use std::fs::{File, OpenOptions};
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use actix_files::{Files, NamedFile};
use actix_web::{
    get, guard, head, post, web, App, HttpResponse, HttpServer,
};
use csv::{Writer, WriterBuilder};
use serde::Deserialize;

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
    hash: String,
    rating: String,
    comment: String,
    username: String,
    secret: String,
}

#[post("/ratings")]
async fn ratings(
    state: web::Data<AppState>,
    req: web::Json<RatingReq>,
) -> HttpResponse {
    let dataset = &state.datashed;
    let base_dir = dataset.base_dir();
    let path = req.path.clone();
    let hash = req.hash.clone();
    let username = req.username.clone();
    let comment = req.comment.clone();

    let Ok(config) = dataset.config() else {
        return HttpResponse::InternalServerError().finish();
    };

    let Some(user) = config.users.get(&username) else {
        return HttpResponse::Unauthorized().finish();
    };

    if user.secret != req.secret {
        return HttpResponse::Unauthorized().finish();
    }

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
        &hash,
        &rating,
        &comment,
        &username,
        &created_at,
    ]);

    if result.is_err() {
        return HttpResponse::InternalServerError()
            .body("could not write record!");
    }

    let _ = writer.flush();

    HttpResponse::Ok().finish()
}

#[get("/index.ipc")]
async fn index(
    state: web::Data<AppState>,
) -> actix_web::Result<NamedFile> {
    let path = &state.datashed.base_dir().join("index.ipc");
    Ok(NamedFile::open(path)?)
}

#[head("/health-check")]
async fn health_check() -> HttpResponse {
    HttpResponse::Ok().finish()
}

impl Serve {
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
                .service(index)
                .service(
                    Files::new("/data", data_dir.clone())
                        .method_guard(guard::Get()),
                )
                .service(ratings)
        })
        .workers(2)
        .bind((addr, port))?
        .run()
        .await;

        Ok(())
    }
}
