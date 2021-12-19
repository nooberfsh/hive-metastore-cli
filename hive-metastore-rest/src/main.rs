use axum::{routing::post, response::IntoResponse, Json, Router, AddExtensionLayer};
use serde::{Deserialize, Serialize};

use hive_metastore_cli::*;
use axum::extract::Extension;

mod config;

#[tokio::main]
async fn main() {
    // initialize tracing
    tracing_subscriber::fmt::init();

    let config = config::parse_config("config.toml").unwrap();
    let client = HiveMetastoreCli::new(&config.metastore.addr).await.unwrap();

    let app = Router::new()
        .route("/get_table", post(get_table))
        .layer(AddExtensionLayer::new(client));

    // run our app with hyper
    // `axum::Server` is a re-export of `hyper::Server`
    tracing::debug!("listening on {}", config.rest.addr);
    axum::Server::bind(&config.rest.addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

fn to_resp<T: Serialize>(res: Result<T, HiveMetastoreError>) -> impl IntoResponse {
    let res = res.map_err(|e| e.to_string());
    Json(res)
}

async fn get_table(Extension(client): Extension<HiveMetastoreCli>, Json(payload): Json<GetTable>) -> impl IntoResponse {
    let res = client.get_table(&payload.db, &payload.tbl).await;
    to_resp(res)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GetTable {
    db: String,
    tbl: String,
}
