use axum::{routing::post, Json, Router, AddExtensionLayer};
use serde::{Deserialize, Serialize};
use anyhow::Result;

use hive_metastore_cli::*;
use axum::extract::Extension;

mod config;

#[tokio::main]
async fn main() -> Result<()> {
    // initialize tracing
    tracing_subscriber::fmt::init();

    let config = config::parse_config("config.toml")?;
    let client = HiveMetastoreCli::new(&config.metastore.addr).await?;

    let app = Router::new()
        .route("/get_table", post(get_table))
        .route("/get_all_tables", post(get_all_tables))
        .route("/get_all_databases", post(get_all_databases))
        .layer(AddExtensionLayer::new(client));

    // run our app with hyper
    // `axum::Server` is a re-export of `hyper::Server`
    tracing::debug!("listening on {}", config.rest.addr);
    axum::Server::bind(&config.rest.addr)
        .serve(app.into_make_service())
        .await?;
    Ok(())
}

fn to_resp<T: Serialize>(res: Result<T, HiveMetastoreError>) -> Json<Result<T, String>> {
    let res = res.map_err(|e| e.to_string());
    Json(res)
}

async fn get_table(Extension(client): Extension<HiveMetastoreCli>, Json(payload): Json<GetTableReq>) -> Json<Result<Table, String>>{
    let res = client.get_table(&payload.db, &payload.tbl).await;
    to_resp(res)
}

async fn get_all_tables(Extension(client): Extension<HiveMetastoreCli>, Json(payload): Json<GetAllTablesReq>) -> Json<Result<Vec<String>, String>> {
    let res = client.get_all_tables(&payload.db).await;
    to_resp(res)
}

async fn get_all_databases(Extension(client): Extension<HiveMetastoreCli>) -> Json<Result<Vec<String>, String>> {
    let res = client.get_all_databases().await;
    to_resp(res)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GetTableReq {
    db: String,
    tbl: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GetAllTablesReq {
    db: String,
}
