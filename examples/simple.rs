use hive_metastore_cli::*;

#[tokio::main]
async fn main() {
    let mut cli = HiveMetastoreCli::new("127.0.0.1:9083").await.unwrap();
    let tbl = cli.get_table("dwd", "dwd_order_order_df").await.unwrap();
    let s = serde_json::to_string(&tbl).unwrap();
    println!("{}", s);
    let dbs = cli.get_all_databases().await.unwrap();
    println!("{}", dbs.join(","));
    let tables = cli.get_all_tables("ads").await.unwrap();
    println!("{}", tables.join(","));
    //cli.test("dm_fumeiti", "ads_prod_fusion_95top_avg_md").await.unwrap();
}
