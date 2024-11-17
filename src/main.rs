use anyhow::Result;
use tokio::time::Instant;
use uuid::Uuid;

use datalake_indexer::config::Config;
use datalake_indexer::handler;
use datalake_indexer::utils::aws::get_aws_client;
use datalake_indexer::utils::datafusion::write_df_to_s3;

#[tokio::main]
async fn main() -> Result<()> {
    println!("start processing");
    let now = Instant::now();
    let client = get_aws_client("eu-central-1").await;
    let config = Config::new()?;
    let df = handler(client.clone(), config.clone()).await?;
    let id = Uuid::new_v4().to_string();
    let key = format!("{}item={}/id={}-table=data_indexer.parquet", &config.prefix_target, &config.item_name, &id);
    write_df_to_s3(client, &config.bucket_target, &key, df).await?;
    println!("end processing elapsed: {:.2?}", now.elapsed());
    
    Ok(())
}