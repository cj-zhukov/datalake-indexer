use anyhow::Result;
use aws_sdk_s3::Client;
use datafusion::prelude::*;
use std::path::Path;

pub mod config;
pub mod utils;
pub mod file_data;

use config::Config;
use file_data::FileData;
use utils::aws::list_keys_to_map;

pub async fn handler(client: Client, config: Config) -> Result<DataFrame> {
    println!("start running handler for data indexer");
    println!("reading data from: {}{}", &config.prefix_source, &config.item_name);
    let prefix = format!("{}{}", &config.prefix_source, &config.item_name);
    let files = list_keys_to_map(client.clone(), &config.bucket_source, &prefix).await?;

    println!("start processing data");
    let mut file_data_all = vec![];
    for (file, (file_size, dt)) in files {
        let path = Path::new(&file);
        let file_name = path.file_name().map(|x| x.to_string_lossy().to_string());
        let file_type = path.extension().map(|x| x.to_string_lossy().to_string());
        let file_path = path.parent().map(|x| x.to_string_lossy().to_string());
        let file_url = Some(format!("s3://{}/{}", &config.bucket_source, file));
        let dt_fmt = None;

        let file_data = FileData::new(
            file_name.as_deref(), 
            file_type.as_deref(), 
            file_path.as_deref(), 
            file_size,
            file_url.as_deref(),
            dt,
            dt_fmt,
        );

        file_data_all.push(file_data);
    }

    println!("start converting to df");
    let ctx = SessionContext::new();
    let df = FileData::to_df(ctx, &mut file_data_all).await?;

    Ok(df)
}