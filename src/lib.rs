use std::path::Path;

use anyhow::Result;
use aws_sdk_s3::Client;
use chrono::prelude::*;
use datafusion::prelude::*;
use uuid::Uuid;

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
        let file_id = Uuid::new_v4().to_string();
        let path = Path::new(&file);
        let file_name = path.file_name().map(|x| x.to_string_lossy().to_string());
        let file_type = path.extension().map(|x| x.to_string_lossy().to_string());
        let file_path = path.parent().map(|x| x.to_string_lossy().to_string());
        let file_url = Some(format!("s3://{}/{}", &config.bucket_source, file));
        let dt_fmt = match dt {
            Some(dt) => {
                let dt = Utc.timestamp_opt(dt, 0).unwrap();
                Some(dt.to_string())
            },
            None => None
        };

        let file_data = FileData::new(
            Some(&file_id),
            file_name.as_deref(), 
            file_type.as_deref(), 
            file_path.as_deref(), 
            file_size,
            file_url.as_deref(),
            dt,
            dt_fmt.as_deref(),
        );

        file_data_all.push(file_data);
    }

    println!("start converting to df");
    let ctx = SessionContext::new();
    let df = FileData::to_df(ctx, &mut file_data_all).await?;

    Ok(df)
}