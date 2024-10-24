use std::sync::Arc;

use anyhow::Result;
use datafusion::prelude::*;
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};

pub struct FileData {
    file_name: Option<String>, 
    file_type: Option<String>,
    file_path: Option<String>,
    file_size: Option<i64>,
    file_url: Option<String>,
    dt: Option<i64>,
    dt_fmt: Option<String>,
}

impl FileData {
    pub fn new(
        file_name: Option<&str>, 
        file_type: Option<&str>, 
        file_path: Option<&str>, 
        file_size: Option<i64>, 
        file_url: Option<&str>,
        dt: Option<i64>,
        dt_fmt: Option<String>,
    ) -> Self {
        Self { 
            file_name: file_name.map(|x| x.to_string()), 
            file_type: file_type.map(|x| x.to_string()), 
            file_path: file_path.map(|x| x.to_string()), 
            file_size, 
            file_url: file_url.map(|x| x.to_string()),
            dt,
            dt_fmt: dt_fmt.map(|x| x.to_string()),
        }
    }

    pub async fn to_df(ctx: SessionContext, records: &Vec<Self>) -> Result<DataFrame> {
        let schema = Schema::new(vec![
            Field::new("file_name", DataType::Utf8, true),
            Field::new("file_type", DataType::Utf8, true),
            Field::new("file_size", DataType::Int64, true),
            Field::new("file_path", DataType::Utf8, true),
            Field::new("file_url", DataType::Utf8, true),
            Field::new("dt", DataType::Int64, true),
            Field::new("dt_fmt", DataType::Utf8, true),
        ]);

        let mut file_names = vec![];
        let mut file_types = vec![];
        let mut file_sizes = vec![];
        let mut file_paths = vec![];
        let mut file_urls = vec![];
        let mut dts = vec![];
        let mut dts_fmt = vec![];

        for record in records {
            file_names.push(record.file_name.as_deref());
            file_types.push(record.file_type.as_deref());
            file_sizes.push(record.file_size);
            file_paths.push(record.file_path.as_deref());
            file_urls.push(record.file_url.as_deref());
            dts.push(record.dt);
            dts_fmt.push(record.dt_fmt.as_deref());
        }

        let batch = RecordBatch::try_new(
            Arc::new(schema),
        vec![
                Arc::new(StringArray::from(file_names)),
                Arc::new(StringArray::from(file_types)),
                Arc::new(Int64Array::from(file_sizes)),
                Arc::new(StringArray::from(file_paths)),
                Arc::new(StringArray::from(file_urls)),
                Arc::new(Int64Array::from(dts)),
                Arc::new(StringArray::from(dts_fmt)),
            ],
        )?;

        let df = ctx.read_batch(batch)?;

        Ok(df)
    }
}
