use datafusion::prelude::*;
use datafusion::execution::options::ParquetReadOptions;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::arrow_writer::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::basic::{ Compression, Encoding };
use datafusion::parquet::file::properties::WriterVersion;
use datafusion::arrow::util::pretty::print_batches;
use serde_derive::Deserialize;
use std::fs;
use std::collections::HashMap;
use futures::executor::block_on;

fn main() {

    #[derive(Deserialize)]
    struct Config {
        file_name: String,
        directory: String,
        format: String,
        compression: String,
        print: bool,
        register: Vec<HashMap<String, String>>,
        sql_text: Sql
    }

    #[derive(Deserialize)]
    struct Sql {
        sql : Vec<String>
    }

    match fs::read_to_string("./Input.toml") {
        Ok(file_val) => {let file_str: String = file_val;
            match toml::from_str(&file_str) {
                Ok(toml_val) => { let config: Config = toml_val;
                    let context = SessionContext::new();
                    let sql_stmt_len: i64 = config.sql_text.sql.len().try_into().unwrap();
                    let sql_stmt_len_iter: usize = sql_stmt_len.try_into().unwrap();
                    let file_path: String = config.directory + &config.file_name + &config.format;
                    for table_kv in config.register.iter(){
                        for (table, path) in table_kv {
                            block_on(register(&context, &table, &path))
                        };
                    };
                    println!("\n\n{} sql statement(s) to path: \n {}", &sql_stmt_len, &file_path);
                    let mut count = 0;
                    for i in 0..sql_stmt_len_iter{
                        count += 1;
                        println!("\ncount: {}\n\n-sql-start----------\n{}-sql-end------------", &count, &config.sql_text.sql[i]);
                        if i == sql_stmt_len_iter - 1 {
                            let batch_grp = block_on(output(&context, &config.sql_text.sql[i]));
                            //let file = fs::File::create(&file_path);
                            match fs::File::create(&file_path) {
                                Ok(path_val) => {
                                    let file = &path_val;
                                    let props = WriterProperties::builder()
                                        .set_compression(Compression::SNAPPY)
                                        .set_created_by("cloudcafe.app".to_string())
                                        .set_writer_version(WriterVersion::PARQUET_1_0)
                                        //.set_data_pagesize_limit()
                                        //.set_data_page_row_count_limit(300)
                                        .set_encoding(Encoding::PLAIN)
                                        .build();
                                    let mut writer = ArrowWriter::try_new(file, 
                                                        batch_grp[0].schema(), 
                                                        Some(props)).unwrap();
                                    for batch in &batch_grp {
                                        writer.write(&batch).unwrap();
                                    };
                                    writer.close().unwrap();
                                    if &config.print == &true {
                                            print_batches(&batch_grp).unwrap();
                                    }
                                }
                                Err(e) => println!("error -> cannot open file path to save to. Check if directory exists.\t{}\n{}", e, &file_path),
                            } } else {
                            block_on(run(&context, &config.sql_text.sql[i]));
                            }
                    }
                }
                Err(e) => println!("error -> cannot deserialize Input.toml.\t{}", e),
            };
        }
        Err(e) => println!("error -> no Input.toml file found.\t{}", e),
    }

async fn register(context: &SessionContext, table: &str, path: &str) {
    return context.register_parquet(table, path, ParquetReadOptions::default()).await.unwrap()
}

async fn output(context: &SessionContext, sql_str: &str) -> Vec<RecordBatch> {
    return context.sql(sql_str).await.unwrap().collect().await.unwrap()
}

async fn run(context: &SessionContext, sql_str: &str) {
    context.sql(sql_str).await.unwrap().collect().await.unwrap();
    return
}
}
