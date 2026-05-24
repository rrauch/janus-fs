use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, SqliteConnection};
use std::path::Path;
use std::{env, fs};
use tokio::runtime::Runtime;

fn main() -> anyhow::Result<()> {
    // Set up a temporary sqlite database, so SQLx's compile time
    // syntax check has something to connect to
    let db_file = Path::new(env::var("OUT_DIR")?.as_str()).join("_sqlx_sqlite_db.tmp");
    if db_file.exists() {
        fs::remove_file(&db_file)?;
    }

    {
        // Create a new db and run migrations to create the schema
        let db_file = db_file.clone();
        let rt = Runtime::new()?;
        rt.block_on(async move {
            let mut conn = SqliteConnection::connect_with(
                &SqliteConnectOptions::new()
                    .create_if_missing(true)
                    .filename(db_file),
            )
            .await?;
            sqlx::migrate!("./migrations").run(&mut conn).await?;
            Ok::<_, anyhow::Error>(())
        })?;
    }

    // pass the path to the newly created sqlite db to sqlx via the `DATABASE_URL` env variable
    println!(
        "cargo:rustc-env=DATABASE_URL=sqlite:{}",
        db_file.clone().into_os_string().into_string().unwrap()
    );
    println!("cargo:rerun-if-changed=migrations");
    Ok(())
}
