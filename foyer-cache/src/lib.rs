mod chunk;
mod disk_cache;
mod metadata;

pub use chunk::FoyerChunkCache;
pub use foyer::Error as FoyerError;
pub use metadata::FoyerMetadataCache;

const DEFAULT_MEM_BUF_SIZE: usize = 1024 * 1024;

#[cfg(test)]
mod tests {
    fn init_tracing() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_test_writer()
            .try_init();
    }

    #[tokio::test]
    async fn test() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        init_tracing();
        Ok(())
    }
}
