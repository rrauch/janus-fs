use anyhow::anyhow;
use bytesize::ByteSize;
use clap::{Args, Parser, ValueEnum};
use ct_codecs::{Decoder, Hex};
use foyer_cache::{FoyerChunkCache, FoyerMetadataCache};
use sia_io::indexd::client::AppKey;
use sia_io::indexd::{AppDetails, AppId};
use sia_io::renterd::client::ApiPassword;
use sia_io::renterd::BucketName;
use sia_nfs::SiaNfs;
use std::num::ParseIntError;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use tokio::signal::unix::{signal, SignalKind};
use tracing::{Instrument, Level};
use tracing_subscriber::EnvFilter;
use url::Url;
use zeroize::Zeroize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Backend {
    Indexd,
    Renterd,
}

#[derive(Debug, Args)]
struct IndexdArgs {
    /// URL for indexd's API endpoint.
    #[arg(long, short = 'i', env, value_hint = clap::ValueHint::Url, default_value = "https://sia.storage"
    )]
    indexd_endpoint: Url,

    /// Appkey for the indexd API.
    #[arg(long, short = 'k', env)]
    indexd_appkey: Option<String>,
}

#[derive(Debug, Args)]
struct RenterdArgs {
    /// URL for renterd's API endpoint.
    #[arg(long, short = 'e', env, value_hint = clap::ValueHint::Url)]
    renterd_api_endpoint: Option<Url>,

    /// Password for the renterd API.
    #[arg(long, short = 's', env)]
    renterd_api_password: Option<String>,

    /// renterd Bucket to export.
    #[arg(long, short = 'b', env)]
    bucket: Option<String>,
}

#[derive(Debug, Parser)]
#[command(version)]
/// Exports Sia stored data via NFS.
/// Connects to backend, allowing direct NFS access.
struct Arguments {
    vfs_id: String,

    /// Backend to use.
    #[arg(long, env, value_enum, default_value_t = Backend::Indexd)]
    backend: Backend,

    #[command(flatten)]
    indexd: IndexdArgs,

    #[command(flatten)]
    renterd: RenterdArgs,

    /// Directory to store persistent data in. Will be created if it doesn't exist.
    #[arg(long, short = 'd', env, value_hint = clap::ValueHint::DirPath)]
    data_dir: PathBuf,
    /// Optional directory to store the content cache in. Defaults to `DATA_DIR` if not set. Will be created if it doesn't exist.
    #[arg(long, short = 'c', env)]
    cache_dir: Option<PathBuf>,
    /// Maximum size of content cache. Set to `0` to disable.
    #[arg(long, short = 'm', env)]
    #[clap(default_value = "2 GiB")]
    max_cache_size: ByteSize,
    /// Maximum size of metadata cache. Set to `0` to disable.
    #[arg(long, short = 'n', env)]
    #[clap(default_value = "256 MiB")]
    max_metadata_cache_size: ByteSize,
    /// Host and port to listen on.
    #[arg(long, short = 'l', env)]
    #[clap(default_value = "localhost:12000")]
    listen_address: String,
    /// UID of files and directories
    #[arg(long, env = "INODE_UID")]
    #[clap(default_value = "1000")]
    uid: u32,
    /// GID of files and directories
    #[arg(long, env = "INODE_GID")]
    #[clap(default_value = "1000")]
    gid: u32,
    /// Unix file permissions.
    #[arg(long, env)]
    #[clap(default_value = "0600")]
    #[clap(value_parser = parse_octal)]
    file_mode: u32,
    /// Unix directory permissions.
    #[arg(long, env)]
    #[clap(default_value = "0700")]
    #[clap(value_parser = parse_octal)]
    dir_mode: u32,
    /// Time without write activity after which a new file is considered complete.
    #[arg(long, env)]
    #[clap(default_value = "10s")]
    #[clap(value_parser = humantime::parse_duration)]
    write_autocommit_after: Duration,
}

enum ConfiguredBackend {
    Indexd(sia_io::indexd::client::Client),
    Renterd(sia_io::renterd::client::Client),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        //.without_time()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(Level::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let mut arguments = Arguments::parse();

    tokio::fs::create_dir_all(&arguments.data_dir).await?;

    let backend = match arguments.backend {
        Backend::Indexd => {
            let mut app_key_arg = arguments
                .indexd
                .indexd_appkey
                .take()
                .ok_or_else(|| anyhow!("Indexd Appkey is not set"))?;
            let app_key = parse_appkey(app_key_arg.as_str())?;
            app_key_arg.zeroize();

            let app_details = AppDetails::builder()
                .id(AppId::from_str(
                    "b9f0bda1b97b7d44ae6369ac830851a115311bb59aa2d848beda6ae95d10adff",
                )?)
                .name("sia_nfs test app")
                .description("for sia_nfs unit & integration tests only")
                .service_url(Url::parse("https://github.com/rrauch/sia_nfs/")?)
                .build();

            let indexd = sia_io::indexd::client::Client::builder()
                .indexd_endpoint(arguments.indexd.indexd_endpoint)
                .app_key(&app_key)
                .app_details(app_details)
                .build()
                .await?;

            ConfiguredBackend::Indexd(indexd)
        }
        Backend::Renterd => {
            let api_password = if arguments
                .renterd
                .renterd_api_password
                .as_ref()
                .map(|s| s.as_str())
                .unwrap_or("")
                .is_empty()
            {
                None
            } else {
                Some(ApiPassword::from(
                    arguments
                        .renterd
                        .renterd_api_password
                        .take()
                        .expect("password to be set"),
                ))
            };

            let renterd = sia_io::renterd::client::Client::builder()
                .api_endpoint(
                    arguments
                        .renterd
                        .renterd_api_endpoint
                        .ok_or_else(|| anyhow!("renterd api endpoint not set"))?,
                )
                .maybe_api_password(api_password)
                .bucket(
                    arguments
                        .renterd
                        .bucket
                        .as_ref()
                        .map(|b| -> Result<_, anyhow::Error> {
                            Ok(BucketName::from_str(b.as_str())?)
                        })
                        .unwrap_or_else(|| Ok(BucketName::default()))?,
                )
                .build()?;
            ConfiguredBackend::Renterd(renterd)
        }
    };

    let cache = if arguments.max_cache_size.as_u64() > 0
        || arguments.max_metadata_cache_size.as_u64() > 0
    {
        let path = arguments
            .cache_dir
            .unwrap_or_else(|| arguments.data_dir.clone());

        let maybe_metadata_cache = if arguments.max_metadata_cache_size.as_u64() > 0 {
            Some(
                FoyerMetadataCache::builder()
                    .max_disk_space(arguments.max_metadata_cache_size.as_u64())
                    .disk_path(path.join("metadata_cache"))
                    .build()
                    .await?,
            )
        } else {
            None
        };

        let maybe_chunk_cache = if arguments.max_cache_size.as_u64() > 0 {
            Some(
                FoyerChunkCache::builder()
                    .max_disk_space(arguments.max_cache_size.as_u64())
                    .disk_path(path.join("chunk_cache"))
                    .build()
                    .await?,
            )
        } else {
            None
        };

        sia_io::cache::Cache::builder()
            .maybe_metadata_l2_cache(maybe_metadata_cache)
            .maybe_chunk_l2_cache(maybe_chunk_cache)
            .build()
    } else {
        sia_io::cache::Cache::default()
    };

    let sia_builder = sia_io::Client::builder().cache(cache);
    let sia = match backend {
        ConfiguredBackend::Indexd(indexd) => sia_builder.backend(indexd).build().await?,
        ConfiguredBackend::Renterd(renterd) => sia_builder.backend(renterd).build().await?,
    };

    let sia_nfs = SiaNfs::new(
        sia,
        arguments.vfs_id.as_str(),
        false,
        &arguments.data_dir,
        &arguments.listen_address,
        arguments.uid,
        arguments.gid,
        arguments.file_mode,
        arguments.dir_mode,
        arguments.write_autocommit_after,
    )
    .await?;

    let run_fut = sia_nfs.run();

    let mut sigint = signal(SignalKind::interrupt()).unwrap();
    let mut sigterm = signal(SignalKind::terminate()).unwrap();

    let span = tracing::trace_span!("main");

    async move {
        tokio::select! {
            _ = sigint.recv() => {
                tracing::info!("SIGINT received, shutting down")
            }
            _ = sigterm.recv() => {
                tracing::info!("SIGTERM received, shutting down")
            }
            res = run_fut => {
                match res {
                    Ok(()) => tracing::info!("run finished, shutting down"),
                    Err(err) => {
                        return Err(err);
                    }
                }
            }
        }
        Ok(())
    }
    .instrument(span)
    .await
}

fn parse_appkey(hex: &str) -> Result<AppKey, anyhow::Error> {
    let bytes = Hex::decode_to_vec(hex, None)?;
    Ok(AppKey::import(
        bytes.try_into().map_err(|_| anyhow!("invalid AppKey"))?,
    ))
}

fn parse_octal(src: &str) -> Result<u32, ParseIntError> {
    u32::from_str_radix(src, 8)
}
