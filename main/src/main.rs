use anyhow::{anyhow, bail};
use bytesize::ByteSize;
use clap::{Args, Parser, Subcommand, ValueEnum};
use colored::Colorize;
use ct_codecs::{Decoder, Hex};
use foyer_cache::{FoyerChunkCache, FoyerMetadataCache};
use sia_io::indexd::client::AppKey;
use sia_io::indexd::{AppDetails, AppId};
use sia_io::renterd::BucketName;
use sia_io::renterd::client::ApiPassword;
use sia_nfs::SiaNfs;
use sia_vfs::vfs::config::Config;
use sia_vfs::vfs::{BranchName, Head, TagName, Vfs, VfsId};
use std::num::ParseIntError;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::signal::unix::{SignalKind, signal};
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
    #[arg(long, short = 'i', env, value_hint = clap::ValueHint::Url, default_value = "https://sia.storage")]
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

#[derive(Debug, Args)]
struct BackendArgs {
    /// Backend to use.
    #[arg(long, env, value_enum, default_value_t = Backend::Indexd)]
    backend: Backend,

    #[command(flatten)]
    indexd: IndexdArgs,

    #[command(flatten)]
    renterd: RenterdArgs,
}

#[derive(Debug, Args)]
struct CacheArgs {
    /// Optional directory to store the content cache in. Defaults to `DATA_DIR` if not set. Will be created if it doesn't exist.
    #[arg(long, short = 'c', env)]
    cache_dir: Option<PathBuf>,
    /// Maximum size of content cache. Set to `0` to disable.
    #[arg(long, short = 'm', env, default_value = "2 GiB")]
    max_cache_size: ByteSize,
    /// Maximum size of metadata cache. Set to `0` to disable.
    #[arg(long, short = 'n', env, default_value = "256 MiB")]
    max_metadata_cache_size: ByteSize,
}

#[derive(Debug, Parser)]
#[command(version)]
/// Exports Sia stored data via NFS.
struct Arguments {
    /// Directory to store persistent data in. Will be created if it doesn't exist.
    #[arg(long, short = 'd', env, value_hint = clap::ValueHint::DirPath)]
    data_dir: PathBuf,

    #[command(flatten)]
    backend: BackendArgs,

    #[command(flatten)]
    cache: CacheArgs,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve a VFS via NFS.
    Serve(ServeArgs),
    /// Scan backend for available VFSs
    Scan,
    /// Filesystem management commands.
    Fs {
        #[command(subcommand)]
        command: FsCommand,
    },
}

#[derive(Debug, Subcommand)]
enum FsCommand {
    /// Create a new VFS.
    Create(FsCreateArgs),
    /// Delete an existing VFS (dangerous).
    Delete(FsDeleteArgs),
}

#[derive(Debug, Args)]
struct FsCreateArgs {
    /// Optional description of new file system.
    #[arg(long)]
    description: Option<String>,
}

#[derive(Debug, Args)]
struct FsDeleteArgs {
    /// ID of file system to permanently delete.
    vfs_id: String,
}

#[derive(Debug, Args)]
struct ServeArgs {
    /// Id of file system to serve.
    vfs_id: String,

    /// Branch name to serve.
    #[arg(long, conflicts_with = "tag")]
    branch: Option<String>,

    /// Tag name to serve.
    #[arg(long, conflicts_with = "branch")]
    tag: Option<String>,

    /// Serve read-only.
    #[arg(long)]
    read_only: bool,

    /// Host and port to listen on.
    #[arg(long, short = 'l', env, default_value = "localhost:12000")]
    listen_address: String,
    /// UID of files and directories
    #[arg(long, env = "INODE_UID", default_value = "1000")]
    uid: u32,
    /// GID of files and directories
    #[arg(long, env = "INODE_GID", default_value = "1000")]
    gid: u32,
    /// Unix file permissions.
    #[arg(long, env, default_value = "0600", value_parser = parse_octal)]
    file_mode: u32,
    /// Unix directory permissions.
    #[arg(long, env, default_value = "0700", value_parser = parse_octal)]
    dir_mode: u32,
    /// Time without write activity after which a new file is considered complete.
    #[arg(long, env, default_value = "10s", value_parser = humantime::parse_duration)]
    write_autocommit_after: Duration,
}

enum ConfiguredBackend {
    Indexd(sia_io::indexd::client::Client),
    Renterd(sia_io::renterd::client::Client),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(Level::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let mut arguments = Arguments::parse();

    tokio::fs::create_dir_all(&arguments.data_dir).await?;

    let backend = build_backend(&mut arguments.backend).await?;
    let cache = build_cache(&arguments.cache, &arguments.data_dir).await?;

    let sia_builder = sia_io::Client::builder().cache(cache);
    let sia = match backend {
        ConfiguredBackend::Indexd(indexd) => sia_builder.backend(indexd).build().await?,
        ConfiguredBackend::Renterd(renterd) => sia_builder.backend(renterd).build().await?,
    };

    match arguments.command {
        Command::Serve(args) => serve(sia, &arguments.data_dir, args).await?,
        Command::Scan => scan(sia).await?,
        Command::Fs {
            command: FsCommand::Create(args),
        } => create_fs(sia, args).await?,
        Command::Fs {
            command: FsCommand::Delete(args),
        } => delete_fs(sia, args).await?,
    }

    Ok(())
}

async fn build_backend(args: &mut BackendArgs) -> anyhow::Result<ConfiguredBackend> {
    match args.backend {
        Backend::Indexd => {
            let mut app_key_arg = args
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
                .indexd_endpoint(args.indexd.indexd_endpoint.clone())
                .app_key(&app_key)
                .app_details(app_details)
                .build()
                .await?;

            Ok(ConfiguredBackend::Indexd(indexd))
        }
        Backend::Renterd => {
            let api_password = if args
                .renterd
                .renterd_api_password
                .as_deref()
                .unwrap_or("")
                .is_empty()
            {
                None
            } else {
                Some(ApiPassword::from(
                    args.renterd
                        .renterd_api_password
                        .take()
                        .expect("password to be set"),
                ))
            };

            let renterd = sia_io::renterd::client::Client::builder()
                .api_endpoint(
                    args.renterd
                        .renterd_api_endpoint
                        .clone()
                        .ok_or_else(|| anyhow!("renterd api endpoint not set"))?,
                )
                .maybe_api_password(api_password)
                .bucket(
                    args.renterd
                        .bucket
                        .as_ref()
                        .map(|b| -> Result<_, anyhow::Error> {
                            Ok(BucketName::from_str(b.as_str())?)
                        })
                        .unwrap_or_else(|| Ok(BucketName::default()))?,
                )
                .build()?;
            Ok(ConfiguredBackend::Renterd(renterd))
        }
    }
}

async fn build_cache(args: &CacheArgs, data_dir: &PathBuf) -> anyhow::Result<sia_io::cache::Cache> {
    if args.max_cache_size.as_u64() == 0 && args.max_metadata_cache_size.as_u64() == 0 {
        return Ok(sia_io::cache::Cache::default());
    }

    let path = args.cache_dir.clone().unwrap_or_else(|| data_dir.clone());

    let maybe_metadata_cache = if args.max_metadata_cache_size.as_u64() > 0 {
        Some(
            FoyerMetadataCache::builder()
                .max_disk_space(args.max_metadata_cache_size.as_u64())
                .disk_path(path.join("metadata_cache"))
                .build()
                .await?,
        )
    } else {
        None
    };

    let maybe_chunk_cache = if args.max_cache_size.as_u64() > 0 {
        Some(
            FoyerChunkCache::builder()
                .max_disk_space(args.max_cache_size.as_u64())
                .disk_path(path.join("chunk_cache"))
                .build()
                .await?,
        )
    } else {
        None
    };

    Ok(sia_io::cache::Cache::builder()
        .maybe_metadata_l2_cache(maybe_metadata_cache)
        .maybe_chunk_l2_cache(maybe_chunk_cache)
        .build())
}

async fn serve(sia: sia_io::Client, data_dir: &PathBuf, args: ServeArgs) -> anyhow::Result<()> {
    let head = match (args.branch, args.tag) {
        (Some(branch), None) => Some(Head::from(BranchName::from_str(branch.as_str())?)),
        (None, Some(tag)) => Some(Head::from(TagName::from_str(tag.as_str())?)),
        (None, None) => None,
        (Some(_), Some(_)) => bail!("invalid configuration, branch and tag are mutually exclusive"),
    };

    let sia_nfs = SiaNfs::new(
        sia,
        args.vfs_id.as_str(),
        head,
        args.read_only,
        data_dir,
        &args.listen_address,
        args.uid,
        args.gid,
        args.file_mode,
        args.dir_mode,
        args.write_autocommit_after,
    )
    .await?;

    let run_fut = sia_nfs.run();

    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;

    let span = tracing::trace_span!("serve");

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
                    Err(err) => return Err(err),
                }
            }
        }
        Ok(())
    }
    .instrument(span)
    .await
}

async fn scan(sia: sia_io::Client) -> anyhow::Result<()> {
    action_preview("Scan Backend", None, &sia);

    let configs = Vfs::scan(&sia).await?;

    const INDENT: &str = "    ";

    println!();
    println!("{} ✅", "Backend Scan Complete".green().bold());
    println!();
    println!("{}", "SCAN RESULT".cyan().bold());

    println!("{}{} {}", INDENT, "TOTAL FOUND:".bold(), configs.len());
    println!();

    for config in configs {
        println!(
            "{}---------------------------------------------------------------------------",
            INDENT
        );
        print_config(&config, INDENT);
    }

    Ok(())
}

fn print_config(config: &Config, indent: &str) {
    println!("{}{}", indent, "VFS ID:".bold());
    println!("{}{}", indent, config.vfs_id());
    println!();
    if let Some(description) = config.description() {
        println!("{}{}", indent, "DESCRIPTION:".bold());
        println!("{}{}", indent, description);
        println!();
    }
    println!("{}{}", indent, "LAST MODIFIED:".bold());
    println!("{}{}", indent, config.last_modified());
    println!();

    for (head, entry) in config.heads().iter() {
        let indent = format!("{}   ", indent);
        let indent = indent.as_str();

        match head {
            Head::Branch(name) => {
                println!("{}{}", indent, "BRANCH:".bold());
                println!("{}{} ({})", indent, name, entry.commit_id());
            }
            Head::Tag(name) => {
                println!("{}{}", indent, "TAG:".bold());
                println!("{}{} ({})", indent, name, entry.commit_id());
            }
        }
        println!();
        if let Some(description) = entry.description() {
            println!("{}{}", indent, "DESCRIPTION:".bold());
            println!("{}{}", indent, description);
            println!();
        }
        println!();
    }
}

async fn create_fs(sia: sia_io::Client, args: FsCreateArgs) -> anyhow::Result<()> {
    action_preview(
        "Create New File System",
        Some("Create a new, empty file system on the selected backend."),
        &sia,
    );

    if !ask_proceed().await {
        println!(" ❌ {}", "Aborting".red());
        println!();
        return Ok(());
    }

    let vfs_id = Vfs::create_new(args.description, &sia).await?;

    const INDENT: &str = "    ";

    println!();
    println!("{} ✅", "File System Creation Complete".green().bold());
    println!();

    let configs = Vfs::scan(&sia).await?;
    let config = configs
        .into_iter()
        .find(|c| c.vfs_id() == &vfs_id)
        .ok_or_else(|| anyhow!("newly created VFS with id '{}' not found", &vfs_id))?;

    print_config(&config, INDENT);
    Ok(())
}

async fn delete_fs(sia: sia_io::Client, args: FsDeleteArgs) -> anyhow::Result<()> {
    let vfs_id = VfsId::from_str(args.vfs_id.as_str()).map_err(|_| anyhow!("invalid vfs id"))?;
    action_preview(
        "Delete File System",
        Some("File System will be permanently deleted! All data will be erased!"),
        &sia,
    );

    let configs = Vfs::scan(&sia).await?;

    if let Some(config) = configs.into_iter().find(|c| c.vfs_id() == &vfs_id) {
        print_config(&config, "    ");
        println!();
    } else {
        println!();
        println!(
            "{} {}",
            "No File System Config found with ID".yellow().bold(),
            &vfs_id
        );
        println!(
            "{}", "Abandoned Objects associated with the File System might still be present and can be deleted".yellow(),
        );
        println!();
    }

    if !ask_proceed().await {
        println!(" ❌ {}", "Aborting".red());
        println!();
        return Ok(());
    }

    let deleted_objects = Vfs::delete_fs(&vfs_id, &sia).await?;
    if deleted_objects == 0 {
        println!();
        println!(" ❌ {}", "No objects found to delete".red().bold());
        println!();
        bail!("File System deletion failed");
    }
    println!();
    println!("{} ✅", "File System Deletion Complete".green().bold());
    println!("{} {}", "Objects deleted:".bold(), deleted_objects);
    println!();

    Ok(())
}

fn action_preview(action: impl AsRef<str>, details: Option<&str>, sia: &sia_io::Client) {
    println!("{} {}", "ACTION:".bold(), action.as_ref().cyan().bold());

    match sia.backend() {
        sia_io::Backend::Indexd(indexd) => {
            println!("{} {}", "BACKEND:".bold(), "indexd");
            println!("{} {}", "ENDPOINT:".bold(), indexd.endpoint());
        }
        sia_io::Backend::Renterd(renterd) => {
            println!("{} {}", "BACKEND:".bold(), "renterd");
            println!("{} {}", "ENDPOINT:".bold(), renterd.endpoint());
            println!("{} {}", "BUCKET:".bold(), renterd.bucket());
        }
    }

    println!();
    if let Some(details) = details {
        println!("{}", details);
        println!();
    }
}

async fn ask_confirmation(question: &str) -> bool {
    let mut reader = BufReader::new(tokio::io::stdin());
    let mut line = String::new();
    loop {
        println!("{}", question);
        match reader.read_line(&mut line).await {
            Ok(_) => {
                let resp = line.trim();
                if resp.eq_ignore_ascii_case("y") || resp.eq_ignore_ascii_case("yes") {
                    return true;
                }
                if resp.eq_ignore_ascii_case("n") || resp.eq_ignore_ascii_case("no") {
                    return false;
                }
                println!("{}", "Only y/n accepted, please try again".red());
            }
            Err(_) => return false,
        }
        line.clear();
    }
}

async fn ask_proceed() -> bool {
    ask_confirmation("Do you want to proceed (y/n)?").await
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
