use anyhow::{anyhow, bail};
use bytesize::ByteSize;
use clap::{Args, Parser, Subcommand, ValueEnum};
use colored::Colorize;
use ct_codecs::{Decoder, Encoder, Hex};
use foyer_cache::{FoyerChunkCache, FoyerMetadataCache};
use janus_fs::JanusNfs;
use janus_io::RemoteStorage;
use janus_io::confidential::{Confidential, NewSecretExt, RevealExt};
use janus_io::indexd::client::AppKey;
use janus_io::indexd::{AppDetails, AppId};
use janus_io::renterd::BucketName;
use janus_io::renterd::client::ApiPassword;
use janus_vfs::vfs::commit::CommitId;
use janus_vfs::vfs::config::Config;
use janus_vfs::vfs::{BranchName, Head, TagName, Vfs, VfsId};
use std::num::{NonZeroU32, ParseIntError};
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
/// Exports a JanusFS Volume via NFS.
struct Arguments {
    #[command(subcommand)]
    command: Command,

    #[command(flatten)]
    remote_storage: RemoteStorageArgs,
}

#[derive(Debug, Args)]
struct RemoteStorageArgs {
    /// Directory to store persistent data in. Will be created if it doesn't exist.
    #[arg(long, short = 'd', env, value_hint = clap::ValueHint::DirPath)]
    data_dir: PathBuf,

    #[command(flatten)]
    backend: BackendArgs,

    #[command(flatten)]
    cache: CacheArgs,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve a Volume.
    Serve {
        #[command(subcommand)]
        command: ServeCommand,
    },
    /// Scan backend for available Volumes
    Scan,
    /// Volume management commands.
    Volume {
        #[command(subcommand)]
        command: VolumeCommand,
    },
    /// Branch management commands.
    Branch {
        #[command(subcommand)]
        command: BranchCommand,
    },
    /// Tag management commands.
    Tag {
        #[command(subcommand)]
        command: TagCommand,
    },
    /// Additional Tools
    Tools {
        #[command(subcommand)]
        command: ToolsCommand,
    },
}

#[derive(Debug, Subcommand)]
enum ServeCommand {
    /// Serve a Volume via NFS.
    Nfs(NfsArgs),
}

#[derive(Debug, Subcommand)]
enum VolumeCommand {
    /// Create a new Volume.
    Create(VolumeCreateArgs),
    /// Delete an existing Volume (dangerous).
    Delete(VolumeDeleteArgs),
}

#[derive(Debug, Args)]
struct VolumeCreateArgs {
    /// Optional description of new Volume.
    #[arg(long)]
    description: Option<String>,
    /// Optional Chunk Size.
    #[arg(long)]
    chunk_size: Option<ByteSize>,
}

#[derive(Debug, Args)]
struct VolumeDeleteArgs {
    /// ID of Volume to permanently delete.
    volume_id: String,
}

#[derive(Debug, Subcommand)]
enum BranchCommand {
    /// Create a new Branch.
    Create(BranchCreateArgs),
    /// Delete an existing Branch (dangerous).
    Delete(BranchDeleteArgs),
}

#[derive(Debug, Args)]
struct BranchCreateArgs {
    /// Name of new branch
    name: String,
    /// Optional description of new branch.
    #[arg(long)]
    description: Option<String>,
    /// ID of Volume.
    volume_id: String,
    /// Commit ID associated with new branch.
    commit: String,
}

#[derive(Debug, Args)]
struct BranchDeleteArgs {
    /// Name of branch to delete.
    name: String,
    /// ID of Volume.
    volume_id: String,
}

#[derive(Debug, Subcommand)]
enum TagCommand {
    /// Create a new Tag.
    Create(TagCreateArgs),
    /// Delete an existing Tag (dangerous).
    Delete(TagDeleteArgs),
}

#[derive(Debug, Args)]
struct TagCreateArgs {
    /// Name of new tag.
    name: String,
    /// Optional description of new tag.
    #[arg(long)]
    description: Option<String>,
    /// ID of Volume.
    volume_id: String,
    /// Commit ID associated with new tag.
    commit: String,
}

#[derive(Debug, Args)]
struct TagDeleteArgs {
    /// Name of tag to delete.
    name: String,
    /// ID of Volume.
    volume_id: String,
}

#[derive(Debug, Args)]
struct NfsArgs {
    /// ID of Volume to serve.
    volume_id: String,

    /// Branch name to serve.
    #[arg(long, conflicts_with = "tag")]
    branch: Option<String>,

    /// Tag name to serve.
    #[arg(long, conflicts_with = "branch")]
    tag: Option<String>,

    /// Serve read-only.
    #[arg(long)]
    read_only: bool,

    /// Automatic sync frequency. In seconds.
    #[arg(long, short = 'f', env, value_parser = parse_duration_secs)]
    sync_frequency: Option<Duration>,

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
}

#[derive(Debug, Subcommand)]
enum ToolsCommand {
    /// Indexd-related tools.
    Indexd {
        #[command(subcommand)]
        command: IndexdToolsCommand,
    },
}

#[derive(Debug, Subcommand)]
enum IndexdToolsCommand {
    /// Show account status & details.
    Status,
    /// Connect JanusFS to your indexer account.
    Authorize,
}

enum ConfiguredBackend {
    Indexd(janus_io::indexd::client::Client),
    Renterd(janus_io::renterd::client::Client),
}

async fn remote_storage(arguments: &mut RemoteStorageArgs) -> anyhow::Result<RemoteStorage> {
    tokio::fs::create_dir_all(&arguments.data_dir).await?;

    let backend = build_backend(&mut arguments.backend).await?;
    let cache = build_cache(&arguments.cache, &arguments.data_dir).await?;

    let remote_storage_builder = RemoteStorage::builder().cache(cache);
    Ok(match backend {
        ConfiguredBackend::Indexd(indexd) => remote_storage_builder.backend(indexd).build().await?,
        ConfiguredBackend::Renterd(renterd) => {
            remote_storage_builder.backend(renterd).build().await?
        }
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let arguments = Arguments::parse();
    let mut remote_storage_args = arguments.remote_storage;

    match arguments.command {
        Command::Serve {
            command: ServeCommand::Nfs(args),
        } => {
            serve_nfs(
                remote_storage(&mut remote_storage_args).await?,
                &remote_storage_args.data_dir,
                args,
            )
            .await?
        }
        Command::Scan => scan(remote_storage(&mut remote_storage_args).await?).await?,
        Command::Volume {
            command: VolumeCommand::Create(args),
        } => create_volume(remote_storage(&mut remote_storage_args).await?, args).await?,
        Command::Volume {
            command: VolumeCommand::Delete(args),
        } => delete_volume(remote_storage(&mut remote_storage_args).await?, args).await?,
        Command::Branch {
            command: BranchCommand::Create(args),
        } => create_branch(remote_storage(&mut remote_storage_args).await?, args).await?,
        Command::Branch {
            command: BranchCommand::Delete(args),
        } => delete_branch(remote_storage(&mut remote_storage_args).await?, args).await?,
        Command::Tag {
            command: TagCommand::Create(args),
        } => create_tag(remote_storage(&mut remote_storage_args).await?, args).await?,
        Command::Tag {
            command: TagCommand::Delete(args),
        } => delete_tag(remote_storage(&mut remote_storage_args).await?, args).await?,
        Command::Tools {
            command:
                ToolsCommand::Indexd {
                    command: IndexdToolsCommand::Status,
                },
        } => indexd_status(&mut remote_storage_args).await?,
        Command::Tools {
            command:
                ToolsCommand::Indexd {
                    command: IndexdToolsCommand::Authorize,
                },
        } => indexd_auth(&remote_storage_args).await?,
    }

    Ok(())
}

async fn indexd_auth(arguments: &RemoteStorageArgs) -> anyhow::Result<()> {
    action_preview("Connect to indexer", None, None);
    let endpoint = arguments.backend.indexd.indexd_endpoint.clone();
    let handle =
        janus_io::indexd::client::Client::acquire_authorization(endpoint, app_details()?).await?;

    println!();
    println!("Open the following link in your browser and follow the authorization flow:");
    println!();
    println!("  {}", handle.url());
    println!();
    println!("Then return to the console to complete the process.");

    let handle = handle.await_authorization().await?;

    let mnemonic;

    loop {
        println!();
        println!("To finalize the authorization process, please enter your mnemonic phrase.");
        println!("Make sure its a 12-word English bip39 compatible phrase:");
        let mnemonic1 = tokio::task::spawn_blocking(|| read_mnemonic()).await?;

        println!("Please re-enter your mnemonic phrase:");
        let mnemonic2 = tokio::task::spawn_blocking(|| read_mnemonic()).await?;

        if mnemonic1.reveal() == mnemonic2.reveal() {
            mnemonic = Some(mnemonic1);
            break;
        }

        println!();
        println!(
            "{} Please try again.",
            "Mnemonic phrases do NOT match".red()
        );
    }

    let mnemonic = mnemonic.ok_or_else(|| anyhow!("mnemonic phrase is missing"))?;

    let app_key = handle.finalize(&mnemonic).await?;

    let hex_key = Hex::encode_to_string(app_key.reveal().export())?;

    println!();
    println!("{} ✅", "Indexer Authorization succeeded".green().bold());
    println!();

    println!(
        "{} The following key is your INDEXD_APPKEY: ",
        "IMPORTANT!".bold()
    );
    println!();
    println!("    {}", hex_key);
    println!();
    println!("{}", "KEEP IT PRIVATE".bold());
    println!();

    Ok(())
}

fn read_mnemonic() -> Confidential<String> {
    loop {
        match try_read_mnemonic() {
            Ok(mnemonic) => return mnemonic,
            Err(err) => {
                eprintln!("{} {}", "Error reading your mnemonic phrase:".red(), err);
                eprintln!("Please try again");
            }
        }
    }
}

fn try_read_mnemonic() -> anyhow::Result<Confidential<String>> {
    let mnemonic = rpassword::prompt_password("Enter mnemonic phrase: ")?.confidential();
    if !is_valid_word_count(mnemonic.reveal()) {
        bail!("mnemonic has an invalid word count. Word count must be 12");
    }
    Ok(mnemonic)
}

fn is_valid_word_count(phrase: &str) -> bool {
    let count = phrase.split_whitespace().count();
    matches!(count, 12)
}

async fn indexd_status(arguments: &mut RemoteStorageArgs) -> anyhow::Result<()> {
    let indexd = match build_backend(&mut arguments.backend).await? {
        ConfiguredBackend::Indexd(indexd) => indexd,
        _ => bail!("backend needs to be indexd"),
    };

    let backend = janus_io::Backend::from(indexd);
    action_preview("Display Account details", None, Some(&backend));

    let indexd = if let janus_io::Backend::Indexd(indexd) = backend {
        indexd
    } else {
        unreachable!()
    };

    let account = indexd.account().await?;

    const INDENT: &str = "    ";

    println!();
    println!("{} ✅", "Account Details retrieved".green().bold());
    println!();

    println!("{}{}", INDENT, "ACCOUNT KEY:".bold());
    println!("{}{}", INDENT, account.account_key);
    println!();

    println!("{}{}", INDENT, "ACCOUNT STATUS:".bold());
    if account.ready {
        println!("{}{}", INDENT, "ready".green());
    } else {
        println!("{}{}", INDENT, "not ready".yellow());
    }
    println!();

    println!("{}{}", INDENT, "LAST USED:".bold());
    println!("{}{}", INDENT, account.last_used);
    println!();

    println!("{}{}", INDENT, "PINNED DATA:".bold());
    println!("{}{}", INDENT, ByteSize::b(account.pinned_data));
    println!();

    println!("{}{}", INDENT, "MAX PINNED DATA:".bold());
    println!("{}{}", INDENT, ByteSize::b(account.max_pinned_data));
    println!();

    println!("{}{}", INDENT, "PINNED SIZE:".bold());
    println!("{}{}", INDENT, ByteSize::b(account.pinned_size));
    println!();

    println!("{}{}", INDENT, "REMAINING STORAGE:".bold());
    println!("{}{}", INDENT, ByteSize::b(account.remaining_storage));
    println!();

    Ok(())
}

fn app_details() -> anyhow::Result<AppDetails> {
    Ok(AppDetails::builder()
        .id(AppId::from_str(
            "b9f0bda1b97b7d44ae6369ac830851a115311bb59aa2d848beda6ae95d10adff",
        )?)
        .name("JanusFS")
        .description("Local-first storage, synced to remote backends")
        .service_url(Url::parse("https://github.com/rrauch/janus-fs/")?)
        .build())
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

            let app_details = app_details()?;

            let indexd = janus_io::indexd::client::Client::builder()
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

            let renterd = janus_io::renterd::client::Client::builder()
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

async fn build_cache(
    args: &CacheArgs,
    data_dir: &PathBuf,
) -> anyhow::Result<janus_io::cache::Cache> {
    if args.max_cache_size.as_u64() == 0 && args.max_metadata_cache_size.as_u64() == 0 {
        return Ok(janus_io::cache::Cache::default());
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

    Ok(janus_io::cache::Cache::builder()
        .maybe_metadata_l2_cache(maybe_metadata_cache)
        .maybe_chunk_l2_cache(maybe_chunk_cache)
        .build())
}

async fn serve_nfs(
    remote_storage: janus_io::RemoteStorage,
    data_dir: &PathBuf,
    args: NfsArgs,
) -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(Level::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let head = match (args.branch, args.tag) {
        (Some(branch), None) => Some(Head::from(BranchName::from_str(branch.as_str())?)),
        (None, Some(tag)) => Some(Head::from(TagName::from_str(tag.as_str())?)),
        (None, None) => None,
        (Some(_), Some(_)) => bail!("invalid configuration, branch and tag are mutually exclusive"),
    };

    let janus_nfs = JanusNfs::new(
        remote_storage,
        args.volume_id.as_str(),
        head,
        args.read_only,
        args.sync_frequency,
        data_dir,
        &args.listen_address,
        args.uid,
        args.gid,
        args.file_mode,
        args.dir_mode,
    )
    .await?;

    let run_fut = janus_nfs.run();

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

async fn scan(remote_storage: janus_io::RemoteStorage) -> anyhow::Result<()> {
    action_preview("Scan Backend", None, Some(remote_storage.backend()));

    let configs = Vfs::scan(&remote_storage).await?;

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
    println!("{}{}", indent, "VOLUME ID:".bold());
    println!("{}{}", indent, config.vfs_id());
    if let Some(description) = config.description() {
        println!("{}{}", indent, description);
        println!();
    }
    println!();
    println!("{}{}", indent, "CHUNK SIZE:".bold());
    println!(
        "{}{}",
        indent,
        ByteSize::b(config.chunk_size().get() as u64)
    );
    println!();
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
        if let Some(description) = entry.description() {
            println!("{}{}", indent, description);
        }
        println!();
        println!();
    }
}

async fn create_volume(
    remote_storage: janus_io::RemoteStorage,
    args: VolumeCreateArgs,
) -> anyhow::Result<()> {
    action_preview(
        "Create New Volume",
        Some("Create a new, empty Volume on the selected backend."),
        Some(remote_storage.backend()),
    );

    if !ask_proceed().await {
        println!(" ❌ {}", "Aborting".red());
        println!();
        return Ok(());
    }

    let chunk_size = match args.chunk_size {
        Some(b) => {
            let b = u32::try_from(b.as_u64())
                .ok()
                .and_then(NonZeroU32::new)
                .ok_or_else(|| anyhow!("invalid chunk size"))?;
            Some(b)
        }
        None => None,
    };

    let vfs_id = Vfs::create_new(args.description, chunk_size, &remote_storage).await?;

    const INDENT: &str = "    ";

    println!();
    println!("{} ✅", "Volume Creation Complete".green().bold());
    println!();

    let configs = Vfs::scan(&remote_storage).await?;
    let config = configs
        .into_iter()
        .find(|c| c.vfs_id() == &vfs_id)
        .ok_or_else(|| anyhow!("newly created Volume with ID '{}' not found", &vfs_id))?;

    print_config(&config, INDENT);
    Ok(())
}

async fn delete_volume(
    remote_storage: janus_io::RemoteStorage,
    args: VolumeDeleteArgs,
) -> anyhow::Result<()> {
    let vfs_id =
        VfsId::from_str(args.volume_id.as_str()).map_err(|_| anyhow!("invalid volume id"))?;
    action_preview(
        "Delete Volume",
        Some("Volume will be permanently deleted! All data will be erased!"),
        Some(remote_storage.backend()),
    );

    let configs = Vfs::scan(&remote_storage).await?;

    if let Some(config) = configs.into_iter().find(|c| c.vfs_id() == &vfs_id) {
        print_config(&config, "    ");
        println!();
    } else {
        println!();
        println!(
            "{} {}",
            "No Volume Config found with ID".yellow().bold(),
            &vfs_id
        );
        println!(
            "{}", "Abandoned Objects associated with the Volume might still be present and can be deleted".yellow(),
        );
        println!();
    }

    if !ask_proceed().await {
        println!(" ❌ {}", "Aborting".red());
        println!();
        return Ok(());
    }

    let deleted_objects = Vfs::delete_fs(&vfs_id, &remote_storage).await?;
    if deleted_objects == 0 {
        println!();
        println!(" ❌ {}", "No objects found to delete".red().bold());
        println!();
        bail!("Volume deletion failed");
    }
    println!();
    println!("{} ✅", "Volume Deletion Complete".green().bold());
    println!("{} {}", "Objects deleted:".bold(), deleted_objects);
    println!();

    Ok(())
}

async fn create_branch(
    remote_storage: janus_io::RemoteStorage,
    args: BranchCreateArgs,
) -> anyhow::Result<()> {
    let branch_name = BranchName::from_str(args.name.as_str())?;
    create_head(
        remote_storage,
        branch_name.into(),
        args.description,
        args.volume_id,
        args.commit,
    )
    .await
}

async fn create_tag(
    remote_storage: janus_io::RemoteStorage,
    args: TagCreateArgs,
) -> anyhow::Result<()> {
    let tag_name = TagName::from_str(args.name.as_str())?;
    create_head(
        remote_storage,
        tag_name.into(),
        args.description,
        args.volume_id,
        args.commit,
    )
    .await
}

async fn create_head(
    remote_storage: janus_io::RemoteStorage,
    head: Head,
    description: Option<String>,
    volume_id: String,
    commit_id: String,
) -> anyhow::Result<()> {
    let vfs_id = VfsId::from_str(volume_id.as_str()).map_err(|_| anyhow!("invalid volume id"))?;
    let commit_id =
        CommitId::from_str(commit_id.as_str()).map_err(|_| anyhow!("invalid commit id"))?;

    let title = match &head {
        Head::Branch(_) => "Create New Branch",
        Head::Tag(_) => "Create New Tag",
    };
    action_preview(title, None, Some(remote_storage.backend()));

    const INDENT: &str = "    ";

    match &head {
        Head::Branch(name) => {
            println!("{}{}", INDENT, "NEW BRANCH NAME:".bold());
            println!("{}{}", INDENT, name);
        }
        Head::Tag(name) => {
            println!("{}{}", INDENT, "NEW TAG NAME:".bold());
            println!("{}{}", INDENT, name);
        }
    }
    println!();
    if let Some(description) = &description {
        println!("{}{}", INDENT, "DESCRIPTION:".bold());
        println!("{}{}", INDENT, description);
        println!();
    }

    println!("{}{}", INDENT, "VOLUME-ID:".bold());
    println!("{}{}", INDENT, &vfs_id);
    println!();

    println!("{}{}", INDENT, "COMMIT-ID:".bold());
    println!("{}{}", INDENT, &commit_id);
    println!();

    if !ask_proceed().await {
        println!(" ❌ {}", "Aborting".red());
        println!();
        return Ok(());
    }

    let config = match head {
        Head::Branch(branch_name) => {
            Vfs::create_branch(
                &vfs_id,
                &remote_storage,
                branch_name,
                description,
                commit_id,
            )
            .await?
        }
        Head::Tag(tag_name) => {
            Vfs::create_tag(&vfs_id, &remote_storage, tag_name, description, commit_id).await?
        }
    };

    println!();
    println!("{} ✅", "Creation Complete".green().bold());
    println!();

    print_config(&config, INDENT);

    Ok(())
}

async fn delete_branch(
    remote_storage: janus_io::RemoteStorage,
    args: BranchDeleteArgs,
) -> anyhow::Result<()> {
    let branch_name = BranchName::from_str(args.name.as_str())?;
    delete_head(remote_storage, branch_name.into(), args.volume_id).await
}

async fn delete_tag(
    remote_storage: janus_io::RemoteStorage,
    args: TagDeleteArgs,
) -> anyhow::Result<()> {
    let tag_name = TagName::from_str(args.name.as_str())?;
    delete_head(remote_storage, tag_name.into(), args.volume_id).await
}

async fn delete_head(
    remote_storage: janus_io::RemoteStorage,
    head: Head,
    volume_id: String,
) -> anyhow::Result<()> {
    let vfs_id = VfsId::from_str(volume_id.as_str()).map_err(|_| anyhow!("invalid volume id"))?;

    let title = match &head {
        Head::Branch(_) => "Delete Branch",
        Head::Tag(_) => "Delete Tag",
    };
    action_preview(
        title,
        Some("Permanently delete the selected branch/tag"),
        Some(remote_storage.backend()),
    );

    const INDENT: &str = "    ";

    match &head {
        Head::Branch(name) => {
            println!("{}{}", INDENT, "BRANCH NAME:".bold());
            println!("{}{}", INDENT, name);
        }
        Head::Tag(name) => {
            println!("{}{}", INDENT, "TAG NAME:".bold());
            println!("{}{}", INDENT, name);
        }
    }
    println!();
    println!("{}{}", INDENT, "VOLUME-ID:".bold());
    println!("{}{}", INDENT, &vfs_id);
    println!();

    if !ask_proceed().await {
        println!(" ❌ {}", "Aborting".red());
        println!();
        return Ok(());
    }

    let config = match head {
        Head::Branch(branch_name) => {
            Vfs::delete_branch(&vfs_id, &remote_storage, branch_name).await?
        }
        Head::Tag(tag_name) => Vfs::delete_tag(&vfs_id, &remote_storage, tag_name).await?,
    };

    println!();
    println!("{} ✅", "Deletion Complete".green().bold());
    println!();

    print_config(&config, INDENT);

    Ok(())
}

fn action_preview(
    action: impl AsRef<str>,
    details: Option<&str>,
    backend: Option<&janus_io::Backend>,
) {
    println!("{} {}", "ACTION:".bold(), action.as_ref().cyan().bold());

    match backend {
        Some(janus_io::Backend::Indexd(indexd)) => {
            println!("{} {}", "BACKEND:".bold(), "indexd");
            println!("{} {}", "ENDPOINT:".bold(), indexd.endpoint());
        }
        Some(janus_io::Backend::Renterd(renterd)) => {
            println!("{} {}", "BACKEND:".bold(), "renterd");
            println!("{} {}", "ENDPOINT:".bold(), renterd.endpoint());
            println!("{} {}", "BUCKET:".bold(), renterd.bucket());
        }
        _ => {}
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

fn parse_duration_secs(src: &str) -> Result<Duration, ParseIntError> {
    u64::from_str(src).map(Duration::from_secs)
}
