![JanusFS](logo.svg)

A content-addressed storage system with local-first writes, backed by the Sia decentralized storage network.

## Description

JanusFS stores your data on the network but keeps writes fast by applying them locally first. It syncs in the
background, caches aggressively, and avoids storing the same data twice.

If you know Git, the model will feel familiar: every change creates a new addressable state called a **commit**. Each
commit has an ID you can point a branch or tag at. That gives you branching and tagging for free.

## How it works

- **Local-first writes.** You write locally. Writes finish at local disk speed, even offline.
- **Periodic sync.** JanusFS pushes and pulls changes in the background to stay in sync with remote storage.
- **Local cache.** Cached data is read locally, so you skip the network when you don't need it.
- **Chunked storage.** Data is split into chunks, so you can read or write part of a file without fetching the whole
  thing.
- **Content addressing.** Each chunk is identified by its content, so identical chunks are stored only once. You get
  deduplication automatically.

Everything runs in userspace. There's no kernel module or FUSE dependency to manage.

## The Git-like model

Every change produces a new addressable state (a commit). This gives you two things without extra work:

- **Branches** - mountable read-write or read-only.
- **Tags (snapshots)** - mountable read-only.

You create and delete both through the CLI.

## Storage backends

Pick a backend with the `--backend` flag:

- **`indexd`** (default) - connects to the Sia network through an indexer. You authorize it against your indexer
  account.
- **`renterd`** - connects through your own `renterd` instance and a chosen bucket.

## Docker

Run JanusFS in a container. Build the image yourself, or pull the pre-built one.

### Build locally

```bash
git clone https://github.com/rrauch/janus-fs.git
cd janus-fs
docker build ./ -t janus-fs
```

### Pull from GitHub Container Registry

```bash
docker pull ghcr.io/rrauch/janus-fs:latest
```

## Using the CLI

The `janus-fs` command handles setup, management, and serving. The sections below walk through it.

### Quick start (indexd backend)

**1. Get an indexd app key.**

Run the authorize flow. It connects JanusFS to your indexer account and gives you an app key:

```bash
janus-fs tools indexd authorize
```

**2. Check your account status.**

```bash
janus-fs -k <INDEXD_APPKEY> tools indexd status
```

Wait until the account status shows **ready** before continuing. This can take a moment.

**3. Create a storage volume.**

```bash
janus-fs -d <DATA_DIR> -k <INDEXD_APPKEY> volume create --description "my first volume"
```

Replace `<DATA_DIR>` with a directory of your choice for persistent local data. JanusFS creates it if it doesn't exist.
This command prints a volume ID (`volume_id`) that you'll use below.

**4. Serve it over NFS.**

```bash
janus-fs -d <DATA_DIR> -k <INDEXD_APPKEY> serve nfs <volume_id>
```

By default this listens on `localhost:12000`. Mount it with your system's NFS client to start reading and writing.

### Command reference

All commands share the global storage options: `--data-dir`, `--backend`, and cache settings.
See [Global options](#global-options) below.

#### Discover volumes, branches, and tags

Scan the backend to see what it holds. This lists every volume, along with its known branches and tags and their commit
IDs.

```bash
janus-fs -d <DATA_DIR> scan
```

Use the commit IDs from this output when you create a branch or tag.

#### Manage volumes

```bash
# Create a volume (prints a new volume_id)
janus-fs -d <DATA_DIR> volume create --description "notes"

# Delete a volume permanently (this cannot be undone)
janus-fs -d <DATA_DIR> volume delete <volume_id>
```

#### Manage branches

A branch points at a commit and can be served read-write or read-only. Get commit IDs by running `scan` (see
[Discover volumes, branches, and tags](#discover-volumes-branches-and-tags)).

```bash
# Create a branch from a commit
janus-fs -d <DATA_DIR> branch create <name> <volume_id> <commit_id> --description "feature work"

# Delete a branch
janus-fs -d <DATA_DIR> branch delete <name> <volume_id>
```

#### Manage tags

A tag is a read-only snapshot of a commit. Get commit IDs by running `scan` (see
[Discover volumes, branches, and tags](#discover-volumes-branches-and-tags)).

```bash
# Create a tag from a commit
janus-fs -d <DATA_DIR> tag create <name> <volume_id> <commit_id> --description "backup 2026-12-17"

# Delete a tag
janus-fs -d <DATA_DIR> tag delete <name> <volume_id>
```

#### Serve over NFS

```bash
janus-fs -d <DATA_DIR> serve nfs <volume_id> [OPTIONS]
```

Serve a branch or a tag (not both):

- `--branch <name>` - serve a branch.
- `--tag <name>` - serve a tag (always read-only).
- `--read-only` - serve read-only.

Other NFS options:

| Option                     | Default           | Description                                            |
|----------------------------|-------------------|--------------------------------------------------------|
| `--listen-address`, `-l`   | `localhost:12000` | Host and port to listen on.                            |
| `--uid`                    | `1000`            | UID for files and directories.                         |
| `--gid`                    | `1000`            | GID for files and directories.                         |
| `--file-mode`              | `0600`            | Unix file permissions (octal).                         |
| `--dir-mode`               | `0700`            | Unix directory permissions (octal).                    |
| `--write-autocommit-after` | `10s`             | Idle time after which a file write counts as complete. |

### Global options

These apply to every command and can also be set through environment variables.

#### Storage

| Option             | Description                                                            |
|--------------------|------------------------------------------------------------------------|
| `--data-dir`, `-d` | Directory for persistent local data. Created if missing. **Required.** |
| `--backend`        | `indexd` (default) or `renterd`.                                       |

#### indexd backend

| Option                    | Default               | Description                 |
|---------------------------|-----------------------|-----------------------------|
| `--indexd-endpoint`, `-i` | `https://sia.storage` | indexd API endpoint URL.    |
| `--indexd-appkey`, `-k`   | -                     | App key for the indexd API. |

#### renterd backend

| Option                         | Description               |
|--------------------------------|---------------------------|
| `--renterd-api-endpoint`, `-e` | renterd API endpoint URL. |
| `--renterd-api-password`, `-s` | renterd API password.     |
| `--bucket`, `-b`               | Bucket to use.            |

#### Cache

| Option                            | Default                    | Description                                   |
|-----------------------------------|----------------------------|-----------------------------------------------|
| `--cache-dir`, `-c`               | falls back to `--data-dir` | Directory for cache data. Created if missing. |
| `--max-cache-size`, `-m`          | `2 GiB`                    | Content cache size. Set to `0` to disable.    |
| `--max-metadata-cache-size`, `-n` | `256 MiB`                  | Metadata cache size. Set to `0` to disable.   |

All options accept environment variables. For example, set `DATA_DIR` instead of passing `--data-dir` on every call.

## Using it as a library

If you want to use JanusFS directly in your own Rust code, the `janus-vfs` crate exposes the same storage system the CLI
is built on.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as
defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.

## Acknowledgements

This project has been made possible by the [Sia Foundation's Grant program](https://sia.tech/grants).