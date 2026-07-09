# Multi-stage Dockerfile:
# The `builder` stage compiles the binary and gathers all dependencies in the `/export/` directory.
FROM debian:13 AS builder
RUN apt-get update && apt-get -y upgrade \
 && apt-get -y install wget curl build-essential gcc make libssl-dev pkg-config cmake git

# Install the latest Rust build environment.
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install the `depres` utility for dependency resolution.
RUN cd /usr/local/src/ \
 && git clone https://github.com/rrauch/depres.git \
 && cd depres \
 && git checkout 717d0098751024c1282d42c2ee6973e6b53002dc \
 && cargo build --release \
 && cp target/release/depres /usr/local/bin/

COPY Cargo.* /usr/local/src/janus_fs/
COPY foyer-cache /usr/local/src/janus_fs/foyer-cache/
COPY janus-fs /usr/local/src/janus_fs/janus-fs/
COPY janus-io /usr/local/src/janus_fs/janus-io/
COPY janus-vfs /usr/local/src/janus_fs/janus-vfs/

# Build the `janus-nfs` binary.
RUN cd /usr/local/src/janus_fs/ \
 && cargo build --release \
 && cp ./target/release/janus-fs /usr/local/bin/

# Use `depres` to identify all required files for the final image.
RUN depres /bin/sh /bin/bash /bin/ls /usr/local/bin/janus-fs \
    /etc/ssl/certs/ \
    /usr/share/ca-certificates/ \
    >> /tmp/export.list

# Copy all required files into the `/export/` directory.
RUN cat /tmp/export.list \
 # remove all duplicates
 && cat /tmp/export.list | sort -o /tmp/export.list -u - \
 && mkdir -p /export/ \
 && rm -rf /export/* \
 # copying all necessary files
 && cat /tmp/export.list | xargs cp -a --parents -t /export/ \
 && mkdir -p /export/tmp && chmod 0777 /export/tmp


# The final stage creates a minimal image with all necessary files.
FROM scratch
WORKDIR /

# Copy files from the `builder` stage.
COPY --from=builder /export/ /

VOLUME /data
EXPOSE 12000
ENV DATA_DIR="/data/"
ENV LISTEN_ADDRESS="0.0.0.0:12000"

ENTRYPOINT ["/usr/local/bin/janus-fs"]
