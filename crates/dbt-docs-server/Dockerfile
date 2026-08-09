# syntax=docker/dockerfile:1

# Self-hosted dbt docs server. Fetches the released dbt Core binary from GitHub
# Releases (no source build, no cargo, no build context) and serves
# `dbt docs serve`.
#
# Build (multi-arch capable via BuildKit TARGETARCH):
#   docker build -f crates/dbt-docs-server/Dockerfile -t dbt-docs-server crates/dbt-docs-server
#
# Run: mount a target/ dir containing target/index/*.parquet (from
# `dbt --write-index <run|build|compile>`). The named volume caches the ADBC
# DuckDB driver, downloaded from public.cdn.getdbt.com on first boot (needs
# outbound HTTPS once; offline after):
#   docker run --rm -p 8580:8580 \
#     -v "$PWD/target:/data/target:ro" \
#     -v dbt-adbc-cache:/var/cache/dbt \
#     dbt-docs-server

ARG DEBIAN_TAG=bookworm-slim

FROM debian:${DEBIAN_TAG} AS fetch

# Pinned for reproducible builds; override with `--build-arg DBT_VERSION=...`.
# Releases: https://github.com/dbt-labs/dbt-core/releases
ARG DBT_VERSION=2.0.0-alpha.5
ARG TARGETARCH

RUN --mount=target=/var/lib/apt/lists,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean \
    && apt-get update \
    && apt-get -y --no-install-recommends install ca-certificates curl

WORKDIR /tmp

# Map Docker's TARGETARCH onto the release's Rust target triple, download the
# tarball plus SHA256SUMS, verify the digest, then extract the single binary
# (named dbt-sa-cli in the archive) onto PATH as `dbt`. tar and gzip ship in the
# debian base image, so no extra package is needed for extraction.
RUN <<'EOF'
set -eux
case "$TARGETARCH" in
  amd64) triple=x86_64-unknown-linux-gnu ;;
  arm64) triple=aarch64-unknown-linux-gnu ;;
  *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;;
esac
base="dbt-core-${DBT_VERSION}-${triple}"
url="https://github.com/dbt-labs/dbt-core/releases/download/v${DBT_VERSION}"
curl -fsSL -o "${base}.tar.gz" "${url}/${base}.tar.gz"
curl -fsSL -o SHA256SUMS "${url}/SHA256SUMS"
grep " ${base}.tar.gz\$" SHA256SUMS > wanted.sha256
sha256sum -c wanted.sha256
tar -xzf "${base}.tar.gz"
install -Dm755 "${base}/dbt-sa-cli" /out/dbt
EOF

FROM debian:${DEBIAN_TAG} AS runtime

LABEL org.opencontainers.image.title="dbt-docs-server" \
      org.opencontainers.image.description="Self-hosted dbt docs server (dbt docs serve)." \
      org.opencontainers.image.source="https://github.com/dbt-labs/dbt-core" \
      org.opencontainers.image.licenses="Apache-2.0"

# ca-certificates + libstdc++6: the ADBC DuckDB driver is fetched over HTTPS on
# first boot and its .so is C++. curl backs the HEALTHCHECK below.
RUN --mount=target=/var/lib/apt/lists,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean \
    && apt-get update \
    && apt-get -y --no-install-recommends install ca-certificates curl libstdc++6 \
    && useradd --system --uid 10001 --user-group --no-create-home dbt

COPY --from=fetch /out/dbt /usr/local/bin/dbt

# Bind all interfaces (server defaults to 127.0.0.1); map the port at run time.
# DBT_LOG_PATH points file logging at a writable dir -- dbt otherwise creates
# ./logs relative to the working directory, which the non-root user can't do at /.
ENV DBT_DOCS_HOST=0.0.0.0 \
    DBT_DOCS_PORT=8580 \
    DBT_DOCS_TARGET_PATH=/data/target \
    XDG_CACHE_HOME=/var/cache/dbt \
    DBT_LOG_PATH=/var/log/dbt \
    HOME=/home/dbt

# Dirs the non-root user must be able to write: driver cache (persist
# /var/cache/dbt via a named volume at run time), log dir, home, mount point.
RUN install -d -o dbt -g dbt /var/cache/dbt /var/log/dbt /home/dbt /data/target

WORKDIR /home/dbt
USER dbt
EXPOSE 8580

# Liveness: /api/v1/health returns 200 as soon as the server is listening.
# start-interval fast-polls during the start-period so the container reaches
# `healthy` quickly. Port is the fixed in-container bind (8580), independent of
# any host-side port remapping.
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --start-interval=2s --retries=10 \
    CMD ["curl", "-sfS", "--head", "--max-time", "5", "http://127.0.0.1:8580/api/v1/health"]

ENTRYPOINT ["dbt", "docs", "serve", "--no-open"]
