# Docker Publish

This repo ships the main runtime image to Docker Hub as:

- `malisper/pgrust`

The publish flow is local/manual. It builds from the main [Dockerfile](/Dockerfile) and pushes a multi-arch image for:

- `linux/amd64`
- `linux/arm64`

## Prerequisites

- Docker with `buildx`
- Docker Hub login via `docker login`
- clean git worktree

## Nightly Publish

Run:

```bash
scripts/deploy_docker_images.sh nightly
```

This publishes:

- `malisper/pgrust:nightly`
- `malisper/pgrust:nightly-YYYYMMDD-<shortsha>`
- `malisper/pgrust:sha-<shortsha>`

`nightly` is the moving alpha tag. Nightly publishes do not update `latest`.

## Release Publish

Run:

```bash
scripts/deploy_docker_images.sh release 0.1.0
```

This publishes:

- `malisper/pgrust:0.1.0`
- `malisper/pgrust:latest`
- `malisper/pgrust:sha-<shortsha>`

Release mode requires `HEAD` to be tagged as either:

- `0.1.0`
- `v0.1.0`

`latest` tracks the most recent explicit release, not the latest nightly build.

## Smoke Test

Before pushing, the script:

- builds a host-architecture image with `--load`
- starts a container locally
- verifies the server reached `pgrust: listening on 0.0.0.0:5432`

After push, it verifies the manifest includes both target platforms.

## Early Access

This publish flow does not use [Dockerfile.early-access](/Dockerfile.early-access). The early-access image/tarball path remains separate and manual.
