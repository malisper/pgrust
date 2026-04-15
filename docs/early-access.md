# Early Access Docker Handoff

This path is manual-only and separate from the repo's existing `Dockerfile`.

Use it when you want to hand an evaluator a prebuilt Docker image without giving
them repo access or publishing to a registry.

## Build a release tarball

Run:

```bash
scripts/build_early_access_tarball.sh
```

Or provide an explicit version tag:

```bash
scripts/build_early_access_tarball.sh ea-2026-04-14
```

The script:

- builds `Dockerfile.early-access` for `linux/arm64`
- tags the image as `pgrust-early-access:<version>` and `pgrust-early-access:latest`
- smoke-tests container startup
- writes a sendable folder under `target/early-access/<version>/`

## Send to the evaluator

Share the whole folder:

- `target/early-access/<version>/`

That folder contains:

- the compressed image tarball
- the SHA-256 checksum
- `load-image.sh`
- `run-container.sh`
- `README.txt`

## Evaluator install and run

On an Apple Silicon Mac with Docker Desktop:

```bash
cd <version>
shasum -a 256 -c pgrust-early-access-<version>-linux-arm64.tar.gz.sha256
./load-image.sh
./run-container.sh
```

If you want persistent data instead of a disposable container:

```bash
docker run --rm -p 5432:5432 -v pgrust-early-access-data:/var/lib/postgresql/data pgrust-early-access:<version>
```

## Updating an evaluator

Build a new tarball with a new version tag, send the new files, and have the
evaluator run `docker load` again. No automatic publishing happens on repo
pushes.
