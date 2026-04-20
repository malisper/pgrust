#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE_REPO="malisper/pgrust"
DOCKERFILE="$ROOT_DIR/Dockerfile"
PLATFORMS="linux/amd64,linux/arm64"
BUILDER_NAME="${BUILDER_NAME:-pgrust-publish}"
SMOKE_CONTAINER="${SMOKE_CONTAINER:-pgrust-publish-smoke}"
SMOKE_PORT="${SMOKE_PORT:-5544}"

usage() {
    cat <<'EOF'
Usage:
  scripts/deploy_docker_images.sh nightly
  scripts/deploy_docker_images.sh release <version>

Channels:
  nightly            Publish nightly, nightly-YYYYMMDD-<sha>, and sha-<sha>
  release <version>  Publish <version>, latest, and sha-<sha>

Notes:
  - Publishes the main runtime image from Dockerfile
  - Publishes linux/amd64 and linux/arm64 to Docker Hub
  - Requires a clean git worktree
  - Requires docker login to Docker Hub
EOF
}

require_tool() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required tool: $1" >&2
        exit 1
    fi
}

normalize_source_url() {
    local remote
    remote="$(git -C "$ROOT_DIR" remote get-url origin)"
    case "$remote" in
        git@github.com:*)
            remote="${remote#git@github.com:}"
            remote="${remote%.git}"
            printf 'https://github.com/%s\n' "$remote"
            ;;
        https://github.com/*)
            printf '%s\n' "${remote%.git}"
            ;;
        *)
            printf '%s\n' "$remote"
            ;;
    esac
}

host_platform() {
    case "$(uname -m)" in
        x86_64|amd64)
            printf 'linux/amd64\n'
            ;;
        arm64|aarch64)
            printf 'linux/arm64\n'
            ;;
        *)
            echo "unsupported host architecture: $(uname -m)" >&2
            exit 1
            ;;
    esac
}

ensure_clean_worktree() {
    if ! git -C "$ROOT_DIR" diff --quiet || ! git -C "$ROOT_DIR" diff --cached --quiet; then
        echo "refusing to publish with a dirty worktree" >&2
        exit 1
    fi
    if [[ -n "$(git -C "$ROOT_DIR" ls-files --others --exclude-standard)" ]]; then
        echo "refusing to publish with untracked files in the worktree" >&2
        exit 1
    fi
}

ensure_docker_auth() {
    if ! docker info >/dev/null 2>&1; then
        echo "docker daemon is not reachable; start Docker Desktop or fix the active docker context" >&2
        exit 1
    fi

    if [[ ! -f "${HOME}/.docker/config.json" ]]; then
        echo "docker does not appear to be logged in to Docker Hub; run: docker login" >&2
        exit 1
    fi
}

ensure_builder() {
    if ! docker buildx inspect "$BUILDER_NAME" >/dev/null 2>&1; then
        docker buildx create --name "$BUILDER_NAME" --driver docker-container --use >/dev/null
    else
        docker buildx use "$BUILDER_NAME" >/dev/null
    fi
    docker buildx inspect --bootstrap >/dev/null
}

verify_release_tag() {
    local version="$1"
    local allowed_one="$version"
    local allowed_two="v$version"
    local tag
    while IFS= read -r tag; do
        if [[ "$tag" == "$allowed_one" || "$tag" == "$allowed_two" ]]; then
            return 0
        fi
    done < <(git -C "$ROOT_DIR" tag --points-at HEAD)
    echo "release mode requires HEAD to be tagged with $allowed_one or $allowed_two" >&2
    exit 1
}

run_smoke_test() {
    local smoke_image="$1"
    local logs

    docker rm -f "$SMOKE_CONTAINER" >/dev/null 2>&1 || true
    trap 'docker rm -f "$SMOKE_CONTAINER" >/dev/null 2>&1 || true' EXIT

    docker run -d --rm \
        --name "$SMOKE_CONTAINER" \
        -p "${SMOKE_PORT}:5432" \
        "$smoke_image" >/dev/null

    sleep 2
    logs="$(docker logs "$SMOKE_CONTAINER" 2>&1 || true)"
    if [[ "$logs" != *"pgrust: listening on 0.0.0.0:5432"* ]]; then
        printf '%s\n' "$logs"
        echo "smoke test failed: container did not reach listening state" >&2
        exit 1
    fi

    docker rm -f "$SMOKE_CONTAINER" >/dev/null 2>&1 || true
    trap - EXIT
}

main() {
    require_tool git
    require_tool docker
    require_tool sed
    require_tool grep

    if [[ ! -f "$DOCKERFILE" ]]; then
        echo "missing Dockerfile at $DOCKERFILE" >&2
        exit 1
    fi

    if ! docker buildx version >/dev/null 2>&1; then
        echo "docker buildx is required" >&2
        exit 1
    fi

    if [[ $# -lt 1 ]]; then
        usage
        exit 1
    fi

    local channel="$1"
    local version=""
    local normalized_version=""
    local created_at
    local date_tag
    local full_sha
    local short_sha
    local source_url
    local version_label
    local primary_ref
    local host_arch_platform
    local smoke_image
    local -a tags
    local -a build_tag_args

    case "$channel" in
        nightly)
            if [[ $# -ne 1 ]]; then
                usage
                exit 1
            fi
            ;;
        release)
            if [[ $# -ne 2 ]]; then
                usage
                exit 1
            fi
            version="$2"
            normalized_version="${version#v}"
            if [[ -z "$normalized_version" ]]; then
                echo "release version must not be empty" >&2
                exit 1
            fi
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "unknown channel: $channel" >&2
            usage
            exit 1
            ;;
    esac

    ensure_clean_worktree
    ensure_docker_auth
    ensure_builder

    full_sha="$(git -C "$ROOT_DIR" rev-parse HEAD)"
    short_sha="$(git -C "$ROOT_DIR" rev-parse --short=12 HEAD)"
    created_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    date_tag="$(date -u +%Y%m%d)"
    source_url="$(normalize_source_url)"
    host_arch_platform="$(host_platform)"

    if [[ "$channel" == "nightly" ]]; then
        tags=(
            "$IMAGE_REPO:nightly"
            "$IMAGE_REPO:nightly-${date_tag}-${short_sha}"
            "$IMAGE_REPO:sha-${short_sha}"
        )
        version_label="nightly-${date_tag}-${short_sha}"
    else
        verify_release_tag "$normalized_version"
        tags=(
            "$IMAGE_REPO:${normalized_version}"
            "$IMAGE_REPO:latest"
            "$IMAGE_REPO:sha-${short_sha}"
        )
        version_label="$normalized_version"
    fi
    primary_ref="${tags[0]}"

    build_tag_args=()
    for tag in "${tags[@]}"; do
        build_tag_args+=(-t "$tag")
    done

    smoke_image="$IMAGE_REPO:smoke-${short_sha}"

    echo "Publishing Docker image"
    echo "  repo:       $IMAGE_REPO"
    echo "  channel:    $channel"
    echo "  dockerfile: $DOCKERFILE"
    echo "  commit:     $full_sha"
    echo "  platforms:  $PLATFORMS"
    echo "  source:     $source_url"
    echo "  tags:"
    for tag in "${tags[@]}"; do
        echo "    - $tag"
    done

    echo
    echo "Smoke-building host image for $host_arch_platform"
    docker buildx build \
        --platform "$host_arch_platform" \
        -f "$DOCKERFILE" \
        -t "$smoke_image" \
        --label "org.opencontainers.image.source=$source_url" \
        --label "org.opencontainers.image.revision=$full_sha" \
        --label "org.opencontainers.image.created=$created_at" \
        --label "org.opencontainers.image.version=$version_label" \
        --load \
        "$ROOT_DIR"

    echo "Smoke-testing container startup on localhost:$SMOKE_PORT"
    run_smoke_test "$smoke_image"
    docker image rm -f "$smoke_image" >/dev/null 2>&1 || true

    echo
    echo "Publishing multi-arch image"
    docker buildx build \
        --platform "$PLATFORMS" \
        -f "$DOCKERFILE" \
        "${build_tag_args[@]}" \
        --label "org.opencontainers.image.source=$source_url" \
        --label "org.opencontainers.image.revision=$full_sha" \
        --label "org.opencontainers.image.created=$created_at" \
        --label "org.opencontainers.image.version=$version_label" \
        --push \
        "$ROOT_DIR"

    echo
    echo "Verifying pushed manifest for $primary_ref"
    local inspect_output
    inspect_output="$(docker buildx imagetools inspect "$primary_ref")"
    printf '%s\n' "$inspect_output"
    if [[ "$inspect_output" != *"linux/amd64"* || "$inspect_output" != *"linux/arm64"* ]]; then
        echo "manifest verification failed: expected linux/amd64 and linux/arm64" >&2
        exit 1
    fi
}

main "$@"
