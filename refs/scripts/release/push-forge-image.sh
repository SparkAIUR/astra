#!/usr/bin/env bash
set -euo pipefail

TAG=${1:-}
if [[ -z "$TAG" ]]; then
  echo "usage: $0 <vX.Y.Z>"
  exit 1
fi

if [[ ! "$TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "error: tag must be stable semver (vX.Y.Z), got '${TAG}'" >&2
  exit 1
fi

TAGS=(
  "-t" "docker.io/halceon/astra-forge:${TAG}"
  "-t" "docker.io/halceon/astra-forge:latest"
)

echo "Pushing multi-arch forge image tags for ${TAG}:"
echo "  - docker.io/halceon/astra-forge:${TAG}"
echo "  - docker.io/halceon/astra-forge:latest"
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f Dockerfile.forge \
  "${TAGS[@]}" \
  --push \
  .

echo "Pushed forge image tags for ${TAG}"
