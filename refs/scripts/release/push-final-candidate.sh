#!/usr/bin/env bash
set -euo pipefail

TAG=${1:-}
if [[ -z "$TAG" ]]; then
  echo "usage: $0 <vX.Y.Z[-rcN]>"
  exit 1
fi

if [[ ! "$TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-rc[0-9]+)?$ ]]; then
  echo "error: tag must be semver or prerelease semver (vX.Y.Z[-rcN]), got '${TAG}'" >&2
  exit 1
fi

TAGS=(
  "-t" "docker.io/halceon/astra:${TAG}"
  "-t" "docker.io/halceon/astra:latest"
  "-t" "docker.io/nudevco/astra:${TAG}"
  "-t" "docker.io/nudevco/astra:latest"
)

echo "Pushing multi-arch image tags for ${TAG}:"
echo "  - docker.io/halceon/astra:${TAG}"
echo "  - docker.io/halceon/astra:latest"
echo "  - docker.io/nudevco/astra:${TAG}"
echo "  - docker.io/nudevco/astra:latest"
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  "${TAGS[@]}" \
  --push \
  .

echo "Pushed multi-arch tags for ${TAG}"
