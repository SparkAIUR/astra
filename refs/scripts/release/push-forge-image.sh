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

IS_PRERELEASE=false
if [[ "$TAG" == *-rc* ]]; then
  IS_PRERELEASE=true
fi

TAGS=(
  "-t" "docker.io/halceon/astra-forge:${TAG}"
)

echo "Pushing multi-arch forge image tags for ${TAG}:"
echo "  - docker.io/halceon/astra-forge:${TAG}"

if [[ "${IS_PRERELEASE}" != "true" ]]; then
  TAGS+=("-t" "docker.io/halceon/astra-forge:latest")
  echo "  - docker.io/halceon/astra-forge:latest"
else
  echo "  - latest forge tag skipped for prerelease ${TAG}"
fi

docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f Dockerfile.forge \
  "${TAGS[@]}" \
  --push \
  .

echo "Pushed forge image tags for ${TAG}"
