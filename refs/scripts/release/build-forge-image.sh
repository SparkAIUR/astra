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

IMAGE_TAGGED="docker.io/halceon/astra-forge:${TAG}"
IS_PRERELEASE=false
if [[ "$TAG" == *-rc* ]]; then
  IS_PRERELEASE=true
fi

echo "Building local forge image tags:"
echo "  - ${IMAGE_TAGGED}"
BUILD_ARGS=(
  -f Dockerfile.forge
  -t "${IMAGE_TAGGED}"
)

if [[ "${IS_PRERELEASE}" != "true" ]]; then
  IMAGE_LATEST="docker.io/halceon/astra-forge:latest"
  echo "  - ${IMAGE_LATEST}"
  BUILD_ARGS+=(-t "${IMAGE_LATEST}")
else
  echo "  - latest forge tag skipped for prerelease ${TAG}"
fi

docker buildx build \
  "${BUILD_ARGS[@]}" \
  --load \
  .

echo "Built local forge image tags for ${TAG}"
