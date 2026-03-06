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

IMAGE_TAGGED="docker.io/halceon/astra-forge:${TAG}"
IMAGE_LATEST="docker.io/halceon/astra-forge:latest"

echo "Building local forge image tags:"
echo "  - ${IMAGE_TAGGED}"
echo "  - ${IMAGE_LATEST}"
docker buildx build \
  -f Dockerfile.forge \
  -t "${IMAGE_TAGGED}" \
  -t "${IMAGE_LATEST}" \
  --load \
  .

echo "Built local forge image tags for ${TAG}"
