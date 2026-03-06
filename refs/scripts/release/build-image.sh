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

IMAGE_HALCEON="docker.io/halceon/astra:${TAG}"
IMAGE_HALCEON_LATEST="docker.io/halceon/astra:latest"
IMAGE_NUDEVCO="docker.io/nudevco/astra:${TAG}"
IMAGE_NUDEVCO_LATEST="docker.io/nudevco/astra:latest"

echo "Building local image tags:"
echo "  - ${IMAGE_HALCEON}"
echo "  - ${IMAGE_HALCEON_LATEST}"
echo "  - ${IMAGE_NUDEVCO}"
echo "  - ${IMAGE_NUDEVCO_LATEST}"
docker buildx build \
  -t "${IMAGE_HALCEON}" \
  -t "${IMAGE_HALCEON_LATEST}" \
  -t "${IMAGE_NUDEVCO}" \
  -t "${IMAGE_NUDEVCO_LATEST}" \
  --load \
  .

echo "Built local tags for ${TAG}"
