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

IMAGE_HALCEON="docker.io/halceon/astra:${TAG}"
IMAGE_NUDEVCO="docker.io/nudevco/astra:${TAG}"

echo "Building local image tags:"
echo "  - ${IMAGE_HALCEON}"
echo "  - ${IMAGE_NUDEVCO}"
docker buildx build \
  -t "${IMAGE_HALCEON}" \
  -t "${IMAGE_NUDEVCO}" \
  --load \
  .

echo "Built local tags for ${TAG}"
