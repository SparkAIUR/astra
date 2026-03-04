#!/usr/bin/env bash
set -euo pipefail

ENDPOINT=${ENDPOINT:-127.0.0.1:2379}
export ETCDCTL_API=3

etcdctl --endpoints="${ENDPOINT}" put /demo/key1 value1
etcdctl --endpoints="${ENDPOINT}" get /demo/key1
