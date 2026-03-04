#!/usr/bin/env bash

astra_ts() {
  date -u +%Y-%m-%dT%H:%M:%SZ
}

astra_log() {
  printf '[astra-deploy][%s] %s\n' "$(astra_ts)" "$*"
}

astra_warn() {
  printf '[astra-deploy][%s][warn] %s\n' "$(astra_ts)" "$*" >&2
}

astra_error() {
  printf '[astra-deploy][%s][error] %s\n' "$(astra_ts)" "$*" >&2
}

astra_die() {
  astra_error "$*"
  exit 1
}

astra_bool_true() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

astra_require_tools() {
  local missing=0
  local tool
  for tool in "$@"; do
    if ! command -v "${tool}" >/dev/null 2>&1; then
      astra_error "missing required tool: ${tool}"
      missing=1
    fi
  done
  [ "${missing}" -eq 0 ]
}

astra_run() {
  local dry_run=${1:-false}
  shift
  if astra_bool_true "${dry_run}"; then
    printf '[astra-deploy][%s][dry-run] ' "$(astra_ts)"
    printf '%q ' "$@"
    printf '\n'
    return 0
  fi
  "$@"
}

astra_run_shell() {
  local dry_run=${1:-false}
  shift
  local cmd=${1:?command required}
  if astra_bool_true "${dry_run}"; then
    printf '[astra-deploy][%s][dry-run] bash -lc %q\n' "$(astra_ts)" "${cmd}"
    return 0
  fi
  bash -lc "${cmd}"
}

astra_confirm() {
  local prompt=${1:?prompt required}
  local non_interactive=${2:-false}
  local yes_flag=${3:-false}

  if astra_bool_true "${yes_flag}"; then
    return 0
  fi

  if astra_bool_true "${non_interactive}"; then
    return 1
  fi

  if [ ! -t 0 ]; then
    return 1
  fi

  local answer
  read -r -p "${prompt} [y/N]: " answer || return 1
  case "${answer}" in
    y|Y|yes|YES) return 0 ;;
    *) return 1 ;;
  esac
}

astra_retry() {
  local attempts=${1:?attempts required}
  local delay_secs=${2:?delay required}
  shift 2

  local n
  for n in $(seq 1 "${attempts}"); do
    if "$@"; then
      return 0
    fi
    sleep "${delay_secs}"
  done
  return 1
}

astra_detect_public_ip() {
  local ip

  ip=$(ip -o -4 route get 1.1.1.1 2>/dev/null | awk '{for (i=1; i<=NF; i++) if ($i=="src") {print $(i+1); exit}}')
  if [ -z "${ip}" ]; then
    ip=$(hostname -I 2>/dev/null | awk '{print $1}')
  fi

  printf '%s\n' "${ip}"
}

astra_is_ubuntu_2404_amd64() {
  local id=""
  local version=""
  local arch

  arch=$(dpkg --print-architecture 2>/dev/null || uname -m)
  if [ -r /etc/os-release ]; then
    # shellcheck disable=SC1091
    . /etc/os-release
    id=${ID:-}
    version=${VERSION_ID:-}
  fi

  [ "${id}" = "ubuntu" ] && [ "${version}" = "24.04" ] && [ "${arch}" = "amd64" ]
}

astra_normalize_docker_hub_repo() {
  local repo=${1:?repo required}
  repo=${repo#docker.io/}
  repo=${repo#registry-1.docker.io/}
  printf '%s\n' "${repo}"
}

astra_resolve_latest_stable_tag() {
  local repo=${1:?repo required}
  local max_pages=${2:-20}

  astra_require_tools curl jq >/dev/null || return 1

  local normalized
  normalized=$(astra_normalize_docker_hub_repo "${repo}")

  local page=1
  local tags=""

  while [ "${page}" -le "${max_pages}" ]; do
    local url="https://registry.hub.docker.com/v2/repositories/${normalized}/tags?page_size=100&page=${page}"
    local payload
    payload=$(curl -fsSL "${url}") || return 1

    local page_tags
    page_tags=$(printf '%s' "${payload}" | jq -r '.results[]?.name // empty' | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' || true)
    if [ -n "${page_tags}" ]; then
      tags+=$'\n'"${page_tags}"
    fi

    local next
    next=$(printf '%s' "${payload}" | jq -r '.next // empty')
    if [ -z "${next}" ]; then
      break
    fi
    page=$((page + 1))
  done

  printf '%s\n' "${tags}" | sed '/^$/d' | sort -V -r | head -n 1
}

astra_find_auto_disk_device() {
  python3 - <<'PY'
import json
import subprocess
import sys


def run(*cmd):
    return subprocess.check_output(cmd, text=True).strip()


try:
    root_source = run("findmnt", "-no", "SOURCE", "/")
except Exception:
    root_source = ""

root_pk = ""
if root_source.startswith("/dev/"):
    try:
        root_pk = run("lsblk", "-no", "PKNAME", root_source)
    except Exception:
        root_pk = ""

try:
    data = json.loads(run("lsblk", "-J", "-o", "PATH,TYPE,PKNAME,MOUNTPOINT,FSTYPE"))
except Exception:
    print("")
    sys.exit(0)

part_candidates = []
disk_candidates = []


def walk(nodes):
    for node in nodes or []:
        path = node.get("path") or ""
        node_type = node.get("type") or ""
        pkname = node.get("pkname") or ""
        mountpoint = node.get("mountpoint") or ""
        fstype = node.get("fstype") or ""
        children = node.get("children") or []

        is_root = path == root_source
        same_root_disk = bool(root_pk and (pkname == root_pk or path == f"/dev/{root_pk}"))

        if node_type == "part":
            if path and not mountpoint and not fstype and not is_root and not same_root_disk:
                part_candidates.append(path)
        elif node_type == "disk":
            if path and not mountpoint and not fstype and not is_root and not same_root_disk and not children:
                disk_candidates.append(path)

        walk(children)


walk(data.get("blockdevices"))

for candidate in sorted(part_candidates):
    print(candidate)
    sys.exit(0)

for candidate in sorted(disk_candidates):
    print(candidate)
    sys.exit(0)

print("")
PY
}

astra_prepare_disk() {
  local device=${1:?device required}
  local mount_path=${2:?mount path required}
  local dry_run=${3:-false}
  local non_interactive=${4:-false}
  local yes_flag=${5:-false}

  [ -b "${device}" ] || astra_die "device is not a block device: ${device}"

  astra_log "preparing disk device=${device} mount=${mount_path}"
  astra_run "${dry_run}" mkdir -p "${mount_path}"

  local fstype
  fstype=$(lsblk -no FSTYPE "${device}" 2>/dev/null | head -n 1 | tr -d '[:space:]')

  if [ -z "${fstype}" ]; then
    if ! astra_confirm "Device ${device} has no filesystem. Format as ext4?" "${non_interactive}" "${yes_flag}"; then
      astra_die "refusing to format ${device} without confirmation; use --yes or interactive confirmation"
    fi
    astra_run "${dry_run}" mkfs.ext4 -F "${device}"
  else
    astra_log "existing filesystem detected on ${device}: ${fstype}"
  fi

  local uuid
  uuid=$(blkid -s UUID -o value "${device}" 2>/dev/null || true)
  if [ -z "${uuid}" ] && ! astra_bool_true "${dry_run}"; then
    astra_die "failed to determine UUID for ${device}"
  fi

  if [ -n "${uuid}" ]; then
    if ! grep -q "UUID=${uuid}" /etc/fstab; then
      local entry="UUID=${uuid} ${mount_path} ext4 defaults,nofail 0 2"
      if astra_bool_true "${dry_run}"; then
        astra_log "would append to /etc/fstab: ${entry}"
      else
        printf '%s\n' "${entry}" >> /etc/fstab
      fi
    fi
  else
    astra_warn "UUID unresolved in dry-run; skipping /etc/fstab update"
  fi

  astra_run "${dry_run}" mount -a
  astra_run "${dry_run}" findmnt "${mount_path}"
}
