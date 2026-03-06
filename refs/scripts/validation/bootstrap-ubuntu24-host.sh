#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[bootstrap] %s\n' "$*"
}

die() {
  printf '[bootstrap][error] %s\n' "$*" >&2
  exit 1
}

workspace_override=""
repo_override=""
results_override=""
logs_override=""
bin_override=""
ghz_override=""
grpcurl_override=""
etcd_override=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --workspace) workspace_override=${2:?missing value for --workspace}; shift 2 ;;
    --repo-dir) repo_override=${2:?missing value for --repo-dir}; shift 2 ;;
    --results-dir) results_override=${2:?missing value for --results-dir}; shift 2 ;;
    --logs-dir) logs_override=${2:?missing value for --logs-dir}; shift 2 ;;
    --bin-dir) bin_override=${2:?missing value for --bin-dir}; shift 2 ;;
    --ghz-version) ghz_override=${2:?missing value for --ghz-version}; shift 2 ;;
    --grpcurl-version) grpcurl_override=${2:?missing value for --grpcurl-version}; shift 2 ;;
    --etcd-version) etcd_override=${2:?missing value for --etcd-version}; shift 2 ;;
    --help|-h)
      cat <<'EOF'
Usage: bootstrap-ubuntu24-host.sh [options]
  --workspace <path>       Base workspace (default: /root/astra-lab)
  --repo-dir <path>        Repo directory
  --results-dir <path>     Results directory
  --logs-dir <path>        Logs directory
  --bin-dir <path>         Workspace binary helper directory
  --ghz-version <tag>      ghz release tag (default: v0.121.0)
  --grpcurl-version <tag>  grpcurl release tag (default: v1.9.3)
  --etcd-version <tag>     etcd release tag (default: v3.6.8)
EOF
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

ASTRA_WORKSPACE=${workspace_override:-${ASTRA_WORKSPACE:-/root/astra-lab}}
ASTRA_REPO_DIR=${repo_override:-${ASTRA_REPO_DIR:-${ASTRA_WORKSPACE}/repo}}
ASTRA_RESULTS_DIR=${results_override:-${ASTRA_RESULTS_DIR:-${ASTRA_WORKSPACE}/results}}
ASTRA_LOG_DIR=${logs_override:-${ASTRA_LOG_DIR:-${ASTRA_WORKSPACE}/logs}}
ASTRA_BIN_DIR=${bin_override:-${ASTRA_BIN_DIR:-${ASTRA_WORKSPACE}/bin}}

GHZ_VERSION=${ghz_override:-${GHZ_VERSION:-v0.121.0}}
GRPCURL_VERSION=${grpcurl_override:-${GRPCURL_VERSION:-v1.9.3}}
ETCD_VERSION=${etcd_override:-${ETCD_VERSION:-v3.6.8}}

if [ "${EUID:-$(id -u)}" -ne 0 ]; then
  die "this script must run as root"
fi

if [ -r /etc/os-release ]; then
  # shellcheck disable=SC1091
  . /etc/os-release
else
  die "missing /etc/os-release"
fi

if [ "${ID:-}" != "ubuntu" ]; then
  die "unsupported distro: expected ubuntu, got '${ID:-unknown}'"
fi
if [ "${VERSION_ID:-}" != "24.04" ]; then
  die "unsupported ubuntu version: expected 24.04, got '${VERSION_ID:-unknown}'"
fi
if [ "$(dpkg --print-architecture)" != "amd64" ]; then
  die "unsupported architecture: expected amd64"
fi

APT_UPDATED=0

apt_update_once() {
  if [ "${APT_UPDATED}" -eq 0 ]; then
    log "running apt-get update"
    apt-get update -y
    APT_UPDATED=1
  fi
}

install_apt_packages_if_missing() {
  local pkg
  local missing=()
  for pkg in "$@"; do
    if ! dpkg -s "${pkg}" >/dev/null 2>&1; then
      missing+=("${pkg}")
    fi
  done
  if [ "${#missing[@]}" -eq 0 ]; then
    return 0
  fi
  apt_update_once
  log "installing apt packages: ${missing[*]}"
  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${missing[@]}"
}

github_asset_url() {
  local repo=$1
  local tag=$2
  local pattern=$3
  local api="https://api.github.com/repos/${repo}/releases/tags/${tag}"
  local url
  url=$(
    curl -fsSL "${api}" \
      | jq -r --arg re "${pattern}" '.assets[] | select(.name | test($re; "i")) | .browser_download_url' \
      | head -n 1
  )
  if [ -z "${url}" ] || [ "${url}" = "null" ]; then
    die "failed to find release asset for ${repo}@${tag} matching /${pattern}/"
  fi
  printf '%s\n' "${url}"
}

install_binary_from_release_archive() {
  local cmd_name=$1
  local repo=$2
  local tag=$3
  local asset_regex=$4
  local bin_glob=$5
  local target_name=$6

  if command -v "${cmd_name}" >/dev/null 2>&1; then
    log "${cmd_name} already installed at $(command -v "${cmd_name}")"
    return 0
  fi

  local url
  url=$(github_asset_url "${repo}" "${tag}" "${asset_regex}")
  log "downloading ${cmd_name} from ${url}"

  local archive tmpdir candidate
  archive=$(mktemp)
  tmpdir=$(mktemp -d)
  curl -fL "${url}" -o "${archive}"

  case "${url}" in
    *.tar.gz|*.tgz)
      tar -xzf "${archive}" -C "${tmpdir}"
      ;;
    *.zip)
      unzip -q "${archive}" -d "${tmpdir}"
      ;;
    *)
      install -m 0755 "${archive}" "/usr/local/bin/${target_name}"
      rm -f "${archive}"
      rm -rf "${tmpdir}"
      return 0
      ;;
  esac

  candidate=$(find "${tmpdir}" -type f -name "${bin_glob}" -perm -u+x | head -n 1 || true)
  if [ -z "${candidate}" ]; then
    rm -f "${archive}"
    rm -rf "${tmpdir}"
    die "could not find binary ${bin_glob} in downloaded archive for ${cmd_name}"
  fi

  install -m 0755 "${candidate}" "/usr/local/bin/${target_name}"
  rm -f "${archive}"
  rm -rf "${tmpdir}"
}

ensure_docker_and_compose() {
  if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
    log "docker + docker compose already installed"
    return 0
  fi

  install_apt_packages_if_missing ca-certificates curl gnupg
  install -m 0755 -d /etc/apt/keyrings

  if [ ! -f /etc/apt/keyrings/docker.asc ]; then
    log "installing docker apt key"
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
    chmod a+r /etc/apt/keyrings/docker.asc
  fi

  local codename
  codename="${UBUNTU_CODENAME:-noble}"
  cat >/etc/apt/sources.list.d/docker.list <<EOF
deb [arch=amd64 signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${codename} stable
EOF
  APT_UPDATED=0
  apt_update_once
  log "installing docker engine + compose plugin"
  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    docker-ce \
    docker-ce-cli \
    containerd.io \
    docker-buildx-plugin \
    docker-compose-plugin

  if command -v systemctl >/dev/null 2>&1; then
    systemctl enable --now docker || true
  fi
}

prepare_workspace() {
  log "preparing workspace: ${ASTRA_WORKSPACE}"
  mkdir -p "${ASTRA_WORKSPACE}" "${ASTRA_REPO_DIR}" "${ASTRA_RESULTS_DIR}" "${ASTRA_LOG_DIR}" "${ASTRA_BIN_DIR}"
}

write_env_files() {
  install -d -m 0755 /etc/astra
  cat >/etc/astra/bootstrap.env <<EOF
export ASTRA_WORKSPACE="${ASTRA_WORKSPACE}"
export ASTRA_REPO_DIR="${ASTRA_REPO_DIR}"
export ASTRA_RESULTS_DIR="${ASTRA_RESULTS_DIR}"
export ASTRA_LOG_DIR="${ASTRA_LOG_DIR}"
export ASTRA_BIN_DIR="${ASTRA_BIN_DIR}"
export PATH="${ASTRA_BIN_DIR}:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:\$PATH"
EOF

  cat >/etc/profile.d/astra.sh <<'EOF'
if [ -f /etc/astra/bootstrap.env ]; then
  # shellcheck disable=SC1091
  . /etc/astra/bootstrap.env
fi
EOF
  chmod 0644 /etc/profile.d/astra.sh
}

install_remote_wrappers() {
  cat >/usr/local/bin/astra-run <<'EOF'
#!/usr/bin/env bash
set -eo pipefail

[ -f /etc/profile ] && . /etc/profile
[ -f /etc/profile.d/astra.sh ] && . /etc/profile.d/astra.sh
[ -f "${HOME}/.bashrc" ] && . "${HOME}/.bashrc"

if [ "$#" -eq 0 ]; then
  cd "${ASTRA_REPO_DIR:-$HOME}"
  exec bash -l
fi

if [ "$#" -eq 1 ]; then
  exec bash -lc "$1"
fi

exec "$@"
EOF
  chmod 0755 /usr/local/bin/astra-run

  cat >/usr/local/bin/astra-env <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

[ -f /etc/profile.d/astra.sh ] && . /etc/profile.d/astra.sh
printf 'ASTRA_WORKSPACE=%s\n' "${ASTRA_WORKSPACE:-}"
printf 'ASTRA_REPO_DIR=%s\n' "${ASTRA_REPO_DIR:-}"
printf 'ASTRA_RESULTS_DIR=%s\n' "${ASTRA_RESULTS_DIR:-}"
printf 'ASTRA_LOG_DIR=%s\n' "${ASTRA_LOG_DIR:-}"
printf 'PATH=%s\n' "${PATH:-}"
EOF
  chmod 0755 /usr/local/bin/astra-env

  cat >/usr/local/bin/astra-check <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[ -f /etc/profile.d/astra.sh ] && . /etc/profile.d/astra.sh

for cmd in docker ghz grpcurl etcdctl python3 jq make git; do
  if command -v "$cmd" >/dev/null 2>&1; then
    printf '[ok] %s -> %s\n' "$cmd" "$(command -v "$cmd")"
  else
    printf '[missing] %s\n' "$cmd"
  fi
done
EOF
  chmod 0755 /usr/local/bin/astra-check
}

ensure_root_bashrc_block() {
  local bashrc=/root/.bashrc
  touch "${bashrc}"
  if grep -q 'ASTRA-BOOTSTRAP-BLOCK' "${bashrc}"; then
    return 0
  fi
  cat >>"${bashrc}" <<'EOF'

# >>> ASTRA-BOOTSTRAP-BLOCK >>>
[ -f /etc/profile.d/astra.sh ] && . /etc/profile.d/astra.sh
alias cda='cd "${ASTRA_REPO_DIR}"'
alias cdr='cd "${ASTRA_RESULTS_DIR}"'
alias dco='docker compose'
# <<< ASTRA-BOOTSTRAP-BLOCK <<<
EOF
}

install_tooling() {
  install_apt_packages_if_missing \
    awscli \
    bash-completion \
    ca-certificates \
    coreutils \
    curl \
    git \
    jq \
    make \
    python3 \
    python3-venv \
    rsync \
    tar \
    unzip \
    xz-utils

  ensure_docker_and_compose
  install_binary_from_release_archive \
    ghz \
    bojand/ghz \
    "${GHZ_VERSION}" \
    'linux.*(amd64|x86_64).*(tar\.gz|zip)$' \
    'ghz*' \
    ghz
  install_binary_from_release_archive \
    grpcurl \
    fullstorydev/grpcurl \
    "${GRPCURL_VERSION}" \
    'linux.*(amd64|x86_64).*(tar\.gz|zip)$' \
    'grpcurl' \
    grpcurl
  install_binary_from_release_archive \
    etcdctl \
    etcd-io/etcd \
    "${ETCD_VERSION}" \
    'linux-amd64.*\.tar\.gz$' \
    'etcdctl' \
    etcdctl
}

main() {
  log "bootstrapping ubuntu 24.04 amd64 host"
  install_tooling
  prepare_workspace
  write_env_files
  install_remote_wrappers
  ensure_root_bashrc_block

  log "bootstrap complete"
  astra-check || true
  printf '\n'
  log "quick use:"
  log "  ssh root@host 'astra-env'"
  log "  ssh root@host \"astra-run 'cd \\\"\\\$ASTRA_REPO_DIR\\\" && pwd && docker compose version'\""
}

main "$@"
