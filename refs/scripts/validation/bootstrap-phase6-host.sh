#!/usr/bin/env bash
set -euo pipefail

INSTALL_CLUSTERLOADER2=${INSTALL_CLUSTERLOADER2:-false}
INSTALL_K3S_VERSION=${INSTALL_K3S_VERSION:-}
KWOK_VERSION=${KWOK_VERSION:-}
ETCD_VERSION=${ETCD_VERSION:-v3.6.8}

log() {
  printf '[phase6-bootstrap][%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

need_root() {
  if [ "${EUID}" -ne 0 ]; then
    echo "run as root" >&2
    exit 1
  fi
}

apt_install_base() {
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -y
  apt-get install -y \
    ca-certificates \
    curl \
    wget \
    jq \
    tar \
    gzip \
    python3 \
    python3-pip \
    sysstat \
    util-linux \
    iproute2 \
    net-tools \
    conntrack \
    socat \
    ebtables \
    ethtool
}

install_k3s() {
  if command -v k3s >/dev/null 2>&1; then
    log "k3s already installed: $(k3s --version | head -n1)"
    return 0
  fi

  log "installing k3s"
  if [ -n "${INSTALL_K3S_VERSION}" ]; then
    curl -sfL https://get.k3s.io | \
      INSTALL_K3S_SKIP_ENABLE=true \
      INSTALL_K3S_SKIP_START=true \
      INSTALL_K3S_VERSION="${INSTALL_K3S_VERSION}" \
      sh -
  else
    curl -sfL https://get.k3s.io | \
      INSTALL_K3S_SKIP_ENABLE=true \
      INSTALL_K3S_SKIP_START=true \
      sh -
  fi
}

install_etcd_binaries() {
  if command -v etcd >/dev/null 2>&1 && command -v etcdutl >/dev/null 2>&1; then
    log "etcd already installed: $(etcd --version 2>/dev/null | head -n1)"
    log "etcdutl already installed: $(etcdutl version 2>/dev/null | head -n1)"
    return 0
  fi

  local version="${ETCD_VERSION}"
  if command -v etcdctl >/dev/null 2>&1; then
    local raw
    raw=$(etcdctl version 2>/dev/null | awk -F': ' '/etcdctl version/{print $2; exit}')
    if [ -n "${raw}" ]; then
      version="v${raw#v}"
    fi
  fi

  log "installing etcd binaries ${version}"
  local tmp_dir
  tmp_dir=$(mktemp -d)
  curl -fsSL -o "${tmp_dir}/etcd.tar.gz" \
    "https://github.com/etcd-io/etcd/releases/download/${version}/etcd-${version}-linux-amd64.tar.gz"
  tar -xzf "${tmp_dir}/etcd.tar.gz" -C "${tmp_dir}"
  install -m 0755 "${tmp_dir}/etcd-${version}-linux-amd64/etcd" /usr/local/bin/etcd
  install -m 0755 "${tmp_dir}/etcd-${version}-linux-amd64/etcdctl" /usr/local/bin/etcdctl
  install -m 0755 "${tmp_dir}/etcd-${version}-linux-amd64/etcdutl" /usr/local/bin/etcdutl
  rm -rf "${tmp_dir}"
}

install_kubectl() {
  if command -v kubectl >/dev/null 2>&1; then
    log "kubectl already installed: $(kubectl version --client --output=yaml 2>/dev/null | awk '/gitVersion:/{print $2; exit}')"
    return 0
  fi

  local kube_version
  if command -v k3s >/dev/null 2>&1; then
    kube_version=$(k3s --version | awk 'NR==1{print $3}' | cut -d+ -f1)
  else
    kube_version=$(curl -fsSL https://dl.k8s.io/release/stable.txt)
  fi

  log "installing kubectl ${kube_version}"
  curl -fsSL -o /usr/local/bin/kubectl "https://dl.k8s.io/release/${kube_version}/bin/linux/amd64/kubectl"
  chmod +x /usr/local/bin/kubectl
}

install_kwok() {
  if [ -z "${KWOK_VERSION}" ]; then
    KWOK_VERSION=$(curl -fsSL https://api.github.com/repos/kubernetes-sigs/kwok/releases/latest | jq -r '.tag_name')
  fi

  if command -v kwok >/dev/null 2>&1 && command -v kwokctl >/dev/null 2>&1; then
    log "kwok already installed: $(kwok --version 2>/dev/null | head -n1 || true)"
    log "kwokctl already installed: $(kwokctl --version 2>/dev/null | head -n1 || true)"
    return 0
  fi

  log "installing kwok ${KWOK_VERSION}"
  curl -fsSL -o /usr/local/bin/kwok \
    "https://github.com/kubernetes-sigs/kwok/releases/download/${KWOK_VERSION}/kwok-linux-amd64"
  chmod +x /usr/local/bin/kwok

  curl -fsSL -o /usr/local/bin/kwokctl \
    "https://github.com/kubernetes-sigs/kwok/releases/download/${KWOK_VERSION}/kwokctl-linux-amd64"
  chmod +x /usr/local/bin/kwokctl

  curl -fsSL -o /usr/local/share/kwok.yaml \
    "https://github.com/kubernetes-sigs/kwok/releases/download/${KWOK_VERSION}/kwok.yaml"
}

install_clusterloader2_optional() {
  if [ "${INSTALL_CLUSTERLOADER2}" != "true" ]; then
    return 0
  fi

  if command -v clusterloader2 >/dev/null 2>&1; then
    log "clusterloader2 already installed"
    return 0
  fi

  log "installing clusterloader2 (optional)"
  apt-get install -y golang-go make git

  local tmp_dir
  tmp_dir=$(mktemp -d)
  git clone --depth 1 https://github.com/kubernetes/perf-tests.git "${tmp_dir}/perf-tests"
  (
    cd "${tmp_dir}/perf-tests/clusterloader2"
    make
    install -m 0755 ./_output/bin/clusterloader2 /usr/local/bin/clusterloader2
  )
  rm -rf "${tmp_dir}"
}

tune_kernel_limits() {
  cat >/etc/sysctl.d/99-phase6-k3s.conf <<'EOF'
fs.inotify.max_user_instances = 8192
fs.inotify.max_user_watches = 1048576
fs.file-max = 2097152
EOF
  sysctl --system >/dev/null 2>&1 || true
}

print_manifest() {
  log "tool manifest:"
  for tool in docker etcd etcdctl etcdutl ghz grpcurl k3s kubectl kwok kwokctl clusterloader2 iostat pidstat vmstat jq; do
    if command -v "${tool}" >/dev/null 2>&1; then
      printf '  - %s: %s\n' "${tool}" "$(${tool} --version 2>/dev/null | head -n1 || command -v "${tool}")"
    else
      printf '  - %s: <missing>\n' "${tool}"
    fi
  done
}

main() {
  need_root
  apt_install_base
  install_k3s
  install_etcd_binaries
  install_kubectl
  install_kwok
  install_clusterloader2_optional
  tune_kernel_limits
  print_manifest
}

main "$@"
