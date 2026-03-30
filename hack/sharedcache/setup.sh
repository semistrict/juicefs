#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
NUM_MOUNTS="${NUM_MOUNTS:-3}"
CACHE_SIZE="${CACHE_SIZE:-8G}"

# Lima VM config (macOS only)
VM_NAME="jfs-cache"

log() { echo "==> $*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

# run() executes a command either locally (Linux) or in the Lima VM (macOS).
if [ "$(uname -s)" = "Linux" ]; then
    run() { bash -c "$*"; }
    NEED_LIMA=false
else
    run() { limactl shell "$VM_NAME" -- bash -c "$*"; }
    NEED_LIMA=true
fi

cmd_up() {
    if [ "$NEED_LIMA" = true ]; then
        log "Creating Lima VM: $VM_NAME"
        if ! limactl list -q 2>/dev/null | grep -q "^${VM_NAME}$"; then
            limactl create --name="$VM_NAME" "$SCRIPT_DIR/lima.yaml"
        fi
        limactl start "$VM_NAME" || true
    fi

    log "Checking prerequisites..."
    local missing=""
    run 'command -v go' >/dev/null 2>&1       || missing="$missing go"
    run 'command -v redis-cli' >/dev/null 2>&1 || missing="$missing redis"
    run 'command -v fusermount3 || command -v fusermount' >/dev/null 2>&1 || missing="$missing fuse3"
    run 'command -v gcc' >/dev/null 2>&1       || missing="$missing build-essential"

    if [ -n "$missing" ]; then
        die "Missing:$missing — install them first (or use Lima on macOS: see lima.yaml)"
    fi

    run 'redis-cli ping' >/dev/null 2>&1 || die "Redis is not running"
    log "All prerequisites OK"
}

cmd_build() {
    log "Building JuiceFS..."
    run "
        cd '$REPO_ROOT'
        go build -ldflags='-s -w' -o /tmp/juicefs .
        sudo cp /tmp/juicefs /usr/local/bin/juicefs
        juicefs version
    "

    log "Building checker..."
    run "
        cd '$REPO_ROOT'
        go build -o /tmp/checker ./hack/sharedcache/checker/
        sudo cp /tmp/checker /usr/local/bin/checker
    "
    log "Build complete"
}

cmd_start() {
    log "Starting cache server and $NUM_MOUNTS JuiceFS mounts..."

    cmd_stop 2>/dev/null || true
    run "sudo rm -rf /var/jfs-test-* /tmp/jfs-cache-*.sock /mnt/jfs*"

    run "
        sudo mkdir -p /var/jfs-test-cache /var/jfs-test-object
        sudo chown \$(id -u):\$(id -g) /var/jfs-test-cache /var/jfs-test-object
        for i in \$(seq 0 $((NUM_MOUNTS - 1))); do
            sudo mkdir -p /mnt/jfs\$i
        done
    "

    run "redis-cli -n 1 FLUSHDB" >/dev/null

    log "Formatting JuiceFS volume..."
    run "juicefs format --storage file --bucket /var/jfs-test-object redis://127.0.0.1:6379/1 test-vol"

    log "Starting cache server..."
    run "
        nohup juicefs cache-server \
            --cache-dir=/var/jfs-test-cache \
            --cache-size=$CACHE_SIZE \
            --metrics=0.0.0.0:9568 \
            /tmp/jfs-cache-data.sock \
            /tmp/jfs-cache-ctrl.sock \
            > /tmp/cache-server.log 2>&1 &
        echo \$! > /tmp/cache-server.pid
    "
    run "
        for i in \$(seq 1 30); do
            [ -S /tmp/jfs-cache-data.sock ] && [ -S /tmp/jfs-cache-ctrl.sock ] && break
            sleep 0.1
        done
        [ -S /tmp/jfs-cache-data.sock ] || { echo 'cache server sockets not found'; exit 1; }
    "
    log "Cache server started (pid $(run 'cat /tmp/cache-server.pid'))"

    for i in $(seq 0 $((NUM_MOUNTS - 1))); do
        log "Mounting /mnt/jfs$i..."
        run "
            sudo juicefs mount \
                --cache-server-data=/tmp/jfs-cache-data.sock \
                --cache-server-ctrl=/tmp/jfs-cache-ctrl.sock \
                --cache-size=0 \
                --metrics=127.0.0.1:$((9570 + i)) \
                --no-usage-report \
                -d \
                redis://127.0.0.1:6379/1 \
                /mnt/jfs$i
        "
    done

    run "
        for i in \$(seq 0 $((NUM_MOUNTS - 1))); do
            mountpoint -q /mnt/jfs\$i || { echo '/mnt/jfs'\$i' not mounted'; exit 1; }
        done
    "
    log "All $NUM_MOUNTS mounts ready"
}

cmd_stop() {
    log "Stopping mounts and cache server..."
    for i in $(seq 0 $((NUM_MOUNTS - 1))); do
        run "sudo juicefs umount /mnt/jfs$i 2>/dev/null" || true
    done
    run "[ -f /tmp/cache-server.pid ] && kill \$(cat /tmp/cache-server.pid) 2>/dev/null" || true
    run "rm -f /tmp/cache-server.pid"
    log "Stopped"
}

cmd_test() {
    log "Running consistency checker..."

    local mount_list=""
    for i in $(seq 0 $((NUM_MOUNTS - 1))); do
        [ -n "$mount_list" ] && mount_list="$mount_list,"
        mount_list="${mount_list}/mnt/jfs$i"
    done

    run "checker \
        --mounts=$mount_list \
        --files=${NUM_FILES:-1000} \
        --writers=${WRITERS:-8} \
        --readers=${READERS:-8} \
        --read-rounds=${READ_ROUNDS:-3} \
        --min-size=${MIN_SIZE:-524288} \
        --max-size=${MAX_SIZE:-10485760}"

    log "Cache server metrics:"
    run "wget -qO- http://127.0.0.1:9568/metrics | grep -E '^juicefs_blockcache_(writes|drops|evicts|write_bytes) '" || true

    log "Client cache metrics:"
    for i in $(seq 0 $((NUM_MOUNTS - 1))); do
        log "  /mnt/jfs$i:"
        run "wget -qO- http://127.0.0.1:$((9570 + i))/metrics | grep -E 'juicefs_blockcache_(hits|miss|hit_bytes|miss_bytes)\{'" || true
    done
}

cmd_logs() {
    run "cat /tmp/cache-server.log"
}

cmd_destroy() {
    if [ "$NEED_LIMA" = true ]; then
        log "Destroying Lima VM: $VM_NAME"
        limactl delete --force "$VM_NAME" 2>/dev/null || true
        log "VM destroyed"
    else
        cmd_stop 2>/dev/null || true
        run "sudo rm -rf /var/jfs-test-* /tmp/jfs-cache-*.sock /mnt/jfs*"
        log "Cleaned up"
    fi
}

usage() {
    cat <<'EOF'
Shared cache integration test.

Runs a cache-server + multiple FUSE mounts on the same machine, then
exercises them with a consistency checker that writes 1000 files from
random mounts and verifies SHA-256 hashes across all mounts.

On macOS, uses a Lima VM. On Linux, runs directly.

Usage: setup.sh <command>

Commands:
  up       Prepare the environment (Lima VM on macOS, prereq check on Linux)
  build    Build juicefs and the checker binary
  start    Format volume, start cache-server, mount N JuiceFS instances
  test     Run the consistency checker workload
  stop     Unmount and stop cache server
  logs     Show cache server logs
  destroy  Tear everything down (delete VM on macOS, cleanup on Linux)

Environment variables:
  NUM_MOUNTS   Number of FUSE mounts (default: 3)
  CACHE_SIZE   Cache server size (default: 8G)
  NUM_FILES    Files to write (default: 1000)
  WRITERS      Writer goroutines (default: 8)
  READERS      Reader goroutines (default: 8)
  READ_ROUNDS  Read rounds (default: 3)
  MIN_SIZE     Min file size in bytes (default: 524288)
  MAX_SIZE     Max file size in bytes (default: 10485760)

Quick start (Linux):
  sudo apt install redis fuse3 build-essential
  ./setup.sh up && ./setup.sh build && ./setup.sh start && ./setup.sh test

Quick start (macOS with Lima):
  brew install lima
  ./setup.sh up && ./setup.sh build && ./setup.sh start && ./setup.sh test
EOF
}

case "${1:-}" in
    up)       cmd_up ;;
    build)    cmd_build ;;
    start)    cmd_start ;;
    test)     cmd_test ;;
    stop)     cmd_stop ;;
    logs)     cmd_logs ;;
    destroy)  cmd_destroy ;;
    *)        usage ;;
esac
