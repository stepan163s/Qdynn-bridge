#!/usr/bin/env bash
# Local-file deployment. QDYNN_ROOT stages files offline without touching services.
set -euo pipefail
umask 077
usage() { echo 'Usage: manage.sh install BINARY CONFIG | update BINARY | rollback' >&2; exit 2; }
[[ $# -ge 1 ]] || usage
operation=$1; shift
case "$operation:$#" in install:2|update:1|rollback:0) ;; *) usage;; esac
root=${QDYNN_ROOT:-}
if [[ -n $root ]]; then
    [[ $root = /* && $root != / && $root != *'/../'* ]] || { echo 'QDYNN_ROOT must be an absolute staging directory' >&2; exit 2; }
else
    [[ $(uname -s) = Linux && $EUID = 0 ]] || { echo 'Run on Linux as root, or set QDYNN_ROOT for offline staging' >&2; exit 2; }
    command -v systemctl >/dev/null
fi
base="$root/usr/local/bin"
conf="$root/etc/qdynn/config.toml"
unit="$root/etc/systemd/system/qdynn-bridge.service"
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
mkdir -p "$base" "$(dirname "$conf")" "$(dirname "$unit")"
# mkdir is portable and prevents two concurrent updates; never remove another owner's lock.
lock="$base/.qdynn-deploy-lock"
mkdir "$lock" 2>/dev/null || { echo 'Another deployment holds the lock; inspect before removing it' >&2; exit 1; }
candidate="$base/.qdynn-candidate"
trap 'rm -f "$candidate"; rmdir "$lock"' EXIT
binary="$base/qdynn-bridge"
previous="$base/qdynn-bridge.previous"
validate() {
    # Ignore inherited QDYNN_* overrides; validate exactly the installed config.
    env -i PATH="$PATH" "$1" check-config --config-file "$2" >/dev/null
}
activate() {
    [[ -z $root ]] || return 0
    systemctl restart qdynn-bridge.service || return 1
    sleep 1
    systemctl is-active --quiet qdynn-bridge.service
}
case $operation in
    install)
        [[ ! -e $binary && ! -e $conf && ! -e $unit ]] || { echo 'Installation exists: use update; existing config/unit will not be overwritten' >&2; exit 1; }
        install -m 0755 "$1" "$candidate"
        validate "$candidate" "$2"
        if [[ -z $root ]]; then
            id qdynn >/dev/null 2>&1 || useradd --system --user-group --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin qdynn
        fi
        install -m 0640 "$2" "$conf"
        install -m 0644 "$script_dir/qdynn-bridge.service" "$unit"
        if [[ -z $root ]]; then chown root:qdynn "$conf"; chmod 0750 "$(dirname "$conf")"; chown root:qdynn "$(dirname "$conf")"; fi
        mv -f "$candidate" "$binary"
        if [[ -z $root ]]; then
            systemctl daemon-reload
            systemctl enable qdynn-bridge.service
            activate || { echo 'Service failed to start; inspect journalctl -u qdynn-bridge. Config retained.' >&2; exit 1; }
        fi
        ;;
    update|rollback)
        [[ -f $binary && -f $conf ]] || { echo 'Install first' >&2; exit 1; }
        if [[ $operation = update ]]; then source_binary=$1; else source_binary=$previous; fi
        [[ -f $source_binary ]] || { echo 'No candidate/previous binary' >&2; exit 1; }
        install -m 0755 "$source_binary" "$candidate"
        validate "$candidate" "$conf"
        # Keep current executable for automatic rollback if restart fails.
        install -m 0755 "$binary" "$lock/original"
        mv -f "$candidate" "$binary"
        if ! activate; then
            install -m 0755 "$lock/original" "$candidate"
            mv -f "$candidate" "$binary"
            rm -f "$lock/original"
            activate || { echo 'Rollback restart also failed; inspect the service journal' >&2; exit 1; }
            echo 'Candidate failed to start; previous binary restored' >&2
            exit 1
        fi
        mv -f "$lock/original" "$previous"
        ;;
esac
if [[ -n $root ]]; then echo 'OK: files staged offline; no users or services changed';
else echo 'OK: relay service active; run qdynn-bridge doctor --config-file /etc/qdynn/config.toml'; fi
