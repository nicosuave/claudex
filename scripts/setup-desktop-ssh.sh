#!/bin/bash
# Compatibility entry point for the native desktop installer.
set -euo pipefail
project=$(cd "$(dirname "$0")/.." && pwd)
case "${1:-}" in
  --prepare-only)
    printf 'Preparation-only mode was removed. Run the native install command to install the SSH connection and service together.\n' >&2
    exit 2 ;;
  --authorize-local) shift ;;
esac
binary=${CLAUDE_CODEX_INSTALLER:-$project/target/release/claude-codex-server}
if ! test -x "$binary"; then
  printf 'Build first: cargo build --locked --release --bin claude-codex-server\nOr set CLAUDE_CODEX_INSTALLER to the built executable.\n' >&2
  exit 1
fi
exec "$binary" install "$@"
