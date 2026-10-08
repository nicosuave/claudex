#!/bin/bash
# Compatibility entry point for the installed native service manager.
set -euo pipefail
binary=${CLAUDE_CODEX_INSTALLER:-$HOME/Library/Application Support/claude-codex/bin/claude-codex-server}
if ! test -x "$binary"; then
  printf 'Run claude-codex-server install first.\n' >&2
  exit 1
fi
exec "$binary" service install "$@"
