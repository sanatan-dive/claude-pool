#!/bin/sh
# Sign in to each MCP server once and use that sign-in from every Claude account.
#   share-mcp.sh login [name]   sign in (once per server; tokens are kept in ~/.mcp-auth)
#   share-mcp.sh apply          add every server to every account in the pool config
#   share-mcp.sh list           show the servers file
set -eu

servers=${CLAUDE_POOL_MCP_SERVERS:-$HOME/.config/claude-pool/mcp-servers.txt}
config=${CLAUDE_POOL_CONFIG:-$HOME/.config/claude-pool/config.json}
remote=${CLAUDE_POOL_MCP_REMOTE:-mcp-remote@0.14.3}

usage() { sed -n '2,5p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
die() { echo "share-mcp: $*" >&2; exit 1; }

entries() {
  [ -f "$servers" ] || die "$servers not found; copy mcp-servers.example.txt there and keep the servers you use"
  grep -v '^[[:space:]]*#' "$servers" | grep -v '^[[:space:]]*$'
}

account_dirs() {
  [ -f "$config" ] || die "$config not found; run ./install.sh first"
  python3 -c 'import json,sys
for a in json.load(open(sys.argv[1]))["accounts"]: print(a["config_dir"])' "$config"
}

case "${1:-}" in
  login)
    want=${2:-}
    entries | while read -r name url port; do
      [ -z "$want" ] || [ "$want" = "$name" ] || continue
      echo "== $name: open the link below and approve (callback on port $port)"
      npx -y "$remote" "$url" "$port" </dev/null || echo "share-mcp: $name sign-in failed" >&2
    done
    ;;
  apply)
    account_dirs | while read -r dir; do
      entries | while read -r name url port; do
        CLAUDE_CONFIG_DIR="$dir" claude mcp remove --scope user "$name" </dev/null >/dev/null 2>&1 || true
        CLAUDE_CONFIG_DIR="$dir" claude mcp add --scope user "$name" -- npx -y "$remote" "$url" "$port" </dev/null >/dev/null \
          && echo "added $name -> $dir" || echo "share-mcp: could not add $name to $dir" >&2
      done
    done
    ;;
  list) entries ;;
  *) usage ;;
esac
