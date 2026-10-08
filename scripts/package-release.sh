#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
if [[ $# != 4 ]]; then
  echo "Usage: $0 VERSION RUST_TARGET PLATFORM ARCH" >&2
  exit 2
fi
version=$1
target=$2
platform=$3
arch=$4
manifest_version=$(awk '/^version = / {gsub(/"/, "", $3); print $3; exit}' Cargo.toml)
[[ "$version" == "$manifest_version" && "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]
case "$target:$platform:$arch" in
  aarch64-apple-darwin:macos:arm64|x86_64-apple-darwin:macos:x86_64|aarch64-unknown-linux-gnu:linux:arm64|x86_64-unknown-linux-gnu:linux:x86_64) ;;
  *) echo "Unsupported release target/platform/architecture" >&2; exit 2 ;;
esac

artifacts="$root/target/release-assets"
stage=$(mktemp -d "$root/target/release-stage.XXXXXX")
trap 'rm -rf "$stage"' EXIT
mkdir -p "$artifacts"
install -m 755 "target/$target/release/claude-codex-server" "$stage/claudex"
cp LICENSE NOTICE "$stage/"
cp -R licenses "$stage/licenses"
artifact="claudex-${version}-${platform}-${arch}.tar.gz"
COPYFILE_DISABLE=1 tar -C "$stage" -czf "$artifacts/$artifact" claudex LICENSE NOTICE licenses
(
  cd "$artifacts"
  shasum -a 256 "$artifact" > "$artifact.sha256"
)
echo "$artifacts/$artifact"
