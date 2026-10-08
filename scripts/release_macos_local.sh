#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
repo=nicosuave/claudex
manifest_version=$(awk '/^version = / {gsub(/"/, "", $3); print $3; exit}' Cargo.toml)
version=${1:-$manifest_version}
[[ "$version" == "$manifest_version" && "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]
tag="v$version"
profile=${NOTARY_PROFILE:-sidequery-notarization}
artifacts="$root/target/release-assets"

[[ "$(uname -s)" == Darwin ]] || { echo "Run macOS releases on a Mac" >&2; exit 1; }
[[ "$(git rev-parse HEAD)" == "$(git rev-parse "$tag^{commit}")" ]] || {
  echo "HEAD must be the commit tagged $tag" >&2; exit 1;
}
if [[ -n "$(git status --porcelain --untracked-files=normal)" ]]; then
  echo "Release from a clean checkout" >&2
  exit 1
fi
remote_tag=$(git ls-remote origin "refs/tags/$tag^{}" "refs/tags/$tag" | awk 'NR == 1 {hash=$1} /\^\{\}$/ {hash=$1} END {print hash}')
[[ "$remote_tag" == "$(git rev-parse HEAD)" ]] || { echo "Remote tag does not match HEAD" >&2; exit 1; }
gh release view "$tag" --repo "$repo" >/dev/null

# Use the maintainer's existing Keychain entries; never export signing secrets.
identity=${CODESIGN_IDENTITY:-}
if [[ -z "$identity" ]]; then
  identity=$(security find-identity -v -p codesigning | sed -n '/"Developer ID Application:/{s/.*"\(Developer ID Application:.*\)"/\1/p;q;}')
fi
[[ -n "$identity" ]] || { echo "No Developer ID Application signing identity found" >&2; exit 1; }
xcrun notarytool history --keychain-profile "$profile" --output-format json >/dev/null

# Production builds bypass Boxington and any compiler wrapper deliberately.
# rustup selects a matching cargo/rustc pair with both macOS targets installed.
cargo=$(rustup which cargo)
rustc=$(rustup which rustc)
mkdir -p "$artifacts"

build_macos() {
  local target=$1 arch=$2
  local binary="$root/target/$target/release/claude-codex-server"
  local archive="$artifacts/claudex-${version}-macos-${arch}-notarization.zip"
  local result="$artifacts/notarization-${arch}.json"
  env RUSTC_WORKSPACE_WRAPPER= CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER= \
    RUSTC="$rustc" RUSTC_WRAPPER= CARGO_BUILD_RUSTC_WRAPPER= \
    CARGO_TARGET_DIR="$root/target" \
    CARGO_ENCODED_RUSTFLAGS="--remap-path-prefix=$root=claudex"$'\x1f'"--remap-path-prefix=$HOME/.cargo=cargo"$'\x1f'"--remap-path-prefix=$HOME/.rustup=rustup" \
    "$cargo" build --locked --release --target "$target" --bin claude-codex-server
  codesign --force --options runtime --timestamp --sign "$identity" \
    --identifier com.nicosuave.claudex "$binary"
  codesign --verify --strict "$binary"
  ditto -c -k --keepParent "$binary" "$archive"
  xcrun notarytool submit "$archive" --keychain-profile "$profile" --wait --output-format json > "$result"
  [[ "$(plutil -extract status raw "$result")" == Accepted ]] || {
    echo "Notarization was not accepted; inspect $result" >&2; exit 1;
  }
  "$root/scripts/package-release.sh" "$version" "$target" macos "$arch"
  rm -f "$archive"
}

build_macos aarch64-apple-darwin arm64
build_macos x86_64-apple-darwin x86_64

# Never overwrite published artifacts. A partial upload can be resumed manually
# after comparing checksums, without replacing the immutable version tag.
gh release upload "$tag" --repo "$repo" \
  "$artifacts/claudex-${version}-macos-arm64.tar.gz" \
  "$artifacts/claudex-${version}-macos-arm64.tar.gz.sha256" \
  "$artifacts/claudex-${version}-macos-x86_64.tar.gz" \
  "$artifacts/claudex-${version}-macos-x86_64.tar.gz.sha256"

# The initial formula is added after all four archives exist. Subsequent
# releases use the tap's existing generic update workflow, as Comradex does.
if gh api repos/nicosuave/homebrew-tap/contents/Formula/claudex.rb >/dev/null 2>&1; then
  "$root/scripts/update-homebrew.sh" "$version"
else
  echo "First release: add Formula/claudex.rb to nicosuave/homebrew-tap using the published archive checksums."
fi
echo "Published signed and notarized macOS assets for $tag"
