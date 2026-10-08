# Releasing Claudex

The release flow follows Comradex: CI on Linux and macOS, Linux release builds in
GitHub Actions, and signed/notarized macOS builds on the maintainer's Mac. Apple
signing certificates and notarization credentials stay in the local Keychain.

## Prepare and tag

1. Update the package version in `Cargo.toml` and `Cargo.lock`. The advertised
   Codex protocol version is independent; do not change it for a product release.
2. Commit and push the reviewed source. Require the `ci` workflow to pass for
   that exact commit on both platforms. The native-provider tests are opt-in and
   need local authenticated installations; CI runs the deterministic fixture suite.
3. From the clean release commit, create and push `vVERSION`:

   ```sh
   git tag v0.1.0
   git push origin v0.1.0
   ```

The `release` workflow builds Linux ARM64 and x86-64 archives, smoke-tests the
executables, verifies checksums, and creates the GitHub release. Production
release builds do not restore or publish compiler caches.

## macOS signing and publication

Wait for the Linux workflow to succeed and create the GitHub release. On the
maintainer's Mac, with both Rust macOS targets installed and GitHub CLI logged in:

```sh
rustup target add aarch64-apple-darwin x86_64-apple-darwin
scripts/release_macos_local.sh 0.1.0
```

The script requires the local and remote tag to match HEAD and a clean checkout.
It uses the existing Developer ID Application identity and the
`sidequery-notarization` Keychain profile, or the `CODESIGN_IDENTITY` and
`NOTARY_PROFILE` overrides. It builds without compiler wrappers, remaps local
source paths out of compiler output, signs both architectures, requires accepted
Apple notarization, and uploads archives with SHA-256 files. Standalone command
line binaries cannot be stapled; the notarization ticket is served by Apple.

Archives contain `claudex`, `LICENSE`, `NOTICE`, and the vendored protocol license.
The Cargo binary target retains the name `claude-codex-server`; release archives
and Homebrew expose the user command as `claudex`. The installed desktop launcher
and protocol identity retain their existing names for compatibility.

Published tags and assets are immutable. The scripts do not force-push tags or
overwrite assets. If a run partially uploads, compare the existing checksums and
resume only the missing uploads. Fix a bad published artifact in a new version.

## Homebrew

The formula is `nicosuave/homebrew-tap:Formula/claudex.rb`. On the first release,
add the formula only after all four archives exist, using their published hashes.
Thereafter the macOS release script calls `scripts/update-homebrew.sh VERSION`,
which dispatches the tap's generic workflow, correlates the run to Claudex and
the requested version, waits for success, and retries once for CDN propagation.

Verify the resulting formula's four URLs/checksums against the release assets,
then check a real installation:

```sh
brew update
brew install nicosuave/tap/claudex
brew test nicosuave/tap/claudex
claudex --version
claudex install --help
```

Installing the formula alone does not start a service or change SSH settings.
`claudex install` creates/updates the dedicated desktop connection and refuses
to restart active chats. `claudex doctor` verifies an already-installed instance.
