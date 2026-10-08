#![cfg(unix)]

use std::{fs, os::unix::fs::PermissionsExt, path::Path, process::Command};

fn run(root: &Path, program: &str, args: &[&str]) -> std::process::Output {
    let result = Command::new(program)
        .args(args)
        .current_dir(root)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{program}: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    result
}

#[test]
fn published_archives_have_executable_licenses_and_verifiable_checksums() {
    let fixture = tempfile::tempdir().unwrap();
    let root = fixture.path();
    fs::create_dir_all(root.join("scripts")).unwrap();
    fs::create_dir_all(root.join("licenses")).unwrap();
    fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/package-release.sh"),
        root.join("scripts/package-release.sh"),
    )
    .unwrap();
    fs::write(root.join("Cargo.toml"), "[package]\nversion = \"1.2.3\"\n").unwrap();
    fs::write(root.join("LICENSE"), "project license").unwrap();
    fs::write(root.join("NOTICE"), "dependency notice").unwrap();
    fs::write(root.join("licenses/protocol.txt"), "protocol license").unwrap();

    for (target, platform, arch) in [
        ("aarch64-apple-darwin", "macos", "arm64"),
        ("x86_64-apple-darwin", "macos", "x86_64"),
        ("aarch64-unknown-linux-gnu", "linux", "arm64"),
        ("x86_64-unknown-linux-gnu", "linux", "x86_64"),
    ] {
        let release = root.join(format!("target/{target}/release"));
        fs::create_dir_all(&release).unwrap();
        let binary = release.join("claude-codex-server");
        fs::write(&binary, "#!/bin/sh\nprintf 'release-fixture\\n'\n").unwrap();
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o755)).unwrap();
        run(
            root,
            "/bin/bash",
            &[
                "scripts/package-release.sh",
                "1.2.3",
                target,
                platform,
                arch,
            ],
        );
        let archive = format!("claudex-1.2.3-{platform}-{arch}.tar.gz");
        let assets = root.join("target/release-assets");
        run(
            &assets,
            "shasum",
            &["-a", "256", "--check", &format!("{archive}.sha256")],
        );
        let unpacked = root.join(format!("unpacked-{platform}-{arch}"));
        fs::create_dir(&unpacked).unwrap();
        run(
            &unpacked,
            "tar",
            &["-xzf", assets.join(archive).to_str().unwrap()],
        );
        let output = run(&unpacked, "./claudex", &["--version"]);
        assert_eq!(output.stdout, b"release-fixture\n");
        for name in ["LICENSE", "NOTICE", "licenses/protocol.txt"] {
            assert_eq!(
                fs::read(unpacked.join(name)).unwrap(),
                fs::read(root.join(name)).unwrap()
            );
        }
    }
    let mismatch = Command::new("/bin/bash")
        .args([
            "scripts/package-release.sh",
            "1.2.4",
            "aarch64-apple-darwin",
            "macos",
            "arm64",
        ])
        .current_dir(root)
        .status()
        .unwrap();
    assert!(!mismatch.success());
}
