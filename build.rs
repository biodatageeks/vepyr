fn main() {
    // Capture rustc version at compile time for ABI compatibility checks.
    let output = std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .expect("failed to run rustc --version");
    let version = String::from_utf8_lossy(&output.stdout).trim().to_string();
    println!("cargo:rustc-env=RUSTC_VERSION={version}");
}
