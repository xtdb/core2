use std::error::Error;
use std::process::Command;

fn main() -> Result<(), Box<dyn Error>> {
    let output = Command::new("git").args(&["rev-parse", "HEAD"]).output()?;
    println!(
        "cargo:rustc-env=GIT_HASH={}",
        String::from_utf8(output.stdout)?
    );

    Ok(())
}
