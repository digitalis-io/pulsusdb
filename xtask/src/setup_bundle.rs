//! `cargo xtask setup-bundle <inputs> <out-dir>` (issue #746): the setup
//! form's files, from a file of `field=value` lines. A field given on
//! several lines holds each value on its own line, which is how `hosts`
//! and `keepers` are listed. Blank lines and lines starting with `#` are
//! skipped.

use std::path::PathBuf;

#[derive(clap::Parser)]
pub struct Args {
    /// The inputs: `field=value` per line, the setup form's field names.
    inputs: PathBuf,
    /// The directory the files are written under.
    out_dir: PathBuf,
}

pub fn run(args: Args) -> anyhow::Result<()> {
    let text = std::fs::read_to_string(&args.inputs)
        .map_err(|e| anyhow::anyhow!("{}: {e}", args.inputs.display()))?;
    let mut pairs = Vec::new();
    for (n, line) in text.lines().enumerate() {
        if line.trim().is_empty() || line.starts_with('#') {
            continue;
        }
        let (field, value) = line.split_once('=').ok_or_else(|| {
            anyhow::anyhow!("{}:{}: not field=value", args.inputs.display(), n + 1)
        })?;
        pairs.push((field.to_string(), value.to_string()));
    }
    let v = pulsus_setup::validate(&pulsus_setup::Inputs::from_pairs(pairs)).map_err(|errors| {
        let lines: Vec<String> = errors
            .iter()
            .map(|e| format!("{}: {}", e.field, e.message))
            .collect();
        anyhow::anyhow!("the inputs are refused:\n{}", lines.join("\n"))
    })?;
    let files = pulsus_setup::bundle(&v);
    pulsus_setup::write_bundle(&args.out_dir, &files)?;
    println!(
        "{} files written to {}",
        files.len(),
        args.out_dir.display()
    );
    Ok(())
}
