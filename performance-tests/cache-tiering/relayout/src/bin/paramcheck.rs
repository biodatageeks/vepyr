//! paramcheck A.parquet B.parquet [ORIGINAL.parquet]
//! Exit 1 if A and B differ in any Parquet setting (page boundaries excluded).
use std::path::PathBuf;

use relayout::{page_stats, settings_differences, summarize};

fn main() -> relayout::Res<()> {
    let files: Vec<PathBuf> = std::env::args().skip(1).map(PathBuf::from).collect();
    if files.len() < 2 {
        return Err("usage: paramcheck A.parquet B.parquet [ORIGINAL.parquet]".into());
    }
    let diffs = settings_differences(&files[0], &files[1])?;
    let original_vs_a = match files.get(2) {
        Some(o) => Some(settings_differences(o, &files[0])?),
        None => None,
    };
    let mut per_file = Vec::new();
    for f in &files {
        let s = summarize(f)?;
        per_file.push(serde_json::json!({
            "file": f.display().to_string(),
            "rows": s.rows, "runs": s.runs, "start_pages": s.start_pages, "bytes": s.bytes,
            "pages": page_stats(f)?,
        }));
    }
    let report = serde_json::json!({
        "identical_settings": diffs.is_empty(),
        "differences_a_b": diffs,
        "differences_original_a": original_vs_a,
        "files": per_file,
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    if !diffs.is_empty() {
        std::process::exit(1);
    }
    Ok(())
}
