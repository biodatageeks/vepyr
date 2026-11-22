//! relayout --input IN.parquet --output OUT.parquet --layout tiered|sorted [--batch-rows 8192]
use std::path::PathBuf;

use relayout::{relayout, Layout};

fn main() -> relayout::Res<()> {
    let args: Vec<String> = std::env::args().collect();
    let get = |flag: &str| {
        args.iter()
            .position(|a| a == flag)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };
    let usage = "usage: relayout --input IN --output OUT --layout tiered|sorted [--batch-rows N]";
    let input = PathBuf::from(get("--input").ok_or(usage)?);
    let output = PathBuf::from(get("--output").ok_or(usage)?);
    let layout = match get("--layout").as_deref() {
        Some("tiered") => Layout::Tiered,
        Some("sorted") => Layout::Sorted,
        _ => return Err(usage.into()),
    };
    let batch_rows: usize = get("--batch-rows")
        .map(|v| v.parse())
        .transpose()?
        .unwrap_or(8192);
    if output.exists() {
        return Err(format!("{} exists; refusing to overwrite", output.display()).into());
    }
    let s = relayout(&input, &output, layout, batch_rows)?;
    println!(
        "{}",
        serde_json::json!({"rows": s.rows, "runs": s.runs, "start_pages": s.start_pages, "bytes": s.bytes})
    );
    Ok(())
}
