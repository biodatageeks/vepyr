//! Throwaway page-count measurement for the vepyr paper "76% fewer pages" claim.
//! PageDir / CoalescingAsyncReader / build_probe_starts are copied verbatim (logic)
//! from datafusion-bio-functions 46d8c20 (parquet_cache/page_dir.rs,
//! parquet_cache/variation_lookup.rs, cache/lookup_exec.rs, allele.rs); the reader
//! additionally records the requested and coalesced byte ranges.

use std::collections::HashSet;
use std::io::BufRead;
use std::sync::{Arc, Mutex};

use arrow_array::{Array, RecordBatch, UInt32Array, UInt64Array};
use bytes::Bytes;
use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection,
    RowSelector,
};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

const COALESCE_GAP_BYTES: u64 = 64 * 1024;

// ---------------------------------------------------------------- PageDir (copy)
struct PageDir {
    min: Vec<u64>,
    max: Vec<u64>,
    first_row: Vec<u64>,
    end_row: Vec<u64>,
    runs: Vec<(usize, usize)>,
}

impl PageDir {
    fn build(meta: &ArrowReaderMetadata, key_leaf: usize) -> Self {
        use parquet::file::page_index::column_index::ColumnIndexMetaData;
        let md = meta.metadata();
        let ci = md.column_index().expect("column index");
        let oi = md.offset_index().expect("offset index");
        let (mut min, mut max, mut first_row, mut end_row) = (vec![], vec![], vec![], vec![]);
        let mut rg_start = 0u64;
        for rg in 0..md.num_row_groups() {
            let rg_rows = md.row_group(rg).num_rows() as u64;
            let (mins, maxs): (Vec<u64>, Vec<u64>) = match &ci[rg][key_leaf] {
                ColumnIndexMetaData::INT32(p) => (
                    p.min_values().iter().map(|&v| v as u32 as u64).collect(),
                    p.max_values().iter().map(|&v| v as u32 as u64).collect(),
                ),
                _ => panic!("not int32"),
            };
            let locs = oi[rg][key_leaf].page_locations();
            for p in 0..mins.len() {
                let fr = rg_start + locs[p].first_row_index as u64;
                let er = if p + 1 < locs.len() {
                    rg_start + locs[p + 1].first_row_index as u64
                } else {
                    rg_start + rg_rows
                };
                min.push(mins[p]);
                max.push(maxs[p]);
                first_row.push(fr);
                end_row.push(er);
            }
            rg_start += rg_rows;
        }
        let mut runs = vec![];
        let mut s = 0usize;
        for i in 1..min.len() {
            if min[i] < min[i - 1] {
                runs.push((s, i));
                s = i;
            }
        }
        if !min.is_empty() {
            runs.push((s, min.len()));
        }
        Self { min, max, first_row, end_row, runs }
    }

    /// Same as `resolve_ranges`, but also returns the (sorted, deduped) page ids.
    fn resolve(&self, positions: &[u64]) -> (Vec<usize>, Vec<(u64, u64)>) {
        let mut pages: Vec<usize> = Vec::new();
        for &p in positions {
            for &(rs, re) in &self.runs {
                let mut lo = rs;
                let mut hi = re;
                while lo < hi {
                    let m = (lo + hi) / 2;
                    if self.max[m] < p {
                        lo = m + 1;
                    } else {
                        hi = m;
                    }
                }
                let mut pg = lo;
                while pg < re && self.min[pg] <= p {
                    pages.push(pg);
                    pg += 1;
                }
                if self.min[rs] <= p && p <= self.max[rs] {
                    pages.push(rs);
                }
            }
        }
        pages.sort_unstable();
        pages.dedup();
        let mut out: Vec<(u64, u64)> = Vec::new();
        for &pg in &pages {
            let (a, b) = (self.first_row[pg], self.end_row[pg]);
            if let Some(last) = out.last_mut()
                && last.1 == a
            {
                last.1 = b;
            } else {
                out.push((a, b));
            }
        }
        (pages, out)
    }

    fn run_of(&self, pg: usize) -> usize {
        self.runs.iter().position(|&(a, b)| pg >= a && pg < b).unwrap()
    }
}

fn selection_from_ranges(ranges: &[(u64, u64)]) -> RowSelection {
    let mut sel = Vec::new();
    let mut prev = 0u64;
    for &(a, b) in ranges {
        if a > prev {
            sel.push(RowSelector::skip((a - prev) as usize));
        }
        sel.push(RowSelector::select((b - a) as usize));
        prev = b;
    }
    RowSelection::from(sel)
}

fn selection_from_offsets(offsets: &[u64]) -> RowSelection {
    let mut sel = Vec::new();
    let mut prev = 0u64;
    for &off in offsets {
        if off > prev {
            sel.push(RowSelector::skip((off - prev) as usize));
        }
        sel.push(RowSelector::select(1));
        prev = off + 1;
    }
    RowSelection::from(sel)
}

// ------------------------------------------------- recording coalescing reader
#[derive(Default)]
struct Log {
    requested: Vec<(u64, u64)>,
    spans: Vec<(u64, u64)>,
}

struct RecReader {
    file: tokio::fs::File,
    gap: u64,
    log: Arc<Mutex<Log>>,
}

impl parquet::arrow::async_reader::AsyncFileReader for RecReader {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<u64>,
    ) -> futures::future::BoxFuture<'_, parquet::errors::Result<Bytes>> {
        use futures::FutureExt;
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        async move {
            let len = (range.end - range.start) as usize;
            self.file.seek(std::io::SeekFrom::Start(range.start)).await.unwrap();
            let mut buf = vec![0u8; len];
            self.file.read_exact(&mut buf).await.unwrap();
            let mut l = self.log.lock().unwrap();
            l.requested.push((range.start, range.end));
            l.spans.push((range.start, range.end));
            Ok(Bytes::from(buf))
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> futures::future::BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        use futures::FutureExt;
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        let gap = self.gap;
        async move {
            let n = ranges.len();
            let mut idx: Vec<usize> = (0..n).collect();
            idx.sort_by_key(|&i| ranges[i].start);
            let mut out: Vec<Option<Bytes>> = vec![None; n];
            {
                let mut l = self.log.lock().unwrap();
                for r in &ranges {
                    l.requested.push((r.start, r.end));
                }
            }
            let mut i = 0;
            while i < n {
                let mut j = i;
                let span_start = ranges[idx[i]].start;
                let mut span_end = ranges[idx[i]].end;
                while j + 1 < n && ranges[idx[j + 1]].start <= span_end.saturating_add(gap) {
                    j += 1;
                    span_end = span_end.max(ranges[idx[j]].end);
                }
                let len = (span_end - span_start) as usize;
                self.file.seek(std::io::SeekFrom::Start(span_start)).await.unwrap();
                let mut buf = vec![0u8; len];
                self.file.read_exact(&mut buf).await.unwrap();
                self.log.lock().unwrap().spans.push((span_start, span_end));
                let span = Bytes::from(buf);
                for &k in &idx[i..=j] {
                    let off = (ranges[k].start - span_start) as usize;
                    let l = (ranges[k].end - ranges[k].start) as usize;
                    out[k] = Some(span.slice(off..off + l));
                }
                i = j + 1;
            }
            Ok(out.into_iter().map(|x| x.unwrap()).collect())
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _o: Option<&'a ArrowReaderOptions>,
    ) -> futures::future::BoxFuture<
        'a,
        parquet::errors::Result<Arc<parquet::file::metadata::ParquetMetaData>>,
    > {
        use futures::FutureExt;
        async move { Err(parquet::errors::ParquetError::General("unused".into())) }.boxed()
    }
}

// ------------------------------------------------------ probe starts (copy)
fn push_unique(v: &mut Vec<i64>, s: i64) {
    if !v.contains(&s) {
        v.push(s);
    }
}
fn vcf_to_vep_input_start(pos: i64, r: &str, a: &str) -> i64 {
    let is_indel = r.len() != a.len();
    if is_indel && !r.is_empty() && !a.is_empty() && r.as_bytes()[0] == a.as_bytes()[0] {
        return pos + 1;
    }
    pos
}
fn common_prefix_len(l: &str, r: &str) -> usize {
    l.as_bytes().iter().zip(r.as_bytes()).take_while(|(a, b)| a == b).count()
}
fn canonical_event_lengths(r: &str, a: &str) -> (usize, usize) {
    let rb = r.as_bytes();
    let ab = a.as_bytes();
    let (mut rs, mut as_) = (0, 0);
    while rs < rb.len() && as_ < ab.len() && rb[rs] == ab[as_] {
        rs += 1;
        as_ += 1;
    }
    let (mut re, mut ae) = (rb.len(), ab.len());
    while re > rs && ae > as_ && rb[re - 1] == ab[ae - 1] {
        re -= 1;
        ae -= 1;
    }
    (re - rs, ae - as_)
}
fn build_probe_starts(s: i64, e: i64, r: &str, alt: &str, extended: bool) -> Vec<i64> {
    let mut v = Vec::with_capacity(6);
    push_unique(&mut v, s);
    if !extended {
        return v;
    }
    if s == e {
        push_unique(&mut v, s + 1);
    } else {
        push_unique(&mut v, e);
    }
    for a in alt.split(['|', ',']).filter(|a| !a.is_empty()) {
        push_unique(&mut v, vcf_to_vep_input_start(s, r, a));
        let sh = common_prefix_len(r, a);
        if sh == 0 {
            continue;
        }
        push_unique(&mut v, s + sh as i64);
    }
    for a in alt.split(['|', ',']).filter(|a| !a.is_empty()) {
        let (rl, al) = canonical_event_lengths(r, a);
        if rl == 0 || al != 0 {
            continue;
        }
        let dl = rl as i64;
        let mx = dl.min(32);
        for base in [s, s - 1] {
            for sh in 0..=mx {
                let cs = base + sh;
                let ce = cs + dl - 1;
                if cs > e || ce < s {
                    continue;
                }
                push_unique(&mut v, cs);
            }
        }
    }
    v
}

// ------------------------------------------------------------------- writer
fn props(schema: &arrow_schema::Schema, sort_cols: &[&str]) -> WriterProperties {
    let sorting: Vec<SortingColumn> = sort_cols
        .iter()
        .filter_map(|n| schema.index_of(n).ok())
        .map(|idx| SortingColumn { column_idx: idx as i32, descending: false, nulls_first: false })
        .collect();
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_dictionary_enabled(false)
        .set_data_page_size_limit(4 * 1024)
        .set_data_page_row_count_limit(512)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_max_row_group_row_count(Some(1_000_000))
        .set_sorting_columns(Some(sorting))
        .build()
}

/// mode "sorted": stable sort by start; mode "identity": original order.
fn rewrite(src: &str, dst: &str, mode: &str, chunk: usize) {
    let f = std::fs::File::open(src).unwrap();
    let b = ParquetRecordBatchReaderBuilder::try_new(f).unwrap().with_batch_size(65536);
    let schema = b.schema().clone();
    let reader = b.build().unwrap();
    let batches: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();
    let all = arrow_select::concat::concat_batches(&schema, &batches).unwrap();
    drop(batches);
    let n = all.num_rows();
    let starts = all
        .column(schema.index_of("start").unwrap())
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap()
        .clone();
    let mut idx: Vec<u64> = (0..n as u64).collect();
    if mode == "sorted" {
        idx.sort_by_key(|&i| starts.value(i as usize)); // stable
    }
    let sort_cols: &[&str] = if mode == "sorted" { &["start"] } else { &["tier", "start"] };
    let out = std::fs::File::create(dst).unwrap();
    let mut w = ArrowWriter::try_new(out, schema.clone(), Some(props(&schema, sort_cols))).unwrap();
    for c in idx.chunks(chunk) {
        let ix = UInt64Array::from(c.to_vec());
        let cols: Vec<_> = all
            .columns()
            .iter()
            .map(|col| arrow_select::take::take(col.as_ref(), &ix, None).unwrap())
            .collect();
        let rb = RecordBatch::try_new(schema.clone(), cols).unwrap();
        w.write(&rb).unwrap();
    }
    w.close().unwrap();
    eprintln!("wrote {dst} rows={n}");
}

// ------------------------------------------------------------------ measure
fn page_locs_for_leaf(meta: &ArrowReaderMetadata, leaf: usize) -> Vec<(u64, u64)> {
    let md = meta.metadata();
    let oi = md.offset_index().unwrap();
    let mut v = vec![];
    for rg in 0..md.num_row_groups() {
        for loc in oi[rg][leaf].page_locations() {
            v.push((loc.offset as u64, loc.offset as u64 + loc.compressed_page_size as u64));
        }
    }
    v
}

/// Count pages (from `locs`) fully inside any of `spans`, and pages intersecting.
fn pages_in_spans(locs: &[(u64, u64)], spans: &[(u64, u64)]) -> (usize, usize) {
    let mut full = 0;
    let mut touch = 0;
    for &(a, b) in locs {
        let mut f = false;
        let mut t = false;
        for &(s, e) in spans {
            if a >= s && b <= e {
                f = true;
            }
            if a < e && b > s {
                t = true;
            }
        }
        full += f as usize;
        touch += t as usize;
    }
    (full, touch)
}

fn merge_spans(mut v: Vec<(u64, u64)>) -> Vec<(u64, u64)> {
    v.sort();
    let mut out: Vec<(u64, u64)> = vec![];
    for (a, b) in v {
        if let Some(l) = out.last_mut()
            && a <= l.1
        {
            l.1 = l.1.max(b);
        } else {
            out.push((a, b));
        }
    }
    out
}

async fn measure(path: &str, tsv: &str, buf: usize, extended: bool, payload: bool) {
    let file = std::fs::File::open(path).unwrap();
    #[allow(deprecated)]
    let meta = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true))
        .unwrap();
    let pq_schema = meta.parquet_schema();
    let start_leaf = pq_schema.columns().iter().position(|c| c.name() == "start").unwrap();
    let pd = PageDir::build(&meta, start_leaf);
    let start_locs = page_locs_for_leaf(&meta, start_leaf);
    let nleaves = pq_schema.columns().len();
    let all_locs: Vec<Vec<(u64, u64)>> =
        (0..nleaves).map(|l| page_locs_for_leaf(&meta, l)).collect();
    let total_all_pages: usize = all_locs.iter().map(|v| v.len()).sum();
    let start_bytes: u64 = start_locs.iter().map(|(a, b)| b - a).sum();
    println!(
        "FILE {path}\n  rows={} row_groups={} start_pages={} runs={:?} start_page_bytes={} avg_start_page_bytes={:.0} all_leaf_pages={}",
        meta.metadata().file_metadata().num_rows(),
        meta.metadata().num_row_groups(),
        pd.min.len(),
        pd.runs,
        start_bytes,
        start_bytes as f64 / start_locs.len() as f64,
        total_all_pages
    );
    if pd.runs.len() > 1 {
        let (a, b) = pd.runs[0];
        println!("  run0 pages={} rows={}..{}", b - a, pd.first_row[a], pd.end_row[b - 1]);
    }

    let recs: Vec<(i64, String, String)> = std::io::BufReader::new(std::fs::File::open(tsv).unwrap())
        .lines()
        .map(|l| {
            let l = l.unwrap();
            let mut it = l.split('\t');
            (
                it.next().unwrap().parse().unwrap(),
                it.next().unwrap().to_string(),
                it.next().unwrap().to_string(),
            )
        })
        .collect();

    // warm-start set (rows of run 0) for hypothetical warm-first policies
    let warm_set: HashSet<u32> = if pd.runs.len() > 1 {
        let wrows = pd.end_row[pd.runs[0].1 - 1] as usize;
        let f = std::fs::File::open(path).unwrap();
        let r = ParquetRecordBatchReaderBuilder::try_new(f).unwrap()
            .with_projection(ProjectionMask::leaves(pq_schema, [start_leaf]))
            .with_limit(wrows).build().unwrap();
        let mut s = HashSet::new();
        for b in r { let b = b.unwrap(); for &v in b.column(0).as_any().downcast_ref::<UInt32Array>().unwrap().values() { s.insert(v); } }
        s
    } else { HashSet::new() };
    println!("  warm_distinct_starts={}", warm_set.len());
    for (bi, chunk) in recs.chunks(buf).enumerate() {
        for ext in [false, true] {
            let mut probes: Vec<u64> = vec![];
            let mut recs_warm = 0usize;
            for (pos, r, a) in chunk {
                let e = pos + r.len() as i64 - 1;
                let ps = build_probe_starts(*pos, e, r, a, ext);
                if ps.iter().any(|&p| warm_set.contains(&(p as u32))) { recs_warm += 1; }
                for p in ps { probes.push(p as u64); }
            }
            probes.sort_unstable(); probes.dedup();
            let (all_pages, _) = pd.resolve(&probes);
            // H: every probe looks in the warm run; only probes absent from the warm tier look in the cold run
            let cold_probes: Vec<u64> = probes.iter().copied().filter(|&p| !warm_set.contains(&(p as u32))).collect();
            let (pw, _) = pd.resolve(&probes);
            let (pc, _) = pd.resolve(&cold_probes);
            let mut h: HashSet<usize> = pw.into_iter().filter(|&g| pd.run_of(g) == 0).collect();
            let ncw = h.len();
            let cold_only: HashSet<usize> = pc.into_iter().filter(|&g| pd.runs.len() < 2 || pd.run_of(g) == 1).collect();
            for g in &cold_only { h.insert(*g); }
            // H2: records with a warm hit skip the cold run entirely (record-level)
            let mut cold_rec_probes: Vec<u64> = vec![];
            for (pos, r, a) in chunk {
                let e = pos + r.len() as i64 - 1;
                let ps = build_probe_starts(*pos, e, r, a, ext);
                if !ps.iter().any(|&p| warm_set.contains(&(p as u32))) { for p in ps { cold_rec_probes.push(p as u64); } }
            }
            cold_rec_probes.sort_unstable(); cold_rec_probes.dedup();
            let (pc2, _) = pd.resolve(&cold_rec_probes);
            let cold2: HashSet<usize> = pc2.into_iter().filter(|&g| pd.runs.len() < 2 || pd.run_of(g) == 1).collect();
            println!("  HYP buf={bi} ext={ext} probes={} recs_with_warm_hit={recs_warm} all_pages={} warm_run_pages={ncw} probe_level_warm_first={} (cold {}) record_level_warm_first={} (cold {})",
                probes.len(), all_pages.len(), h.len(), cold_only.len(), ncw + cold2.len(), cold2.len());
        }
    }
    println!("  buf\trecs\tfirst_pos\tlast_pos\tprobes\tcand_pages\twarm_run_pages\tcold_run_pages\tranges\tp2_req_ranges\tp2_req_bytes\tp2_ops\tp2_bytes\tp2_start_pages_in_fetched_full\tp2_touch\tmatched_rows\tp3_req_ranges\tp3_ops\tp3_bytes\tp3_all_leaf_pages_in_fetched");
    for (bi, chunk) in recs.chunks(buf).enumerate() {
        let mut probes: Vec<u32> = vec![];
        for (pos, r, a) in chunk {
            let e = pos + r.len() as i64 - 1;
            for p in build_probe_starts(*pos, e, r, a, extended) {
                probes.push(p as u32);
            }
        }
        probes.sort_unstable();
        probes.dedup();
        let probe_set: HashSet<u32> = probes.iter().copied().collect();
        let p64: Vec<u64> = probes.iter().map(|&p| p as u64).collect();
        let (pages, ranges) = pd.resolve(&p64);
        let mut per_run = vec![0usize; pd.runs.len()];
        for &pg in &pages {
            per_run[pd.run_of(pg)] += 1;
        }

        // phase 2
        let log = Arc::new(Mutex::new(Log::default()));
        let reader = RecReader {
            file: tokio::fs::File::open(path).await.unwrap(),
            gap: COALESCE_GAP_BYTES,
            log: log.clone(),
        };
        let mut stream = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, meta.clone())
            .with_projection(ProjectionMask::leaves(pq_schema, [start_leaf]))
            .with_row_selection(selection_from_ranges(&ranges))
            .with_batch_size(8192)
            .build()
            .unwrap();
        let mut offsets = vec![];
        let mut cursor = ranges.iter().flat_map(|&(a, b)| a..b);
        while let Some(b) = stream.try_next().await.unwrap() {
            let sa = b.column(0).as_any().downcast_ref::<UInt32Array>().unwrap();
            for &v in sa.values() {
                let off = cursor.next().unwrap();
                if probe_set.contains(&v) {
                    offsets.push(off);
                }
            }
        }
        let (req, spans) = {
            let l = log.lock().unwrap();
            (l.requested.clone(), l.spans.clone())
        };
        let req_bytes: u64 = req.iter().map(|(a, b)| b - a).sum();
        let span_bytes: u64 = spans.iter().map(|(a, b)| b - a).sum();
        let (full, touch) = pages_in_spans(&start_locs, &merge_spans(spans.clone()));

        // phase 3 (all root columns)
        let (mut p3r, mut p3o, mut p3b, mut p3p) = (0usize, 0usize, 0u64, 0usize);
        if payload {
            let log3 = Arc::new(Mutex::new(Log::default()));
            let reader = RecReader {
                file: tokio::fs::File::open(path).await.unwrap(),
                gap: COALESCE_GAP_BYTES,
                log: log3.clone(),
            };
            let mut stream =
                ParquetRecordBatchStreamBuilder::new_with_metadata(reader, meta.clone())
                    .with_row_selection(selection_from_offsets(&offsets))
                    .with_batch_size(8192)
                    .build()
                    .unwrap();
            while let Some(_b) = stream.try_next().await.unwrap() {}
            let l = log3.lock().unwrap();
            p3r = l.requested.len();
            p3o = l.spans.len();
            p3b = l.spans.iter().map(|(a, b)| b - a).sum();
            let ms = merge_spans(l.spans.clone());
            p3p = all_locs.iter().map(|locs| pages_in_spans(locs, &ms).0).sum();
        }
        // pages (row ranges of the start column) that hold at least one matched row
        let mut hit: Vec<usize> = offsets.iter().map(|&o| pd.first_row.partition_point(|&fr| fr <= o) - 1).collect();
        hit.sort_unstable(); hit.dedup();
        let hit_warm = if pd.runs.len() > 1 { hit.iter().filter(|&&g| pd.run_of(g) == 0).count() } else { 0 };
        // start-column-only take at the matched offsets (coalesced)
        let log4 = Arc::new(Mutex::new(Log::default()));
        let reader = RecReader { file: tokio::fs::File::open(path).await.unwrap(), gap: COALESCE_GAP_BYTES, log: log4.clone() };
        let mut st = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, meta.clone())
            .with_projection(ProjectionMask::leaves(pq_schema, [start_leaf]))
            .with_row_selection(selection_from_offsets(&offsets)).with_batch_size(8192).build().unwrap();
        while let Some(_b) = st.try_next().await.unwrap() {}
        let (q4, o4, b4, f4) = { let l = log4.lock().unwrap(); let ms = merge_spans(l.spans.clone());
            (l.requested.len(), l.spans.len(), l.spans.iter().map(|(a,b)| b-a).sum::<u64>(), pages_in_spans(&start_locs, &ms).0) };
        println!("  HIT buf={bi} matched_rows={} hit_pages={} (warm {hit_warm}, cold {}) startonly_take: req_pages={q4} ops={o4} bytes={b4} start_pages_in_fetched={f4}",
            offsets.len(), hit.len(), hit.len() - hit_warm);
        let _ = req_bytes;
        println!(
            "  {bi}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            chunk.len(),
            chunk[0].0,
            chunk[chunk.len() - 1].0,
            probes.len(),
            pages.len(),
            per_run.first().copied().unwrap_or(0),
            if per_run.len() > 1 { per_run[1] } else { 0 },
            ranges.len(),
            req.len(),
            req_bytes,
            spans.len(),
            span_bytes,
            full,
            touch,
            offsets.len(),
            p3r,
            p3o,
            p3b,
            p3p
        );
    }
}

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    match a[1].as_str() {
        "rewrite" => rewrite(&a[2], &a[3], &a[4], a[5].parse().unwrap()),
        "measure" => {
            measure(&a[2], &a[3], a[4].parse().unwrap(), a[5] == "ext", a[6] == "payload").await
        }
        _ => panic!("usage"),
    }
}
