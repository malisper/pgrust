//! COPY/INSERT append path: RG builders, analyze-then-pick encoders, footer
//! publish (docs/design/pgrcolumnar-impl.md §2, §5).

use std::collections::HashMap;

use ::datum::Datum;
use ::types_core::primitive::{Oid, TransactionId};
use ::types_error::{PgError, PgResult};
use ::tuplesort_seams::{CbIngestSort, CbSortKeyKind};

use crate::format::*;
use crate::hll::Hll;
use crate::reader::read_header_opt;
use crate::segfile::SegFile;
use ::types_error::ERRCODE_FEATURE_NOT_SUPPORTED;
use crate::varlena_bytes;

// ---- per-table writer options (CREATE TABLE ... USING cbstore WITH (...)) --

/// Table-level codec policy (the v6 per-column codec menu, plan §3.2).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CodecChoice {
    /// Sample-based per-chunk pick (day-0 evidence, ch-microbench note §1):
    /// LZ4 unless ZSTD's sampled ratio gain clears the class threshold.
    Auto,
    Lz4,
    Zstd,
    /// No compressed frames (v5 behavior for int/text raw lanes).
    Plain,
}

pub const ZSTD_LEVEL_DEFAULT: i32 = 3;

/// Options resolved against the relation's columns at writer open.
#[derive(Clone, Debug)]
pub struct CbWriterOpts {
    // (column index, sort-key kind) in declared cluster-key order.
    pub cluster_key: Vec<(u16, CbSortKeyKind)>,
    // load-r2 PRESORT instrument (PGRUST_COPY_PRESORT="col,col,..."; default
    // unset): sort-on-ingest exactly like cluster_key but WITHOUT any format
    // surface — no RG_FLAG_CLUSTERED, no footer cluster_key stanza — so a
    // load of unsorted input under presort must produce a part byte-identical
    // to a plain load of the same rows fed pre-sorted by the same key. Only
    // resolved when the relation declares no cluster_key; parallel COPY
    // admission refuses it (sort-on-ingest drains serially).
    pub presort_key: Vec<(u16, CbSortKeyKind)>,
    // Per-column codec choice (explicit override or the table default).
    pub codec: Vec<CodecChoice>,
    pub zstd_level: i32,
    // Per-column intcodec (DeltaFor) ingest gate. Default from the global
    // kill switch PGRUST_CBSTORE_INTCODEC (on unless off/0/false);
    // per-column overrides via PGRUST_CBSTORE_INTCODEC_COLS
    // ("UserID=off,URLHash=on"), resolved against the relation in
    // writer_opts_of — unknown names error (the codec_cols posture).
    pub delta: Vec<bool>,
}

// Global intcodec ingest kill switch, read once per writer open.
pub(crate) fn intcodec_env_default() -> bool {
    match std::env::var("PGRUST_CBSTORE_INTCODEC") {
        Ok(v) => !matches!(v.trim(), "off" | "0" | "false"),
        Err(_) => true,
    }
}

fn apply_intcodec_cols_env(
    rel: &::types_rel::Relation<'_>,
    delta: &mut [bool],
) -> PgResult<()> {
    let Ok(cols) = std::env::var("PGRUST_CBSTORE_INTCODEC_COLS") else {
        return Ok(());
    };
    for pair in cols.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (name, v) = pair.split_once('=').ok_or_else(|| {
            Box::new(PgError::error(format!(
                "cbstore: bad PGRUST_CBSTORE_INTCODEC_COLS entry \"{pair}\""
            )))
        })?;
        let idx = col_index_of(rel, name.trim())? as usize;
        delta[idx] = match v.trim() {
            "on" | "1" | "true" => true,
            "off" | "0" | "false" => false,
            other => {
                return Err(Box::new(PgError::error(format!(
                    "cbstore: bad PGRUST_CBSTORE_INTCODEC_COLS value \"{other}\""
                ))))
            }
        };
    }
    Ok(())
}

impl CbWriterOpts {
    pub fn plain(ncols: usize) -> CbWriterOpts {
        CbWriterOpts {
            cluster_key: Vec::new(),
            presort_key: Vec::new(),
            // Train #8 ingest default: LZ4 (matches PgrcolumnarOptions::default).
            codec: vec![CodecChoice::Lz4; ncols],
            zstd_level: ZSTD_LEVEL_DEFAULT,
            delta: vec![intcodec_env_default(); ncols],
        }
    }
}

fn sort_kind_of(t: ColType) -> CbSortKeyKind {
    match t {
        ColType::I16 => CbSortKeyKind::Int16,
        ColType::I32 | ColType::Date => CbSortKeyKind::Int32,
        ColType::I64 | ColType::Timestamp => CbSortKeyKind::Int64,
        ColType::Text => CbSortKeyKind::TextC,
    }
}

fn col_index_of(rel: &::types_rel::Relation<'_>, name: &str) -> PgResult<u16> {
    for (i, a) in rel.rd_att.attrs.iter().enumerate() {
        if !a.attisdropped && a.attname.name_str() == name.as_bytes() {
            return Ok(i as u16);
        }
    }
    Err(Box::new(
        PgError::error(format!(
            "cbstore: cluster/codec option references unknown column \"{name}\""
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    ))
}

/// Resolve the relation's pgrcolumnar reloptions into writer terms.
pub fn writer_opts_of(
    rel: &::types_rel::Relation<'_>,
    coltypes: &[ColType],
) -> PgResult<CbWriterOpts> {
    let mut out = CbWriterOpts::plain(coltypes.len());
    let Some(o) = rel.rd_options.as_ref().and_then(|o| o.pgrcolumnar()) else {
        apply_intcodec_cols_env(rel, &mut out.delta)?;
        apply_presort_env(rel, coltypes, &mut out)?;
        return Ok(out);
    };
    out.zstd_level = o.zstd_level;
    let table_choice = match o.codec {
        ::types_rel::PgrcolumnarCodec::Auto => CodecChoice::Auto,
        ::types_rel::PgrcolumnarCodec::Lz4 => CodecChoice::Lz4,
        ::types_rel::PgrcolumnarCodec::Zstd => CodecChoice::Zstd,
        ::types_rel::PgrcolumnarCodec::Plain => CodecChoice::Plain,
    };
    out.codec = vec![table_choice; coltypes.len()];
    for part in o.cluster_key().split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let idx = col_index_of(rel, part)?;
        out.cluster_key.push((idx, sort_kind_of(coltypes[idx as usize])));
    }
    if out.cluster_key.len() > CB_CLUSTER_KEY_MAX_COLS {
        return Err(Box::new(PgError::error(format!(
            "cbstore: cluster_key supports at most {CB_CLUSTER_KEY_MAX_COLS} columns"
        ))));
    }
    for pair in o.codec_cols().split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (name, codec) = pair.split_once('=').ok_or_else(|| {
            Box::new(PgError::error(format!("cbstore: bad codec_cols entry \"{pair}\"")))
        })?;
        let idx = col_index_of(rel, name.trim())? as usize;
        out.codec[idx] = match codec.trim() {
            "auto" => CodecChoice::Auto,
            "lz4" => CodecChoice::Lz4,
            "zstd" => CodecChoice::Zstd,
            "plain" => CodecChoice::Plain,
            other => {
                return Err(Box::new(PgError::error(format!(
                    "cbstore: unknown codec \"{other}\" in codec_cols"
                ))))
            }
        };
    }
    apply_intcodec_cols_env(rel, &mut out.delta)?;
    apply_presort_env(rel, coltypes, &mut out)?;
    Ok(out)
}

// load-r2 presort instrument: PGRUST_COPY_PRESORT="col,col,..." engages the
// cluster-key ingest sorter WITHOUT the cluster-key format surface (see
// CbWriterOpts::presort_key). A declared cluster_key wins; the env is then
// ignored (the reloption is the table's contract).
fn apply_presort_env(
    rel: &::types_rel::Relation<'_>,
    coltypes: &[ColType],
    out: &mut CbWriterOpts,
) -> PgResult<()> {
    if !out.cluster_key.is_empty() {
        return Ok(());
    }
    let Ok(cols) = std::env::var("PGRUST_COPY_PRESORT") else {
        return Ok(());
    };
    for name in cols.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let idx = col_index_of(rel, name)?;
        out.presort_key.push((idx, sort_kind_of(coltypes[idx as usize])));
    }
    if out.presort_key.len() > CB_CLUSTER_KEY_MAX_COLS {
        return Err(Box::new(PgError::error(format!(
            "cbstore: PGRUST_COPY_PRESORT supports at most {CB_CLUSTER_KEY_MAX_COLS} columns"
        ))));
    }
    Ok(())
}

// ---- codec engine ----------------------------------------------------------

/// Per-chunk compression driver: sample-pick between LZ4/ZSTD, then frame.
pub(crate) struct CodecCtx {
    pub choice: CodecChoice,
    pub zstd_level: i32,
    /// intcodec: DeltaFor may be considered for this column's chunks.
    pub delta: bool,
    /// Sub-granule dict frames (CHUNK_FLAG_DICT_FRAMED): split compressed
    /// dict blobs at entry boundaries into `dict_frame_target`-raw-byte
    /// frames so readers can materialize per touched frame. Off by default
    /// (frozen-bank layouts unchanged); armed by PGRUST_CBSTORE_DICT_FRAMES=1
    /// at ingest (experimental bank cuts).
    pub dict_frames: bool,
    /// Target RAW bytes per dict frame (PGRUST_CBSTORE_DICT_FRAME_KB,
    /// default 128 KiB). Blobs at or under one target stay single-frame
    /// legacy Lz4Dict.
    pub dict_frame_target: usize,
}

// Write-side sub-granule dict-frame arm (see CHUNK_FLAG_DICT_FRAMED).
pub(crate) fn dict_frames_env() -> bool {
    static ON: pgsync::OnceLock<bool> = pgsync::OnceLock::new();
    *ON.get_or_init(|| {
        matches!(std::env::var("PGRUST_CBSTORE_DICT_FRAMES").as_deref(), Ok("1") | Ok("on"))
    })
}

pub(crate) fn dict_frame_target_env() -> usize {
    static KB: pgsync::OnceLock<usize> = pgsync::OnceLock::new();
    *KB.get_or_init(|| {
        std::env::var("PGRUST_CBSTORE_DICT_FRAME_KB")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|&kb| kb > 0)
            .unwrap_or(128)
    }) * 1024
}

impl CodecCtx {
    pub(crate) fn new(choice: CodecChoice, zstd_level: i32, delta: bool) -> CodecCtx {
        CodecCtx {
            choice,
            zstd_level,
            delta,
            dict_frames: dict_frames_env(),
            dict_frame_target: dict_frame_target_env(),
        }
    }

    pub(crate) fn compress(&self, codec: Codec, data: &[u8]) -> Vec<u8> {
        match codec {
            Codec::Lz4 => lz4_flex::compress(data),
            #[cfg(not(target_family = "wasm"))]
            Codec::Zstd => zstd::bulk::compress(data, self.zstd_level)
                .expect("cbstore: zstd compress failed"),
            // wasm32: zstd-sys links C and has no wasm build; the codec
            // picker never selects Zstd on this target (see pick()).
            #[cfg(target_family = "wasm")]
            Codec::Zstd => panic!("cbstore: ZSTD codec is not supported on wasm32-wasip1"),
            Codec::None => unreachable!("cbstore: compress with Codec::None"),
        }
    }

    /// Pick a codec from one sampled frame. `narrow_hot` marks 1-2B int lanes
    /// (decode-hot; day-0 §1: keep LZ4 unless ZSTD's win is outsized).
    /// Returns None when no codec clears the >=10% win-vs-raw gate.
    pub(crate) fn pick(&self, sample: &[u8], narrow_hot: bool) -> Option<Codec> {
        if sample.is_empty() {
            return None;
        }
        let wins = |comp: usize| comp * 10 <= sample.len() * 9;
        match self.choice {
            CodecChoice::Plain => None,
            CodecChoice::Lz4 => {
                wins(lz4_flex::compress(sample).len()).then_some(Codec::Lz4)
            }
            #[cfg(not(target_family = "wasm"))]
            CodecChoice::Zstd => {
                wins(self.compress(Codec::Zstd, sample).len()).then_some(Codec::Zstd)
            }
            #[cfg(not(target_family = "wasm"))]
            CodecChoice::Auto => {
                let lz4 = lz4_flex::compress(sample).len();
                let zst = self.compress(Codec::Zstd, sample).len();
                if !wins(lz4) && !wins(zst) {
                    return None;
                }
                // ZSTD must beat LZ4 by >=20% (day-0: its ratio edge is
                // 1.2-20x where it matters), >=30% on decode-hot narrow ints.
                let num = if narrow_hot { 7 } else { 8 };
                if zst * 10 <= lz4 * num {
                    Some(Codec::Zstd)
                } else if wins(lz4) {
                    Some(Codec::Lz4)
                } else {
                    Some(Codec::Zstd)
                }
            }
            // wasm32: no zstd build (zstd-sys links C); Zstd/Auto degrade to
            // the LZ4-only decision so cbstore writes stay functional and
            // wasm-written tables remain readable on every target.
            #[cfg(target_family = "wasm")]
            CodecChoice::Zstd | CodecChoice::Auto => {
                let _ = narrow_hot;
                wins(lz4_flex::compress(sample).len()).then_some(Codec::Lz4)
            }
        }
    }
}

// Compressed-frame image: u32 raw_len | u32 comp_len | bytes, align4-padded.
pub(crate) fn push_frame(body: &mut Vec<u8>, raw_len: usize, comp: &[u8]) {
    put_u32(body, raw_len as u32);
    put_u32(body, comp.len() as u32);
    body.extend_from_slice(comp);
    while body.len() % 4 != 0 {
        body.push(0);
    }
}

pub(crate) fn frame_len(comp_len: usize) -> usize {
    align4(8 + comp_len)
}

#[cfg(test)]
pub(crate) fn test_codec_ctx() -> CodecCtx {
    // delta off: the pre-intcodec encoder tests pin For/Raw/Const shapes;
    // DeltaFor coverage lives in the dedicated intcodec tests.
    CodecCtx::new(CodecChoice::Auto, ZSTD_LEVEL_DEFAULT, false)
}

// intcodec ingest counters (gate observability, text_gate posture).
pub(crate) static INTCODEC_DELTA_CHOSEN: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static INTCODEC_KEPT_PLAIN: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// (delta_chosen, kept_plain) chunk counts since boot. `kept_plain` counts
/// only chunks where the delta arm was evaluated and lost.
pub fn intcodec_stats() -> (u64, u64) {
    use std::sync::atomic::Ordering::Relaxed;
    (INTCODEC_DELTA_CHOSEN.load(Relaxed), INTCODEC_KEPT_PLAIN.load(Relaxed))
}

struct IntBuilder {
    vals: Vec<i64>,
}

struct TextBuilder {
    // Per-row byte ranges into `blob` (header-less string bytes).
    offs: Vec<(u32, u32)>,
    blob: Vec<u8>,
}

enum ColBuilder {
    Int(IntBuilder),
    Text(TextBuilder),
}

/// v7 global-stitch builder for one text column: a part-lifetime interner
/// assigning first-appearance global ids to the column's distinct strings,
/// plus per sealed RG the local-code -> global-id map (local codes are the
/// RG's byte-sorted dict order). At finish() ids are re-ranked byte-sorted
/// (global code = byte rank over the part's union dict) and the per-RG maps
/// are written as stitch blobs. Poisoned (and emptied) the moment any RG of
/// the column is not dict-encoded — a partial global code space would break
/// the part-scope facts consumers rely on.
struct StitchCol {
    ids: HashMap<Box<[u8]>, u32, DictBuild>,
    rg_maps: Vec<Vec<u32>>,
    poisoned: bool,
}

impl StitchCol {
    fn new() -> StitchCol {
        StitchCol {
            ids: HashMap::with_hasher(DictBuild::new()),
            rg_maps: Vec::new(),
            poisoned: false,
        }
    }

    fn poison(&mut self) {
        self.poisoned = true;
        self.ids = HashMap::with_hasher(DictBuild::new());
        self.rg_maps = Vec::new();
    }

    /// Finalize one column: byte-rank the interned ids and build the per-RG
    /// local->global blob images (EXACTLY the bytes finish() writes). None =
    /// the column writes no stitch (poisoned / empty), matching the inline
    /// stanza's skip.
    fn finalize(self) -> Option<(u64, Vec<Vec<u8>>)> {
        if self.poisoned || self.ids.is_empty() {
            return None;
        }
        let gndv = self.ids.len();
        let mut by_bytes: Vec<(Box<[u8]>, u32)> = self.ids.into_iter().collect();
        by_bytes.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let mut rank = vec![0u32; gndv];
        for (r, &(_, id)) in by_bytes.iter().enumerate() {
            rank[id as usize] = r as u32;
        }
        let blobs = self
            .rg_maps
            .iter()
            .map(|map| {
                let mut blob: Vec<u8> = Vec::with_capacity(map.len() * 4);
                for &gid in map {
                    put_u32(&mut blob, rank[gid as usize]);
                }
                blob
            })
            .collect();
        Some((gndv as u64, blobs))
    }
}

// ---- load-r3 M2: column-sharded stitch pool (parallel COPY opt-in) ---------
// The inline stitch pipeline costs ~80 s of the 165 s 100M parsort wall
// (intern 44.7 s on the ordered-commit path + rank/blob 35.5 s at finish,
// measured 2026-07-15). Both stages are per-COLUMN independent: interning
// order only matters WITHIN a column (first-seen ids in RG order), and the
// final blobs are byte-rank coded (internal ids never reach disk). The pool
// shards live text columns across worker threads; each column is owned by
// exactly one thread and fed over a FIFO channel in commit order, so its
// intern order — and therefore every on-disk byte — is identical to the
// inline path. Engaged only via CbWriter::install_stitch_pool (parallel COPY
// driver, env opt-in); absent = inline code verbatim.
enum StitchMsg {
    Rg { col: usize, entries: Option<Vec<Box<[u8]>>> },
}

struct StitchPool {
    // permit-s4 row 10: per-thread unbounded pgsync::mailbox (send never
    // parks — mpsc::channel parity). Finalize-by-close is reproduced
    // exactly: clearing `txs` drops the last sender of each mailbox, the
    // owning thread drains remaining messages and exits (drain-then-exit =
    // mpsc disconnect semantics), and only then is it joined.
    txs: Vec<pgsync::MailboxSender<StitchMsg>>,
    // permit-s5 row 16: pgsync::thread handles — under the permit scheduler
    // the pool threads are door-registered (child-side synthetic vpids,
    // spawn-fenced) and BOTH close-then-join sites (drop path + finalize
    // path, the census D1 pair) become hooked Join parks woken by the
    // target's exit. Native arm = the std re-export, byte-identical.
    handles: Vec<pgsync::thread::JoinHandle<Vec<(usize, u64, Vec<Vec<u8>>)>>>,
    /// col -> owning thread index (None = int column / no stitch).
    assign: Vec<Option<usize>>,
}

impl Drop for StitchPool {
    fn drop(&mut self) {
        // Abort path: close the channels and reap the threads (pure
        // computation; they exit on channel close).
        self.txs.clear();
        for h in self.handles.drain(..) {
            let _ = h.join();
        }
    }
}

fn stitch_pool_thread(
    owned: Vec<(usize, StitchCol)>,
    rx: pgsync::MailboxReceiver<StitchMsg>,
) -> Vec<(usize, u64, Vec<Vec<u8>>)> {
    let mut cols: HashMap<usize, StitchCol> = owned.into_iter().collect();
    while let Some(StitchMsg::Rg { col, entries }) = rx.recv() {
        let Some(st) = cols.get_mut(&col) else { continue };
        if st.poisoned {
            continue;
        }
        match entries {
            Some(entries) => {
                let mut rg_map: Vec<u32> = Vec::with_capacity(entries.len());
                for e in entries {
                    let next = st.ids.len() as u32;
                    rg_map.push(*st.ids.entry(e).or_insert(next));
                }
                st.rg_maps.push(rg_map);
            }
            None => st.poison(),
        }
    }
    let mut out: Vec<(usize, u64, Vec<Vec<u8>>)> = cols
        .into_iter()
        .filter_map(|(c, st)| st.finalize().map(|(gndv, blobs)| (c, gndv, blobs)))
        .collect();
    out.sort_by_key(|x| x.0);
    out
}

// ---- PGRUST_CBSTORE_FASTHASH (load-speed prototype, DEFAULT OFF) -----------
// The ingest dictionary passes (per-RG dict build + the v7 stitch intern
// table) hash every text value through std's SipHash — measured ~8% of the
// 10M-bank COPY wall (load-speed lane perf, 2026-07-14). The maps only
// DEDUP: dict order is byte-sorted after the build and stitch ids are
// re-ranked byte-sorted at finish(), so the hasher never reaches the on-disk
// bytes — output is byte-identical either way. FASTHASH=1 swaps in an
// FxHash-style multiply-mix byte hasher (dedup-grade quality is sufficient;
// a collision only costs a memcmp probe).
fn fasthash_enabled() -> bool {
    static ON: pgsync::OnceLock<bool> = pgsync::OnceLock::new();
    *ON.get_or_init(|| {
        matches!(std::env::var("PGRUST_CBSTORE_FASTHASH").as_deref(), Ok("1") | Ok("on"))
    })
}

const FXK: u64 = 0x517cc1b727220a95;

enum DictHasher {
    Sip(std::collections::hash_map::DefaultHasher),
    Fast(u64),
}

impl std::hash::Hasher for DictHasher {
    #[inline]
    fn finish(&self) -> u64 {
        match self {
            DictHasher::Sip(h) => h.finish(),
            DictHasher::Fast(h) => {
                // final avalanche (murmur3 fmix64) so hashbrown's bucket bits
                // (low) and control byte (high 7) both see mixed entropy
                let mut v = *h;
                v ^= v >> 33;
                v = v.wrapping_mul(0xff51afd7ed558ccd);
                v ^ (v >> 33)
            }
        }
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        match self {
            DictHasher::Sip(h) => std::hash::Hasher::write(h, bytes),
            DictHasher::Fast(h) => {
                let mut chunks = bytes.chunks_exact(8);
                for c in &mut chunks {
                    let w = u64::from_le_bytes(c.try_into().unwrap());
                    *h = (h.rotate_left(5) ^ w).wrapping_mul(FXK);
                }
                let r = chunks.remainder();
                if !r.is_empty() {
                    let mut buf = [0u8; 8];
                    buf[..r.len()].copy_from_slice(r);
                    // fold the tail length in so "ab" and "ab\0" differ
                    buf[7] ^= r.len() as u8;
                    *h = (h.rotate_left(5) ^ u64::from_le_bytes(buf)).wrapping_mul(FXK);
                }
            }
        }
    }
}

#[derive(Clone)]
struct DictBuild {
    fast: bool,
    sip: std::collections::hash_map::RandomState,
}

impl DictBuild {
    fn new() -> DictBuild {
        DictBuild { fast: fasthash_enabled(), sip: std::collections::hash_map::RandomState::new() }
    }
}

impl std::hash::BuildHasher for DictBuild {
    type Hasher = DictHasher;
    #[inline]
    fn build_hasher(&self) -> DictHasher {
        if self.fast {
            DictHasher::Fast(0)
        } else {
            DictHasher::Sip(std::hash::BuildHasher::build_hasher(&self.sip))
        }
    }
}

// Writer-side stitch kill switch (byte-identity-preserving A/B: stitch
// tables are additive metadata; consumers fail open without them).
fn stitch_write_enabled() -> bool {
    static ON: pgsync::OnceLock<bool> = pgsync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_CBSTORE_STITCH").as_deref(), Ok("0") | Ok("off"))
    })
}

// v8 NDV-lane kill switch (default ON): register-blob persistence, the
// reopen-append sketch continuation, and the exact-stitch-gndv scalar
// preference. PGRUST_CBSTORE_NDV_REGS=0/off restores the pre-v8 NDV
// behavior exactly (no blobs, all-zero directory, reopen invalidates the
// sketch to unknown, scalars from the HLL estimate alone).
fn ndv_regs_enabled() -> bool {
    static ON: pgsync::OnceLock<bool> = pgsync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_CBSTORE_NDV_REGS").as_deref(), Ok("0") | Ok("off"))
    })
}

// Reopen-append sketch reload: deserialize every column's v8 register blob
// (directory entries are all-zero where absent). All-or-nothing, mirroring
// CbWriter.ndv's Option<Vec<Hll>>: the writer tracks all columns or none,
// so a single absent/unknown blob keeps the invalidate-to-unknown fallback.
fn load_ndv_regs(
    file: &mut SegFile,
    dir: &[(u64, u32)],
    ncols: usize,
) -> PgResult<Option<Vec<Hll>>> {
    if !ndv_regs_enabled() || dir.len() != ncols {
        return Ok(None);
    }
    let total = file.total_len();
    let mut hlls = Vec::with_capacity(ncols);
    for &(off, len) in dir {
        if off == 0 || len == 0 || off.checked_add(len as u64).is_none_or(|e| e > total) {
            return Ok(None);
        }
        let mut blob = vec![0u8; len as usize];
        file.read_exact_at(&mut blob, off)?;
        match Hll::from_blob(&blob) {
            Some(h) => hlls.push(h),
            None => return Ok(None),
        }
    }
    Ok(Some(hlls))
}

pub struct CbWriter {
    file: SegFile,
    xid: TransactionId,
    // Command that opened this writer: buffered ingest is per-statement
    // (tuple_insert buffers until the statement-end flush), so a writer left
    // behind by an errored statement must not leak rows into the next one.
    cid: ::types_core::CommandId,
    frozen: bool,
    ncols: usize,
    coltypes: Vec<ColType>,
    builders: Vec<ColBuilder>,
    nbuf: usize,
    // Committed footer chain state.
    write_off: u64,
    rgs: Vec<FooterRg>,
    fingerprint: u64,
    // Ingest-time per-column NDV sketches; None when appending to a part
    // with committed RGs (the finalized footer count cannot be merged into
    // a fresh sketch, so NDV is recorded as unknown).
    ndv: Option<Vec<Hll>>,
    // v5 whole-part sorted-asc trackers; None when appending to committed
    // RGs (the seam to the committed tail is unproven — the NDV precedent).
    sorted: Option<Vec<bool>>,
    prev_int: Vec<i64>,
    prev_text: Vec<Vec<u8>>,
    has_prev: bool,
    // v6 writer options (cluster key + codec menu).
    opts: CbWriterOpts,
    // Sort-on-ingest (plan §3.1): with a declared cluster key, rows buffer
    // into a spill-capable tuplesort and only reach append_row on the sorted
    // drain at finish(); RGs sealed from the drain carry RG_FLAG_CLUSTERED.
    sorter: Option<Box<dyn CbIngestSort>>,
    draining_clustered: bool,
    // v7 per-text-column global-stitch builders; None when appending to a
    // part with committed RGs (their local dicts were never interned — the
    // NDV/sorted invalidation precedent) or when stitch writing is disabled.
    // Int columns hold None slots.
    stitch: Option<Vec<Option<StitchCol>>>,
    // load-r3 M2: column-sharded stitch pool (install_stitch_pool). When
    // present it REPLACES self.stitch for the ordered-commit path; None =
    // inline interning verbatim.
    stitch_pool: Option<StitchPool>,
    // load-r3 M0: commit_encoded_rg phase-wall accumulators (RG granularity,
    // negligible cost, always accumulated; the parallel COPY leader's trace
    // line reads them via commit_phase_split()).
    commit_stitch: std::time::Duration,
    commit_pwrite: std::time::Duration,
    commit_meta: std::time::Duration,
    commit_bytes: u64,
    // load-r3 M1a: finish() phase walls — stitch rank+blob writes, footer
    // build+write, pad_and_sync+header (read via finish_phase_split()).
    finish_stitch: std::time::Duration,
    finish_footer: std::time::Duration,
    finish_sync: std::time::Duration,
    finish_blob_bytes: u64,
}

pub struct FooterRg {
    pub file_off: u64,
    pub nrows: u32,
    pub xmin: TransactionId,
    pub flags: u32,
    // Per column: (chunk_off relative to RG start, min, max).
    pub chunks: Vec<(u64, i64, i64)>,
    // Per column i128 sums; meaningful only when flags & RG_FLAG_SUMS
    // (empty on RGs parsed from v<=3 footers).
    pub sums: Vec<i128>,
    // v7 per-granule text length stats: (sum(octet_length), non-null count,
    // empty-string count) per GRANULES_PER_RG granule slot x flagged text
    // column ascending (index = slot * nlencols + rank). Meaningful only when
    // flags & RG_FLAG_LENSTATS; empty on RGs parsed from v<=6 footers.
    pub lenstats: Vec<(u64, u32, u32)>,
    // v7 per-granule zero/empty counts, GRANULES_PER_RG x ncols
    // (granule-major, then column ascending); meaningful only when
    // flags & RG_FLAG_ZEROCNT (empty on RGs parsed from v<=6 footers —
    // finish() then emits zeros without the flag).
    pub zerocnt: Vec<u32>,
}

thread_local! {
    static WRITERS: std::cell::RefCell<HashMap<Oid, CbWriter>> =
        std::cell::RefCell::new(HashMap::new());
}

pub fn coltypes_of(rel: &::types_rel::Relation<'_>) -> PgResult<Vec<ColType>> {
    rel.rd_att
        .attrs
        .iter()
        .map(|a| {
            ColType::of_type_oid(a.atttypid).ok_or_else(|| {
                Box::new(
                    PgError::error(format!(
                        "cbstore does not support the type of column \"{}\" (type oid {})",
                        String::from_utf8_lossy(a.attname.name_str()),
                        a.atttypid
                    ))
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                )
            })
        })
        .collect()
}

fn open_writer(rel: &::types_rel::Relation<'_>) -> PgResult<CbWriter> {
    let coltypes = coltypes_of(rel)?;
    let opts = writer_opts_of(rel, &coltypes)?;
    let path = crate::rel_main_path(rel);
    // This writer grows the main fork by direct pwrites, invisible to md's
    // process-global size cache: poison the key so every size probe of this
    // fork takes the real lseek walk (exactness preserved by construction).
    smgr_seams::smgr_nblocks_cache_poison::call(
        ::types_storage::RelFileLocatorBackend {
            locator: rel.rd_locator.get(),
            backend: rel.rd_backend,
        },
        ::types_core::ForkNumber::MAIN_FORKNUM,
    );
    let file = SegFile::open_rw(&path)?;
    let xid = xact_seams::get_current_transaction_id::call()?;
    // RG_FLAG_FROZEN bypasses the per-RG xmin visibility gate entirely, so
    // it is only sound when an abort of the writing (sub)transaction also
    // unlinks the file (or its whole relfilenode) it froze into. C parity:
    // copyfrom.c's FREEZE precheck demands the rel be created OR truncated
    // in the CURRENT subtransaction. A pre-existing empty part must NOT
    // freeze — a `BEGIN; COPY; ROLLBACK` publish into it would otherwise
    // stay visible forever.
    let cur_subid = xact_seams::get_current_sub_transaction_id::call();
    let frozen_ok = cur_subid != ::types_core::InvalidSubTransactionId
        && (rel.rd_createSubid.get() == cur_subid
            || rel.rd_newRelfilelocatorSubid.get() == cur_subid);
    let fingerprint = schema_fingerprint(
        &rel.rd_att.attrs.iter().map(|a| (a.atttypid, a.attlen)).collect::<Vec<_>>(),
    );
    let cid = xact_seams::get_current_command_id::call(false)?;
    let mut w = open_writer_inner(file, xid, cid, frozen_ok, coltypes, fingerprint, opts)?;
    if !w.opts.cluster_key.is_empty() || !w.opts.presort_key.is_empty() {
        // presort_key is only resolved when no cluster_key is declared
        // (apply_presort_env), so exactly one of the two is non-empty here.
        let key_src =
            if w.opts.cluster_key.is_empty() { &w.opts.presort_key } else { &w.opts.cluster_key };
        let keys: Vec<(i16, CbSortKeyKind)> =
            key_src.iter().map(|&(c, k)| (c as i16 + 1, k)).collect();
        // SAFETY: lifetime erasure on the relcache tupdesc; the COPY/INSERT
        // statement keeps the relation open for the writer's lifetime (the
        // begin_index_btree contract), and the stale-xid check in
        // multi_insert drops writers abandoned by error unwinds.
        let tup_desc: std::rc::Rc<::types_tuple::TupleDescData<'static>> =
            unsafe { std::mem::transmute(rel.rd_att.clone()) };
        w.sorter = Some(::tuplesort_seams::pgrcolumnar_ingest_sort::call(
            tup_desc,
            &keys,
            init_small::globals::maintenance_work_mem(),
        )?);
    }
    Ok(w)
}

/// TEST SUPPORT (dict-tier round-trip / bench rigs): a writer over an
/// explicit path + coltypes, bypassing Relation and xact-seam resolution
/// (xid 1, frozen-ok — the sealed row groups need no visibility seams to
/// scan). `append_row`/`finish` ride the exact production write path.
#[doc(hidden)]
pub fn open_writer_at(path: &str, coltypes: Vec<ColType>) -> PgResult<CbWriter> {
    let opts = CbWriterOpts::plain(coltypes.len());
    open_writer_inner(SegFile::open_rw(path)?, 1, 0, true, coltypes, 0x5aa5, opts)
}

fn open_writer_inner(
    file: SegFile,
    xid: TransactionId,
    cid: ::types_core::CommandId,
    frozen_ok: bool,
    coltypes: Vec<ColType>,
    fingerprint: u64,
    opts: CbWriterOpts,
) -> PgResult<CbWriter> {
    let len = file.total_len();
    let ncols = coltypes.len();
    let mut w = CbWriter {
        file,
        xid,
        cid,
        // Freeze-on-load: first write into a file created by our own
        // transaction (empty part) makes RGs all-visible-on-commit.
        frozen: false,
        ncols,
        coltypes,
        builders: Vec::new(),
        nbuf: 0,
        write_off: CB_HEADER_LEN,
        rgs: Vec::new(),
        fingerprint,
        ndv: Some((0..ncols).map(|_| Hll::default()).collect()),
        sorted: Some(vec![true; ncols]),
        prev_int: vec![0; ncols],
        prev_text: vec![Vec::new(); ncols],
        has_prev: false,
        opts,
        sorter: None,
        draining_clustered: false,
        stitch: None,
        stitch_pool: None,
        commit_stitch: std::time::Duration::ZERO,
        commit_pwrite: std::time::Duration::ZERO,
        commit_meta: std::time::Duration::ZERO,
        commit_bytes: 0,
        finish_stitch: std::time::Duration::ZERO,
        finish_footer: std::time::Duration::ZERO,
        finish_sync: std::time::Duration::ZERO,
        finish_blob_bytes: 0,
    };
    if stitch_write_enabled() {
        w.stitch = Some(
            w.coltypes
                .iter()
                .map(|t| if t.is_text() { Some(StitchCol::new()) } else { None })
                .collect(),
        );
    }
    w.reset_builders();
    // Empty part; also a part whose header page is still a zero hole (a
    // pre-header-first writer aborted mid-COPY before ever publishing) —
    // both mean "no committed row group can exist", so both (re)initialize.
    let mut init_header = len < CB_HEADER_LEN;
    if len >= CB_HEADER_LEN {
        let mut hdr = [0u8; CB_HEADER_LEN as usize];
        w.file.read_exact_at(&mut hdr, 0)?;
        match read_header_opt(&hdr)? {
            None => init_header = true,
            Some((footer_off, fp, version)) => {
                if fp != w.fingerprint {
                    return Err(Box::new(PgError::error(
                        "cbstore: schema fingerprint mismatch".to_string(),
                    )));
                }
                if footer_off != 0 {
                    let (rgs, footer_end, _ndv, _sorted, _ckey, _lenflags, _stitch, ndvregs) =
                        crate::reader::read_footer_rgs(
                        &mut w.file,
                        footer_off,
                        w.ncols,
                        version,
                        true,
                    )?;
                    if !rgs.is_empty() {
                        // v8: continue the persisted register sketch across
                        // the reopen instead of invalidating (registers are
                        // per-bucket maxima — continue == recompute exactly).
                        // Every column must deserialize; any absent/unknown
                        // blob (pre-v8 chain, kill switch at the earlier
                        // write, foreign precision) keeps the historical
                        // invalidate-to-unknown fallback for the whole part.
                        w.ndv = load_ndv_regs(&mut w.file, &ndvregs, w.ncols)?;
                        w.sorted = None;
                        w.stitch = None;
                    }
                    w.rgs = rgs;
                    w.write_off = align64(footer_end.max(footer_off));
                }
            }
        }
    }
    if init_header {
        w.frozen = frozen_ok;
        // Header-first ingest (abort safety): publish a valid empty-part
        // header (footer_off = 0) BEFORE any row-group bytes hit the file.
        // An abort mid-COPY then leaves a readable empty part plus dead
        // bytes past the header — not a zero hole that wedges every later
        // read_header. Durable up front so a crash-kill mid-COPY restarts
        // into the same readable state.
        w.write_header(0)?;
        w.file.pad_and_sync(CB_HEADER_LEN)?;
    }
    Ok(w)
}

impl CbWriter {
    fn write_header(&mut self, footer_off: u64) -> PgResult<()> {
        let mut hdr = Vec::with_capacity(CB_HEADER_LEN as usize);
        put_u64(&mut hdr, CB_MAGIC);
        put_u32(&mut hdr, CB_VERSION);
        put_u32(&mut hdr, self.ncols as u32);
        put_u64(&mut hdr, footer_off);
        put_u64(&mut hdr, self.fingerprint);
        hdr.resize(CB_HEADER_LEN as usize, 0);
        self.file.write_all_at(&hdr, 0)
    }

    fn reset_builders(&mut self) {
        self.builders = self
            .coltypes
            .iter()
            .map(|t| {
                if t.is_text() {
                    ColBuilder::Text(TextBuilder { offs: Vec::new(), blob: Vec::new() })
                } else {
                    ColBuilder::Int(IntBuilder { vals: Vec::new() })
                }
            })
            .collect();
        self.nbuf = 0;
    }

    // COPY/INSERT entry: rows detour through the cluster-key sorter when one
    // is declared, reaching append_row only on the sorted drain in finish().
    fn ingest_row(&mut self, values: &[Datum], isnull: &[bool]) -> PgResult<()> {
        if let Some(s) = &mut self.sorter {
            if let Some(c) = isnull[..self.ncols].iter().position(|&n| n) {
                let _ = c;
                return Err(Box::new(
                    PgError::error("cbstore does not support NULL values".to_string())
                        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                ));
            }
            return s.put_row(&values[..self.ncols], &isnull[..self.ncols]);
        }
        self.append_row(values, isnull)
    }

    /// pub for the test-support writer (`open_writer_at`); production
    /// callers reach this through multi_insert/tuple_insert (via ingest_row).
    #[doc(hidden)]
    pub fn append_row(&mut self, values: &[Datum], isnull: &[bool]) -> PgResult<()> {
        append_row_into(
            &self.coltypes,
            &mut self.builders,
            self.ndv.as_mut(),
            self.sorted.as_mut(),
            &mut self.prev_int,
            &mut self.prev_text,
            self.has_prev,
            values,
            isnull,
        )?;
        self.has_prev = true;
        self.nbuf += 1;
        if self.nbuf == RG_ROWS {
            self.seal_rg()?;
        }
        Ok(())
    }

    fn seal_rg(&mut self) -> PgResult<()> {
        if self.nbuf == 0 {
            return Ok(());
        }
        let flags = (if self.frozen { RG_FLAG_FROZEN } else { 0 })
            | RG_FLAG_SUMS
            | RG_FLAG_LENSTATS
            | RG_FLAG_ZEROCNT
            | (if self.draining_clustered { RG_FLAG_CLUSTERED } else { 0 });
        // Capture dict entries only where the column's stitch is live (a
        // poisoned or absent stitch skips the boxed-key allocations, exactly
        // as the inline interning did).
        let capture: Vec<bool> = (0..self.ncols)
            .map(|c| {
                self.stitch
                    .as_ref()
                    .is_some_and(|s| s[c].as_ref().is_some_and(|st| !st.poisoned))
            })
            .collect();
        let builders = std::mem::take(&mut self.builders);
        let mut sealed =
            seal_rg_body(&self.coltypes, &self.opts, &builders, self.nbuf, self.xid, flags, &capture);
        self.register_stitch(&mut sealed);
        let file_off = align64(self.write_off);
        self.file.write_all_at(&sealed.body, file_off)?;
        self.write_off = file_off + sealed.body.len() as u64;
        self.rgs.push(FooterRg {
            file_off,
            nrows: sealed.nrows,
            xmin: self.xid,
            flags,
            chunks: sealed.chunks,
            sums: sealed.sums,
            lenstats: sealed.lenstats,
            zerocnt: sealed.zerocnt,
        });
        self.reset_builders();
        Ok(())
    }

    /// v7 stitch registration for one sealed RG (serial seal and parallel
    /// ordered commit share it): intern captured dict entries in local-code
    /// order; a dict-less text RG poisons the column.
    fn register_stitch(&mut self, sealed: &mut SealedRg) {
        let Some(cols) = self.stitch.as_mut() else { return };
        for c in 0..self.ncols {
            let Some(st) = cols[c].as_mut() else { continue };
            if st.poisoned {
                continue;
            }
            match sealed.dicts[c].take() {
                Some(entries) => {
                    let mut rg_map: Vec<u32> = Vec::with_capacity(entries.len());
                    for e in entries {
                        let next = st.ids.len() as u32;
                        rg_map.push(*st.ids.entry(e).or_insert(next));
                    }
                    st.rg_maps.push(rg_map);
                }
                // Not dict-encoded: the column's global code space would be
                // partial — drop the stitch for the whole column (consumers
                // fail open to the per-epoch paths).
                None => st.poison(),
            }
        }
    }

    /// pub for the test-support writer (`open_writer_at`).
    #[doc(hidden)]
    pub fn finish(&mut self) -> PgResult<()> {
        // Cluster-key drain: sort the buffered ingest and feed it through the
        // ordinary append path (NDV/sorted trackers and RG seals see rows in
        // final order, so their metadata is exact for the sorted part).
        if let Some(mut sorter) = self.sorter.take() {
            sorter.sort()?;
            // Presort (env instrument) drains WITHOUT the cluster-key format
            // surface: no RG_FLAG_CLUSTERED, and the footer stanza already
            // stamps from opts.cluster_key (empty) — the drained part must be
            // byte-identical to a plain load of pre-sorted input.
            self.draining_clustered = !self.opts.cluster_key.is_empty();
            let mut values = vec![Datum::null(); self.ncols];
            let mut isnull = vec![false; self.ncols];
            while sorter.next_row(&mut values, &mut isnull)? {
                self.append_row(&values, &isnull)?;
            }
        }
        self.seal_rg()?;
        // v7 stitch blobs: re-rank the interned global ids byte-sorted
        // (global code = byte rank over the part's union dict, lifting the
        // per-RG CHUNK_FLAG_DICT_SORTED property to part scope) and write
        // the per-RG local->global u32 maps into the file body ahead of the
        // footer. On append-refinalize these become dead bytes, the
        // footer-chain precedent.
        let nrgs = self.rgs.len();
        let mut stitch_gndv = vec![0u64; self.ncols];
        let mut stitch_dir = vec![(0u64, 0u32); nrgs * self.ncols];
        let ft0 = std::time::Instant::now();
        let finalized: Vec<(usize, u64, Vec<Vec<u8>>)> = if let Some(mut pool) =
            self.stitch_pool.take()
        {
            // load-r3 M2: close the feed channels; the threads finalize
            // their columns (byte-rank + blob images) in parallel.
            pool.txs.clear();
            let handles: Vec<_> = pool.handles.drain(..).collect();
            drop(pool);
            let mut results: Vec<(usize, u64, Vec<Vec<u8>>)> = Vec::new();
            for h in handles {
                let r = h.join().map_err(|_| {
                    Box::new(PgError::error("stitch pool thread panicked".to_string()))
                })?;
                results.extend(r);
            }
            // Column-ascending = the inline stanza's write order exactly.
            results.sort_by_key(|x| x.0);
            results
        } else if let Some(cols) = self.stitch.take() {
            cols.into_iter()
                .enumerate()
                .filter_map(|(c, st)| {
                    let st = st?;
                    if st.rg_maps.len() != nrgs {
                        return None;
                    }
                    st.finalize().map(|(gndv, blobs)| (c, gndv, blobs))
                })
                .collect()
        } else {
            Vec::new()
        };
        for (c, gndv, blobs) in finalized {
            if blobs.len() != nrgs {
                continue;
            }
            for (rg, blob) in blobs.iter().enumerate() {
                let off = align64(self.write_off);
                self.file.write_all_at(blob, off)?;
                self.write_off = off + blob.len() as u64;
                self.finish_blob_bytes += blob.len() as u64;
                stitch_dir[rg * self.ncols + c] = (off, (blob.len() / 4) as u32);
            }
            stitch_gndv[c] = gndv;
        }
        let ft1 = std::time::Instant::now();
        self.finish_stitch += ft1 - ft0;
        // v8 NDV-register blobs (the append-invalidation fix): persist the
        // per-column sketch registers so a later reopen-append continues the
        // sketch instead of invalidating every column to unknown. File-body
        // blobs ahead of the footer, dead on append-refinalize — the stitch
        // precedent. Sketch upkeep is already paid per value at ingest; this
        // adds only the serialization write (<=4+16KiB/column dense, tens of
        // bytes sparse).
        let mut ndvregs_dir = vec![(0u64, 0u32); self.ncols];
        if ndv_regs_enabled() {
            if let Some(hlls) = &self.ndv {
                for (c, h) in hlls.iter().enumerate() {
                    let blob = h.to_blob();
                    let off = align64(self.write_off);
                    self.file.write_all_at(&blob, off)?;
                    self.write_off = off + blob.len() as u64;
                    self.finish_blob_bytes += blob.len() as u64;
                    ndvregs_dir[c] = (off, blob.len() as u32);
                }
            }
        }
        // Footer.
        let mut f: Vec<u8> = Vec::with_capacity(64 + self.rgs.len() * (24 + self.ncols * 24));
        put_u32(&mut f, self.rgs.len() as u32);
        put_u32(&mut f, self.ncols as u32);
        // v7 prelude tail: per-column length-stats flags (1 = the column has
        // per-granule entries in the trailing length-stats section). In the
        // prelude — not appended — because read_footer_rgs must size the
        // body read from nrgs/ncols alone.
        for t in &self.coltypes {
            f.push(t.is_text() as u8);
        }
        for rg in &self.rgs {
            put_u64(&mut f, rg.file_off);
            put_u32(&mut f, rg.nrows);
            put_u32(&mut f, rg.xmin);
            put_u32(&mut f, rg.flags);
            put_u32(&mut f, 0);
        }
        for rg in &self.rgs {
            for &(off, min, max) in &rg.chunks {
                put_u64(&mut f, off);
                put_i64(&mut f, min);
                put_i64(&mut f, max);
            }
        }
        // v2 NDV section; a distinct count can never be 0 for a nonempty
        // part, so 0 encodes unknown (append-invalidated sketch). Where a
        // v7 global dict exists, its entry count IS the column's exact
        // distinct count (ingest rejects NULLs; the union dict spans every
        // RG) — prefer exactness over the ~1% HLL estimate.
        let total_rows: u64 = self.rgs.iter().map(|rg| rg.nrows as u64).sum();
        for c in 0..self.ncols {
            let est = if ndv_regs_enabled() && stitch_gndv[c] > 0 {
                stitch_gndv[c].clamp(1, total_rows.max(1))
            } else {
                match &self.ndv {
                    Some(hlls) => hlls[c].estimate().clamp(1, total_rows.max(1)),
                    None => 0,
                }
            };
            put_u64(&mut f, est);
        }
        // v4 sums section; RGs preserved from a v<=3 footer write zeros and
        // lack RG_FLAG_SUMS.
        for rg in &self.rgs {
            for c in 0..self.ncols {
                put_i128(&mut f, rg.sums.get(c).copied().unwrap_or(0));
            }
        }
        // v5 sorted section; 0 = unknown (append-invalidated tracker).
        for c in 0..self.ncols {
            f.push(self.sorted.as_ref().is_some_and(|s| s[c]) as u8);
        }
        // v6 cluster-key section (fixed width): the declared key this writer
        // sorted under (0 keys = none); adaptive traversal gates per RG on
        // RG_FLAG_CLUSTERED.
        debug_assert!(self.opts.cluster_key.len() <= CB_CLUSTER_KEY_MAX_COLS);
        f.extend_from_slice(&(self.opts.cluster_key.len() as u16).to_le_bytes());
        for slot in 0..CB_CLUSTER_KEY_MAX_COLS {
            let c = self.opts.cluster_key.get(slot).map(|&(c, _)| c).unwrap_or(0);
            f.extend_from_slice(&c.to_le_bytes());
        }
        // v7 length-stats section (format.rs doc): rg-major, GRANULES_PER_RG
        // fixed granule slots, flagged columns ascending. RGs preserved from
        // v<=6 footers carry no entries (empty lenstats) and lack
        // RG_FLAG_LENSTATS — they write zeros.
        let nlencols = self.coltypes.iter().filter(|t| t.is_text()).count();
        for rg in &self.rgs {
            for slot in 0..GRANULES_PER_RG * nlencols {
                let (s, n, e) = rg.lenstats.get(slot).copied().unwrap_or((0, 0, 0));
                put_u64(&mut f, s);
                put_u32(&mut f, n);
                put_u32(&mut f, e);
            }
        }
        // v7 stitch section (after length stats): per-column global NDV
        // (0 = no stitch), then the per-(RG, column) stitch-blob directory
        // (all-zero where absent).
        for c in 0..self.ncols {
            put_u64(&mut f, stitch_gndv[c]);
        }
        for rg in 0..nrgs {
            for c in 0..self.ncols {
                let (off, cnt) = stitch_dir[rg * self.ncols + c];
                put_u64(&mut f, off);
                put_u32(&mut f, cnt);
            }
        }
        // v7 zero-count section (after stitch); RGs preserved from a v<=6
        // footer write zeros and lack RG_FLAG_ZEROCNT.
        for rg in &self.rgs {
            for g in 0..GRANULES_PER_RG {
                for c in 0..self.ncols {
                    put_u32(&mut f, rg.zerocnt.get(g * self.ncols + c).copied().unwrap_or(0));
                }
            }
        }
        // v8 NDV-registers blob directory (after zero counts); all-zero
        // where the sketch is absent (invalidated chain or kill switch).
        for &(off, len) in &ndvregs_dir {
            put_u64(&mut f, off);
            put_u32(&mut f, len);
        }
        let crc = crc32c(&f);
        let flen = (f.len() + 16) as u64;
        put_u64(&mut f, flen);
        put_u32(&mut f, crc);
        put_u32(&mut f, CB_FOOTER_MAGIC);

        let footer_off = align64(self.write_off);
        self.file.write_all_at(&f, footer_off)?;
        self.write_off = footer_off + flen;
        let ft2 = std::time::Instant::now();
        self.finish_footer += ft2 - ft1;

        // Data + footer durable before the publish (impl doc §5); segment
        // tails padded to BLCKSZ multiples for md's block accounting. Any
        // segment file this statement minted (each 1 GiB spill) then needs
        // its DIRECTORY ENTRY made durable too — fdatasync persists file
        // data, not the dirent — strictly before footer_off publishes a
        // pointer into it, or a crash after commit can drop the segment
        // while the header still points past it (GH issue #2).
        self.file.pad_and_sync(self.write_off)?;
        self.file.sync_dir_if_minted()?;
        self.write_header(footer_off)?;
        self.file.sync_data()?;
        self.finish_sync += ft2.elapsed();
        Ok(())
    }

    /// load-r3 M1a: finish() phase walls — (stitch_rank_blobs_s, footer_s,
    /// sync_s, blob_bytes). Sorter-drain time (serial presort only) is not
    /// included; the parallel path never has a sorter.
    pub fn finish_phase_split(&self) -> (f64, f64, f64, u64) {
        (
            self.finish_stitch.as_secs_f64(),
            self.finish_footer.as_secs_f64(),
            self.finish_sync.as_secs_f64(),
            self.finish_blob_bytes,
        )
    }
}

/// One row into the per-column builders + optional NDV/sorted trackers —
/// the serial writer and the parallel chunk encoder share it (byte-identical
/// parts demand one implementation of the tracking rules).
#[allow(clippy::too_many_arguments)]
fn append_row_into(
    coltypes: &[ColType],
    builders: &mut [ColBuilder],
    mut ndv: Option<&mut Vec<Hll>>,
    mut sorted: Option<&mut Vec<bool>>,
    prev_int: &mut [i64],
    prev_text: &mut [Vec<u8>],
    has_prev: bool,
    values: &[Datum],
    isnull: &[bool],
) -> PgResult<()> {
    for c in 0..coltypes.len() {
        if isnull[c] {
            return Err(Box::new(
                PgError::error("cbstore does not support NULL values".to_string())
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
        match &mut builders[c] {
            ColBuilder::Int(b) => {
                let v = match coltypes[c] {
                    ColType::I16 => values[c].as_i16() as i64,
                    ColType::I32 | ColType::Date => values[c].as_i32() as i64,
                    ColType::I64 | ColType::Timestamp => values[c].as_i64(),
                    ColType::Text => unreachable!(),
                };
                if let Some(ndv) = ndv.as_deref_mut() {
                    ndv[c].add_i64(v);
                }
                if let Some(sorted) = sorted.as_deref_mut() {
                    if sorted[c] && has_prev && v < prev_int[c] {
                        sorted[c] = false;
                    }
                    prev_int[c] = v;
                }
                b.vals.push(v);
            }
            ColBuilder::Text(b) => {
                let bytes = varlena_bytes(values[c])?;
                if let Some(ndv) = ndv.as_deref_mut() {
                    ndv[c].add_bytes(bytes);
                }
                if let Some(sorted) = sorted.as_deref_mut() {
                    if sorted[c] {
                        if has_prev && bytes < prev_text[c].as_slice() {
                            sorted[c] = false;
                        } else {
                            prev_text[c].clear();
                            prev_text[c].extend_from_slice(bytes);
                        }
                    }
                }
                let off = b.blob.len() as u32;
                b.blob.extend_from_slice(bytes);
                b.offs.push((off, bytes.len() as u32));
            }
        }
    }
    Ok(())
}

/// One sealed row group's encoded image + footer-side metadata, produced
/// from buffered builders alone — no file handle, no part-lifetime state.
/// The serial writer's seal_rg and the parallel COPY chunk encoder both
/// seal through [`seal_rg_body`]: byte-identical parts demand exactly one
/// implementation.
pub struct SealedRg {
    pub(crate) body: Vec<u8>,
    pub(crate) nrows: u32,
    pub(crate) flags: u32,
    pub(crate) chunks: Vec<(u64, i64, i64)>,
    pub(crate) sums: Vec<i128>,
    pub(crate) lenstats: Vec<(u64, u32, u32)>,
    pub(crate) zerocnt: Vec<u32>,
    /// Per-column captured dict entries (byte-sorted local-code order);
    /// None for int columns, non-dict text chunks, and un-captured columns.
    pub(crate) dicts: Vec<Option<Vec<Box<[u8]>>>>,
}

/// Encode one RG from its builders: body image (header, per-column chunks,
/// length patched, align64-padded) + footer metadata (chunk minmax/offsets,
/// sums, v7 lenstats/zerocnt) + captured dicts per `capture` column.
fn seal_rg_body(
    coltypes: &[ColType],
    opts: &CbWriterOpts,
    builders: &[ColBuilder],
    nbuf: usize,
    xid: TransactionId,
    flags: u32,
    capture: &[bool],
) -> SealedRg {
    let ncols = coltypes.len();
    let nrows = nbuf;
    let ngranules = nrows.div_ceil(GRANULE_ROWS) as u32;
    let mut body: Vec<u8> = Vec::with_capacity(1 << 20);
    // rg_header placeholder; length patched at the end.
    put_u32(&mut body, CB_RG_MAGIC);
    put_u32(&mut body, nrows as u32);
    put_u32(&mut body, xid);
    put_u32(&mut body, flags);
    put_u64(&mut body, 0);
    put_u64(&mut body, 0);

    let mut chunk_meta: Vec<(u64, i64, i64)> = Vec::with_capacity(ncols);
    let mut sums: Vec<i128> = Vec::with_capacity(ncols);
    let mut dicts: Vec<Option<Vec<Box<[u8]>>>> = (0..ncols).map(|_| None).collect();
    for (c, b) in builders.iter().enumerate() {
        while body.len() % 64 != 0 {
            body.push(0);
        }
        let chunk_off = body.len() as u64;
        let cc = CodecCtx::new(opts.codec[c], opts.zstd_level, opts.delta[c]);
        let (min, max) = match b {
            ColBuilder::Int(ib) => encode_int_chunk(&mut body, &ib.vals, ngranules, &cc),
            ColBuilder::Text(tb) => {
                let (min, max, cap) =
                    encode_text_chunk(&mut body, tb, ngranules, &cc, capture[c]);
                dicts[c] = cap;
                (min, max)
            }
        };
        chunk_meta.push((chunk_off, min, max));
        sums.push(match b {
            ColBuilder::Int(ib) => ib.vals.iter().map(|&v| v as i128).sum(),
            ColBuilder::Text(_) => 0,
        });
    }
    // v7 per-granule length stats, straight off the buffered per-row
    // (off, len) ranges (exact octet_length: the stored payload byte
    // count). Granule slots past the last row stay zero. pgrcolumnar stores
    // no NULLs (append_row errors), so nonnull = granule rows.
    let nlencols = coltypes.iter().filter(|t| t.is_text()).count();
    let mut lenstats: Vec<(u64, u32, u32)> = Vec::new();
    if nlencols > 0 {
        lenstats = vec![(0u64, 0u32, 0u32); GRANULES_PER_RG * nlencols];
        let mut rank = 0usize;
        for b in builders.iter() {
            let ColBuilder::Text(tb) = b else { continue };
            for g in 0..nrows.div_ceil(GRANULE_ROWS) {
                let lo = g * GRANULE_ROWS;
                let hi = (lo + GRANULE_ROWS).min(nrows);
                let mut sum = 0u64;
                let mut empty = 0u32;
                for &(_, len) in &tb.offs[lo..hi] {
                    sum += len as u64;
                    empty += (len == 0) as u32;
                }
                lenstats[g * nlencols + rank] = (sum, (hi - lo) as u32, empty);
            }
            rank += 1;
        }
    }
    // v7 per-granule zero/empty counts, off the same buffered values as
    // the sums/minmax pass (no extra decode; ints count value == 0, text
    // counts the empty string). Granule slots past the RG's last row
    // stay zero (partial RGs zero-pad).
    let mut zerocnt = vec![0u32; GRANULES_PER_RG * ncols];
    for (c, b) in builders.iter().enumerate() {
        for g in 0..nrows.div_ceil(GRANULE_ROWS) {
            let lo = g * GRANULE_ROWS;
            let hi = (lo + GRANULE_ROWS).min(nrows);
            let zc = match b {
                ColBuilder::Int(ib) => ib.vals[lo..hi].iter().filter(|&&v| v == 0).count(),
                ColBuilder::Text(tb) => {
                    tb.offs[lo..hi].iter().filter(|&&(_, len)| len == 0).count()
                }
            };
            zerocnt[g * ncols + c] = zc as u32;
        }
    }
    // Patch total RG length.
    let total = align64(body.len() as u64);
    body[16..24].copy_from_slice(&total.to_le_bytes());
    body.resize(total as usize, 0);
    SealedRg {
        body,
        nrows: nrows as u32,
        flags,
        chunks: chunk_meta,
        sums,
        lenstats,
        zerocnt,
        dicts,
    }
}

// ---- parallel COPY ingest (morsel-parallel COPY lane) ----------------------
//
// Shape (docs/design/load-speed-2026-07.md §5 lever 1): N workers each
// parse+convert+encode WHOLE 65,536-row RGs off input chunks; ONE ordered
// committer (the COPY leader) owns the file, the footer chain, and every
// part-lifetime tracker (NDV sketches, sorted trackers, the v7 stitch
// interner) — commits happen in INPUT ORDER, so the part is byte-identical
// to a serial COPY of the same stream.

/// The immutable per-statement encode plan the leader hands its workers:
/// everything a chunk encoder needs, none of the part-lifetime state.
pub struct ParallelIngestPlan {
    pub xid: TransactionId,
    /// RG flags every parallel-sealed RG carries (frozen decided at writer
    /// open; never RG_FLAG_CLUSTERED — cluster-key tables refuse parallel).
    pub flags: u32,
    pub coltypes: Vec<ColType>,
    pub opts: CbWriterOpts,
    /// Part tracks NDV (fresh part): workers carry per-chunk HLL sketches.
    pub want_ndv: bool,
    /// Part tracks sorted-asc: workers carry per-chunk order probes.
    pub want_sorted: bool,
    /// Per-column stitch capture (text columns of a stitch-active writer).
    pub capture: Vec<bool>,
}

/// Per-chunk sorted-order probe for one column: chunk-internal sortedness +
/// the chunk's first/last values, letting the ordered committer replay the
/// serial tracker across chunk seams exactly.
pub struct ColOrderProbe {
    pub sorted: bool,
    pub first_int: i64,
    pub last_int: i64,
    pub first_text: Vec<u8>,
    pub last_text: Vec<u8>,
}

/// One worker-encoded row group, ready for the ordered committer.
pub struct EncodedRg {
    sealed: SealedRg,
    hll: Option<Vec<Hll>>,
    probe: Option<Vec<ColOrderProbe>>,
}

impl EncodedRg {
    pub fn nrows(&self) -> u32 {
        self.sealed.nrows
    }

    /// Encoded body bytes (in-flight memory accounting).
    pub fn body_len(&self) -> usize {
        self.sealed.body.len()
    }
}

/// Worker-side chunk encoder: append parsed rows, seal into an [`EncodedRg`].
/// Exactly one RG per encoder — the segmentator cuts chunks at RG_ROWS row
/// boundaries so RG seams fall exactly where the serial writer's would.
pub struct RgChunkEncoder {
    plan: std::sync::Arc<ParallelIngestPlan>,
    builders: Vec<ColBuilder>,
    nbuf: usize,
    hll: Option<Vec<Hll>>,
    sorted: Option<Vec<bool>>,
    prev_int: Vec<i64>,
    prev_text: Vec<Vec<u8>>,
    first_int: Vec<i64>,
    first_text: Vec<Vec<u8>>,
    has_prev: bool,
}

impl RgChunkEncoder {
    pub fn new(plan: std::sync::Arc<ParallelIngestPlan>) -> RgChunkEncoder {
        let ncols = plan.coltypes.len();
        let builders = plan
            .coltypes
            .iter()
            .map(|t| {
                if t.is_text() {
                    ColBuilder::Text(TextBuilder { offs: Vec::new(), blob: Vec::new() })
                } else {
                    ColBuilder::Int(IntBuilder { vals: Vec::new() })
                }
            })
            .collect();
        RgChunkEncoder {
            builders,
            nbuf: 0,
            hll: plan.want_ndv.then(|| (0..ncols).map(|_| Hll::default()).collect()),
            sorted: plan.want_sorted.then(|| vec![true; ncols]),
            prev_int: vec![0; ncols],
            prev_text: vec![Vec::new(); ncols],
            first_int: vec![0; ncols],
            first_text: vec![Vec::new(); ncols],
            has_prev: false,
            plan,
        }
    }

    pub fn append_row(&mut self, values: &[Datum], isnull: &[bool]) -> PgResult<()> {
        debug_assert!(self.nbuf < RG_ROWS, "chunk encoder overfilled past one RG");
        append_row_into(
            &self.plan.coltypes,
            &mut self.builders,
            self.hll.as_mut(),
            self.sorted.as_mut(),
            &mut self.prev_int,
            &mut self.prev_text,
            self.has_prev,
            values,
            isnull,
        )?;
        if !self.has_prev {
            // First row: the probe's chunk-first values (prev_* just took
            // them — text prev updates on the first row because the chunk
            // tracker starts sorted).
            self.first_int.copy_from_slice(&self.prev_int);
            self.first_text.clone_from(&self.prev_text);
        }
        self.has_prev = true;
        self.nbuf += 1;
        Ok(())
    }

    pub fn rows(&self) -> usize {
        self.nbuf
    }

    pub fn seal(self) -> EncodedRg {
        assert!(self.nbuf > 0, "sealing an empty chunk encoder");
        let sealed = seal_rg_body(
            &self.plan.coltypes,
            &self.plan.opts,
            &self.builders,
            self.nbuf,
            self.plan.xid,
            self.plan.flags,
            &self.plan.capture,
        );
        let probe = self.sorted.map(|sorted| {
            (0..self.plan.coltypes.len())
                .map(|c| ColOrderProbe {
                    sorted: sorted[c],
                    first_int: self.first_int[c],
                    last_int: self.prev_int[c],
                    first_text: self.first_text[c].clone(),
                    last_text: self.prev_text[c].clone(),
                })
                .collect()
        });
        EncodedRg { sealed, hll: self.hll, probe }
    }
}

/// Parallel COPY (leader): a writer for ordered RG commits. The ordinary
/// open path — header init / append-to-committed-part invalidation / freeze
/// decision all identical to serial COPY. The caller owns admission (checked
/// BEFORE opening: pgrcolumnar AM, no cluster key — `writer_opts_of`).
pub fn begin_parallel_ingest(rel: &::types_rel::Relation<'_>) -> PgResult<CbWriter> {
    open_writer(rel)
}

/// load-r2 L3-1: the parallel load-sort opens the writer PLAIN — the sort
/// happens upstream in the COPY workers (memcmp-key runs + k-way merge),
/// and the merged rows drain through append_row/seal exactly like a plain
/// load of pre-sorted input (byte-identity contract). The presort sorter
/// the open installed is dropped here; opts.presort_key stays as the
/// caller's key-spec record.
pub fn begin_parallel_ingest_presorted(rel: &::types_rel::Relation<'_>) -> PgResult<CbWriter> {
    let mut w = open_writer(rel)?;
    w.sorter = None;
    Ok(w)
}

impl CbWriter {
    /// The encode plan for this writer's parallel workers; None when the
    /// writer sorts on ingest (cluster key) — that drain is serial by
    /// construction and admission should have refused already.
    pub fn parallel_ingest_plan(&self) -> Option<ParallelIngestPlan> {
        if self.sorter.is_some() {
            return None;
        }
        Some(ParallelIngestPlan {
            xid: self.xid,
            flags: (if self.frozen { RG_FLAG_FROZEN } else { 0 })
                | RG_FLAG_SUMS
                | RG_FLAG_LENSTATS
                | RG_FLAG_ZEROCNT,
            coltypes: self.coltypes.clone(),
            opts: self.opts.clone(),
            want_ndv: self.ndv.is_some(),
            want_sorted: self.sorted.is_some(),
            capture: (0..self.ncols)
                .map(|c| self.stitch.as_ref().is_some_and(|s| s[c].is_some()))
                .collect(),
        })
    }

    /// load-r3 M2: shard the live stitch columns across `nthreads` worker
    /// threads (see StitchPool). MUST be called AFTER parallel_ingest_plan
    /// (the plan snapshots capture flags from self.stitch). No-op unless
    /// this is a fresh part with live stitch builders and no committed RGs —
    /// the fail-closed refusal leaves the inline path verbatim.
    pub fn install_stitch_pool(&mut self, nthreads: usize) -> PgResult<()> {
        if self.stitch_pool.is_some() || !self.rgs.is_empty() {
            return Ok(());
        }
        let live = self
            .stitch
            .as_ref()
            .map(|s| s.iter().filter(|c| c.is_some()).count())
            .unwrap_or(0);
        if live == 0 {
            return Ok(());
        }
        let n = nthreads.clamp(1, 16).min(live);
        let cols = self.stitch.take().expect("live > 0");
        let mut assign: Vec<Option<usize>> = vec![None; self.ncols];
        let mut per_thread: Vec<Vec<(usize, StitchCol)>> = (0..n).map(|_| Vec::new()).collect();
        let mut next = 0usize;
        for (c, st) in cols.into_iter().enumerate() {
            if let Some(st) = st {
                assign[c] = Some(next % n);
                per_thread[next % n].push((c, st));
                next += 1;
            }
        }
        let mut txs = Vec::with_capacity(n);
        let mut handles = Vec::with_capacity(n);
        for owned in per_thread {
            let (tx, rx) = pgsync::mailbox::<StitchMsg>(None);
            // permit-s5 row 16: spawn through the pgsync::thread wrapper
            // (the spawn door for identity-less utility threads).
            let h = pgsync::thread::Builder::new()
                .name("cb-stitch".into())
                .spawn(move || stitch_pool_thread(owned, rx))
                .map_err(|e| {
                    Box::new(PgError::error(format!("stitch pool spawn failed: {e}")))
                })?;
            txs.push(tx);
            handles.push(h);
        }
        self.stitch_pool = Some(StitchPool { txs, handles, assign });
        Ok(())
    }

    /// Ordered commit of one worker-encoded RG (input order — the caller's
    /// contract). Replays exactly what the serial seal path does: body write
    /// at align64(write_off), footer row, stitch interning in RG order, NDV
    /// sketch union (register-identical to serial adds), sorted-tracker
    /// stitching across the chunk seam.
    pub fn commit_encoded_rg(&mut self, enc: EncodedRg) -> PgResult<()> {
        debug_assert!(self.nbuf == 0, "ordered commit into a writer with buffered rows");
        debug_assert!(self.sorter.is_none(), "ordered commit into a sorting writer");
        let mut sealed = enc.sealed;
        let t0 = std::time::Instant::now();
        if let Some(pool) = &self.stitch_pool {
            // Column-sharded interning: hand each captured dict to its
            // owning thread (FIFO per column = RG order = identical ids).
            for c in 0..self.ncols {
                let Some(t) = pool.assign[c] else { continue };
                let entries = sealed.dicts[c].take();
                if pool.txs[t].send(StitchMsg::Rg { col: c, entries }).is_err() {
                    return Err(Box::new(PgError::error(
                        "stitch pool thread exited early".to_string(),
                    )));
                }
            }
        } else {
            self.register_stitch(&mut sealed);
        }
        let t1 = std::time::Instant::now();
        self.commit_stitch += t1 - t0;
        let file_off = align64(self.write_off);
        self.file.write_all_at(&sealed.body, file_off)?;
        let t2 = std::time::Instant::now();
        self.commit_pwrite += t2 - t1;
        self.commit_bytes += sealed.body.len() as u64;
        self.write_off = file_off + sealed.body.len() as u64;
        if let Some(ndv) = &mut self.ndv {
            let hll = enc
                .hll
                .as_ref()
                .expect("parallel plan carries NDV sketches when the part tracks NDV");
            for (n, h) in ndv.iter_mut().zip(hll.iter()) {
                n.merge(h);
            }
        }
        if let Some(sorted) = &mut self.sorted {
            let probes = enc
                .probe
                .as_ref()
                .expect("parallel plan carries order probes when the part tracks sorted");
            for c in 0..self.ncols {
                if !sorted[c] {
                    continue;
                }
                let p = &probes[c];
                let seam_broken = self.has_prev
                    && match self.coltypes[c] {
                        ColType::Text => p.first_text.as_slice() < self.prev_text[c].as_slice(),
                        _ => p.first_int < self.prev_int[c],
                    };
                if !p.sorted || seam_broken {
                    sorted[c] = false;
                } else {
                    match self.coltypes[c] {
                        ColType::Text => self.prev_text[c].clone_from(&p.last_text),
                        _ => self.prev_int[c] = p.last_int,
                    }
                }
            }
        }
        if sealed.nrows > 0 {
            self.has_prev = true;
        }
        self.rgs.push(FooterRg {
            file_off,
            nrows: sealed.nrows,
            xmin: self.xid,
            flags: sealed.flags,
            chunks: sealed.chunks,
            sums: sealed.sums,
            lenstats: sealed.lenstats,
            zerocnt: sealed.zerocnt,
        });
        self.commit_meta += t2.elapsed();
        Ok(())
    }

    /// load-r3 M0: cumulative commit_encoded_rg phase walls —
    /// (stitch_s, pwrite_s, meta_s, body_bytes). meta = NDV merge + sorted
    /// probes + footer push.
    pub fn commit_phase_split(&self) -> (f64, f64, f64, u64) {
        (
            self.commit_stitch.as_secs_f64(),
            self.commit_pwrite.as_secs_f64(),
            self.commit_meta.as_secs_f64(),
            self.commit_bytes,
        )
    }

    /// Statement-end publish for the parallel driver (the ordinary finish:
    /// stitch blobs, footer, header, durable).
    pub fn finish_parallel_ingest(&mut self) -> PgResult<()> {
        self.finish()
    }
}

fn granule_minmax<T: Copy, F: Fn(T) -> i64>(vals: &[T], g: usize, f: F) -> (i64, i64) {
    let lo = g * GRANULE_ROWS;
    let hi = (lo + GRANULE_ROWS).min(vals.len());
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    for &v in &vals[lo..hi] {
        let x = f(v);
        min = min.min(x);
        max = max.max(x);
    }
    (min, max)
}

// Arm a bloom only where granule zone maps are uninformative (the average
// granule covers most of the chunk's range — i.e. the column is not a
// leading sort key of the load order) and the RG is high-NDV.
const BLOOM_MIN_NDV: usize = 4_096;

fn bloom_armed(vals: &[i64], min: i64, max: i64, gmm: &[(i64, i64)]) -> bool {
    let range = max as i128 - min as i128;
    if range == 0 || gmm.is_empty() {
        return false;
    }
    let sum: i128 = gmm.iter().map(|&(lo, hi)| hi as i128 - lo as i128).sum();
    if (sum / gmm.len() as i128) * 2 < range {
        return false;
    }
    let mut seen = HashMap::with_capacity(vals.len());
    for &v in vals {
        seen.insert(v, ());
        if seen.len() >= BLOOM_MIN_NDV {
            return true;
        }
    }
    false
}

// Serialize granule g's plain FOR/Raw payload image into `out`.
fn enc_int_granule(out: &mut Vec<u8>, vals: &[i64], g: usize, encoding: Encoding, width: u8, base: i64) {
    let lo = g * GRANULE_ROWS;
    let hi = (lo + GRANULE_ROWS).min(vals.len());
    match encoding {
        Encoding::For => match width {
            1 => out.extend(vals[lo..hi].iter().map(|&v| (v - base) as u8)),
            2 => {
                for &v in &vals[lo..hi] {
                    out.extend_from_slice(&(((v - base) as u16).to_le_bytes()));
                }
            }
            4 => {
                for &v in &vals[lo..hi] {
                    out.extend_from_slice(&(((v - base) as u32).to_le_bytes()));
                }
            }
            _ => unreachable!(),
        },
        Encoding::Raw => {
            for &v in &vals[lo..hi] {
                out.extend_from_slice(&v.to_le_bytes());
            }
        }
        _ => unreachable!(),
    }
}

// Serialize granule g's DeltaFor raw image into `out` (format.rs §DeltaFor):
// [first i64 | dmin i64 | width u8 | pad7 | (d[i]-dmin) at width, i in 1..n].
// All arithmetic wraps (i64 bounds legal); width 0 = every delta == dmin.
fn enc_delta_granule(out: &mut Vec<u8>, vals: &[i64], g: usize) {
    let lo = g * GRANULE_ROWS;
    let hi = (lo + GRANULE_ROWS).min(vals.len());
    let v = &vals[lo..hi];
    let mut dmin = 0i64;
    let mut dmax = 0i64;
    for i in 1..v.len() {
        let d = v[i].wrapping_sub(v[i - 1]);
        if i == 1 {
            (dmin, dmax) = (d, d);
        } else {
            dmin = dmin.min(d);
            dmax = dmax.max(d);
        }
    }
    let range = (dmax as i128 - dmin as i128) as u128;
    let width: u8 = if range == 0 {
        0
    } else if range <= u8::MAX as u128 {
        1
    } else if range <= u16::MAX as u128 {
        2
    } else if range <= u32::MAX as u128 {
        4
    } else {
        8
    };
    out.extend_from_slice(&v[0].to_le_bytes());
    out.extend_from_slice(&dmin.to_le_bytes());
    out.push(width);
    out.extend_from_slice(&[0u8; 7]);
    // (d - dmin) truncated to `width` bytes: the true difference is < 2^(8w)
    // for w <= 4 by the range test, and mod-2^64 for w == 8 — the decoder
    // adds dmin back in the same wrapping domain either way.
    match width {
        0 => {}
        1 => {
            for i in 1..v.len() {
                out.push(v[i].wrapping_sub(v[i - 1]).wrapping_sub(dmin) as u8);
            }
        }
        2 => {
            for i in 1..v.len() {
                let d = v[i].wrapping_sub(v[i - 1]).wrapping_sub(dmin) as u16;
                out.extend_from_slice(&d.to_le_bytes());
            }
        }
        4 => {
            for i in 1..v.len() {
                let d = v[i].wrapping_sub(v[i - 1]).wrapping_sub(dmin) as u32;
                out.extend_from_slice(&d.to_le_bytes());
            }
        }
        _ => {
            for i in 1..v.len() {
                let d = v[i].wrapping_sub(v[i - 1]).wrapping_sub(dmin) as u64;
                out.extend_from_slice(&d.to_le_bytes());
            }
        }
    }
}

pub(crate) fn encode_int_chunk(
    body: &mut Vec<u8>,
    vals: &[i64],
    ngranules: u32,
    cc: &CodecCtx,
) -> (i64, i64) {
    let n = vals.len();
    let min = vals.iter().copied().min().unwrap();
    let max = vals.iter().copied().max().unwrap();
    let (encoding, width) = if min == max {
        (Encoding::Const, 0u8)
    } else {
        let range = (max as i128 - min as i128) as u128;
        let w = if range <= u8::MAX as u128 {
            1
        } else if range <= u16::MAX as u128 {
            2
        } else if range <= u32::MAX as u128 {
            4
        } else {
            8
        };
        if w == 8 { (Encoding::Raw, 8) } else { (Encoding::For, w) }
    };
    let ng = ngranules as usize;
    let mut gmm = Vec::with_capacity(ng);
    for g in 0..ng {
        gmm.push(granule_minmax(vals, g, |v| v));
    }
    let mut flags = 0u16;
    if encoding != Encoding::Const {
        flags |= CHUNK_FLAG_BLOCK_ZM;
        if bloom_armed(vals, min, max, &gmm) {
            flags |= CHUNK_FLAG_BLOOM;
        }
    }
    // v6 codec menu: sample granule 0 to pick a codec, then frame every
    // granule; keep the plain zero-decode lane unless the full chunk clears
    // the >=10% win gate too. Narrow FOR lanes (1-2B) are the decode-hot
    // class — pick() holds them to a stricter ZSTD threshold.
    let mut encoding = encoding;
    let mut width = width;
    let mut frames: Vec<(Vec<u8>, u32)> = Vec::new();
    let mut codec = Codec::None;
    if encoding != Encoding::Const {
        let mut plain = Vec::with_capacity(GRANULE_ROWS * width as usize);
        enc_int_granule(&mut plain, vals, 0, encoding, width, min);
        if let Some(c) = cc.pick(&plain, encoding == Encoding::For && width <= 2) {
            let mut frames_len = 0usize;
            let mut raw_len = 0usize;
            frames.reserve(ng);
            for g in 0..ng {
                if g > 0 {
                    plain.clear();
                    enc_int_granule(&mut plain, vals, g, encoding, width, min);
                }
                let comp = cc.compress(c, &plain);
                frames_len += frame_len(comp.len());
                raw_len += plain.len();
                frames.push((comp, plain.len() as u32));
            }
            if frames_len * 10 <= raw_len * 9 {
                codec = c;
            } else {
                frames.clear();
            }
        }
    }
    let mut payload_len = match encoding {
        Encoding::Const => 0u64,
        _ if codec != Codec::None => {
            frames.iter().map(|(f, _)| frame_len(f.len()) as u64).sum()
        }
        _ => (n * width as usize) as u64,
    };
    // intcodec delta arm: encode every granule's DeltaFor image, sample
    // granule 0 for the frame codec, and switch the whole chunk to DeltaFor
    // iff the framed delta payload beats the plain arm's FINAL size (incl.
    // its codec win) by >=10%. Deterministic — the decision is fully
    // recorded in the chunk header (encoding + codec tags).
    if cc.delta && encoding != Encoding::Const {
        let mut dframes: Vec<(Vec<u8>, u32)> = Vec::with_capacity(ng);
        let mut dcodec = Codec::None;
        let mut dtotal = 0u64;
        let mut img = Vec::with_capacity(24 + GRANULE_ROWS * 8);
        for g in 0..ng {
            img.clear();
            enc_delta_granule(&mut img, vals, g);
            if g == 0 {
                // Delta lanes decode through a prefix sum regardless, so the
                // narrow_hot LZ4 bias does not apply — let ZSTD compete on
                // ratio (survey: delta+ZSTD dominates on the analytics banks).
                dcodec = cc.pick(&img, false).unwrap_or(Codec::None);
            }
            let stored = match dcodec {
                Codec::None => img.clone(),
                c => {
                    let comp = cc.compress(c, &img);
                    // Per-granule incompressible guard: a frame must never
                    // expand past its raw image (keeps worst case bounded).
                    if comp.len() >= img.len() { img.clone() } else { comp }
                }
            };
            // comp_len == raw_len marks an in-place (uncompressed) frame.
            dtotal += frame_len(stored.len()) as u64;
            dframes.push((stored, img.len() as u32));
        }
        if dtotal * 10 <= payload_len * 9 {
            encoding = Encoding::DeltaFor;
            width = 0;
            codec = dcodec;
            frames = dframes;
            payload_len = dtotal;
            INTCODEC_DELTA_CHOSEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        } else {
            INTCODEC_KEPT_PLAIN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
    ChunkHeader {
        encoding,
        width,
        flags,
        ngranules,
        aux: min,
        payload_len,
        codec,
    }
    .encode(body);
    let mut frame_off = 0u64;
    // DeltaFor granules are ALWAYS framed (even under Codec::None), so their
    // directory offsets are frame offsets unconditionally.
    let framed = codec != Codec::None || encoding == Encoding::DeltaFor;
    for (g, &(gmin, gmax)) in gmm.iter().enumerate() {
        let off = if framed {
            let o = frame_off;
            frame_off += frame_len(frames[g].0.len()) as u64;
            o
        } else {
            ((g * GRANULE_ROWS) * width as usize) as u64
        };
        put_u64(body, off);
        put_i64(body, gmin);
        put_i64(body, gmax);
    }
    if flags & CHUNK_FLAG_BLOCK_ZM != 0 {
        for g in 0..ng {
            for b in 0..BLOCKS_PER_GRANULE {
                let lo = g * GRANULE_ROWS + b * BLOCK_ROWS;
                let hi = (lo + BLOCK_ROWS).min(n);
                if lo >= n {
                    put_i64(body, i64::MAX);
                    put_i64(body, i64::MIN);
                    continue;
                }
                let mut bmin = i64::MAX;
                let mut bmax = i64::MIN;
                for &v in &vals[lo..hi] {
                    bmin = bmin.min(v);
                    bmax = bmax.max(v);
                }
                put_i64(body, bmin);
                put_i64(body, bmax);
            }
        }
    }
    if flags & CHUNK_FLAG_BLOOM != 0 {
        for g in 0..ng {
            let lo = g * GRANULE_ROWS;
            let hi = (lo + GRANULE_ROWS).min(n);
            let start = body.len();
            body.resize(start + crate::bloom::BLOOM_BYTES, 0);
            for &v in &vals[lo..hi] {
                crate::bloom::bloom_insert(&mut body[start..], v);
            }
        }
    }
    while body.len() % 64 != 0 {
        body.push(0);
    }
    match encoding {
        Encoding::Const => {}
        Encoding::DeltaFor => {
            for (stored, raw_len) in &frames {
                push_frame(body, *raw_len as usize, stored);
            }
        }
        Encoding::For | Encoding::Raw => {
            if codec != Codec::None {
                for (comp, raw_len) in &frames {
                    push_frame(body, *raw_len as usize, comp);
                }
            } else {
                for g in 0..ng {
                    enc_int_granule(body, vals, g, encoding, width, min);
                }
            }
        }
        _ => unreachable!(),
    }
    (min, max)
}

// Text payloads (impl doc §1.4): blob entries are complete 4B-U varlena
// images, 4-byte aligned, so decode publishes pointers with no copies.
//   Dict:    codes[n] (1/2/4 B) | dict_off[ndv] u32 | blob
//   RawText: off[n] u32 | blob
//
// `capture_dict` requests the RG's dict entries back (byte-sorted local-code
// order) for the caller's v7 stitch registration: Some(entries) ⇔ the chunk
// dict-encoded. The caller owns the poison-on-None rule (a non-dict RG kills
// the column's global code space). Interning used to happen inline here; the
// capture split lets the parallel COPY workers encode RGs while the ordered
// committer owns the part-lifetime stitch state — same allocations (one
// boxed key per dict entry), same bytes.
fn encode_text_chunk(
    body: &mut Vec<u8>,
    tb: &TextBuilder,
    ngranules: u32,
    cc: &CodecCtx,
    capture_dict: bool,
) -> (i64, i64, Option<Vec<Box<[u8]>>>) {
    let n = tb.offs.len();

    // Dictionary pass over the row set.
    let mut dict: HashMap<&[u8], u32, DictBuild> =
        HashMap::with_capacity_and_hasher(1024, DictBuild::new());
    let mut order: Vec<(u32, u32)> = Vec::new();
    let mut codes: Vec<u32> = Vec::with_capacity(n);
    let mut dict_blob_len = 0usize;
    for &(off, len) in &tb.offs {
        let s = &tb.blob[off as usize..(off + len) as usize];
        let next = order.len() as u32;
        let code = *dict.entry(s).or_insert_with(|| {
            order.push((off, len));
            dict_blob_len += align4(VARLENA_IMG_HDR + len as usize);
            next
        });
        codes.push(code);
    }
    let ndv = order.len();
    let code_w: usize = if ndv <= 1 << 8 {
        1
    } else if ndv <= 1 << 16 {
        2
    } else {
        4
    };
    let raw_blob_len: usize =
        tb.offs.iter().map(|&(_, l)| align4(VARLENA_IMG_HDR + l as usize)).sum();
    let dict_size = n * code_w + ndv * 4 + dict_blob_len;
    let raw_size = n * 4 + raw_blob_len;
    let use_dict = ndv <= 65_536 && dict_size < raw_size;

    // Zone maps: byte length min/max per granule.
    let (mut min, mut max) = (i64::MAX, i64::MIN);
    let mut gmm = Vec::with_capacity(ngranules as usize);
    for g in 0..ngranules as usize {
        let (gmin, gmax) = granule_minmax(&tb.offs, g, |(_, l)| l as i64);
        gmm.push((gmin, gmax));
        min = min.min(gmin);
        max = max.max(gmax);
    }

    let mut captured: Option<Vec<Box<[u8]>>> = None;
    if use_dict {
        // Byte-order dict sort + code remap (CHUNK_FLAG_DICT_SORTED): codes
        // become rank order so a LIKE-prefix can evaluate as a code-range
        // check. dict[code] is unchanged under the consistent remap.
        let entry = |&(off, len): &(u32, u32)| &tb.blob[off as usize..(off + len) as usize];
        let mut perm: Vec<u32> = (0..ndv as u32).collect();
        perm.sort_unstable_by(|&a, &b| entry(&order[a as usize]).cmp(entry(&order[b as usize])));
        let mut remap = vec![0u32; ndv];
        for (new, &old) in perm.iter().enumerate() {
            remap[old as usize] = new as u32;
        }
        let order: Vec<(u32, u32)> = perm.iter().map(|&o| order[o as usize]).collect();
        for c in codes.iter_mut() {
            *c = remap[*c as usize];
        }
        // v7 stitch capture: the RG's dict entries in local code = byte-rank
        // order, for the caller's part-lifetime interning.
        if capture_dict {
            captured = Some(
                order
                    .iter()
                    .map(|&(off, len)| Box::from(&tb.blob[off as usize..(off + len) as usize]))
                    .collect(),
            );
        }
        // Compressed-dict candidate: one frame over the varlena-image dict
        // blob (codec from the v6 menu, sampled on the blob itself), taken on
        // a >=10% payload win; codes + dict_off stay plain.
        let mut dict_blob: Vec<u8> = Vec::with_capacity(dict_blob_len);
        for &(off, len) in &order {
            push_varlena_image(&mut dict_blob, &tb.blob[off as usize..(off + len) as usize]);
        }
        debug_assert_eq!(dict_blob.len(), dict_blob_len);
        let head_len = align4(n * code_w) + ndv * 4;
        let picked = cc.pick(&dict_blob, false);
        // Sub-granule frame plan (CHUNK_FLAG_DICT_FRAMED): entry-boundary
        // cuts every ~dict_frame_target RAW bytes, byte-sorted order
        // untouched. None = single-frame legacy layout (arm off, no codec
        // win, or the blob fits one target).
        let frames: Option<Vec<(u32, Vec<u8>)>> = (cc.dict_frames
            && picked.is_some()
            && dict_blob_len > cc.dict_frame_target)
            .then(|| {
                let codec = picked.unwrap();
                let mut frames: Vec<(u32, Vec<u8>)> = Vec::new(); // (raw_len, stored)
                let mut lo = 0usize;
                let mut acc = 0usize;
                let push = |lo: usize, hi: usize, frames: &mut Vec<(u32, Vec<u8>)>| {
                    let raw = &dict_blob[lo..hi];
                    let comp = cc.compress(codec, raw);
                    // Incompressible guard: comp_len == raw_len marks a
                    // stored-raw frame (a compressed frame is strictly
                    // shorter by this arm).
                    let stored = if comp.len() < raw.len() { comp } else { raw.to_vec() };
                    frames.push(((hi - lo) as u32, stored));
                };
                for &(_, len) in &order {
                    acc += align4(VARLENA_IMG_HDR + len as usize);
                    if acc - lo >= cc.dict_frame_target {
                        push(lo, acc, &mut frames);
                        lo = acc;
                    }
                }
                if lo < dict_blob_len {
                    push(lo, dict_blob_len, &mut frames);
                }
                frames
            })
            .filter(|f| f.len() > 1);
        let comp = if frames.is_none() {
            picked.map(|c| cc.compress(c, &dict_blob)).unwrap_or_default()
        } else {
            Vec::new()
        };
        let comp_blob_len = match &frames {
            Some(f) => {
                align4(4 + 8 * f.len() + f.iter().map(|(_, s)| s.len()).sum::<usize>())
            }
            None => frame_len(comp.len()),
        };
        let use_comp = picked.is_some()
            && (head_len + comp_blob_len) * 10 <= (head_len + dict_blob_len) * 9;
        let (encoding, codec, stored_blob_len) = if use_comp {
            (Encoding::Lz4Dict, picked.unwrap(), comp_blob_len)
        } else {
            (Encoding::Dict, Codec::None, dict_blob_len)
        };
        let payload_len = (head_len + stored_blob_len) as u64;
        ChunkHeader {
            encoding,
            width: code_w as u8,
            flags: CHUNK_FLAG_DICT_SORTED
                | if use_comp && frames.is_some() { CHUNK_FLAG_DICT_FRAMED } else { 0 },
            ngranules,
            aux: ndv as i64,
            payload_len,
            codec,
        }
        .encode(body);
        for (g, &(gmin, gmax)) in gmm.iter().enumerate() {
            put_u64(body, (g * GRANULE_ROWS * code_w) as u64);
            put_i64(body, gmin);
            put_i64(body, gmax);
        }
        while body.len() % 64 != 0 {
            body.push(0);
        }
        match code_w {
            1 => body.extend(codes.iter().map(|&c| c as u8)),
            2 => {
                for &c in &codes {
                    body.extend_from_slice(&(c as u16).to_le_bytes());
                }
            }
            _ => {
                for &c in &codes {
                    body.extend_from_slice(&c.to_le_bytes());
                }
            }
        }
        while body.len() % 4 != 0 {
            body.push(0);
        }
        // dict_off table (offsets into the DECOMPRESSED blob) then the blob.
        let mut blob_off = 0u32;
        for &(_, len) in &order {
            put_u32(body, blob_off);
            blob_off += align4(VARLENA_IMG_HDR + len as usize) as u32;
        }
        if use_comp {
            let start = body.len();
            match &frames {
                Some(f) => {
                    // Framed region: u32 nframes | per-frame (u32 raw_len |
                    // u32 comp_len) | frames back-to-back | pad4.
                    put_u32(body, f.len() as u32);
                    for (raw_len, stored) in f {
                        put_u32(body, *raw_len);
                        put_u32(body, stored.len() as u32);
                    }
                    for (_, stored) in f {
                        body.extend_from_slice(stored);
                    }
                }
                None => {
                    put_u32(body, dict_blob_len as u32);
                    put_u32(body, comp.len() as u32);
                    body.extend_from_slice(&comp);
                }
            }
            while body.len() - start != comp_blob_len {
                body.push(0);
            }
        } else {
            body.extend_from_slice(&dict_blob);
        }
    } else {
        // Non-dict RG: captured stays None — the caller poisons the
        // column's stitch (a partial global code space would break the
        // part-scope facts consumers rely on).
        //
        // Compressed-text candidate (S3 footprint step): per-granule frames
        // (codec from the v6 menu, sampled on granule 0's blob) over the
        // varlena-image blob, granule-relative offsets; decode is
        // decompress-then-pointer-gather. Taken only on a >=10% payload win
        // so incompressible chunks keep the zero-decode RAWTEXT lane.
        let mut frames: Vec<(Vec<u8>, u32)> = Vec::with_capacity(ngranules as usize);
        let mut offs_rel: Vec<u32> = Vec::with_capacity(n);
        let mut max_raw = 0usize;
        let mut comp_frames_len = 0usize;
        let mut gblob: Vec<u8> = Vec::new();
        let mut picked: Option<Codec> = None;
        for g in 0..ngranules as usize {
            let lo = g * GRANULE_ROWS;
            let hi = (lo + GRANULE_ROWS).min(n);
            gblob.clear();
            for &(off, len) in &tb.offs[lo..hi] {
                offs_rel.push(gblob.len() as u32);
                push_varlena_image(&mut gblob, &tb.blob[off as usize..(off + len) as usize]);
            }
            max_raw = max_raw.max(gblob.len());
            if g == 0 {
                picked = cc.pick(&gblob, false);
            }
            let Some(c) = picked else { break };
            let comp = cc.compress(c, &gblob);
            comp_frames_len += frame_len(comp.len());
            frames.push((comp, gblob.len() as u32));
        }
        let comp_size = n * 4 + comp_frames_len;
        if picked.is_some() && comp_size * 10 <= (n * 4 + raw_blob_len) * 9 {
            let payload_len = (n * 4) as u64 + comp_frames_len as u64;
            ChunkHeader {
                encoding: Encoding::Lz4Text,
                width: 4,
                flags: 0,
                ngranules,
                aux: max_raw as i64,
                payload_len,
                codec: picked.unwrap(),
            }
            .encode(body);
            let mut frame_off = (n * 4) as u64;
            for (g, &(gmin, gmax)) in gmm.iter().enumerate() {
                put_u64(body, frame_off);
                put_i64(body, gmin);
                put_i64(body, gmax);
                frame_off += align4(8 + frames[g].0.len()) as u64;
            }
            while body.len() % 64 != 0 {
                body.push(0);
            }
            for &o in &offs_rel {
                put_u32(body, o);
            }
            for (comp, raw_len) in &frames {
                let start = body.len();
                put_u32(body, *raw_len);
                put_u32(body, comp.len() as u32);
                body.extend_from_slice(comp);
                while body.len() - start != align4(8 + comp.len()) {
                    body.push(0);
                }
            }
        } else {
            let payload_len = (n * 4) as u64 + raw_blob_len as u64;
            ChunkHeader {
                encoding: Encoding::RawText,
                width: 4,
                flags: 0,
                ngranules,
                aux: 0,
                payload_len,
                codec: Codec::None,
            }
            .encode(body);
            for (g, &(gmin, gmax)) in gmm.iter().enumerate() {
                put_u64(body, (g * GRANULE_ROWS * 4) as u64);
                put_i64(body, gmin);
                put_i64(body, gmax);
            }
            while body.len() % 64 != 0 {
                body.push(0);
            }
            let mut blob_off = 0u32;
            for &(_, len) in &tb.offs {
                put_u32(body, blob_off);
                blob_off += align4(VARLENA_IMG_HDR + len as usize) as u32;
            }
            for &(off, len) in &tb.offs {
                push_varlena_image(body, &tb.blob[off as usize..(off + len) as usize]);
            }
        }
    }
    (min, max, captured)
}

const VARLENA_IMG_HDR: usize = 4;

fn push_varlena_image(body: &mut Vec<u8>, s: &[u8]) {
    body.extend_from_slice(&::datum::set_varsize_4b(VARLENA_IMG_HDR + s.len()));
    body.extend_from_slice(s);
    while body.len() % 4 != 0 {
        body.push(0);
    }
}

// ---- AM entry points -------------------------------------------------------

pub fn multi_insert<'mcx>(
    rel: &::types_rel::Relation<'mcx>,
    slots: &mut [&mut ::types_slot::SlotData<'mcx>],
) -> PgResult<()> {
    let oid = rel.rd_id;
    let xid = xact_seams::get_current_transaction_id::call()?;
    let cid = xact_seams::get_current_command_id::call(false)?;
    WRITERS.with(|w| {
        let mut map = w.borrow_mut();
        // Evict writers from another transaction OR another command: buffered
        // ingest is per-statement (the statement-end flush publishes it), so
        // a writer abandoned by an errored statement must not leak its rows
        // into a later statement of the same transaction.
        let stale = map.get(&oid).is_some_and(|cw| cw.xid != xid || cw.cid != cid);
        if stale {
            map.remove(&oid);
        }
        if !map.contains_key(&oid) {
            map.insert(oid, open_writer(rel)?);
        }
        let cw = map.get_mut(&oid).unwrap();
        for slot in slots.iter_mut() {
            // Deform before reading: buffer/heap-backed slots (CTAS from a
            // heap table feeds the scan slot straight in) arrive with
            // tts_nvalid == 0, and reading tts_values/tts_isnull raw off
            // them fabricated NULLs on NULL-free data. No-op on the
            // already-deformed COPY/INSERT..SELECT virtual slots.
            exectuples::slot_getallattrs(slot);
            let base = slot.base();
            debug_assert!(base.tts_nvalid as usize >= cw.ncols);
            cw.ingest_row(&base.tts_values, &base.tts_isnull)?;
        }
        Ok(())
    })
}

pub fn tuple_insert<'mcx>(
    rel: &::types_rel::Relation<'mcx>,
    slot: &mut ::types_slot::SlotData<'mcx>,
) -> PgResult<()> {
    // Single-row inserts buffer like COPY: the row joins the per-(xid, cid)
    // ingest writer and the statement-end flush (ExecModifyTable's pgrcolumnar
    // finish, or COPY's finish_bulk_insert) publishes RG-sized seals. The
    // old finish-per-row form sealed ONE ROW GROUP PER ROW on INSERT..SELECT
    // (24 GB for 2M rows), each with a full footer rewrite.
    let mut slots = [slot];
    multi_insert(rel, &mut slots)
}

pub fn finish_bulk_insert(rel: &::types_rel::Relation<'_>) -> PgResult<()> {
    let oid = rel.rd_id;
    WRITERS.with(|w| {
        let Some(mut cw) = w.borrow_mut().remove(&oid) else {
            return Ok(());
        };
        // Never publish a writer abandoned by an errored statement (or a
        // rolled-back subtransaction): a later statement's flush must drop
        // it, not commit its buffered rows. Mirror of multi_insert's stale
        // eviction; probes without get_current_transaction_id so a row-less
        // statement doesn't force an xid assignment.
        if !xact_seams::transaction_id_is_current_transaction_id::call(cw.xid)
            || cw.cid != xact_seams::get_current_command_id::call(false)?
        {
            return Ok(());
        }
        cw.finish()
    })
}

/// Top-level transaction end (commit AND abort): drop every buffered ingest
/// writer without publishing. Any writer still present here is abandoned —
/// `finish_bulk_insert` already removed (and published) the successful
/// statement's writer at statement end, and the stale checks above never
/// publish a leftover. C parity: copyfrom.c's COPY state lives in
/// statement-lifetime memory contexts, so an error unwind frees it during
/// abort processing — never at backend exit. Without this purge an errored
/// presort COPY leaves its writer in backend TLS until thread teardown,
/// where dropping the sorter's lifetime-erased relcache tupdesc runs after
/// the backend's context arenas are gone (server abort in the arena
/// dealloc). Called from the AM dispatch layer's xact callback; at the
/// XACT_EVENT_ABORT/COMMIT call point the relcache entry and all arenas are
/// still live, so the drops here are the in-lifetime mirror of C's
/// context reset.
pub fn at_eoxact() {
    WRITERS.with(|w| w.borrow_mut().clear());
}

#[cfg(test)]
mod abortsafe_tests {
    use super::*;
    use crate::reader::{part_footer_rows, Part};

    fn tmp(name: &str) -> String {
        let p = std::env::temp_dir()
            .join(format!("cbstore-abortsafe-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_file(&p);
        std::fs::write(&p, []).unwrap();
        p.to_str().unwrap().to_string()
    }

    fn put_ints(w: &mut CbWriter, n: usize) {
        for i in 0..n {
            w.append_row(&[Datum::from_i64(i as i64)], &[false]).unwrap();
        }
    }

    // Abort mid-ingest on a fresh part (writer dropped without finish, row
    // groups already sealed to disk): header-first init keeps the part
    // readable as EMPTY, and a later committed load reads back exactly its
    // own rows.
    #[test]
    fn abort_mid_ingest_fresh_part_stays_readable() {
        let path = tmp("fresh-abort");
        let mut w = open_writer_at(&path, vec![ColType::I64]).unwrap();
        put_ints(&mut w, RG_ROWS + 5); // one RG sealed to disk + 5 buffered
        drop(w); // statement error: no finish, no publish
        assert!(std::fs::metadata(&path).unwrap().len() > CB_HEADER_LEN);
        assert_eq!(part_footer_rows(&path, 1).unwrap(), None);
        assert!(Part::open(&path, 1).unwrap().is_none());

        let mut w2 = open_writer_at(&path, vec![ColType::I64]).unwrap();
        put_ints(&mut w2, 3);
        w2.finish().unwrap();
        assert_eq!(part_footer_rows(&path, 1).unwrap(), Some(3));
        assert_eq!(Part::open(&path, 1).unwrap().unwrap().total_rows(), 3);
        std::fs::remove_file(&path).unwrap();
    }

    // Legacy corruption shape (pre-header-first writers): row-group bytes on
    // disk with a zero hole where the header belongs. Reads treat it as an
    // empty part instead of erroring forever, and a writer reopen heals it.
    #[test]
    fn legacy_zero_header_hole_heals() {
        let path = tmp("zero-hole");
        std::fs::write(&path, vec![0u8; 200_000]).unwrap();
        assert_eq!(part_footer_rows(&path, 1).unwrap(), None);
        assert!(Part::open(&path, 1).unwrap().is_none());

        let mut w = open_writer_at(&path, vec![ColType::I64]).unwrap();
        put_ints(&mut w, 7);
        w.finish().unwrap();
        assert_eq!(part_footer_rows(&path, 1).unwrap(), Some(7));
        assert_eq!(Part::open(&path, 1).unwrap().unwrap().total_rows(), 7);
        std::fs::remove_file(&path).unwrap();
    }

    // A header whose footer_off points past EOF (torn state) must produce a
    // clean error on every read path — never a huge alloc or a slice panic.
    #[test]
    fn torn_footer_pointer_errors_cleanly() {
        let path = tmp("torn-footer");
        let mut hdr = Vec::new();
        put_u64(&mut hdr, CB_MAGIC);
        put_u32(&mut hdr, CB_VERSION);
        put_u32(&mut hdr, 1);
        put_u64(&mut hdr, 1 << 40); // footer_off far past EOF
        put_u64(&mut hdr, 0x5aa5);
        hdr.resize(CB_HEADER_LEN as usize, 0);
        std::fs::write(&path, &hdr).unwrap();
        assert!(part_footer_rows(&path, 1).is_err());
        assert!(Part::open(&path, 1).is_err());
        std::fs::remove_file(&path).unwrap();
    }

    // Header-first: opening a writer over an empty part publishes a valid
    // empty header immediately (readers concurrent with the first COPY see
    // an empty part, and an abort at ANY later point leaves it readable).
    #[test]
    fn writer_open_initializes_header() {
        let path = tmp("init-hdr");
        let w = open_writer_at(&path, vec![ColType::I64]).unwrap();
        drop(w);
        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.len() as u64 >= CB_HEADER_LEN);
        let (footer_off, fp, version) =
            crate::reader::read_header(&bytes[..CB_HEADER_LEN as usize]).unwrap();
        assert_eq!((footer_off, fp, version), (0, 0x5aa5, CB_VERSION));
        std::fs::remove_file(&path).unwrap();
    }
}

#[cfg(test)]
mod lenstats_tests {
    use super::*;

    fn tmp(name: &str) -> String {
        let p = std::env::temp_dir()
            .join(format!("cbstore-lenstats-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_file(&p);
        std::fs::write(&p, []).unwrap();
        p.to_str().unwrap().to_string()
    }

    fn writer_at(path: &str, coltypes: Vec<ColType>) -> CbWriter {
        let opts = CbWriterOpts::plain(coltypes.len());
        open_writer_inner(SegFile::open_rw(path).unwrap(), 1, 0, true, coltypes, 0x5aa5, opts)
            .unwrap()
    }

    fn text_datum(s: &[u8], keep: &mut Vec<Vec<u8>>) -> Datum {
        let mut v = Vec::with_capacity(4 + s.len());
        v.extend_from_slice(&(((s.len() + 4) as u32) << 2).to_le_bytes());
        v.extend_from_slice(s);
        keep.push(v);
        Datum::from_usize(keep.last().unwrap().as_ptr() as usize)
    }

    // Deterministic length pattern with empties sprinkled in (multibyte
    // bytes included — the stats are octet counts, encoding-agnostic).
    fn row_text(i: usize) -> Vec<u8> {
        match i % 7 {
            0 => Vec::new(),
            1 => b"\xc3\xa9".to_vec(), // 2-byte UTF-8
            k => vec![b'x'; k * 3],
        }
    }

    #[test]
    fn lenstats_roundtrip_exact_per_granule() {
        let path = tmp("rt");
        let mut w = writer_at(&path, vec![ColType::I64, ColType::Text]);
        // 2 RGs, the second partial mid-granule (RG_ROWS + 1.5 granules).
        let n = RG_ROWS + GRANULE_ROWS + GRANULE_ROWS / 2;
        let mut keep = Vec::new();
        for i in 0..n {
            let t = row_text(i);
            let vals = [Datum::from_i64(i as i64), text_datum(&t, &mut keep)];
            w.append_row(&vals, &[false, false]).unwrap();
            keep.clear();
        }
        w.finish().unwrap();
        let part = crate::reader::Part::open(&path, 2).unwrap().unwrap();
        assert!(part.has_len_stats(1));
        assert!(!part.has_len_stats(0));
        for rg in 0..part.rgs.len() {
            assert_ne!(part.rgs[rg].flags & RG_FLAG_LENSTATS, 0);
            let rg_rows = part.rgs[rg].nrows as usize;
            for g in 0..rg_rows.div_ceil(GRANULE_ROWS) {
                let lo = rg * RG_ROWS + g * GRANULE_ROWS;
                let hi = lo + (rg_rows - g * GRANULE_ROWS).min(GRANULE_ROWS);
                let (mut sum, mut empty) = (0u64, 0u32);
                for i in lo..hi {
                    let t = row_text(i);
                    sum += t.len() as u64;
                    empty += t.is_empty() as u32;
                }
                assert_eq!(
                    part.granule_len_stats(rg, g, 1),
                    Some((sum, (hi - lo) as u32, empty)),
                    "rg {rg} g {g}"
                );
                assert_eq!(part.granule_len_stats(rg, g, 0), None);
            }
        }
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn lenstats_survive_reopen_append() {
        let path = tmp("reopen");
        let mut w = writer_at(&path, vec![ColType::I32, ColType::Text]);
        let mut keep = Vec::new();
        for i in 0..1000usize {
            let t = row_text(i);
            let vals = [Datum::from_i64(i as i64), text_datum(&t, &mut keep)];
            w.append_row(&vals, &[false, false]).unwrap();
            keep.clear();
        }
        w.finish().unwrap();
        // Reopen-append: the preserved RG's stats must re-emit exactly; the
        // new RG gets its own.
        let mut w2 = writer_at(&path, vec![ColType::I32, ColType::Text]);
        let d = text_datum(b"tail-row", &mut keep);
        w2.append_row(&[Datum::from_i64(7), d], &[false, false]).unwrap();
        w2.finish().unwrap();
        let part = crate::reader::Part::open(&path, 2).unwrap().unwrap();
        assert_eq!(part.rgs.len(), 2);
        let (mut sum, mut empty) = (0u64, 0u32);
        for i in 0..1000usize {
            let t = row_text(i);
            sum += t.len() as u64;
            empty += t.is_empty() as u32;
        }
        assert_eq!(part.granule_len_stats(0, 0, 1), Some((sum, 1000, empty)));
        assert_eq!(part.granule_len_stats(1, 0, 1), Some((8, 1, 0)));
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn lenstats_absent_without_text_columns() {
        let path = tmp("notext");
        let mut w = writer_at(&path, vec![ColType::I64, ColType::I32]);
        w.append_row(&[Datum::from_i64(1), Datum::from_i64(2)], &[false, false]).unwrap();
        w.finish().unwrap();
        let part = crate::reader::Part::open(&path, 2).unwrap().unwrap();
        assert!(!part.has_len_stats(0) && !part.has_len_stats(1));
        assert_eq!(part.granule_len_stats(0, 0, 0), None);
        std::fs::remove_file(&path).unwrap();
    }
}

#[cfg(test)]
mod sorted_flag_tests {
    use super::*;

    fn tmp(name: &str) -> String {
        let p = std::env::temp_dir()
            .join(format!("cbstore-sorted-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_file(&p);
        std::fs::write(&p, []).unwrap();
        p.to_str().unwrap().to_string()
    }

    fn writer_at(path: &str, coltypes: Vec<ColType>) -> CbWriter {
        let opts = CbWriterOpts::plain(coltypes.len());
        open_writer_inner(SegFile::open_rw(path).unwrap(), 1, 0, true, coltypes, 0x5aa5, opts).unwrap()
    }

    // 4B-U inline varlena image; returns the backing buffer + datum.
    fn text_datum(s: &[u8], keep: &mut Vec<Vec<u8>>) -> Datum {
        let mut v = Vec::with_capacity(4 + s.len());
        v.extend_from_slice(&(((s.len() + 4) as u32) << 2).to_le_bytes());
        v.extend_from_slice(s);
        keep.push(v);
        Datum::from_usize(keep.last().unwrap().as_ptr() as usize)
    }

    fn put_rows(w: &mut CbWriter, ints: &[i64], texts: &[&[u8]]) {
        let mut keep = Vec::new();
        for (i, &v) in ints.iter().enumerate() {
            let vals = [Datum::from_i64(v), text_datum(texts[i], &mut keep)];
            w.append_row(&vals, &[false, false]).unwrap();
        }
    }

    #[test]
    fn zerocnt_v7_roundtrip_partial_granule_and_reopen() {
        let path = tmp("zc7");
        let mut w = writer_at(&path, vec![ColType::I64, ColType::Text]);
        // Partial second granule; int zeros every 4th row, empty text every
        // 3rd — exact per-granule expectations below.
        let n = GRANULE_ROWS + 100;
        let ints: Vec<i64> = (0..n as i64).map(|i| if i % 4 == 0 { 0 } else { i }).collect();
        let texts: Vec<Vec<u8>> = (0..n)
            .map(|i| if i % 3 == 0 { Vec::new() } else { format!("t{i}").into_bytes() })
            .collect();
        let trefs: Vec<&[u8]> = texts.iter().map(|t| t.as_slice()).collect();
        put_rows(&mut w, &ints, &trefs);
        w.finish().unwrap();
        let expect = |lo: usize, hi: usize, m: usize| ((lo..hi).filter(|i| i % m == 0).count()) as u32;
        let part = crate::reader::Part::open(&path, 2).unwrap().unwrap();
        assert!(part.rg_has_zerocnt(0));
        assert_eq!(part.granule_zerocnt(0, 0, 0), expect(0, GRANULE_ROWS, 4));
        assert_eq!(part.granule_zerocnt(0, 0, 1), expect(0, GRANULE_ROWS, 3));
        assert_eq!(part.granule_zerocnt(0, 1, 0), expect(GRANULE_ROWS, n, 4));
        assert_eq!(part.granule_zerocnt(0, 1, 1), expect(GRANULE_ROWS, n, 3));
        // Slots past the RG's last granule stay zero.
        assert_eq!(part.granule_zerocnt(0, 2, 0), 0);
        drop(part);

        // Reopen-append: the preserved RG keeps its exact counts and flag
        // (read back through the writer's full footer parse), and the new
        // RG gets its own.
        let mut w2 = writer_at(&path, vec![ColType::I64, ColType::Text]);
        put_rows(&mut w2, &[0, 5], &[b"", b"x"]);
        w2.finish().unwrap();
        let part = crate::reader::Part::open(&path, 2).unwrap().unwrap();
        assert!(part.rg_has_zerocnt(0) && part.rg_has_zerocnt(1));
        assert_eq!(part.granule_zerocnt(0, 0, 0), expect(0, GRANULE_ROWS, 4));
        assert_eq!(part.granule_zerocnt(0, 1, 1), expect(GRANULE_ROWS, n, 3));
        assert_eq!(part.granule_zerocnt(1, 0, 0), 1);
        assert_eq!(part.granule_zerocnt(1, 0, 1), 1);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn sorted_flags_roundtrip_and_seams() {
        let path = tmp("rt");
        let mut w = writer_at(&path, vec![ColType::I64, ColType::Text]);
        // Int non-decreasing across an RG seal; text dips at the last row.
        let n = RG_ROWS + 10;
        let ints: Vec<i64> = (0..n as i64).map(|i| i / 3).collect();
        let mut texts: Vec<Vec<u8>> = (0..n).map(|i| format!("k{:08}", i / 5).into_bytes()).collect();
        texts[n - 1] = b"a-dip".to_vec();
        let trefs: Vec<&[u8]> = texts.iter().map(|t| t.as_slice()).collect();
        put_rows(&mut w, &ints, &trefs);
        w.finish().unwrap();
        let part = crate::reader::Part::open(&path, 2).unwrap().unwrap();
        assert_eq!(part.sorted, vec![1, 0]);

        // Reopen-append invalidates to unknown even for still-sorted data.
        let mut w2 = writer_at(&path, vec![ColType::I64, ColType::Text]);
        assert!(w2.sorted.is_none());
        put_rows(&mut w2, &[i64::MAX], &[b"zzz"]);
        w2.finish().unwrap();
        let part = crate::reader::Part::open(&path, 2).unwrap().unwrap();
        assert_eq!(part.sorted, vec![0, 0]);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn sorted_flags_equal_runs_and_empty_part() {
        let path = tmp("eq");
        let mut w = writer_at(&path, vec![ColType::I32, ColType::Text]);
        // All-equal columns are (vacuously) non-decreasing.
        let ints = vec![7i64; 100];
        let texts: Vec<&[u8]> = vec![b"same"; 100];
        put_rows(&mut w, &ints, &texts);
        w.finish().unwrap();
        let part = crate::reader::Part::open(&path, 2).unwrap().unwrap();
        assert_eq!(part.sorted, vec![1, 1]);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn sorted_flag_int_dip_detected_across_seal() {
        let path = tmp("dip");
        let mut w = writer_at(&path, vec![ColType::I64, ColType::Text]);
        // First row of the second RG dips below the first RG's tail.
        let mut ints: Vec<i64> = (0..(RG_ROWS + 1) as i64).collect();
        ints[RG_ROWS] = -1;
        let texts: Vec<&[u8]> = vec![b"t"; RG_ROWS + 1];
        put_rows(&mut w, &ints, &texts);
        w.finish().unwrap();
        let part = crate::reader::Part::open(&path, 2).unwrap().unwrap();
        assert_eq!(part.sorted, vec![0, 1]);
        std::fs::remove_file(&path).unwrap();
    }
}

#[cfg(test)]
mod dict_sort_tests {
    use super::*;

    fn tb_of(rows: &[&[u8]]) -> TextBuilder {
        let mut tb = TextBuilder { offs: Vec::new(), blob: Vec::new() };
        for r in rows {
            tb.offs.push((tb.blob.len() as u32, r.len() as u32));
            tb.blob.extend_from_slice(r);
        }
        tb
    }

    // Parse an encode_text_chunk image back into (codes, dict payloads).
    fn parse_dict_chunk(body: &[u8], n: usize) -> (ChunkHeader, Vec<u32>, Vec<Vec<u8>>) {
        let hdr = ChunkHeader::decode(&body[..CB_CHUNK_HEADER_LEN]);
        let gdir_end = CB_CHUNK_HEADER_LEN + hdr.ngranules as usize * CB_GRANULE_ENTRY_LEN;
        let p = &body[align64(gdir_end as u64) as usize..];
        let w = hdr.width as usize;
        let ndv = hdr.aux as usize;
        let codes: Vec<u32> = (0..n)
            .map(|i| match w {
                1 => p[i] as u32,
                2 => u16::from_le_bytes(p[i * 2..i * 2 + 2].try_into().unwrap()) as u32,
                _ => get_u32(p, i * 4),
            })
            .collect();
        let codes_len = align4(n * w);
        let off_tab = &p[codes_len..codes_len + ndv * 4];
        let blob = &p[codes_len + ndv * 4..];
        let raw = if hdr.encoding == Encoding::Lz4Dict {
            let raw_len = get_u32(blob, 0) as usize;
            let comp_len = get_u32(blob, 4) as usize;
            lz4_flex::decompress(&blob[8..8 + comp_len], raw_len).unwrap()
        } else {
            blob.to_vec()
        };
        let entries = off_tab
            .chunks_exact(4)
            .map(|c| {
                let o = u32::from_le_bytes(c.try_into().unwrap()) as usize;
                crate::varlena_bytes(Datum::from_usize(raw[o..].as_ptr() as usize))
                    .unwrap()
                    .to_vec()
            })
            .collect();
        (hdr, codes, entries)
    }

    #[test]
    fn dict_chunk_sorted_flag_and_roundtrip() {
        // Appearance order deliberately != byte order; duplicates force the
        // dict encoding; includes "", 0xFF-saturated, and prefix-nested keys.
        let rows: Vec<&[u8]> = vec![
            b"zebra", b"apple", b"zebra", b"", b"ab\xff\xff", b"ab", b"apple", b"aa", b"zebra",
            b"\xff", b"ab", b"abz", b"", b"apple", b"a", b"ab\xff\xff",
        ];
        let mut body = Vec::new();
        encode_text_chunk(&mut body, &tb_of(&rows), 1, &test_codec_ctx(), false);
        let (hdr, codes, entries) = parse_dict_chunk(&body, rows.len());
        assert!(matches!(hdr.encoding, Encoding::Dict | Encoding::Lz4Dict));
        assert_ne!(hdr.flags & CHUNK_FLAG_DICT_SORTED, 0);
        assert!(entries.windows(2).all(|w| w[0] < w[1]), "dict must be strictly byte-sorted");
        for (i, r) in rows.iter().enumerate() {
            assert_eq!(&entries[codes[i] as usize][..], *r, "row {i} decode identity");
        }
    }

    #[test]
    fn rawtext_chunk_carries_no_sorted_flag() {
        // All-distinct incompressible rows: dict loses, RAWTEXT wins; the
        // sorted bit must not leak onto non-dict encodings.
        let rows: Vec<Vec<u8>> = (0..64u32)
            .map(|i| {
                let mut v = i.to_le_bytes().to_vec();
                v.extend((0..48).map(|j| (i.wrapping_mul(2654435761).wrapping_add(j)) as u8));
                v
            })
            .collect();
        let refs: Vec<&[u8]> = rows.iter().map(|v| &v[..]).collect();
        let mut body = Vec::new();
        encode_text_chunk(&mut body, &tb_of(&refs), 1, &test_codec_ctx(), false);
        let hdr = ChunkHeader::decode(&body[..CB_CHUNK_HEADER_LEN]);
        assert!(matches!(hdr.encoding, Encoding::RawText | Encoding::Lz4Text));
        assert_eq!(hdr.flags & CHUNK_FLAG_DICT_SORTED, 0);
    }
}

#[cfg(test)]
mod codec_tests {
    use super::*;
    use crate::reader::ChunkView;

    fn decode_ints(body: &[u8], nrows: usize) -> Vec<i64> {
        let cv = ChunkView::at(body, 0, nrows as u32);
        let (mut out, mut dict, mut arena) = (Vec::new(), Vec::new(), Vec::new());
        let mut got = Vec::new();
        for g in 0..cv.hdr.ngranules as usize {
            cv.decode_granule(g, &mut out, &mut dict, &mut arena);
            got.extend(out.iter().map(|d| d.as_i64()));
        }
        got
    }

    // Repetitive (compressible) i64s across 2.5 granules; force each codec
    // and prove framed round-trips + the codec tag.
    #[test]
    fn int_frames_roundtrip_lz4_and_zstd() {
        let n = GRANULE_ROWS * 2 + GRANULE_ROWS / 2;
        let vals: Vec<i64> = (0..n as i64).map(|i| 1_000_000 + (i % 97) * 3).collect();
        for choice in [CodecChoice::Lz4, CodecChoice::Zstd] {
            let cc = CodecCtx::new(choice, ZSTD_LEVEL_DEFAULT, false);
            let mut body = Vec::new();
            let (min, max) =
                encode_int_chunk(&mut body, &vals, n.div_ceil(GRANULE_ROWS) as u32, &cc);
            assert_eq!((min, max), (1_000_000, 1_000_000 + 96 * 3));
            let cv = ChunkView::at(&body, 0, n as u32);
            let want = if choice == CodecChoice::Lz4 { Codec::Lz4 } else { Codec::Zstd };
            assert_eq!(cv.hdr.codec, want, "{choice:?}");
            assert_eq!(decode_ints(&body, n), vals, "{choice:?}");
        }
    }

    // Auto keeps the plain zero-decode lane on incompressible data.
    #[test]
    fn auto_keeps_plain_on_incompressible_ints() {
        let n = GRANULE_ROWS;
        let vals: Vec<i64> = (0..n as u64).map(|i| crate::hll::mix64(i) as i64).collect();
        let mut body = Vec::new();
        encode_int_chunk(&mut body, &vals, 1, &test_codec_ctx());
        let cv = ChunkView::at(&body, 0, n as u32);
        assert_eq!(cv.hdr.codec, Codec::None);
        assert_eq!(cv.hdr.payload_len, (n * cv.hdr.width as usize) as u64);
        assert_eq!(decode_ints(&body, n), vals);
    }

    // Auto picks a codec on compressible data and the >=10%-win gate holds.
    #[test]
    fn auto_compresses_compressible_ints() {
        let n = GRANULE_ROWS * 2;
        let vals: Vec<i64> = (0..n as i64).map(|i| i / 64).collect();
        let mut body = Vec::new();
        encode_int_chunk(&mut body, &vals, 2, &test_codec_ctx());
        let cv = ChunkView::at(&body, 0, n as u32);
        assert_ne!(cv.hdr.codec, Codec::None);
        assert!(cv.hdr.payload_len as usize * 10 <= n * cv.hdr.width as usize * 9);
        assert_eq!(decode_ints(&body, n), vals);
    }

    // Plain choice = the v5 byte behavior (no frames anywhere).
    #[test]
    fn plain_choice_writes_v5_shape() {
        let n = GRANULE_ROWS;
        let vals: Vec<i64> = (0..n as i64).map(|i| i / 64).collect();
        let cc = CodecCtx::new(CodecChoice::Plain, ZSTD_LEVEL_DEFAULT, false);
        let mut body = Vec::new();
        encode_int_chunk(&mut body, &vals, 1, &cc);
        let cv = ChunkView::at(&body, 0, n as u32);
        assert_eq!(cv.hdr.codec, Codec::None);
        assert_eq!(decode_ints(&body, n), vals);
    }

    // ZSTD text frames: low-NDV rows force the dict lane; the compressed
    // dict blob round-trips under the zstd tag.
    #[test]
    fn zstd_dict_text_roundtrip() {
        let rows: Vec<Vec<u8>> = (0..4096)
            .map(|i| format!("value-{:02}-{}", i % 7, "x".repeat(2000)).into_bytes())
            .collect();
        let refs: Vec<&[u8]> = rows.iter().map(|v| &v[..]).collect();
        let mut tb = TextBuilder { offs: Vec::new(), blob: Vec::new() };
        for r in &refs {
            tb.offs.push((tb.blob.len() as u32, r.len() as u32));
            tb.blob.extend_from_slice(r);
        }
        let cc = CodecCtx::new(CodecChoice::Zstd, ZSTD_LEVEL_DEFAULT, false);
        let mut body = Vec::new();
        encode_text_chunk(&mut body, &tb, 1, &cc, false);
        let cv = ChunkView::at(&body, 0, rows.len() as u32);
        assert_eq!(cv.hdr.encoding, Encoding::Lz4Dict);
        assert_eq!(cv.hdr.codec, Codec::Zstd);
        let (mut out, mut dict, mut arena) = (Vec::new(), Vec::new(), Vec::new());
        cv.decode_granule(0, &mut out, &mut dict, &mut arena);
        for (i, d) in out.iter().enumerate() {
            assert_eq!(crate::varlena_bytes(*d).unwrap(), &refs[i][..], "row {i}");
        }
    }

    // Old-bank read-compat at the chunk level: a legacy Lz4Text image with
    // codec byte 0 (the v<=5 writer's layout) must decode as LZ4.
    #[test]
    fn legacy_lz4text_codec0_decodes() {
        let n = 512usize;
        let rows: Vec<Vec<u8>> =
            (0..n).map(|i| format!("legacy-{}-{}", i % 5, "pad".repeat(16)).into_bytes()).collect();
        // Old-writer layout: header (codec byte 0) | granule dir | offsets |
        // one LZ4 frame per granule.
        let mut gblob = Vec::new();
        let mut offs = Vec::with_capacity(n);
        for r in &rows {
            offs.push(gblob.len() as u32);
            push_varlena_image(&mut gblob, r);
        }
        let comp = lz4_flex::compress(&gblob);
        let mut body = Vec::new();
        ChunkHeader {
            encoding: Encoding::Lz4Text,
            width: 4,
            flags: 0,
            ngranules: 1,
            aux: gblob.len() as i64,
            payload_len: (n * 4 + frame_len(comp.len())) as u64,
            codec: Codec::None, // legacy tag byte
        }
        .encode(&mut body);
        put_u64(&mut body, (n * 4) as u64);
        put_i64(&mut body, 0);
        put_i64(&mut body, 0);
        while body.len() % 64 != 0 {
            body.push(0);
        }
        for &o in &offs {
            put_u32(&mut body, o);
        }
        push_frame(&mut body, gblob.len(), &comp);
        let cv = ChunkView::at(&body, 0, n as u32);
        assert_eq!(cv.hdr.codec, Codec::None);
        assert_eq!(cv.hdr.frame_codec(), Codec::Lz4);
        let (mut out, mut dict, mut arena) = (Vec::new(), Vec::new(), Vec::new());
        cv.decode_granule(0, &mut out, &mut dict, &mut arena);
        for (i, d) in out.iter().enumerate() {
            assert_eq!(crate::varlena_bytes(*d).unwrap(), &rows[i][..], "row {i}");
        }
    }
}

#[cfg(test)]
mod intcodec_tests {
    use super::*;
    use crate::reader::ChunkView;

    fn delta_ctx(choice: CodecChoice) -> CodecCtx {
        CodecCtx::new(choice, ZSTD_LEVEL_DEFAULT, true)
    }

    fn decode_ints(body: &[u8], nrows: usize) -> Vec<i64> {
        let cv = ChunkView::at(body, 0, nrows as u32);
        let (mut out, mut dict, mut arena) = (Vec::new(), Vec::new(), Vec::new());
        let mut got = Vec::new();
        for g in 0..cv.hdr.ngranules as usize {
            cv.decode_granule(g, &mut out, &mut dict, &mut arena);
            got.extend(out.iter().map(|d| d.as_i64()));
        }
        got
    }

    fn encode(vals: &[i64], cc: &CodecCtx) -> (Vec<u8>, i64, i64) {
        let mut body = Vec::new();
        let ng = vals.len().div_ceil(GRANULE_ROWS) as u32;
        let (min, max) = encode_int_chunk(&mut body, vals, ng, cc);
        (body, min, max)
    }

    // Sorted-run BIGINTs (the UserID/EventTime shape): DeltaFor + a codec
    // frame wins, round-trips across granules incl. a partial tail, and the
    // granule directory zone maps stay value-domain-exact.
    #[test]
    fn deltafor_sorted_run_roundtrip() {
        let n = GRANULE_ROWS * 2 + GRANULE_ROWS / 2;
        let mut v = 1_600_000_000_000_000i64; // epoch-us scale
        let vals: Vec<i64> = (0..n)
            .map(|i| {
                v += (i % 7) as i64 * 1_000;
                v
            })
            .collect();
        let (body, min, max) = encode(&vals, &delta_ctx(CodecChoice::Auto));
        assert_eq!((min, max), (vals[0], *vals.last().unwrap()));
        let cv = ChunkView::at(&body, 0, n as u32);
        assert_eq!(cv.hdr.encoding, Encoding::DeltaFor);
        assert_ne!(cv.hdr.codec, Codec::None);
        // >=10% smaller than the plain arm's raw 8B image at minimum.
        assert!((cv.hdr.payload_len as usize) * 10 <= n * 8 * 9);
        assert_eq!(decode_ints(&body, n), vals);
        for g in 0..cv.hdr.ngranules as usize {
            let lo = g * GRANULE_ROWS;
            let hi = (lo + GRANULE_ROWS).min(n);
            let ge = cv.granule(g);
            assert_eq!(ge.min, *vals[lo..hi].iter().min().unwrap(), "g{g}");
            assert_eq!(ge.max, *vals[lo..hi].iter().max().unwrap(), "g{g}");
        }
    }

    // i64 bound wrap: alternating MAX/MIN makes every delta wrap the i64
    // domain; the Plain choice forces the uncompressed-frame (Codec::None,
    // comp_len == raw_len) DeltaFor leg. Exact round-trip required.
    #[test]
    fn deltafor_i64_bounds_wrapping_plain_frames() {
        let n = GRANULE_ROWS + 3; // partial tail granule of 3 rows
        let vals: Vec<i64> =
            (0..n).map(|i| if i % 2 == 0 { i64::MAX } else { i64::MIN }).collect();
        let (body, min, max) = encode(&vals, &delta_ctx(CodecChoice::Plain));
        assert_eq!((min, max), (i64::MIN, i64::MAX));
        let cv = ChunkView::at(&body, 0, n as u32);
        // deltas alternate +1/-1 (wrapped): width-1 image, 8x under plain.
        assert_eq!(cv.hdr.encoding, Encoding::DeltaFor);
        assert_eq!(cv.hdr.codec, Codec::None);
        assert_eq!(decode_ints(&body, n), vals);
    }

    // Strictly descending values: negative deltas throughout.
    #[test]
    fn deltafor_negative_deltas_descending() {
        let n = GRANULE_ROWS * 2;
        let vals: Vec<i64> = (0..n).map(|i| 5_000_000_000i64 - (i as i64) * 3).collect();
        let (body, ..) = encode(&vals, &delta_ctx(CodecChoice::Auto));
        let cv = ChunkView::at(&body, 0, n as u32);
        assert_eq!(cv.hdr.encoding, Encoding::DeltaFor);
        assert_eq!(decode_ints(&body, n), vals);
    }

    // Epoch sentinels: long zero runs punctuated by real timestamps (the
    // ClientEventTime shape). The sentinel jumps force width-8 deltas, so
    // the delta arm rightly LOSES the >=10% gate to the (equally
    // compressible) plain image — the value here is exact round-trip and
    // gate determinism, not a DeltaFor pick.
    #[test]
    fn deltafor_epoch_sentinels() {
        let n = GRANULE_ROWS * 2;
        let vals: Vec<i64> = (0..n)
            .map(|i| if i % 53 == 0 { 1_700_000_000_000_000 + i as i64 } else { 0 })
            .collect();
        let (body, ..) = encode(&vals, &delta_ctx(CodecChoice::Auto));
        let (body2, ..) = encode(&vals, &delta_ctx(CodecChoice::Auto));
        assert_eq!(body, body2, "gate must be deterministic");
        assert_eq!(decode_ints(&body, n), vals);
    }

    // Single-row chunk: min == max collapses to Const before the delta arm;
    // two distinct rows exercise the 1-delta image.
    #[test]
    fn deltafor_tiny_chunks() {
        let (body, ..) = encode(&[42], &delta_ctx(CodecChoice::Auto));
        let cv = ChunkView::at(&body, 0, 1);
        assert_eq!(cv.hdr.encoding, Encoding::Const);
        assert_eq!(decode_ints(&body, 1), vec![42]);

        let (body, ..) = encode(&[42, -7], &delta_ctx(CodecChoice::Plain));
        let cv = ChunkView::at(&body, 0, 2);
        // 24B meta + 0-1B payload never beats the 2x8B plain image by 10%
        // at this size under Plain -> stays Raw; correctness is the point.
        assert_eq!(decode_ints(&body, 2), vec![42, -7]);
        let _ = cv;
    }

    // Incompressible hash-domain values (the WatchID/URLHash shape): the
    // delta arm must lose the gate and keep the plain zero-decode lane.
    #[test]
    fn deltafor_gate_keeps_plain_on_hashes() {
        let n = GRANULE_ROWS;
        let vals: Vec<i64> = (0..n as u64).map(|i| crate::hll::mix64(i) as i64).collect();
        let before = intcodec_stats();
        let (body, ..) = encode(&vals, &delta_ctx(CodecChoice::Auto));
        let after = intcodec_stats();
        let cv = ChunkView::at(&body, 0, n as u32);
        assert_eq!(cv.hdr.encoding, Encoding::Raw);
        assert_eq!(decode_ints(&body, n), vals);
        assert!(after.1 > before.1, "kept_plain counter must move");
    }

    // delta: false (the ingest kill switch's per-chunk effect) never emits
    // DeltaFor even on the friendliest input.
    #[test]
    fn ingest_kill_switch_pins_plain() {
        let n = GRANULE_ROWS;
        let vals: Vec<i64> = (0..n as i64).map(|i| i * 1000).collect();
        let cc = CodecCtx::new(CodecChoice::Auto, ZSTD_LEVEL_DEFAULT, false);
        let (body, ..) = encode(&vals, &cc);
        let cv = ChunkView::at(&body, 0, n as u32);
        assert_ne!(cv.hdr.encoding, Encoding::DeltaFor);
        assert_eq!(decode_ints(&body, n), vals);
    }
}

#[cfg(test)]
mod cluster_key_tests {
    use super::*;

    fn seams_once() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(::tuplesort::init_seams);
    }

    fn tmp(name: &str) -> String {
        let p = std::env::temp_dir()
            .join(format!("cbstore-ckey-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_file(&p);
        std::fs::write(&p, []).unwrap();
        p.to_str().unwrap().to_string()
    }

    // int8 + text tupdesc matching ColType::[I64, Text].
    fn tup_desc() -> std::rc::Rc<::types_tuple::TupleDescData<'static>> {
        use ::types_tuple::*;
        let m: &'static ::mcx::MemoryContext =
            Box::leak(Box::new(::mcx::MemoryContext::new("cbstore-ckey-test")));
        let mcx = m.mcx();
        let mut attrs = ::mcx::PgVec::new_in(mcx);
        let mut compact = ::mcx::PgVec::new_in(mcx);
        for (i, (typid, len, byval, align)) in
            [(20u32, 8i16, true, TYPALIGN_DOUBLE), (25, -1, false, TYPALIGN_INT)]
                .iter()
                .enumerate()
        {
            let att = FormData_pg_attribute {
                attnum: (i + 1) as i16,
                atttypid: *typid,
                attlen: *len,
                attbyval: *byval,
                attalign: *align,
                attstorage: TYPSTORAGE_PLAIN,
                ..Default::default()
            };
            compact.push(CompactAttribute::populate_from(&att));
            attrs.push(att);
        }
        std::rc::Rc::new(TupleDescData {
            natts: 2,
            tdtypeid: 2249,
            tdtypmod: -1,
            tdrefcount: -1,
            constr: None,
            compact_attrs: compact,
            attrs,
        })
    }

    fn text_datum(s: &[u8], keep: &mut Vec<Vec<u8>>) -> Datum {
        let mut v = Vec::with_capacity(4 + s.len());
        v.extend_from_slice(&(((s.len() + 4) as u32) << 2).to_le_bytes());
        v.extend_from_slice(s);
        keep.push(v);
        Datum::from_usize(keep.last().unwrap().as_ptr() as usize)
    }

    // The 3.1 ordering property: rows ingested in adversarial order come out
    // key-sorted (text C-order, then i64), RGs carry RG_FLAG_CLUSTERED, the
    // footer records the declared key, and the v5 sorted flags read exact.
    #[test]
    fn cluster_key_sorts_ingest_and_stamps_metadata() {
        seams_once();
        let path = tmp("sort");
        let coltypes = vec![ColType::I64, ColType::Text];
        let mut opts = CbWriterOpts::plain(2);
        // Key: (text col 1, int col 0) — cross-column and cross-class.
        opts.cluster_key = vec![(1, CbSortKeyKind::TextC), (0, CbSortKeyKind::Int64)];
        let mut w = open_writer_inner(
            SegFile::open_rw(&path).unwrap(), 1, 0, true, coltypes, 0x5aa5, opts,
        )
        .unwrap();
        let keys: Vec<(i16, CbSortKeyKind)> =
            w.opts.cluster_key.iter().map(|&(c, k)| (c as i16 + 1, k)).collect();
        w.sorter =
            Some(::tuplesort_seams::pgrcolumnar_ingest_sort::call(tup_desc(), &keys, 65536).unwrap());

        // > 1 RG of rows, adversarial order (descending + interleaved).
        let n = RG_ROWS + 1234;
        let mut rows: Vec<(i64, Vec<u8>)> = (0..n)
            .map(|i| {
                let r = crate::hll::mix64(i as u64);
                ((r % 1000) as i64, format!("k{:04}", r % 300).into_bytes())
            })
            .collect();
        let mut keep = Vec::new();
        for (v, t) in &rows {
            let vals = [Datum::from_i64(*v), text_datum(t, &mut keep)];
            w.ingest_row(&vals, &[false, false]).unwrap();
        }
        // Nothing sealed before the drain: rows live in the sorter.
        assert_eq!(w.rgs.len(), 0);
        assert_eq!(w.nbuf, 0);
        w.finish().unwrap();

        let part = crate::reader::Part::open(&path, 2).unwrap().unwrap();
        assert_eq!(part.cluster_key, vec![1, 0]);
        assert_eq!(part.total_rows(), n as u64);
        assert!(part.rgs.len() >= 2);
        for rg in &part.rgs {
            assert_ne!(rg.flags & RG_FLAG_CLUSTERED, 0);
        }
        // Text key column is whole-part sorted; int is not (tiebreak only).
        assert_eq!(part.sorted, vec![0, 1]);

        // Read every row back and compare against the host-sorted oracle.
        rows.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        let (mut out_i, mut out_t) = (Vec::new(), Vec::new());
        let (mut dict, mut arena) = (Vec::new(), Vec::new());
        let (mut dict2, mut arena2) = (Vec::new(), Vec::new());
        let mut got: Vec<(i64, Vec<u8>)> = Vec::new();
        for rg in 0..part.rgs.len() {
            let ints = part.chunk(rg, 0);
            let texts = part.chunk(rg, 1);
            // Dict tables are per-RG (build_dict contract: caller clears at
            // RG boundaries).
            dict.clear();
            dict2.clear();
            for g in 0..ints.hdr.ngranules as usize {
                ints.decode_granule(g, &mut out_i, &mut dict, &mut arena);
                texts.decode_granule(g, &mut out_t, &mut dict2, &mut arena2);
                assert_eq!(out_i.len(), out_t.len());
                for (i, t) in out_i.iter().zip(out_t.iter()) {
                    got.push((i.as_i64(), crate::varlena_bytes(*t).unwrap().to_vec()));
                }
            }
        }
        assert_eq!(got.len(), rows.len());
        // Multiset equality by full row; order equality by the key columns
        // (equal-key rows may permute in payload—here the whole row IS the
        // key, so exact equality holds).
        for (i, (g, r)) in got.iter().zip(rows.iter()).enumerate() {
            assert_eq!(g, r, "row {i} of {}", rows.len());
        }
        std::fs::remove_file(&path).unwrap();
    }

    // load-r2 presort instrument: a shuffled ingest drained through
    // presort_key must be BYTE-IDENTICAL to a plain writer fed the same rows
    // pre-sorted by the same key — no RG_FLAG_CLUSTERED on any RG, footer
    // cluster_key empty. Duplicate whole rows ride along (a full-key tie here
    // is a byte-equal row, so tie order cannot perturb the bytes).
    #[test]
    fn presort_drain_is_byte_identical_to_presorted_plain_load() {
        seams_once();
        let coltypes = vec![ColType::I64, ColType::Text];
        let n = RG_ROWS + 777;
        let uniq = (n - 300) as u64;
        let rows: Vec<(i64, Vec<u8>)> = (0..n)
            .map(|i| {
                let r = crate::hll::mix64(i as u64 % uniq); // tail duplicates head
                ((r % 1000) as i64, format!("k{:04}", r % 300).into_bytes())
            })
            .collect();

        // Reference: plain writer, rows fed PRE-SORTED by (text C, then int).
        let mut sorted_rows = rows.clone();
        sorted_rows.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        let path_a = tmp("presort-ref");
        {
            let mut w = open_writer_inner(
                SegFile::open_rw(&path_a).unwrap(), 1, 0, true, coltypes.clone(), 0x5aa5,
                CbWriterOpts::plain(2),
            )
            .unwrap();
            let mut keep = Vec::new();
            for (v, t) in &sorted_rows {
                let vals = [Datum::from_i64(*v), text_datum(t, &mut keep)];
                w.ingest_row(&vals, &[false, false]).unwrap();
            }
            w.finish().unwrap();
        }

        // Armed: presort writer, same rows fed in ingest (adversarial) order.
        let path_b = tmp("presort-armed");
        {
            let mut opts = CbWriterOpts::plain(2);
            opts.presort_key = vec![(1, CbSortKeyKind::TextC), (0, CbSortKeyKind::Int64)];
            let mut w = open_writer_inner(
                SegFile::open_rw(&path_b).unwrap(), 1, 0, true, coltypes, 0x5aa5, opts,
            )
            .unwrap();
            let keys: Vec<(i16, CbSortKeyKind)> =
                w.opts.presort_key.iter().map(|&(c, k)| (c as i16 + 1, k)).collect();
            w.sorter = Some(
                ::tuplesort_seams::pgrcolumnar_ingest_sort::call(tup_desc(), &keys, 65536).unwrap(),
            );
            let mut keep = Vec::new();
            for (v, t) in &rows {
                let vals = [Datum::from_i64(*v), text_datum(t, &mut keep)];
                w.ingest_row(&vals, &[false, false]).unwrap();
            }
            // Nothing sealed before the drain: rows live in the sorter.
            assert_eq!(w.rgs.len(), 0);
            assert_eq!(w.nbuf, 0);
            w.finish().unwrap();
        }

        assert_eq!(
            std::fs::read(&path_a).unwrap(),
            std::fs::read(&path_b).unwrap(),
            "presort part must be byte-identical to the presorted plain load"
        );
        let part = crate::reader::Part::open(&path_b, 2).unwrap().unwrap();
        assert_eq!(part.cluster_key, Vec::<u16>::new());
        for rg in &part.rgs {
            assert_eq!(rg.flags & RG_FLAG_CLUSTERED, 0, "presort must not stamp CLUSTERED");
        }
        std::fs::remove_file(&path_a).unwrap();
        std::fs::remove_file(&path_b).unwrap();
    }

    // No cluster key: ingest_row appends directly (no sorter detour).
    #[test]
    fn no_cluster_key_appends_directly() {
        seams_once();
        let path = tmp("nokey");
        let mut w = open_writer_inner(
            SegFile::open_rw(&path).unwrap(), 1, 0, true,
            vec![ColType::I64, ColType::Text], 0x5aa5, CbWriterOpts::plain(2),
        )
        .unwrap();
        assert!(w.sorter.is_none());
        let mut keep = Vec::new();
        let vals = [Datum::from_i64(7), text_datum(b"x", &mut keep)];
        w.ingest_row(&vals, &[false, false]).unwrap();
        assert_eq!(w.nbuf, 1);
        w.finish().unwrap();
        let part = crate::reader::Part::open(&path, 2).unwrap().unwrap();
        assert_eq!(part.cluster_key, Vec::<u16>::new());
        assert_eq!(part.rgs[0].flags & RG_FLAG_CLUSTERED, 0);
        std::fs::remove_file(&path).unwrap();
    }
}

#[cfg(test)]
mod lz4_decode_seat_tests {
    // Differential coverage of the lz4dec decoder IN ITS REAL SEAT: chunks
    // that the writer admits to Lz4Text / Lz4Dict, decoded back through
    // ChunkView::decode_granule (reader.rs), values compared byte-for-byte.
    use super::*;
    use crate::reader::ChunkView;

    fn tb_of_owned(rows: &[Vec<u8>]) -> TextBuilder {
        let mut tb = TextBuilder { offs: Vec::new(), blob: Vec::new() };
        for r in rows {
            tb.offs.push((tb.blob.len() as u32, r.len() as u32));
            tb.blob.extend_from_slice(r);
        }
        tb
    }

    fn decode_all(body: &[u8], n: usize) -> Vec<Vec<u8>> {
        let hdr = ChunkHeader::decode(&body[..CB_CHUNK_HEADER_LEN]);
        let cv = ChunkView::at(body, 0, n as u32);
        let (mut out, mut dict, mut arena) = (Vec::new(), Vec::new(), Vec::new());
        let mut got = Vec::with_capacity(n);
        for g in 0..hdr.ngranules as usize {
            cv.decode_granule(g, &mut out, &mut dict, &mut arena);
            for d in &out {
                got.push(crate::varlena_bytes(*d).unwrap().to_vec());
            }
        }
        got
    }

    #[test]
    fn lz4text_granule_decode_roundtrip() {
        // All-distinct compressible rows across a partial final granule:
        // dict loses (ndv == n), LZ4 wins >= 10% -> Lz4Text, whose granule
        // frames decode through lz4dec.
        let n = GRANULE_ROWS + 37;
        let rows: Vec<Vec<u8>> = (0..n)
            .map(|i| format!("http://example.com/some/long/path/{i}?pad=aaaaaaaaaaaaaaaaaaaaaaaaaaaa").into_bytes())
            .collect();
        let mut body = Vec::new();
        encode_text_chunk(&mut body, &tb_of_owned(&rows), n.div_ceil(GRANULE_ROWS) as u32, &CodecCtx::new(CodecChoice::Lz4, ZSTD_LEVEL_DEFAULT, false), false);
        let hdr = ChunkHeader::decode(&body[..CB_CHUNK_HEADER_LEN]);
        assert_eq!(hdr.encoding, Encoding::Lz4Text, "test premise: Lz4Text admitted");
        assert_eq!(decode_all(&body, n), rows);
    }

    #[test]
    fn lz4dict_granule_decode_roundtrip() {
        // Repeated compressible entries: dict wins, dict blob compresses
        // >= 10% -> Lz4Dict; build_dict decompresses the blob through lz4dec.
        let n = GRANULE_ROWS + 11;
        let rows: Vec<Vec<u8>> = (0..n)
            .map(|i| format!("searchterm-{:04}-cccccccccccccccccccccccccccc", i % 300).into_bytes())
            .collect();
        let mut body = Vec::new();
        encode_text_chunk(&mut body, &tb_of_owned(&rows), n.div_ceil(GRANULE_ROWS) as u32, &CodecCtx::new(CodecChoice::Lz4, ZSTD_LEVEL_DEFAULT, false), false);
        let hdr = ChunkHeader::decode(&body[..CB_CHUNK_HEADER_LEN]);
        assert_eq!(hdr.encoding, Encoding::Lz4Dict, "test premise: Lz4Dict admitted");
        assert_eq!(decode_all(&body, n), rows);
    }
}

#[cfg(test)]
mod dict_frames_tests {
    use super::*;
    use crate::reader::ChunkView;
    use std::collections::BTreeSet;

    fn frames_cc(target: usize) -> CodecCtx {
        CodecCtx {
            choice: CodecChoice::Lz4,
            zstd_level: ZSTD_LEVEL_DEFAULT,
            delta: false,
            dict_frames: true,
            dict_frame_target: target,
        }
    }

    fn tb(rows: &[Vec<u8>]) -> TextBuilder {
        let mut tb = TextBuilder { offs: Vec::new(), blob: Vec::new() };
        for r in rows {
            tb.offs.push((tb.blob.len() as u32, r.len() as u32));
            tb.blob.extend_from_slice(r);
        }
        tb
    }

    // Uniform-length compressible entries: ndv controls the frame count
    // exactly (entries align4 to the same stored size).
    fn dict_rows(n: usize, ndv: usize) -> Vec<Vec<u8>> {
        (0..n)
            .map(|i| {
                format!("searchterm-{:05}-cccccccccccccccccccccccccccc", i % ndv).into_bytes()
            })
            .collect()
    }

    fn sorted_entries(rows: &[Vec<u8>]) -> Vec<Vec<u8>> {
        rows.iter().cloned().collect::<BTreeSet<_>>().into_iter().collect()
    }

    fn encode(rows: &[Vec<u8>], cc: &CodecCtx) -> Vec<u8> {
        let mut body = Vec::new();
        let ng = rows.len().div_ceil(GRANULE_ROWS) as u32;
        encode_text_chunk(&mut body, &tb(rows), ng, cc, false);
        body
    }

    fn decode_all(body: &[u8], n: usize) -> Vec<Vec<u8>> {
        let hdr = ChunkHeader::decode(&body[..CB_CHUNK_HEADER_LEN]);
        let cv = ChunkView::at(body, 0, n as u32);
        let (mut out, mut dict, mut arena) = (Vec::new(), Vec::new(), Vec::new());
        let mut got = Vec::with_capacity(n);
        for g in 0..hdr.ngranules as usize {
            cv.decode_granule(g, &mut out, &mut dict, &mut arena);
            for d in &out {
                got.push(crate::varlena_bytes(*d).unwrap().to_vec());
            }
        }
        got
    }

    // Framed write engages (flag bit 8) and the EAGER decode path
    // (decode_granule / build_dict) round-trips byte-for-byte, matching the
    // unframed control.
    #[test]
    fn framed_write_eager_decode_roundtrip() {
        let n = GRANULE_ROWS + 11;
        let rows = dict_rows(n, 600);
        let framed = encode(&rows, &frames_cc(4096));
        let control =
            encode(&rows, &CodecCtx::new(CodecChoice::Lz4, ZSTD_LEVEL_DEFAULT, false));
        let fh = ChunkHeader::decode(&framed[..CB_CHUNK_HEADER_LEN]);
        let ch = ChunkHeader::decode(&control[..CB_CHUNK_HEADER_LEN]);
        assert_eq!(fh.encoding, Encoding::Lz4Dict, "premise: dict + codec win");
        assert_ne!(fh.flags & CHUNK_FLAG_DICT_FRAMED, 0, "framed flag set");
        assert_eq!(ch.flags & CHUNK_FLAG_DICT_FRAMED, 0, "control unframed");
        assert_eq!(decode_all(&framed, n), rows);
        assert_eq!(decode_all(&control, n), rows);
    }

    // Default arm (CodecCtx::new, env unset): the framed bit never appears
    // — frozen-bank byte behavior.
    #[test]
    fn frames_off_by_default() {
        let n = GRANULE_ROWS;
        let rows = dict_rows(n, 600);
        let body =
            encode(&rows, &CodecCtx::new(CodecChoice::Lz4, ZSTD_LEVEL_DEFAULT, false));
        let hdr = ChunkHeader::decode(&body[..CB_CHUNK_HEADER_LEN]);
        assert_eq!(hdr.encoding, Encoding::Lz4Dict);
        assert_eq!(hdr.flags & CHUNK_FLAG_DICT_FRAMED, 0);
    }

    // Blob within one frame target: stays single-frame legacy layout even
    // with the arm on.
    #[test]
    fn small_blob_stays_legacy() {
        let n = GRANULE_ROWS;
        let rows = dict_rows(n, 40);
        let body = encode(&rows, &frames_cc(64 * 1024));
        let hdr = ChunkHeader::decode(&body[..CB_CHUNK_HEADER_LEN]);
        assert_eq!(hdr.encoding, Encoding::Lz4Dict);
        assert_eq!(hdr.flags & CHUNK_FLAG_DICT_FRAMED, 0);
        assert_eq!(decode_all(&body, n), rows);
    }

    // Lazy decode: pointer table up front (no frame materializes at build),
    // per-code ensures in a shuffled out-of-order walk, every ensured entry
    // byte-identical; ensure_all completes the arena identically; the
    // granule gather reproduces the rows.
    #[test]
    fn lazy_out_of_order_ensures_match_eager() {
        let n = GRANULE_ROWS + 11;
        let ndv = 600;
        let rows = dict_rows(n, ndv);
        let entries = sorted_entries(&rows);
        let body = encode(&rows, &frames_cc(4096));
        let cv = ChunkView::at(&body, 0, n as u32);
        let (mut codes, mut dict, mut arena) = (Vec::new(), Vec::new(), Vec::new());
        let mut lazy = None;
        assert!(cv.decode_granule_codes(0, &mut codes, &mut dict, &mut arena, &mut lazy));
        let lazy = lazy.expect("framed chunk builds a lazy dict");
        assert!(!lazy.all_done(), "no frame materializes at build");
        assert_eq!(dict.len(), ndv);
        for k in 0..ndv {
            let code = (k * 397) % ndv;
            lazy.ensure_code(code as u32);
            assert_eq!(
                crate::varlena_bytes(dict[code]).unwrap(),
                &entries[code][..],
                "code {code}"
            );
        }
        lazy.ensure_all();
        assert!(lazy.all_done());
        for (code, e) in entries.iter().enumerate() {
            assert_eq!(crate::varlena_bytes(dict[code]).unwrap(), &e[..]);
        }
        for (i, &c) in codes.iter().enumerate() {
            assert_eq!(crate::varlena_bytes(dict[c as usize]).unwrap(), &rows[i][..]);
        }
    }

    // Pad-spill hazard: a TINY final frame (one entry, < OUT_PAD bytes)
    // materialized FIRST, then its predecessors — a predecessor's in-place
    // wild-write window overlaps the done successor, forcing the
    // scratch+copy path; the successor's bytes must survive.
    #[test]
    fn tiny_done_successor_survives_predecessor_ensure() {
        let n = GRANULE_ROWS;
        // 52B aligned images, target 4096 -> 79 entries per cut; ndv =
        // 3*79 + 1 leaves a single-entry (52B, < 64B OUT_PAD) final frame.
        let ndv = 3 * 79 + 1;
        let rows = dict_rows(n, ndv);
        let entries = sorted_entries(&rows);
        let body = encode(&rows, &frames_cc(4096));
        let hdr = ChunkHeader::decode(&body[..CB_CHUNK_HEADER_LEN]);
        assert_ne!(hdr.flags & CHUNK_FLAG_DICT_FRAMED, 0, "premise: framed");
        let cv = ChunkView::at(&body, 0, n as u32);
        let (mut codes, mut dict, mut arena) = (Vec::new(), Vec::new(), Vec::new());
        let mut lazy = None;
        assert!(cv.decode_granule_codes(0, &mut codes, &mut dict, &mut arena, &mut lazy));
        let lazy = lazy.expect("framed chunk builds a lazy dict");
        for code in (0..ndv).rev() {
            lazy.ensure_code(code as u32);
            assert_eq!(
                crate::varlena_bytes(dict[code]).unwrap(),
                &entries[code][..],
                "code {code}"
            );
        }
        for (code, e) in entries.iter().enumerate() {
            assert_eq!(crate::varlena_bytes(dict[code]).unwrap(), &e[..], "recheck {code}");
        }
    }

    // Stored-raw frames (incompressible byte-sorted tail region) round-trip
    // on both the eager and lazy paths.
    #[test]
    fn stored_raw_frames_roundtrip() {
        let n = GRANULE_ROWS;
        let mut vals: Vec<Vec<u8>> = dict_rows(n / 2, 300);
        for i in 0..n / 2 {
            let mut e = b"zz-".to_vec();
            let mut x = (i % 97) as u64 ^ 0x9e37_79b9_7f4a_7c15;
            for _ in 0..48 {
                x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                e.push((x >> 33) as u8);
            }
            vals.push(e);
        }
        let body = encode(&vals, &frames_cc(2048));
        let hdr = ChunkHeader::decode(&body[..CB_CHUNK_HEADER_LEN]);
        assert_eq!(hdr.encoding, Encoding::Lz4Dict, "premise: dict + codec win");
        assert_ne!(hdr.flags & CHUNK_FLAG_DICT_FRAMED, 0, "premise: framed");
        assert_eq!(decode_all(&body, n), vals);
        let entries = sorted_entries(&vals);
        let cv = ChunkView::at(&body, 0, n as u32);
        let (mut codes, mut dict, mut arena) = (Vec::new(), Vec::new(), Vec::new());
        let mut lazy = None;
        assert!(cv.decode_granule_codes(0, &mut codes, &mut dict, &mut arena, &mut lazy));
        let lazy = lazy.expect("framed chunk builds a lazy dict");
        for k in 0..entries.len() {
            let code = (k * 397) % entries.len();
            lazy.ensure_code(code as u32);
            assert_eq!(crate::varlena_bytes(dict[code]).unwrap(), &entries[code][..]);
        }
    }
}

#[cfg(test)]
mod stitch_tests {
    use super::*;
    use crate::reader::Part;
    use std::collections::BTreeSet;

    fn tmp(name: &str) -> String {
        let p = std::env::temp_dir()
            .join(format!("cbstore-stitch-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_file(&p);
        std::fs::write(&p, []).unwrap();
        p.to_str().unwrap().to_string()
    }

    fn writer_at(path: &str, coltypes: Vec<ColType>) -> CbWriter {
        let opts = CbWriterOpts::plain(coltypes.len());
        open_writer_inner(SegFile::open_rw(path).unwrap(), 1, 0, true, coltypes, 0x5aa5, opts)
            .unwrap()
    }

    fn text_datum(s: &[u8], keep: &mut Vec<Vec<u8>>) -> Datum {
        let mut v = Vec::with_capacity(4 + s.len());
        v.extend_from_slice(&(((s.len() + 4) as u32) << 2).to_le_bytes());
        v.extend_from_slice(s);
        keep.push(v);
        Datum::from_usize(keep.last().unwrap().as_ptr() as usize)
    }

    fn put_text_rows(w: &mut CbWriter, texts: &[Vec<u8>]) {
        let mut keep = Vec::new();
        for t in texts {
            let vals = [Datum::from_i64(7), text_datum(t, &mut keep)];
            w.append_row(&vals, &[false, false]).unwrap();
        }
    }

    // Two RGs with overlapping-but-different dict vocabularies: the stitch
    // maps each RG's byte-sorted local codes onto part-global byte ranks,
    // the same string gets the same global code in both RGs, and gndv is
    // the union NDV. Int columns carry no stitch.
    #[test]
    fn stitch_roundtrip_two_rgs() {
        let path = tmp("rt2");
        let mut w = writer_at(&path, vec![ColType::I64, ColType::Text]);
        // RG0: k000..k006 (cycled); RG1: e000..e004 plus k002/k005 overlap.
        let rg0: Vec<Vec<u8>> =
            (0..RG_ROWS).map(|i| format!("k{:03}", i % 7).into_bytes()).collect();
        let rg1: Vec<Vec<u8>> = (0..100)
            .map(|i| {
                if i % 3 == 0 {
                    format!("k{:03}", 2 + 3 * (i % 2)).into_bytes() // k002 / k005
                } else {
                    format!("e{:03}", i % 5).into_bytes()
                }
            })
            .collect();
        put_text_rows(&mut w, &rg0);
        put_text_rows(&mut w, &rg1);
        w.finish().unwrap();

        let part = Part::open(&path, 2).unwrap().unwrap();
        assert_eq!(part.rgs.len(), 2);
        let set0: BTreeSet<&[u8]> = rg0.iter().map(|t| t.as_slice()).collect();
        let set1: BTreeSet<&[u8]> = rg1.iter().map(|t| t.as_slice()).collect();
        let union: Vec<&[u8]> = set0.union(&set1).copied().collect(); // byte-sorted
        assert_eq!(part.stitch_gndv(1), union.len() as u64, "gndv = union NDV");
        assert_eq!(part.stitch_gndv(0), 0, "int column has no stitch");
        assert!(part.stitch(0, 0).is_none());
        let rank = |s: &[u8]| union.iter().position(|&u| u == s).unwrap() as u32;
        for (rg, set) in [(0usize, &set0), (1usize, &set1)] {
            let expected: Vec<u32> = set.iter().map(|s| rank(s)).collect();
            let got = part.stitch(rg, 1).expect("stitch present for all-dict text column");
            assert_eq!(got, expected.as_slice(), "rg {rg}: local byte order -> global rank");
            assert!(got.windows(2).all(|w| w[0] < w[1]), "stitch strictly increasing");
        }
        std::fs::remove_file(&path).unwrap();
    }

    // A non-dict RG (all-distinct incompressible strings: dict and
    // compressed-text both lose) poisons the column's stitch: partial global
    // code spaces are never published.
    #[test]
    fn stitch_poisoned_by_rawtext_rg() {
        let path = tmp("poison");
        let mut w = writer_at(&path, vec![ColType::I64, ColType::Text]);
        let rg0: Vec<Vec<u8>> =
            (0..RG_ROWS).map(|i| format!("k{:03}", i % 7).into_bytes()).collect();
        // Incompressible all-distinct tail RG (the rawtext test recipe).
        let rg1: Vec<Vec<u8>> = (0..200u64)
            .map(|i| {
                let mut x = i.wrapping_mul(0x9E3779B97F4A7C15) | 1;
                let mut s = Vec::with_capacity(64);
                for _ in 0..8 {
                    x ^= x >> 33;
                    x = x.wrapping_mul(0xFF51AFD7ED558CCD);
                    s.extend_from_slice(&x.to_le_bytes());
                }
                s
            })
            .collect();
        put_text_rows(&mut w, &rg0);
        put_text_rows(&mut w, &rg1);
        w.finish().unwrap();

        let part = Part::open(&path, 2).unwrap().unwrap();
        let hdr = part.chunk(1, 1).hdr.encoding;
        assert!(
            matches!(hdr, Encoding::RawText | Encoding::Lz4Text),
            "test premise: tail RG not dict-encoded (got {hdr:?})"
        );
        assert_eq!(part.stitch_gndv(1), 0, "poisoned column publishes no stitch");
        assert!(part.stitch(0, 1).is_none());
        std::fs::remove_file(&path).unwrap();
    }

    // Reopen-append onto committed RGs invalidates the stitch (the
    // NDV/sorted precedent): the preserved RGs' dicts were never interned.
    #[test]
    fn stitch_append_invalidates() {
        let path = tmp("append");
        let mut w = writer_at(&path, vec![ColType::I64, ColType::Text]);
        let rows: Vec<Vec<u8>> = (0..64).map(|i| format!("k{:03}", i % 7).into_bytes()).collect();
        put_text_rows(&mut w, &rows);
        w.finish().unwrap();
        assert_eq!(Part::open(&path, 2).unwrap().unwrap().stitch_gndv(1), 7);

        let mut w2 = writer_at(&path, vec![ColType::I64, ColType::Text]);
        put_text_rows(&mut w2, &rows);
        w2.finish().unwrap();
        let part = Part::open(&path, 2).unwrap().unwrap();
        assert_eq!(part.rgs.len(), 2);
        assert_eq!(part.stitch_gndv(1), 0, "append invalidates the stitch");
        assert!(part.stitch(0, 1).is_none() && part.stitch(1, 1).is_none());
        std::fs::remove_file(&path).unwrap();
    }
}

#[cfg(test)]
mod parallel_ingest_tests {
    use super::*;

    fn tmp(name: &str) -> String {
        let p = std::env::temp_dir()
            .join(format!("cbstore-paringest-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_file(&p);
        std::fs::write(&p, []).unwrap();
        p.to_str().unwrap().to_string()
    }

    // Deterministic row generator: int col (ascending — stays sorted), int
    // col that breaks EXACTLY at the first RG_ROWS seam (exercises the
    // committer's cross-chunk seam stitch), low-NDV text (dict + stitch),
    // high-NDV unique text (non-dict -> stitch poison path).
    fn row(i: usize) -> (i64, i64, String, String) {
        let seam_breaker = if i < RG_ROWS { i as i64 } else { (i as i64) - RG_ROWS as i64 - 1 };
        (
            i as i64,
            seam_breaker,
            format!("k{:04}", (i * 2_654_435_761) % 4_099),
            format!("unique-{i:08}-{}", i * 31),
        )
    }

    fn datums<'a>(
        r: &'a (i64, i64, String, String),
        vbuf: &'a mut Vec<u8>,
        vbuf2: &'a mut Vec<u8>,
    ) -> [Datum; 4] {
        // Text datums as raw 4B-U varlena images (test-support shape used by
        // put_text_rows above).
        fn varlena(buf: &mut Vec<u8>, s: &[u8]) -> Datum {
            buf.clear();
            buf.extend_from_slice(&::datum::set_varsize_4b(4 + s.len()));
            buf.extend_from_slice(s);
            Datum::from_usize(buf.as_ptr() as usize)
        }
        [
            Datum::from_i64(r.0),
            Datum::from_i64(r.1),
            varlena(vbuf, r.2.as_bytes()),
            varlena(vbuf2, r.3.as_bytes()),
        ]
    }

    const COLS: [ColType; 4] = [ColType::I64, ColType::I64, ColType::Text, ColType::Text];

    /// THE ACCEPTANCE ORACLE IN MINIATURE: chunk-encoded RGs committed in
    /// input order produce a part BYTE-IDENTICAL to the serial writer's —
    /// across full/partial RGs, dict + non-dict text (stitch intern AND
    /// poison), NDV sketches, and a sorted tracker broken exactly at the
    /// chunk seam.
    #[test]
    fn parallel_commit_is_byte_identical_to_serial() {
        let n = 2 * RG_ROWS + 1_234;
        let isnull = [false; 4];

        // Serial part.
        let path_a = tmp("serial");
        let mut wa = open_writer_at(&path_a, COLS.to_vec()).unwrap();
        let (mut b1, mut b2) = (Vec::new(), Vec::new());
        for i in 0..n {
            let r = row(i);
            let vals = datums(&r, &mut b1, &mut b2);
            wa.append_row(&vals, &isnull).unwrap();
        }
        wa.finish().unwrap();

        // Parallel-shaped part: chunk encoders sealed off RG_ROWS row
        // chunks, committed in input order.
        let path_b = tmp("parallel");
        let mut wb = open_writer_at(&path_b, COLS.to_vec()).unwrap();
        let plan = std::sync::Arc::new(wb.parallel_ingest_plan().expect("no sorter"));
        let mut i = 0;
        while i < n {
            let hi = (i + RG_ROWS).min(n);
            let mut enc = RgChunkEncoder::new(std::sync::Arc::clone(&plan));
            for j in i..hi {
                let r = row(j);
                let vals = datums(&r, &mut b1, &mut b2);
                enc.append_row(&vals, &isnull).unwrap();
            }
            wb.commit_encoded_rg(enc.seal()).unwrap();
            i = hi;
        }
        wb.finish_parallel_ingest().unwrap();

        let a = std::fs::read(&path_a).unwrap();
        let b = std::fs::read(&path_b).unwrap();
        assert_eq!(a.len(), b.len(), "part sizes differ");
        assert!(a == b, "parallel-committed part is not byte-identical to serial");
        std::fs::remove_file(&path_a).unwrap();
        std::fs::remove_file(&path_b).unwrap();

        // load-r3 M2: the same commits through the column-sharded stitch
        // pool (2 threads over 2 text columns) — bytes must equal serial.
        let path_c = tmp("stitchpool");
        let mut wc = open_writer_at(&path_c, COLS.to_vec()).unwrap();
        let plan = std::sync::Arc::new(wc.parallel_ingest_plan().expect("no sorter"));
        wc.install_stitch_pool(2).unwrap();
        let mut i = 0;
        while i < n {
            let hi = (i + RG_ROWS).min(n);
            let mut enc = RgChunkEncoder::new(std::sync::Arc::clone(&plan));
            for j in i..hi {
                let r = row(j);
                let vals = datums(&r, &mut b1, &mut b2);
                enc.append_row(&vals, &isnull).unwrap();
            }
            wc.commit_encoded_rg(enc.seal()).unwrap();
            i = hi;
        }
        wc.finish_parallel_ingest().unwrap();
        let c = std::fs::read(&path_c).unwrap();
        assert!(c == a, "stitch-pool part is not byte-identical to serial");
        std::fs::remove_file(&path_c).unwrap();
    }

    /// Same oracle under PGRUST_CBSTORE_FASTHASH semantics is covered by the
    /// FASTHASH prototype's own evidence (maps only dedup); here pin the
    /// sorted-seam rule directly: col 1 breaks at the seam, col 0 stays
    /// sorted — read back through the footer.
    #[test]
    fn parallel_commit_sorted_seam() {
        let n = RG_ROWS + 8;
        let isnull = [false; 4];
        let path = tmp("seam");
        let mut w = open_writer_at(&path, COLS.to_vec()).unwrap();
        let plan = std::sync::Arc::new(w.parallel_ingest_plan().unwrap());
        let (mut b1, mut b2) = (Vec::new(), Vec::new());
        let mut i = 0;
        while i < n {
            let hi = (i + RG_ROWS).min(n);
            let mut enc = RgChunkEncoder::new(std::sync::Arc::clone(&plan));
            for j in i..hi {
                let r = row(j);
                let vals = datums(&r, &mut b1, &mut b2);
                enc.append_row(&vals, &isnull).unwrap();
            }
            w.commit_encoded_rg(enc.seal()).unwrap();
            i = hi;
        }
        w.finish_parallel_ingest().unwrap();
        let part = crate::reader::Part::open(&path, COLS.len()).unwrap().unwrap();
        // col0 ascending; col1 breaks exactly at the RG seam; col2 hashes
        // unsorted; col3's zero-padded uniques are lexicographically ascending.
        assert_eq!(part.sorted, vec![1, 0, 0, 1]);
        std::fs::remove_file(&path).unwrap();
    }
}

// v8 NDV-register persistence: the multi-COPY differential (H1: a part
// built by two COPYs must estimate like one) plus the fallback-ladder
// contracts.
#[cfg(test)]
mod ndv_regs_tests {
    use super::*;
    use crate::reader::{part_footer_ndv, part_footer_ndv_hll, read_footer_rgs, Part};

    fn tmp(name: &str) -> String {
        let p =
            std::env::temp_dir().join(format!("cbstore-ndvregs-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_file(&p);
        std::fs::write(&p, []).unwrap();
        p.to_str().unwrap().to_string()
    }

    fn text_datum(s: &[u8], keep: &mut Vec<Vec<u8>>) -> Datum {
        let mut v = Vec::with_capacity(4 + s.len());
        v.extend_from_slice(&(((s.len() + 4) as u32) << 2).to_le_bytes());
        v.extend_from_slice(s);
        keep.push(v);
        Datum::from_usize(keep.last().unwrap().as_ptr() as usize)
    }

    fn put_int_rows(w: &mut CbWriter, range: std::ops::Range<usize>, ndistinct: usize) {
        for i in range {
            w.append_row(&[Datum::from_i64((i % ndistinct) as i64)], &[false]).unwrap();
        }
    }

    // THE H1 differential: 200k rows / 60k distinct ints, loaded as one
    // session vs two (finish + reopen-append + finish). Identical value
    // stream => identical registers => byte-identical footer NDV; before
    // the fix the two-session part invalidated to 0 (unknown).
    #[test]
    fn multi_copy_estimates_like_single_copy_ints() {
        const N: usize = 200_000;
        const D: usize = 60_000;

        let one = tmp("one-copy-int");
        let mut w = open_writer_at(&one, vec![ColType::I64]).unwrap();
        put_int_rows(&mut w, 0..N, D);
        w.finish().unwrap();
        drop(w);
        let est_one = part_footer_ndv(&one, 1).unwrap().unwrap()[0];

        let two = tmp("two-copy-int");
        let mut w1 = open_writer_at(&two, vec![ColType::I64]).unwrap();
        put_int_rows(&mut w1, 0..N / 2, D);
        w1.finish().unwrap();
        drop(w1);
        let mut w2 = open_writer_at(&two, vec![ColType::I64]).unwrap();
        put_int_rows(&mut w2, N / 2..N, D);
        w2.finish().unwrap();
        drop(w2);
        let est_two = part_footer_ndv(&two, 1).unwrap().unwrap()[0];

        assert_ne!(est_two, 0, "H1: append invalidated the sketch to unknown");
        assert_eq!(est_two, est_one, "reopen continuation must equal the single pass");
        let err = (est_one as f64 / D as f64 - 1.0).abs();
        assert!(err < 0.03, "estimate {est_one} vs actual {D} (err {err:.4})");

        // The persisted registers round-trip through the reader-side blob
        // path (the inherited-ANALYZE union source) with the same estimate.
        let sk = part_footer_ndv_hll(&two, 1).unwrap().unwrap();
        assert_eq!(sk[0].as_ref().unwrap().estimate(), est_two);

        std::fs::remove_file(&one).unwrap();
        std::fs::remove_file(&two).unwrap();
    }

    // Text arm with duplicate values STRADDLING the COPY boundary: the
    // continued sketch must dedup across sessions (a per-COPY scalar sum
    // would double-count). Scalars may legitimately differ between the
    // single- and two-session parts (the single part can carry a v7 stitch
    // whose exact gndv the scalar prefers; append invalidates stitch), so
    // parity is asserted on the register sketches.
    #[test]
    fn multi_copy_text_dedups_across_boundary() {
        const N: usize = 160_000;
        const D: usize = 30_000;
        let mut keep = Vec::new();

        let one = tmp("one-copy-text");
        let mut w = open_writer_at(&one, vec![ColType::Text]).unwrap();
        for i in 0..N {
            let d = text_datum(format!("url-{}", i % D).as_bytes(), &mut keep);
            w.append_row(&[d], &[false]).unwrap();
        }
        w.finish().unwrap();
        drop(w);

        let two = tmp("two-copy-text");
        for half in 0..2 {
            let mut w = open_writer_at(&two, vec![ColType::Text]).unwrap();
            for i in half * (N / 2)..(half + 1) * (N / 2) {
                let d = text_datum(format!("url-{}", i % D).as_bytes(), &mut keep);
                w.append_row(&[d], &[false]).unwrap();
            }
            w.finish().unwrap();
        }

        let sk_one = part_footer_ndv_hll(&one, 1).unwrap().unwrap();
        let sk_two = part_footer_ndv_hll(&two, 1).unwrap().unwrap();
        let (e1, e2) =
            (sk_one[0].as_ref().unwrap().estimate(), sk_two[0].as_ref().unwrap().estimate());
        assert_eq!(e1, e2, "identical value stream must yield identical registers");
        let err = (e1 as f64 / D as f64 - 1.0).abs();
        assert!(err < 0.03, "estimate {e1} vs actual {D} (err {err:.4})");
        // Both scalars must be present (nonzero) and near the truth.
        for path in [&one, &two] {
            let s = part_footer_ndv(path, 1).unwrap().unwrap()[0];
            let serr = (s as f64 / D as f64 - 1.0).abs();
            assert!(s > 0 && serr < 0.03, "scalar {s} vs actual {D}");
        }

        std::fs::remove_file(&one).unwrap();
        std::fs::remove_file(&two).unwrap();
    }

    // H5: where a v7 global dict exists its entry count is the column's
    // EXACT distinct count — the v2 scalar must carry it verbatim instead
    // of the ~1% HLL estimate.
    #[test]
    fn stitch_gndv_wins_the_scalar_exactly() {
        const N: usize = 131_072; // two full RGs
        const D: usize = 10_000;
        let path = tmp("stitch-exact");
        let mut keep = Vec::new();
        let mut w = open_writer_at(&path, vec![ColType::Text]).unwrap();
        for i in 0..N {
            let d = text_datum(format!("k{:06}", i % D).as_bytes(), &mut keep);
            w.append_row(&[d], &[false]).unwrap();
        }
        w.finish().unwrap();
        drop(w);

        let mut file = SegFile::open_rw(&path).unwrap();
        let mut hdr = [0u8; CB_HEADER_LEN as usize];
        file.read_exact_at(&mut hdr, 0).unwrap();
        let (footer_off, _, version) = crate::reader::read_header(&hdr).unwrap();
        let (_, _, ndv, _, _, _, stitch, _) =
            read_footer_rgs(&mut file, footer_off, 1, version, false).unwrap();
        assert_eq!(
            stitch.gndv[0], D as u64,
            "precondition: the dict/stitch arm engaged for this shape"
        );
        assert_eq!(ndv[0], D as u64, "scalar must be the exact stitch gndv");
        // The register sketch coexists (append continuation stays armed).
        let sk = part_footer_ndv_hll(&path, 1).unwrap().unwrap();
        assert!(sk[0].is_some());
        std::fs::remove_file(&path).unwrap();
    }

    // Fallback-ladder contracts: an all-zero directory (pre-v8 chain or the
    // kill switch at the earlier write) and an unknown blob shape both keep
    // the historical invalidate-to-unknown reopen behavior.
    #[test]
    fn load_ndv_regs_fallback_contracts() {
        let path = tmp("fallback");
        std::fs::write(&path, vec![0u8; 4096]).unwrap();
        let mut f = SegFile::open_rw(&path).unwrap();
        assert!(load_ndv_regs(&mut f, &[(0, 0)], 1).unwrap().is_none());
        assert!(load_ndv_regs(&mut f, &[], 1).unwrap().is_none()); // pre-v8: empty dir
        assert!(load_ndv_regs(&mut f, &[(1 << 40, 8)], 1).unwrap().is_none()); // oob
        // In-bounds bytes that are not a known blob shape.
        assert!(load_ndv_regs(&mut f, &[(64, 8)], 1).unwrap().is_none());
        // A part is all-or-nothing: one good + one absent column = None.
        let mut w = open_writer_at(&tmp("fb2"), vec![ColType::I64]).unwrap();
        put_int_rows(&mut w, 0..10, 10);
        w.finish().unwrap();
        drop(w);
        std::fs::remove_file(&path).unwrap();
    }

    // An aborted second COPY (no finish) must not poison the continued
    // sketch: the reopen replays from the last committed footer's
    // registers, so a third session continues from COPY 1's state.
    #[test]
    fn aborted_append_leaves_sketch_continuable() {
        const D: usize = 5_000;
        let path = tmp("abort-append");
        let mut w1 = open_writer_at(&path, vec![ColType::I64]).unwrap();
        put_int_rows(&mut w1, 0..RG_ROWS, D);
        w1.finish().unwrap();
        drop(w1);
        // Session 2 seals an RG to disk, then aborts before finish.
        let mut w2 = open_writer_at(&path, vec![ColType::I64]).unwrap();
        put_int_rows(&mut w2, 0..RG_ROWS + 5, D);
        drop(w2);
        // Session 3 commits; the estimate covers COPY 1 + COPY 3 rows.
        let mut w3 = open_writer_at(&path, vec![ColType::I64]).unwrap();
        put_int_rows(&mut w3, 0..100, D);
        w3.finish().unwrap();
        drop(w3);
        let est = part_footer_ndv(&path, 1).unwrap().unwrap()[0];
        let err = (est as f64 / D as f64 - 1.0).abs();
        assert!(est > 0 && err < 0.03, "estimate {est} vs actual {D}");
        let part = Part::open(&path, 1).unwrap().unwrap();
        assert_eq!(part.total_rows(), (RG_ROWS + 100) as u64);
        std::fs::remove_file(&path).unwrap();
    }
}
