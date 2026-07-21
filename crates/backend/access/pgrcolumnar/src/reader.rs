//! mmap read path: part open, footer parse, granule decode kernels
//! (docs/design/pgrcolumnar-impl.md §6).

use ::datum::Datum;
use ::types_error::{PgError, PgResult};

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use crate::format::*;
use crate::segfile::{SegFile, SegMap};
use crate::writer::FooterRg;

// LZ4 frame decode target inside the u64-backed arena: `raw_len` payload
// bytes + the decoder's OUT_PAD tail slack (wild-copy spill scratch, never
// published). The arena is grown monotonically and NEVER shrunk/cleared, so
// (a) reuse across granules skips the zero-fill (the old clear+resize path
// memset the whole buffer before every decompress — a full extra touch of
// the output), and (b) every byte handed to the decoder is initialized.
// Base stays 8-aligned for the varlena images.
fn arena_frame(arena: &mut Vec<u64>, raw_len: usize) -> &mut [u8] {
    let words = (raw_len + crate::lz4dec::OUT_PAD).div_ceil(8);
    if arena.len() < words {
        arena.resize(words, 0);
    }
    // SAFETY: u64 backing reinterpreted as raw_len + OUT_PAD initialized
    // bytes (len >= words holds by the resize above).
    unsafe {
        std::slice::from_raw_parts_mut(
            arena.as_mut_ptr().cast::<u8>(),
            raw_len + crate::lz4dec::OUT_PAD,
        )
    }
}

pub fn read_header(hdr: &[u8]) -> PgResult<(u64, u64, u32)> {
    let version = get_u32(hdr, 8);
    if get_u64(hdr, 0) != CB_MAGIC || !(CB_VERSION_V1..=CB_VERSION).contains(&version) {
        return Err(Box::new(PgError::error("cbstore: bad part header".to_string())));
    }
    Ok((get_u64(hdr, 16), get_u64(hdr, 24), version))
}

/// Abort/crash-tolerant header read: an all-zero header page reads as "no
/// committed footer" (None) instead of erroring. Zeros can only mean a part
/// that never published — writers historically wrote row-group bytes past a
/// still-unwritten header, so a COPY aborted mid-statement left len >=
/// CB_HEADER_LEN files whose first page is a zero hole; a published part's
/// header is written and fsynced at every publish and is never zeroed again.
pub fn read_header_opt(hdr: &[u8]) -> PgResult<Option<(u64, u64, u32)>> {
    if hdr[..CB_HEADER_LEN as usize].iter().all(|&b| b == 0) {
        return Ok(None);
    }
    read_header(hdr).map(Some)
}

// Deterministic footer section lengths for a (version, nrgs, ncols) footer;
// the v7 length-stats section additionally needs the prelude's flag count.
pub(crate) struct FooterLayout {
    pub pre_len: usize,
    pub ndv_len: usize,
    pub sums_len: usize,
    pub sorted_len: usize,
    pub ckey_len: usize,
}

pub(crate) fn footer_layout(version: u32, nrgs: usize, ncols: usize) -> FooterLayout {
    FooterLayout {
        // v7 prelude: nrgs u32 | ncols u32 | ncols length-stats flag bytes.
        pre_len: if version >= CB_VERSION_V7 { 8 + ncols } else { 8 },
        ndv_len: if version >= CB_VERSION_V2 { ncols * 8 } else { 0 },
        sums_len: if version >= CB_VERSION_V4 { nrgs * ncols * 16 } else { 0 },
        sorted_len: if version >= CB_VERSION_V5 { ncols } else { 0 },
        ckey_len: if version >= CB_VERSION_V6 { CB_CLUSTER_KEY_SECTION_LEN } else { 0 },
    }
}

impl FooterLayout {
    // Byte offset (footer-relative) of the v7 length-stats section (the
    // first post-cluster-key v7 section; see format.rs section order).
    pub(crate) fn lenstats_off(&self, nrgs: usize, ncols: usize) -> usize {
        self.pre_len
            + nrgs * 24
            + nrgs * ncols * 24
            + self.ndv_len
            + self.sums_len
            + self.sorted_len
            + self.ckey_len
    }
    // Byte length of the v7 length-stats section given the prelude's
    // flagged-column count.
    pub(crate) fn lenstats_len(&self, nrgs: usize, nlencols: usize) -> usize {
        nrgs * GRANULES_PER_RG * nlencols * CB_LENSTATS_ENTRY_LEN
    }
    // Byte offset (footer-relative) of the v7 stitch section (immediately
    // after the length-stats section).
    pub(crate) fn stitch_off(&self, nrgs: usize, ncols: usize, nlencols: usize) -> usize {
        self.lenstats_off(nrgs, ncols) + self.lenstats_len(nrgs, nlencols)
    }
    // Byte length of the v7 stitch section (gndv array + blob directory).
    pub(crate) fn stitch_len(&self, nrgs: usize, ncols: usize, version: u32) -> usize {
        if version >= CB_VERSION_V7 {
            ncols * 8 + nrgs * ncols * CB_STITCH_DIR_ENTRY_LEN
        } else {
            0
        }
    }
    // Byte offset (footer-relative) of the v7 zero-count section
    // (immediately after the stitch section).
    pub(crate) fn zerocnt_off(&self, nrgs: usize, ncols: usize, nlencols: usize, version: u32) -> usize {
        self.stitch_off(nrgs, ncols, nlencols) + self.stitch_len(nrgs, ncols, version)
    }
    // Byte length of the v7 zero-count section.
    pub(crate) fn zerocnt_len(&self, nrgs: usize, ncols: usize, version: u32) -> usize {
        if version >= CB_VERSION_V7 {
            nrgs * GRANULES_PER_RG * ncols * CB_ZEROCNT_ENTRY_LEN
        } else {
            0
        }
    }
    // Byte length of the v8 NDV-registers blob directory (immediately after
    // the zero-count section; the blobs themselves live in the file body).
    pub(crate) fn ndvregs_len(&self, ncols: usize, version: u32) -> usize {
        if version >= CB_VERSION_V8 {
            ncols * CB_NDVREGS_DIR_ENTRY_LEN
        } else {
            0
        }
    }
}

// want_sums materializes the v4 per-RG sums (and the v7 per-granule length
// stats) into FooterRg for the writer's reopen-append re-emit; readers leave
// both on disk and consume lazily via Part::rg_sum / Part::granule_len_stats.
// The CRC always covers the whole footer body.
/// v7 stitch metadata parsed from the footer: per-column global NDV (0 = no
/// stitch) and the per-(RG, column) stitch-blob directory (file_off, count).
/// Empty vectors on pre-v7 parts.
#[derive(Default)]
pub struct FooterStitch {
    pub gndv: Vec<u64>,
    pub dir: Vec<(u64, u32)>,
}

// Reader-side opens (want_sums=false) read the footer SECTIONED: only the
// sections the parse materializes are pread (prelude + RG directory + chunk
// directory + ndv, sorted flags + cluster key, the v7 stitch section, and
// the 16-byte tail). The sums / length-stats / zero-count bodies — the bulk
// of a v7 footer (nrgs x GRANULES_PER_RG x {nlencols,ncols} entries,
// ~15-23MB at 100M rows vs ~6MB of materialized sections) — stay on disk,
// exactly where their consumers already read them lazily off the mmap
// (Part::rg_sum / granule_len_stats / granule_zerocnt). The whole-body CRC
// cannot be computed over a sectioned read, so the lazy path validates the
// tail magic + body-length words plus every structural bound instead — the
// same trust model as the chunk payloads, which carry no per-chunk CRC.
// PGRUST_CBSTORE_FOOTER_EAGER=1 restores the historical whole-body
// read + CRC (byte-identical A/B and paranoia switch). The writer's
// reopen-append path (want_sums=true) always reads + CRC-validates the
// whole body.
fn footer_eager() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_CBSTORE_FOOTER_EAGER").is_ok_and(|v| v == "1" || v == "on")
    })
}

// Footer-section readahead gate (cold-readahead lane): shares the scan
// readahead's kill switch — PGRUST_CBSTORE_READAHEAD=0/off disables the
// pre-pread WILLNEED hints on the lazy footer path. Default ON.
fn footer_readahead() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_CBSTORE_READAHEAD").as_deref(),
            Ok("0") | Ok("off") | Ok("OFF"),
        )
    })
}

// PGRUST_CBSTORE_FOOTER_DEBUG=1: one CBFOOTER| line per footer read to
// stderr (bytes read vs body length, lazy/eager, wall us) — the read-path
// lane's decomposition channel.
fn footer_debug() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_CBSTORE_FOOTER_DEBUG").is_ok_and(|v| v == "1"))
}

pub fn read_footer_rgs(
    file: &mut SegFile,
    footer_off: u64,
    ncols: usize,
    version: u32,
    want_sums: bool,
) -> PgResult<(Vec<FooterRg>, u64, Vec<u64>, Vec<u8>, Vec<u16>, Vec<u8>, FooterStitch, Vec<(u64, u32)>)>
{
    let t0 = footer_debug().then(std::time::Instant::now);
    let total_len = file.total_len();
    let pre_len = if version >= CB_VERSION_V7 { 8 + ncols } else { 8 };
    if footer_off < CB_HEADER_LEN
        || footer_off.checked_add(pre_len as u64).is_none_or(|e| e > total_len)
    {
        return Err(Box::new(PgError::error(
            "cbstore: corrupt part (footer offset out of bounds)".to_string(),
        )));
    }
    let mut fixed = vec![0u8; pre_len];
    file.read_exact_at(&mut fixed, footer_off)?;
    let nrgs = get_u32(&fixed, 0) as usize;
    let fncols = get_u32(&fixed, 4) as usize;
    if fncols != ncols {
        return Err(Box::new(PgError::error("cbstore: footer ncols mismatch".to_string())));
    }
    let lay = footer_layout(version, nrgs, ncols);
    let nlencols = if version >= CB_VERSION_V7 {
        fixed[8..8 + ncols].iter().filter(|&&b| b != 0).count()
    } else {
        0
    };
    let body_len = lay.zerocnt_off(nrgs, ncols, nlencols, version)
        + lay.zerocnt_len(nrgs, ncols, version)
        + lay.ndvregs_len(ncols, version)
        + 16;
    // Bounds-gate before the allocation and read: a torn/garbage footer word
    // must produce a clean error, not a huge alloc or a read past EOF.
    if body_len as u64 > total_len - footer_off {
        return Err(Box::new(PgError::error(
            "cbstore: corrupt part (footer body out of bounds)".to_string(),
        )));
    }
    // vec![0; n] allocates zeroed (fresh anonymous pages for footers this
    // large): sections the lazy path skips are never written OR read by the
    // parse, so their pages are never faulted in — the allocation costs
    // nothing beyond the mapped range actually pread into.
    let mut buf = vec![0u8; body_len];
    let lazy = !want_sums && !footer_eager();
    let mut bytes_read = body_len;
    if lazy {
        // Materialized-section ranges, footer-relative, ascending and
        // disjoint (each start = previous end + the skipped section's len):
        //   [0, prefix_end)                prelude + RG dir + chunk dir + ndv
        //   sums skipped
        //   [sorted_off, +sorted+ckey)     v5 sorted flags + v6 cluster key
        //   lenstats skipped
        //   [stitch_off, +stitch_len)      v7 stitch gndv + blob directory
        //   zerocnt skipped
        //   [tail_start, body_len)         v8 ndv-regs directory + tail
        //     (the directory sits immediately before the 16-byte
        //      len | crc | magic tail, so one contiguous read covers both)
        let prefix_end = lay.pre_len + nrgs * 24 + nrgs * ncols * 24 + lay.ndv_len;
        let sorted_off = prefix_end + lay.sums_len;
        let stitch_off = lay.stitch_off(nrgs, ncols, nlencols);
        let tail_start = body_len - 16 - lay.ndvregs_len(ncols, version);
        let ranges = [
            (0usize, prefix_end),
            (sorted_off, sorted_off + lay.sorted_len + lay.ckey_len),
            (stitch_off, stitch_off + lay.stitch_len(nrgs, ncols, version)),
            (tail_start, body_len),
        ];
        // Overlap the section preads (cold-readahead lane): hint every
        // materialized range up front so the kernel fetches the later
        // sections while the first (largest) pread drains. Advisory only;
        // shares the scan readahead's kill switch.
        if footer_readahead() {
            for &(s, e) in &ranges {
                if e > s {
                    file.advise_willneed(footer_off + s as u64, (e - s) as u64);
                }
            }
        }
        bytes_read = 0;
        for &(s, e) in &ranges {
            if e > s {
                file.read_exact_at(&mut buf[s..e], footer_off + s as u64)?;
                bytes_read += e - s;
            }
        }
    } else {
        file.read_exact_at(&mut buf, footer_off)?;
    }
    let out = parse_footer_checked(&buf, nrgs, ncols, version, want_sums, !lazy).map(
        |(rgs, ndv, sorted, ckey, lenflags, stitch, ndvregs)| {
            (rgs, footer_off + body_len as u64, ndv, sorted, ckey, lenflags, stitch, ndvregs)
        },
    );
    if let Some(t0) = t0 {
        eprintln!(
            "CBFOOTER|read|body_len={body_len}|bytes_read={bytes_read}|lazy={lazy}|us={}",
            t0.elapsed().as_micros()
        );
    }
    out
}

pub fn parse_footer(
    buf: &[u8],
    nrgs: usize,
    ncols: usize,
    version: u32,
    want_sums: bool,
) -> PgResult<(Vec<FooterRg>, Vec<u64>, Vec<u8>, Vec<u16>, Vec<u8>, FooterStitch, Vec<(u64, u32)>)>
{
    parse_footer_checked(buf, nrgs, ncols, version, want_sums, true)
}

// check_crc=false is the sectioned lazy read (read_footer_rgs): the skipped
// section bytes are zeros, so the whole-body CRC cannot match by
// construction — the tail magic + body-length words are still validated.
fn parse_footer_checked(
    buf: &[u8],
    nrgs: usize,
    ncols: usize,
    version: u32,
    want_sums: bool,
    check_crc: bool,
) -> PgResult<(Vec<FooterRg>, Vec<u64>, Vec<u8>, Vec<u16>, Vec<u8>, FooterStitch, Vec<(u64, u32)>)>
{
    let tail = buf.len() - 16;
    if get_u32(buf, tail + 12) != CB_FOOTER_MAGIC
        || get_u64(buf, tail) != buf.len() as u64
        || (check_crc && get_u32(buf, tail + 8) != crc32c(&buf[..tail]))
    {
        return Err(Box::new(PgError::error("cbstore: corrupt footer".to_string())));
    }
    // v7 prelude tail: per-column length-stats flags; all-0 on pre-v7 parts.
    let mut lenflags = vec![0u8; ncols];
    if version >= CB_VERSION_V7 {
        lenflags.copy_from_slice(&buf[8..8 + ncols]);
    }
    let nlencols = lenflags.iter().filter(|&&b| b != 0).count();
    let mut rgs = Vec::with_capacity(nrgs);
    let mut off = if version >= CB_VERSION_V7 { 8 + ncols } else { 8 };
    for _ in 0..nrgs {
        rgs.push(FooterRg {
            file_off: get_u64(buf, off),
            nrows: get_u32(buf, off + 8),
            xmin: get_u32(buf, off + 12),
            flags: get_u32(buf, off + 16),
            chunks: Vec::with_capacity(ncols),
            sums: Vec::new(),
            lenstats: Vec::new(),
            zerocnt: Vec::new(),
        });
        off += 24;
    }
    for rg in rgs.iter_mut() {
        for _ in 0..ncols {
            rg.chunks.push((get_u64(buf, off), get_i64(buf, off + 8), get_i64(buf, off + 16)));
            off += 24;
        }
    }
    let mut ndv = Vec::new();
    if version >= CB_VERSION_V2 {
        ndv.reserve(ncols);
        for _ in 0..ncols {
            ndv.push(get_u64(buf, off));
            off += 8;
        }
    }
    // The sums section precedes the v5 sorted flags: a lazy (want_sums=false)
    // parse must still advance past it or every later section reads skewed
    // (the compose-v6 cross-member bug class).
    if version >= CB_VERSION_V4 {
        if want_sums {
            for rg in rgs.iter_mut() {
                rg.sums.reserve(ncols);
                for _ in 0..ncols {
                    rg.sums.push(get_i128(buf, off));
                    off += 16;
                }
            }
        } else {
            off += nrgs * ncols * 16;
        }
    }
    // v5 sorted flags; pre-v5 parts read as all-unknown.
    let mut sorted = vec![0u8; ncols];
    if version >= CB_VERSION_V5 {
        sorted.copy_from_slice(&buf[off..off + ncols]);
        off += ncols;
    }
    // v6 cluster-key section; pre-v6 parts read as no-cluster-key.
    let mut cluster_key = Vec::new();
    if version >= CB_VERSION_V6 {
        let nkeys = u16::from_le_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
        if nkeys > CB_CLUSTER_KEY_MAX_COLS {
            return Err(Box::new(PgError::error("cbstore: corrupt footer".to_string())));
        }
        for k in 0..nkeys {
            let o = off + 2 + k * 2;
            cluster_key.push(u16::from_le_bytes(buf[o..o + 2].try_into().unwrap()));
        }
        off += CB_CLUSTER_KEY_SECTION_LEN;
    }
    // v7 per-granule length-stats section: materialized only for the writer's
    // reopen re-emit (want_sums); readers consume it lazily off the mmap
    // (Part::granule_len_stats). Entries of RGs without RG_FLAG_LENSTATS are
    // zeros and stay unmaterialized. A lazy parse must still advance past
    // the section or the later v7 sections read skewed (the sums precedent).
    if version >= CB_VERSION_V7 && nlencols > 0 {
        if want_sums {
            for rg in rgs.iter_mut() {
                if rg.flags & RG_FLAG_LENSTATS != 0 {
                    let nslots = GRANULES_PER_RG * nlencols;
                    rg.lenstats.reserve(nslots);
                    for s in 0..nslots {
                        let e = off + s * CB_LENSTATS_ENTRY_LEN;
                        rg.lenstats.push((
                            get_u64(buf, e),
                            get_u32(buf, e + 8),
                            get_u32(buf, e + 12),
                        ));
                    }
                }
                off += GRANULES_PER_RG * nlencols * CB_LENSTATS_ENTRY_LEN;
            }
        } else {
            off += nrgs * GRANULES_PER_RG * nlencols * CB_LENSTATS_ENTRY_LEN;
        }
    }
    // v7 stitch section (after length stats); pre-v7 parts read as no-stitch.
    let mut stitch = FooterStitch::default();
    if version >= CB_VERSION_V7 {
        stitch.gndv.reserve(ncols);
        for _ in 0..ncols {
            stitch.gndv.push(get_u64(buf, off));
            off += 8;
        }
        stitch.dir.reserve(nrgs * ncols);
        for _ in 0..nrgs * ncols {
            stitch.dir.push((get_u64(buf, off), get_u32(buf, off + 8)));
            off += CB_STITCH_DIR_ENTRY_LEN;
        }
    }
    // v7 zero-count section (after stitch); pre-v7 parts read as
    // no-zero-counts. Parsed into FooterRg only on the writer's full
    // (want_sums) read — the scan side reads the section lazily off the
    // mmap (Part::granule_zerocnt).
    if version >= CB_VERSION_V7 {
        if want_sums {
            for rg in rgs.iter_mut() {
                rg.zerocnt.reserve(GRANULES_PER_RG * ncols);
                for _ in 0..GRANULES_PER_RG * ncols {
                    rg.zerocnt.push(get_u32(buf, off));
                    off += CB_ZEROCNT_ENTRY_LEN;
                }
            }
        } else {
            off += nrgs * GRANULES_PER_RG * ncols * CB_ZEROCNT_ENTRY_LEN;
        }
    }
    // v8 NDV-registers blob directory (after zero counts); pre-v8 parts
    // read as no-registers. All-zero entries mark absent sketches
    // (pre-v8 append chains or the lane kill switch).
    let mut ndvregs = Vec::new();
    if version >= CB_VERSION_V8 {
        ndvregs.reserve(ncols);
        for _ in 0..ncols {
            ndvregs.push((get_u64(buf, off), get_u32(buf, off + 8)));
            off += CB_NDVREGS_DIR_ENTRY_LEN;
        }
    }
    let _ = off;
    Ok((rgs, ndv, sorted, cluster_key, lenflags, stitch, ndvregs))
}

// Planner sizing: header + footer reads only (no SegMap mmap of the data
// body). None: empty table (no committed footer yet).
pub fn part_footer_rows(path: &str, ncols: usize) -> PgResult<Option<u64>> {
    let mut file = SegFile::open_rw(path)?;
    if file.total_len() < CB_HEADER_LEN {
        return Ok(None);
    }
    let mut hdr = [0u8; CB_HEADER_LEN as usize];
    file.read_exact_at(&mut hdr, 0)?;
    let Some((footer_off, _, version)) = read_header_opt(&hdr)? else { return Ok(None) };
    if footer_off == 0 {
        return Ok(None);
    }
    let (rgs, ..) = read_footer_rgs(&mut file, footer_off, ncols, version, false)?;
    Ok(Some(rgs.iter().map(|rg| rg.nrows as u64).sum()))
}

// ANALYZE NDV source: header + footer reads only. None: no committed footer
// or a v1 part; per-entry 0 = unknown (append-invalidated).
pub fn part_footer_ndv(path: &str, ncols: usize) -> PgResult<Option<Vec<u64>>> {
    let mut file = SegFile::open_rw(path)?;
    if file.total_len() < CB_HEADER_LEN {
        return Ok(None);
    }
    let mut hdr = [0u8; CB_HEADER_LEN as usize];
    file.read_exact_at(&mut hdr, 0)?;
    let Some((footer_off, _, version)) = read_header_opt(&hdr)? else { return Ok(None) };
    if footer_off == 0 || version < CB_VERSION_V2 {
        return Ok(None);
    }
    let (_, _, ndv, _, _, _, _, _) = read_footer_rgs(&mut file, footer_off, ncols, version, false)?;
    Ok(Some(ndv))
}

/// v8 per-column NDV register sketches: header + footer directory + blob
/// reads only (no data-body mmap) — the inherited-ANALYZE union source,
/// where per-child SCALARS cannot be combined but register sketches union
/// losslessly. Ok(None): no committed footer (zero committed rows — the
/// caller skips the child). Some(v): per-column Option<Hll>, None entries
/// where the sketch is absent (pre-v8 part, append-invalidated chain,
/// kill switch, or an unknown blob shape).
pub fn part_footer_ndv_hll(path: &str, ncols: usize) -> PgResult<Option<Vec<Option<crate::hll::Hll>>>> {
    let mut file = SegFile::open_rw(path)?;
    if file.total_len() < CB_HEADER_LEN {
        return Ok(None);
    }
    let mut hdr = [0u8; CB_HEADER_LEN as usize];
    file.read_exact_at(&mut hdr, 0)?;
    let Some((footer_off, _, version)) = read_header_opt(&hdr)? else { return Ok(None) };
    if footer_off == 0 {
        return Ok(None);
    }
    if version < CB_VERSION_V8 {
        return Ok(Some((0..ncols).map(|_| None).collect()));
    }
    let (_, _, _, _, _, _, _, ndvregs) =
        read_footer_rgs(&mut file, footer_off, ncols, version, false)?;
    let total_len = file.total_len();
    let mut out: Vec<Option<crate::hll::Hll>> = Vec::with_capacity(ncols);
    for &(off, len) in &ndvregs {
        if off == 0 || len == 0 || off.checked_add(len as u64).is_none_or(|e| e > total_len) {
            out.push(None);
            continue;
        }
        let mut blob = vec![0u8; len as usize];
        file.read_exact_at(&mut blob, off)?;
        out.push(crate::hll::Hll::from_blob(&blob));
    }
    Ok(Some(out))
}

pub struct Part {
    // Process-shared read-only mapping (coldio lane): pool workers'
    // thread-local part caches share one mapping per live part state —
    // see segfile.rs SHARED_MAPS for the measured fault-storm motivation.
    map: std::sync::Arc<SegMap>,
    pub rgs: Vec<FooterRg>,
    pub ncols: usize,
    // Header footer_off this footer was parsed from (part-cache probe key).
    pub footer_off: u64,
    // v5 per-column sorted flags (1 = part rows non-decreasing); all-0 on
    // pre-v5 parts.
    pub sorted: Vec<u8>,
    // Ingest-time per-column NDV from the footer (v2+; per-entry 0 =
    // unknown). Empty on pre-v2 parts. Kept on the cached Part so the
    // planner's footer-NDV consumer never re-reads the footer per plan
    // (fixed-overhead audit 2026-07-14: the standalone part_footer_ndv
    // path re-read + re-parsed the whole ~2MB 100M footer on EVERY
    // planning of a pgrcolumnar rel — ~1.3ms of a ~1.9ms plan constant).
    pub ndv: Vec<u64>,
    // v6 declared cluster key (zero-based column indexes, key order); empty
    // on pre-v6 parts or when none was declared. Per-RG sortedness under it
    // is RG_FLAG_CLUSTERED.
    pub cluster_key: Vec<u16>,
    // File offset of the v4 per-RG sums section (0 = none): sums stay on
    // disk (the sectioned lazy open never reads them) and are read through
    // the mmap only when a metadata-sum consumer asks (rg_sum).
    sums_off: u64,
    // v7 per-granule length-stats section (0 = none), consumed lazily like
    // sums. lenrank maps a column to its dense flagged-column rank (-1 =
    // no entries for the column).
    lenstats_off: u64,
    lenrank: Vec<i16>,
    nlencols: usize,
    // v7 stitch metadata (empty/zero on pre-v7 parts): per-column global
    // NDV + the per-(RG, col) stitch-blob directory; blobs stay on disk and
    // are read through the mmap on demand (Part::stitch).
    stitch: FooterStitch,
    // File offset of the v7 per-granule zero-count section (0 = none):
    // like sums, read lazily through the mmap (granule_zerocnt).
    zerocnt_off: u64,
    // First byte past the row-group data region (stitch blobs and the
    // footer follow): the last row group's readahead extent ceiling.
    // Computed at open from footer metadata only.
    data_end: u64,
    // Immutable part identity for cross-query caches (condition cache): the
    // part-cache staleness probe's exact vocabulary — (st_dev, st_ino,
    // st_size, footer_off), stat'd BEFORE the footer read (a publish racing
    // the open makes the identity stale-in-the-safe-direction: it names a
    // superseded state, so the next open mints a fresh one; never the
    // reverse). Sealed row groups never mutate; every publish grows
    // len/footer_off and every recreate changes the inode, so equal
    // identities imply byte-identical granule content.
    pub identity: crate::condcache::PartIdent,
    // Lazy absolute-granule prefix sums (see granule_starts). OnceLock so a
    // part-cache-shared Part builds it once under any thread.
    granule_starts: std::sync::OnceLock<Vec<u64>>,
    // Lazy per-column on-disk chunk byte totals (see col_bytes). The
    // planner asks on EVERY plan of a pgrcolumnar rel (plancat column-fraction
    // seqscan disk costing) and the walk is O(rgs x ncols) over the footer
    // chunk directory — at 100M (~1.5K RGs x 105 cols) it measured 85% of
    // the replan loop / 76% of the whole serial per-query fixed path
    // (fixedpath2 round-2 perf probe), ~0.2ms of the ~0.25ms
    // post-footer_ndv plan constant. Same class as the round-1 footer_ndv
    // fix: derived purely from this footer parse, so compute once per
    // cached Part.
    col_bytes: std::sync::OnceLock<Vec<u64>>,
}

impl Part {
    // None: empty table (no committed footer yet).
    pub fn open(path: &str, ncols: usize) -> PgResult<Option<Part>> {
        // Identity stat via the Vfs boundary (DST P1 WS-B, contract v1.1
        // FileInfo dev+ino): same single stat(2), same fields consumed —
        // (st_dev, st_ino, st_size), the part-cache identity vocabulary.
        let ident_stat = {
            let mut fi = vfs::FileInfo::zeroed();
            match std::ffi::CString::new(path) {
                Ok(c) if vfs::stat(&c, &mut fi) == 0 => Some(fi),
                _ => None,
            }
        };
        let mut file = SegFile::open_rw(path)?;
        if file.total_len() < CB_HEADER_LEN {
            return Ok(None);
        }
        let mut hdr = [0u8; CB_HEADER_LEN as usize];
        file.read_exact_at(&mut hdr, 0)?;
        let Some((footer_off, _fp, version)) = read_header_opt(&hdr)? else { return Ok(None) };
        if footer_off == 0 {
            return Ok(None);
        }
        let (rgs, _footer_end, ndv, sorted, cluster_key, lenflags, stitch, _ndvregs) =
            read_footer_rgs(&mut file, footer_off, ncols, version, false)?;
        drop(file);
        // Shared mapping keyed by the part-cache identity vocabulary (seg0
        // dev/ino + mapped length); the unstat-able fallback keeps the
        // historical private mapping (same posture as the null identity).
        let map = match ident_stat.as_ref() {
            Some(md) => SegMap::open_shared(path, md.dev, md.ino)?,
            None => SegMap::open(path)?.map(std::sync::Arc::new),
        };
        let Some(map) = map else { return Ok(None) };
        // Structural skippability guard: a footer whose RG directory points
        // past the live mapping is torn state — readers must error cleanly
        // here rather than slice-panic on garbage row-group offsets.
        for rg in &rgs {
            if rg.file_off.saturating_add(CB_RG_HEADER_LEN as u64) > map.bytes().len() as u64 {
                return Err(Box::new(PgError::error(
                    "cbstore: corrupt part (row group out of bounds)".to_string(),
                )));
            }
        }
        let lay = footer_layout(version, rgs.len(), ncols);
        let sums_off = if version >= CB_VERSION_V4 {
            footer_off
                + (lay.pre_len + rgs.len() * 24 + rgs.len() * ncols * 24 + lay.ndv_len) as u64
        } else {
            0
        };
        let nlencols = lenflags.iter().filter(|&&b| b != 0).count();
        let lenstats_off = if version >= CB_VERSION_V7 && nlencols > 0 {
            footer_off + lay.lenstats_off(rgs.len(), ncols) as u64
        } else {
            0
        };
        let mut rank = 0i16;
        let lenrank = lenflags
            .iter()
            .map(|&f| {
                if f != 0 {
                    rank += 1;
                    rank - 1
                } else {
                    -1
                }
            })
            .collect();
        let zerocnt_off = if version >= CB_VERSION_V7 {
            footer_off + lay.zerocnt_off(rgs.len(), ncols, nlencols, version) as u64
        } else {
            0
        };
        // Row-group data region end: the footer follows the last RG, with
        // v7 stitch blobs (if any) between — the smallest stitch-blob offset
        // tightens the bound so readahead never fetches blob/footer bytes.
        let data_end = stitch
            .dir
            .iter()
            .filter(|&&(off, count)| count > 0 && off > 0)
            .map(|&(off, _)| off)
            .min()
            .unwrap_or(footer_off)
            .min(footer_off);
        let identity = {
            match ident_stat {
                Some(md) => crate::condcache::PartIdent {
                    dev: md.dev,
                    ino: md.ino,
                    len: md.size as u64,
                    footer_off,
                },
                // Unstat-able path (should be unreachable past open_rw):
                // a null identity that the condition cache refuses to key on.
                None => crate::condcache::PartIdent { dev: 0, ino: 0, len: 0, footer_off: 0 },
            }
        };
        Ok(Some(Part {
            map,
            rgs,
            ncols,
            footer_off,
            sorted,
            ndv,
            cluster_key,
            sums_off,
            lenstats_off,
            lenrank,
            nlencols,
            stitch,
            zerocnt_off,
            data_end,
            identity,
            granule_starts: std::sync::OnceLock::new(),
            col_bytes: std::sync::OnceLock::new(),
        }))
    }

    /// Per-column on-disk chunk bytes summed over the part's committed row
    /// groups (planner column-fraction seqscan disk costing). Within an RG
    /// the column chunks are laid out contiguously in column order (writer
    /// flush: chunk_off is the running body offset), so column i's bytes
    /// are the offset delta to column i+1's chunk; the last column runs to
    /// the end of the RG body (the next RG's file_off, or this footer's
    /// offset for the newest RG). Stale interior footers left by earlier
    /// COPY batches inflate at most the last column of each batch's tail RG
    /// — noise at costing precision. Values are identical to the historical
    /// per-call walk (lib.rs footer_col_bytes pre-cache), computed once per
    /// cached Part.
    pub fn col_bytes(&self) -> &[u64] {
        self.col_bytes.get_or_init(|| {
            let ncols = self.ncols;
            let mut out = vec![0u64; ncols];
            for (r, rg) in self.rgs.iter().enumerate() {
                let rg_end = self.rgs.get(r + 1).map_or(self.footer_off, |next| next.file_off);
                let body_len = rg_end.saturating_sub(rg.file_off);
                for i in 0..ncols {
                    let start = rg.chunks[i].0.min(body_len);
                    let end = if i + 1 < ncols { rg.chunks[i + 1].0 } else { body_len };
                    out[i] += end.min(body_len).saturating_sub(start);
                }
            }
            out
        })
    }

    /// v7 part-global dict size for a column (0 = no stitch: pre-v7 part,
    /// non-text/never-dict column, or invalidated by append).
    pub fn stitch_gndv(&self, col: usize) -> u64 {
        self.stitch.gndv.get(col).copied().unwrap_or(0)
    }

    /// All-column v7 stitch dict sizes (empty on pre-v7 parts) — the
    /// plan-time DictCode sort-key answerability signal (SE-TOPNNI;
    /// footer_stitch_gndv).
    pub fn stitch_gndv_all(&self) -> &[u64] {
        &self.stitch.gndv
    }

    /// The (rg, col) stitch table: local dict code -> part-global byte-rank
    /// code (length = the RG chunk's local NDV). None when absent. Torn
    /// directory entries (out of bounds / misaligned) also read as None —
    /// stitch consumers fail open to the per-epoch paths.
    pub fn stitch(&self, rg: usize, col: usize) -> Option<&[u32]> {
        if self.stitch_gndv(col) == 0 {
            return None;
        }
        let &(off, count) = self.stitch.dir.get(rg * self.ncols + col)?;
        if count == 0 {
            return None;
        }
        let bytes = self.bytes();
        let end = off.checked_add(count as u64 * 4)?;
        if off % 4 != 0 || end > bytes.len() as u64 {
            debug_assert!(false, "cbstore: torn stitch directory entry");
            return None;
        }
        // SAFETY: bounds and 4-alignment checked above; the mmap base is
        // page-aligned and outlives &self.
        Some(unsafe {
            std::slice::from_raw_parts(bytes.as_ptr().add(off as usize).cast::<u32>(), count as usize)
        })
    }

    // Footer sum for (rg, col); caller gates on RG_FLAG_SUMS (which only a
    // v4+ writer sets, so sums_off != 0 whenever the flag is present).
    pub fn rg_sum(&self, rg: usize, col: usize) -> i128 {
        debug_assert!(self.sums_off != 0 && self.rgs[rg].flags & RG_FLAG_SUMS != 0);
        get_i128(self.bytes(), self.sums_off as usize + (rg * self.ncols + col) * 16)
    }

    /// True when the part carries a v7 length-stats section with entries for
    /// column `col` (per-RG exactness is still RG_FLAG_LENSTATS's business —
    /// see `granule_len_stats`).
    pub fn has_len_stats(&self, col: usize) -> bool {
        self.lenstats_off != 0 && self.lenrank.get(col).is_some_and(|&r| r >= 0)
    }

    /// v7 footer length stats for (rg, granule, col): (sum(octet_length),
    /// non-null count, empty-string count) over the granule's rows. None =
    /// the part/RG/column carries no stats (v<=6 part, preserved RG, or a
    /// non-text column) — callers fall back to the row path.
    pub fn granule_len_stats(&self, rg: usize, g: usize, col: usize) -> Option<(u64, u32, u32)> {
        if self.lenstats_off == 0 || self.rgs[rg].flags & RG_FLAG_LENSTATS == 0 {
            return None;
        }
        let rank = *self.lenrank.get(col)?;
        if rank < 0 {
            return None;
        }
        debug_assert!(g < GRANULES_PER_RG);
        let e = self.lenstats_off as usize
            + ((rg * GRANULES_PER_RG + g) * self.nlencols + rank as usize)
                * CB_LENSTATS_ENTRY_LEN;
        let b = self.bytes();
        Some((get_u64(b, e), get_u32(b, e + 8), get_u32(b, e + 12)))
    }

    /// The v7 zero/empty count for (rg, granule slot, col); caller gates on
    /// `rg_has_zerocnt` (which only a v7+ writer sets, so zerocnt_off != 0
    /// whenever the flag is present).
    pub fn granule_zerocnt(&self, rg: usize, g: usize, col: usize) -> u32 {
        debug_assert!(self.rg_has_zerocnt(rg));
        get_u32(
            self.bytes(),
            self.zerocnt_off as usize
                + ((rg * GRANULES_PER_RG + g) * self.ncols + col) * CB_ZEROCNT_ENTRY_LEN,
        )
    }

    /// The RG carries exact v7 zero/empty counts (sealed by a v7+ writer;
    /// RGs preserved from v<=6 footers read false).
    pub fn rg_has_zerocnt(&self, rg: usize) -> bool {
        self.zerocnt_off != 0 && self.rgs[rg].flags & RG_FLAG_ZEROCNT != 0
    }

    /// EVERY committed RG carries exact v7 zero/empty counts — the
    /// sufficient plan-time condition for the meta zero-count qual answer
    /// (scan.rs meta_agg refuses whole-part on the first visible RG
    /// without them, so a single preserved v<=6 RG voids the arm). O(nrgs)
    /// flag walk over the already-parsed directory (~1.5K RGs at 100M —
    /// noise next to the footer serves this rides beside).
    pub fn zerocnt_all_rgs(&self) -> bool {
        self.zerocnt_off != 0 && self.rgs.iter().all(|rg| rg.flags & RG_FLAG_ZEROCNT != 0)
    }

    pub fn bytes(&self) -> &[u8] {
        self.map.bytes()
    }

    pub fn total_rows(&self) -> u64 {
        self.rgs.iter().map(|rg| rg.nrows as u64).sum()
    }

    /// Part-global granule geometry (the runtime morsel address space — M1
    /// scan pipelines). Granule indexes are ABSOLUTE over the part, in
    /// row-group order; interior RGs can be short (reopen-append seals a
    /// partial trailing RG that later ingests do not refill), so the map is
    /// a real prefix sum over per-RG granule counts, never `rg * 8 + g`.
    /// Entry `rg` = first absolute granule of row group `rg`; entry `nrgs`
    /// = total granule count. Built lazily, immutable thereafter (sealed
    /// RGs never mutate and a Part instance's footer is pinned at open).
    pub fn granule_starts(&self) -> &[u64] {
        self.granule_starts.get_or_init(|| {
            let mut acc = 0u64;
            let mut starts = Vec::with_capacity(self.rgs.len() + 1);
            starts.push(0);
            for rg in &self.rgs {
                acc += (rg.nrows as u64).div_ceil(GRANULE_ROWS as u64);
                starts.push(acc);
            }
            starts
        })
    }

    /// Total granules in the part (absolute granule address-space bound).
    pub fn total_granules(&self) -> u64 {
        self.granule_starts().last().copied().unwrap_or(0)
    }

    /// First absolute granule of the row group strictly after the one
    /// containing `granule` — the hard morsel boundary (row-group edges are
    /// exactly the dictionary-epoch edges: each RG chunk carries its own
    /// local dictionary, so every per-epoch memo keys on the RG index).
    /// Contract mirror of the runtime's `MorselSource::next_boundary_after`:
    /// requires `granule < total_granules()` and returns a bound in
    /// `(granule, total_granules()]`.
    pub fn granule_next_boundary(&self, granule: u64) -> u64 {
        let starts = self.granule_starts();
        match starts.binary_search(&granule) {
            // `granule` IS an RG start: the boundary is the next entry.
            Ok(i) => starts.get(i + 1).copied().unwrap_or_else(|| self.total_granules()),
            // Insertion point names the first entry > granule.
            Err(i) => starts.get(i).copied().unwrap_or_else(|| self.total_granules()),
        }
    }

    /// Locate absolute granule `granule` as (row group, granule-in-rg).
    /// Caller contract: `granule < total_granules()`.
    pub fn locate_granule(&self, granule: u64) -> (usize, usize) {
        let starts = self.granule_starts();
        debug_assert!(granule < self.total_granules());
        let rg = match starts.binary_search(&granule) {
            Ok(i) => i,
            Err(i) => i - 1,
        };
        (rg, (granule - starts[rg]) as usize)
    }

    pub fn chunk(&self, rg: usize, col: usize) -> ChunkView<'_> {
        let m = &self.rgs[rg];
        let base = (m.file_off + m.chunks[col].0) as usize;
        ChunkView::at(self.bytes(), base, m.nrows)
    }

    // End of row group `rg`'s body: the next RG's file offset (the writer
    // appends RGs at ascending align64 offsets), or the data region's end
    // for the last one. 0 = the footer's RG order is not ascending here
    // (foreign/legacy part) — callers skip rather than guess.
    fn rg_end(&self, rg: usize) -> u64 {
        let end = match self.rgs.get(rg + 1) {
            Some(next) => next.file_off,
            None => self.data_end,
        };
        if end <= self.rgs[rg].file_off { 0 } else { end }
    }

    /// Claim-time readahead (parallelism-redesign §2.8): madvise(WILLNEED)
    /// the mmap extents of row group `rg`'s chunks for the given column set
    /// (`cols` ascending, the scan's needed_idx vocabulary). Extents come
    /// from footer metadata only — chunk `c` spans [chunks[c].0,
    /// chunks[c+1].0) of the RG body (the writer lays chunks out in column
    /// order), the last chunk ending at the RG body's end — so this call
    /// never touches (= faults) the mapped bytes itself. Adjacent needed
    /// columns coalesce into one madvise. Purely advisory: kernel errors
    /// are ignored; returns whether at least one advise was issued.
    pub fn advise_willneed(&self, rg: usize, cols: &[u16]) -> bool {
        let Some(m) = self.rgs.get(rg) else { return false };
        let rg_end = self.rg_end(rg);
        if rg_end == 0 {
            return false;
        }
        let bytes = self.bytes();
        let mut issued = false;
        let mut pending: Option<(u64, u64)> = None;
        let mut flush = |ext: (u64, u64)| {
            issued |= advise_extent(bytes, ext.0, ext.1);
        };
        for &c in cols {
            let c = c as usize;
            let Some(&(off, _, _)) = m.chunks.get(c) else { continue };
            let start = m.file_off + off;
            let end = match m.chunks.get(c + 1) {
                Some(&(next_off, _, _)) => m.file_off + next_off,
                None => rg_end,
            };
            // Defensive on foreign layouts: a non-ascending chunk directory
            // yields an empty/inverted extent — skip it, never advise junk.
            let end = end.min(rg_end);
            if end <= start || end > bytes.len() as u64 {
                continue;
            }
            match pending {
                // Consecutive needed columns share a boundary: coalesce.
                Some((ps, pe)) if start <= pe => pending = Some((ps, pe.max(end))),
                Some(ext) => {
                    flush(ext);
                    pending = Some((start, end));
                }
                None => pending = Some((start, end)),
            }
        }
        if let Some(ext) = pending {
            flush(ext);
        }
        issued
    }
}

// One page-aligned madvise(MADV_WILLNEED) over bytes[start..end); advisory
// (errors ignored). Bounds are the caller's business; start/end are only
// page-rounded here.
//
// RAW libc carve-out (DST-P1): this madvise runs over SegMap's mmap'd
// bytes and travels with the segfile map_segs mmap cluster — it stays raw
// until the segmap pread arm lands. Allowlist row retained, tagged
// `# pending: segmap-pread-arm`.
#[cfg(unix)]
fn advise_extent(bytes: &[u8], start: u64, end: u64) -> bool {
    static PAGE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    let page = *PAGE.get_or_init(|| {
        // SAFETY: sysconf is always safe to call.
        let p = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if p > 0 { p as u64 } else { 4096 }
    });
    let a_start = start / page * page;
    let len = (end - a_start) as usize;
    // SAFETY: [a_start, end) lies within the part's live mapping (caller
    // bounds-checked against bytes.len(); the mmap base is page-aligned so
    // rounding start down stays inside it). madvise reads no user memory.
    unsafe {
        libc::madvise(
            bytes.as_ptr().add(a_start as usize) as *mut libc::c_void,
            len,
            libc::MADV_WILLNEED,
        );
    }
    true
}

#[cfg(not(unix))]
fn advise_extent(_bytes: &[u8], _start: u64, _end: u64) -> bool {
    false
}

// Decompress one v6 frame (u32 raw_len | u32 comp_len | bytes) body into
// `dst` (an arena_frame slice: raw_len payload + OUT_PAD slack). LZ4 goes
// through the lane-v2-decode padded kernel (wild copies spill into the pad);
// ZSTD writes exactly raw_len (the pad is untouched slack).
pub(crate) fn decompress_frame_into(codec: Codec, src: &[u8], dst: &mut [u8], raw_len: usize) {
    match codec {
        Codec::Lz4 => {
            crate::lz4dec::decompress_padded(src, dst, raw_len)
                .unwrap_or_else(|e| panic!("cbstore: corrupt LZ4 frame: {e}"));
        }
        #[cfg(not(target_family = "wasm"))]
        Codec::Zstd => {
            let got = zstd::bulk::Decompressor::new()
                .expect("cbstore: zstd decompressor init failed")
                .decompress_to_buffer(src, &mut dst[..raw_len])
                .expect("cbstore: corrupt ZSTD frame");
            assert_eq!(got, raw_len, "cbstore: ZSTD frame length mismatch");
        }
        // wasm32: zstd-sys links C and has no wasm build; cbstore tables
        // carrying ZSTD frames are unreadable on this target (documented
        // ledger out until a pure-Rust decoder is adopted).
        #[cfg(target_family = "wasm")]
        Codec::Zstd => {
            let _ = (src, raw_len);
            panic!("cbstore: ZSTD frames are not supported on wasm32-wasip1");
        }
        Codec::None => unreachable!("cbstore: decompress with Codec::None"),
    }
}

// ---- sub-granule dict frames (CHUNK_FLAG_DICT_FRAMED) ---------------------

// Cumulative observability counters (CBSCAN debug line / A-B attribution):
// lazy tables built, frames those tables carry, frames actually
// decompressed, whole-table ensure sweeps.
pub static DICT_LAZY_BUILDS: AtomicU64 = AtomicU64::new(0);
pub static DICT_FRAMES_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static DICT_FRAMES_ENSURED: AtomicU64 = AtomicU64::new(0);
pub static DICT_ENSURE_ALLS: AtomicU64 = AtomicU64::new(0);

/// (lazy_builds, frames_total, frames_ensured, ensure_alls) — process
/// cumulative.
pub fn dict_frame_stats() -> (u64, u64, u64, u64) {
    (
        DICT_LAZY_BUILDS.load(Relaxed),
        DICT_FRAMES_TOTAL.load(Relaxed),
        DICT_FRAMES_ENSURED.load(Relaxed),
        DICT_ENSURE_ALLS.load(Relaxed),
    )
}

// Read-side lazy kill switch: PGRUST_CBSTORE_DICT_LAZY=0 materializes framed
// dict chunks eagerly at build (byte-identical arena; A/B attribution of the
// lazy term). Default on.
pub(crate) fn dict_lazy_read() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| match std::env::var("PGRUST_CBSTORE_DICT_LAZY") {
        Ok(v) => !matches!(v.trim(), "off" | "0" | "false"),
        Err(_) => true,
    })
}

// Parse the framed blob region: cumulative raw / stored offsets (nframes+1
// each) + the frames-data base offset within `blob`.
fn parse_framed_dict(blob: &[u8]) -> (Vec<u32>, Vec<u32>, usize) {
    let nframes = get_u32(blob, 0) as usize;
    let mut raw_off = Vec::with_capacity(nframes + 1);
    let mut comp_off = Vec::with_capacity(nframes + 1);
    let (mut r, mut c) = (0u32, 0u32);
    raw_off.push(0);
    comp_off.push(0);
    for f in 0..nframes {
        r += get_u32(blob, 4 + f * 8);
        c += get_u32(blob, 8 + f * 8);
        raw_off.push(r);
        comp_off.push(c);
    }
    (raw_off, comp_off, 4 + nframes * 8)
}

/// Lazy sub-framed dictionary arena (one per (scan, RG, column) while the
/// per-RG dict table is cached): the dict Datum table is published at build
/// (pointers into `buf` at the dict_off offsets — the framed layout keeps
/// offsets into the CONCATENATED raw blob), but a frame's bytes materialize
/// only on the first `ensure_code`/`ensure_frame`/`ensure_all` that needs
/// them. CONTRACT (the lazy seam): a dict entry's BYTES may be read only
/// after an ensure covering its code; every publish surface either ensures
/// per requested code (`ColDecode::datum`, the value gathers,
/// `SoaDictTable::datum`) or sweeps (`ensure_all` before whole-dict memo
/// fills / blob kernels / binary searches).
///
/// Single-threaded by construction (per-scan decode state; Cell tracking);
/// `src`/`entry_off` point into the Part mmap the scan pins; `buf`'s heap
/// allocation is stable after build (never resized), so published Datums
/// stay valid exactly as long as the legacy arena's.
pub struct DictLazy {
    // Backing allocation (capacity = total_raw + OUT_PAD bytes, len stays 0:
    // frames materialize through `buf_ptr`; unread gaps stay uninitialized
    // and are unreachable under the ensure contract).
    #[allow(dead_code)]
    buf: Vec<u64>,
    buf_ptr: *mut u8,
    nframes: u32,
    ndict: u32,
    // Cumulative offsets, nframes+1 each: frame f covers raw
    // [raw_off[f], raw_off[f+1]) and stored bytes [comp_off[f], comp_off[f+1])
    // (comp len == raw len marks a stored-raw frame).
    raw_off: Vec<u32>,
    comp_off: Vec<u32>,
    src: *const u8,
    codec: Codec,
    // dict_off table (u32 LE raw-blob offset per code) in the mmap.
    entry_off: *const u8,
    done: Vec<Cell<u64>>,
    ndone: Cell<u32>,
}

impl DictLazy {
    #[inline]
    pub fn all_done(&self) -> bool {
        self.ndone.get() == self.nframes
    }

    /// The published arena base (dict[code] == base + dict_off[code]).
    #[inline]
    pub fn base(&self) -> usize {
        self.buf_ptr as usize
    }

    /// Materialize the frame holding `code`'s entry bytes. Cheap once all
    /// frames are done (one load + compare).
    #[inline]
    pub fn ensure_code(&self, code: u32) {
        if self.all_done() {
            return;
        }
        debug_assert!(code < self.ndict);
        // SAFETY: entry_off spans ndict u32s of the pinned mmap (build).
        let off = unsafe {
            u32::from_le_bytes(
                core::slice::from_raw_parts(self.entry_off.add(code as usize * 4), 4)
                    .try_into()
                    .unwrap(),
            )
        };
        // Entries never straddle a frame edge (writer cuts at entry
        // boundaries), so the owning frame is the last one starting at or
        // before `off`.
        let f = self.raw_off.partition_point(|&r| r <= off) - 1;
        self.ensure_frame(f);
    }

    pub fn ensure_frame(&self, f: usize) {
        let (w, b) = (f / 64, 1u64 << (f % 64));
        if self.done[w].get() & b != 0 {
            return;
        }
        let raw_lo = self.raw_off[f] as usize;
        let raw_hi = self.raw_off[f + 1] as usize;
        let raw_len = raw_hi - raw_lo;
        let comp_lo = self.comp_off[f] as usize;
        let comp_len = self.comp_off[f + 1] as usize - comp_lo;
        // SAFETY: the frame directory came off the chunk (the payload
        // file-trust posture everywhere in this module); src spans the
        // frames region of the pinned mmap.
        let src = unsafe { core::slice::from_raw_parts(self.src.add(comp_lo), comp_len) };
        if comp_len == raw_len {
            // Stored-raw frame (writer incompressible guard).
            // SAFETY: buf carries total_raw + OUT_PAD capacity; raw_hi <=
            // total_raw.
            unsafe {
                core::ptr::copy_nonoverlapping(src.as_ptr(), self.buf_ptr.add(raw_lo), raw_len)
            };
        } else {
            // In-place decompress wild-writes up to OUT_PAD past raw_hi
            // (lz4dec contract). Allowed only when no already-materialized
            // frame owns bytes in that spill span (tiny successor frames can
            // put more than one frame inside OUT_PAD); otherwise decompress
            // to scratch and copy exactly raw_len.
            let mut spill_clear = true;
            let mut g = f + 1;
            while g < self.nframes as usize
                && (self.raw_off[g] as usize) < raw_hi + crate::lz4dec::OUT_PAD
            {
                if self.done[g / 64].get() & (1u64 << (g % 64)) != 0 {
                    spill_clear = false;
                    break;
                }
                g += 1;
            }
            if spill_clear {
                // SAFETY: raw_lo + raw_len + OUT_PAD <= total_raw + OUT_PAD
                // (buf capacity).
                let dst = unsafe {
                    core::slice::from_raw_parts_mut(
                        self.buf_ptr.add(raw_lo),
                        raw_len + crate::lz4dec::OUT_PAD,
                    )
                };
                decompress_frame_into(self.codec, src, dst, raw_len);
            } else {
                let mut scratch = vec![0u8; raw_len + crate::lz4dec::OUT_PAD];
                decompress_frame_into(self.codec, src, &mut scratch, raw_len);
                // SAFETY: as above; exactly raw_len bytes land in place.
                unsafe {
                    core::ptr::copy_nonoverlapping(
                        scratch.as_ptr(),
                        self.buf_ptr.add(raw_lo),
                        raw_len,
                    )
                };
            }
        }
        self.done[w].set(self.done[w].get() | b);
        self.ndone.set(self.ndone.get() + 1);
        DICT_FRAMES_ENSURED.fetch_add(1, Relaxed);
    }

    /// Materialize every frame (whole-dict sweeps: memo fills, blob kernels,
    /// rank binary searches, legacy borrowed lanes).
    pub fn ensure_all(&self) {
        if self.all_done() {
            return;
        }
        DICT_ENSURE_ALLS.fetch_add(1, Relaxed);
        for f in 0..self.nframes as usize {
            self.ensure_frame(f);
        }
    }
}

pub struct ChunkView<'a> {
    part: &'a [u8],
    pub hdr: ChunkHeader,
    gdir_off: usize,
    blockzm_off: usize,
    bloom_off: usize,
    payload_off: usize,
    nrows: u32,
}

// bench/rig fixture path only: a ChunkView over a bare payload image.
#[cfg(feature = "bench-internals")]
pub fn chunk_view_for_bench(part: &[u8], hdr: ChunkHeader, nrows: u32) -> ChunkView<'_> {
    ChunkView { part, hdr, gdir_off: 0, blockzm_off: 0, bloom_off: 0, payload_off: 0, nrows }
}

impl<'a> ChunkView<'a> {
    pub fn at(part: &'a [u8], base: usize, nrows: u32) -> ChunkView<'a> {
        let hdr = ChunkHeader::decode(&part[base..base + CB_CHUNK_HEADER_LEN]);
        let ng = hdr.ngranules as usize;
        let gdir_off = base + CB_CHUNK_HEADER_LEN;
        let blockzm_off = gdir_off + ng * CB_GRANULE_ENTRY_LEN;
        let blockzm_len = if hdr.flags & CHUNK_FLAG_BLOCK_ZM != 0 {
            ng * BLOCKS_PER_GRANULE * CB_BLOCK_ZM_ENTRY_LEN
        } else {
            0
        };
        let bloom_off = blockzm_off + blockzm_len;
        let bloom_len =
            if hdr.flags & CHUNK_FLAG_BLOOM != 0 { ng * crate::bloom::BLOOM_BYTES } else { 0 };
        let payload_off = align64((bloom_off + bloom_len) as u64) as usize;
        ChunkView { part, hdr, gdir_off, blockzm_off, bloom_off, payload_off, nrows }
    }

    pub fn has_block_zm(&self) -> bool {
        self.hdr.flags & CHUNK_FLAG_BLOCK_ZM != 0
    }

    pub fn block_minmax(&self, g: usize, b: usize) -> (i64, i64) {
        debug_assert!(self.has_block_zm());
        let off = self.blockzm_off + (g * BLOCKS_PER_GRANULE + b) * CB_BLOCK_ZM_ENTRY_LEN;
        (get_i64(self.part, off), get_i64(self.part, off + 8))
    }

    pub fn has_bloom(&self) -> bool {
        self.hdr.flags & CHUNK_FLAG_BLOOM != 0
    }

    pub fn bloom_may_contain(&self, g: usize, v: i64) -> bool {
        debug_assert!(self.has_bloom());
        let off = self.bloom_off + g * crate::bloom::BLOOM_BYTES;
        crate::bloom::bloom_may_contain(&self.part[off..off + crate::bloom::BLOOM_BYTES], v)
    }

    pub fn granule(&self, g: usize) -> GranuleEntry {
        let off = self.gdir_off + g * CB_GRANULE_ENTRY_LEN;
        GranuleEntry {
            payload_off: get_u64(self.part, off),
            min: get_i64(self.part, off + 8),
            max: get_i64(self.part, off + 16),
        }
    }

    fn payload(&self) -> &'a [u8] {
        &self.part[self.payload_off..self.payload_off + self.hdr.payload_len as usize]
    }

    fn granule_rows(&self, g: usize) -> usize {
        let lo = g * GRANULE_ROWS;
        (self.nrows as usize - lo).min(GRANULE_ROWS)
    }

    // Granule g's plain fixed-width payload bytes (n rows x width): the
    // in-file slice on unframed chunks, else the v6 granule frame
    // decompressed into `arena` (u64-backed scratch, reused per granule).
    fn fixed_granule_bytes<'s>(
        &'s self,
        g: usize,
        n: usize,
        arena: &'s mut Vec<u64>,
    ) -> &'s [u8] {
        let w = self.hdr.width as usize;
        let p = self.payload();
        if self.hdr.codec == Codec::None {
            let lo = g * GRANULE_ROWS;
            return &p[lo * w..(lo + n) * w];
        }
        let fo = self.granule(g).payload_off as usize;
        let raw_len = get_u32(p, fo) as usize;
        let comp_len = get_u32(p, fo + 4) as usize;
        assert_eq!(raw_len, n * w, "cbstore: framed granule length mismatch");
        let dst = arena_frame(arena, raw_len);
        decompress_frame_into(self.hdr.frame_codec(), &p[fo + 8..fo + 8 + comp_len], dst, raw_len);
        &dst[..raw_len]
    }

    // Build the RG's dictionary Datum table once (keyed on dict.is_empty();
    // the caller clears it at RG boundaries). Lz4Dict decompresses the blob
    // into `arena` — the dict table (and every published Datum) points there
    // for as long as the dict_rg cache holds. Framed chunks (sub-granule
    // frames) materialize FULLY here — this is the eager path; the lane
    // scan's codes decode goes through `build_dict_lazy`.
    fn build_dict(&self, dict: &mut Vec<Datum>, arena: &mut Vec<u64>) {
        if !dict.is_empty() {
            return;
        }
        let ndv = self.hdr.aux as usize;
        let w = self.hdr.width as usize;
        let p = self.payload();
        let codes_len = align4(self.nrows as usize * w);
        let off_tab = &p[codes_len..codes_len + ndv * 4];
        let blob = &p[codes_len + ndv * 4..];
        let blob_base = if self.hdr.encoding == Encoding::Lz4Dict {
            if self.hdr.flags & CHUNK_FLAG_DICT_FRAMED != 0 {
                // Whole-blob contiguous fast path: ascending in-place frame
                // decompresses — frame f's wild spill (<= OUT_PAD) lands in
                // not-yet-written successor bytes, and every frame's region
                // is fully written by its own step.
                let (raw_off, comp_off, data) = parse_framed_dict(blob);
                let total = *raw_off.last().unwrap() as usize;
                let dst = arena_frame(arena, total);
                for f in 0..raw_off.len() - 1 {
                    let (rlo, rhi) = (raw_off[f] as usize, raw_off[f + 1] as usize);
                    let (clo, chi) = (comp_off[f] as usize, comp_off[f + 1] as usize);
                    let src = &blob[data + clo..data + chi];
                    if chi - clo == rhi - rlo {
                        dst[rlo..rhi].copy_from_slice(src);
                    } else {
                        decompress_frame_into(
                            self.hdr.frame_codec(),
                            src,
                            &mut dst[rlo..],
                            rhi - rlo,
                        );
                    }
                }
                dst.as_ptr() as usize
            } else {
                let raw_len = get_u32(blob, 0) as usize;
                let comp_len = get_u32(blob, 4) as usize;
                let dst = arena_frame(arena, raw_len);
                decompress_frame_into(
                    self.hdr.frame_codec(),
                    &blob[8..8 + comp_len],
                    dst,
                    raw_len,
                );
                dst.as_ptr() as usize
            }
        } else {
            blob.as_ptr() as usize
        };
        dict.reserve(ndv);
        for c in off_tab.chunks_exact(4) {
            let o = u32::from_le_bytes(c.try_into().unwrap()) as usize;
            dict.push(Datum::from_usize(blob_base + o));
        }
    }

    /// Lazy dict build for framed chunks (CHUNK_FLAG_DICT_FRAMED): publish
    /// the Datum pointer table over an unmaterialized arena + the DictLazy
    /// ensure state; NO frame decompresses here. false = not a framed chunk
    /// (or lazy reads killed) — the caller takes the eager `build_dict`.
    /// Same dict.is_empty() rebuild key as `build_dict`.
    fn build_dict_lazy(
        &self,
        dict: &mut Vec<Datum>,
        lazy: &mut Option<Box<DictLazy>>,
    ) -> bool {
        if self.hdr.encoding != Encoding::Lz4Dict
            || self.hdr.flags & CHUNK_FLAG_DICT_FRAMED == 0
            || !dict_lazy_read()
        {
            return false;
        }
        if !dict.is_empty() {
            debug_assert!(lazy.is_some());
            return true;
        }
        let ndv = self.hdr.aux as usize;
        let w = self.hdr.width as usize;
        let p = self.payload();
        let codes_len = align4(self.nrows as usize * w);
        let off_tab = &p[codes_len..codes_len + ndv * 4];
        let blob = &p[codes_len + ndv * 4..];
        let (raw_off, comp_off, data) = parse_framed_dict(blob);
        let nframes = raw_off.len() - 1;
        let total = *raw_off.last().unwrap() as usize;
        let words = (total + crate::lz4dec::OUT_PAD).div_ceil(8);
        let mut buf: Vec<u64> = Vec::with_capacity(words);
        let buf_ptr = buf.as_mut_ptr().cast::<u8>();
        let dl = Box::new(DictLazy {
            buf,
            buf_ptr,
            nframes: nframes as u32,
            ndict: ndv as u32,
            raw_off,
            comp_off,
            src: blob[data..].as_ptr(),
            codec: self.hdr.frame_codec(),
            entry_off: off_tab.as_ptr(),
            done: (0..nframes.div_ceil(64)).map(|_| Cell::new(0)).collect(),
            ndone: Cell::new(0),
        });
        DICT_LAZY_BUILDS.fetch_add(1, Relaxed);
        DICT_FRAMES_TOTAL.fetch_add(nframes as u64, Relaxed);
        let base = dl.base();
        dict.reserve(ndv);
        for c in off_tab.chunks_exact(4) {
            let o = u32::from_le_bytes(c.try_into().unwrap()) as usize;
            dict.push(Datum::from_usize(base + o));
        }
        *lazy = Some(dl);
        true
    }

    /// Dict-lane decode of granule g: the granule's codes widened to u32
    /// into `codes` (no per-row dictionary gather) + the RG dict table.
    /// false = the chunk is not dict-encoded (caller decodes Datums).
    /// Framed chunks build the dict LAZILY (`lazy` set; entry bytes
    /// materialize per ensured frame); everything else builds eagerly and
    /// clears `lazy`.
    pub fn decode_granule_codes(
        &self,
        g: usize,
        codes: &mut Vec<u32>,
        dict: &mut Vec<Datum>,
        arena: &mut Vec<u64>,
        lazy: &mut Option<Box<DictLazy>>,
    ) -> bool {
        if !matches!(self.hdr.encoding, Encoding::Dict | Encoding::Lz4Dict) {
            return false;
        }
        if !self.build_dict_lazy(dict, lazy) {
            *lazy = None;
            self.build_dict(dict, arena);
        }
        let n = self.granule_rows(g);
        let lo = g * GRANULE_ROWS;
        let p = self.payload();
        codes.clear();
        codes.reserve(n);
        // Straight-line widen into spare capacity: `push` per element carries
        // a capacity check the autovectorizer can't prove dead across the
        // loop (asm-diff: per-byte push defeats vectorization).
        let dst = &mut codes.spare_capacity_mut()[..n];
        match self.hdr.width as usize {
            1 => {
                for (d, &c) in dst.iter_mut().zip(&p[lo..lo + n]) {
                    d.write(c as u32);
                }
            }
            2 => {
                for (d, c) in dst.iter_mut().zip(p[lo * 2..(lo + n) * 2].chunks_exact(2)) {
                    d.write(u16::from_le_bytes(c.try_into().unwrap()) as u32);
                }
            }
            _ => {
                for (d, c) in dst.iter_mut().zip(p[lo * 4..(lo + n) * 4].chunks_exact(4)) {
                    d.write(u32::from_le_bytes(c.try_into().unwrap()));
                }
            }
        }
        // SAFETY: the zipped loops above wrote exactly the first `n` slots of
        // the spare capacity reserved above.
        unsafe { codes.set_len(n) };
        true
    }

    // Decode granule g into `out` (reused, resized to the granule's rows).
    // Int columns produce sign-extended Datum words; text columns produce
    // pointers to in-file 4B-U varlena images — except Lz4Text, whose
    // pointers land in `arena` (u64-backed for varlena alignment; reused per
    // granule, so published Datums live exactly as long as `out`'s).
    pub fn decode_granule(
        &self,
        g: usize,
        out: &mut Vec<Datum>,
        dict: &mut Vec<Datum>,
        arena: &mut Vec<u64>,
    ) {
        let n = self.granule_rows(g);
        out.clear();
        out.reserve(n);
        let lo = g * GRANULE_ROWS;
        let p = self.payload();
        match self.hdr.encoding {
            Encoding::Const => {
                out.resize(n, Datum::from_i64(self.hdr.aux));
            }
            Encoding::For => {
                let base = self.hdr.aux;
                // v6: framed chunks decompress the granule into `arena` and
                // widen from there; unframed slices the file directly.
                let src = self.fixed_granule_bytes(g, n, arena);
                // Straight-line widen+add into spare capacity (asm-diff:
                // per-element `push` carries a capacity check the
                // autovectorizer can't hoist out of the loop).
                let dst = &mut out.spare_capacity_mut()[..n];
                match self.hdr.width {
                    1 => {
                        for (d, &b) in dst.iter_mut().zip(src) {
                            d.write(Datum::from_i64(base + b as i64));
                        }
                    }
                    2 => {
                        for (d, c) in dst.iter_mut().zip(src.chunks_exact(2)) {
                            d.write(Datum::from_i64(
                                base + u16::from_le_bytes(c.try_into().unwrap()) as i64,
                            ));
                        }
                    }
                    4 => {
                        for (d, c) in dst.iter_mut().zip(src.chunks_exact(4)) {
                            d.write(Datum::from_i64(
                                base + u32::from_le_bytes(c.try_into().unwrap()) as i64,
                            ));
                        }
                    }
                    w => panic!("cbstore: FOR width {w}"),
                }
                // SAFETY: every slot of dst (the first `n` spare slots) was
                // written by exactly one arm above.
                unsafe { out.set_len(out.len() + n) };
            }
            Encoding::Raw => {
                debug_assert_eq!(self.hdr.width, 8);
                let src = self.fixed_granule_bytes(g, n, arena);
                let dst = &mut out.spare_capacity_mut()[..n];
                for (d, c) in dst.iter_mut().zip(src.chunks_exact(8)) {
                    d.write(Datum::from_i64(i64::from_le_bytes(c.try_into().unwrap())));
                }
                // SAFETY: dst (the first `n` spare slots) was fully written above.
                unsafe { out.set_len(out.len() + n) };
            }
            Encoding::DeltaFor => {
                // Always framed (format.rs §DeltaFor). comp_len == raw_len
                // marks an in-place uncompressed frame — the writer's
                // expansion guard never emits a compressed frame at exactly
                // raw_len, so the test is unambiguous.
                let fo = self.granule(g).payload_off as usize;
                let raw_len = get_u32(p, fo) as usize;
                let comp_len = get_u32(p, fo + 4) as usize;
                let img: &[u8] = if comp_len == raw_len {
                    &p[fo + 8..fo + 8 + raw_len]
                } else {
                    let dst = arena_frame(arena, raw_len);
                    decompress_frame_into(
                        self.hdr.frame_codec(),
                        &p[fo + 8..fo + 8 + comp_len],
                        dst,
                        raw_len,
                    );
                    &dst[..raw_len]
                };
                let first = i64::from_le_bytes(img[0..8].try_into().unwrap());
                let dmin = i64::from_le_bytes(img[8..16].try_into().unwrap());
                let w = img[16] as usize;
                debug_assert_eq!(raw_len, 24 + (n - 1) * w);
                let packed = &img[24..];
                // Prefix sum in the wrapping domain: v[i] = v[i-1] +w
                // (dmin +w packed[i-1]); packed entries zero-extend.
                let dst = &mut out.spare_capacity_mut()[..n];
                dst[0].write(Datum::from_i64(first));
                let mut acc = first;
                match w {
                    0 => {
                        for d in dst[1..].iter_mut() {
                            acc = acc.wrapping_add(dmin);
                            d.write(Datum::from_i64(acc));
                        }
                    }
                    1 => {
                        for (d, &b) in dst[1..].iter_mut().zip(&packed[..n - 1]) {
                            acc = acc.wrapping_add(dmin.wrapping_add(b as i64));
                            d.write(Datum::from_i64(acc));
                        }
                    }
                    2 => {
                        for (d, c) in
                            dst[1..].iter_mut().zip(packed[..(n - 1) * 2].chunks_exact(2))
                        {
                            let b = u16::from_le_bytes(c.try_into().unwrap()) as i64;
                            acc = acc.wrapping_add(dmin.wrapping_add(b));
                            d.write(Datum::from_i64(acc));
                        }
                    }
                    4 => {
                        for (d, c) in
                            dst[1..].iter_mut().zip(packed[..(n - 1) * 4].chunks_exact(4))
                        {
                            let b = u32::from_le_bytes(c.try_into().unwrap()) as i64;
                            acc = acc.wrapping_add(dmin.wrapping_add(b));
                            d.write(Datum::from_i64(acc));
                        }
                    }
                    _ => {
                        for (d, c) in
                            dst[1..].iter_mut().zip(packed[..(n - 1) * 8].chunks_exact(8))
                        {
                            let b = i64::from_le_bytes(c.try_into().unwrap());
                            acc = acc.wrapping_add(dmin.wrapping_add(b));
                            d.write(Datum::from_i64(acc));
                        }
                    }
                }
                // SAFETY: dst[0] plus the n-1 zipped slots above cover all n.
                unsafe { out.set_len(out.len() + n) };
            }
            Encoding::Dict | Encoding::Lz4Dict => {
                self.build_dict(dict, arena);
                let w = self.hdr.width as usize;
                // The dictionary gather (dict[code]) is a data-dependent
                // random access — no autovectorizer or NEON gather beats a
                // scalar loop here. Still drop the per-element `push` check
                // on the write side.
                let dst = &mut out.spare_capacity_mut()[..n];
                match w {
                    1 => {
                        for (d, &c) in dst.iter_mut().zip(&p[lo..lo + n]) {
                            d.write(dict[c as usize]);
                        }
                    }
                    2 => {
                        for (d, c) in dst.iter_mut().zip(p[lo * 2..(lo + n) * 2].chunks_exact(2)) {
                            d.write(dict[u16::from_le_bytes(c.try_into().unwrap()) as usize]);
                        }
                    }
                    _ => {
                        for (d, c) in dst.iter_mut().zip(p[lo * 4..(lo + n) * 4].chunks_exact(4)) {
                            d.write(dict[u32::from_le_bytes(c.try_into().unwrap()) as usize]);
                        }
                    }
                }
                // SAFETY: dst (the first `n` spare slots) was fully written above.
                unsafe { out.set_len(out.len() + n) };
            }
            Encoding::RawText => {
                let offs_len = self.nrows as usize * 4;
                // One bounds check per granule; per-row offsets add to the
                // blob base unchecked, same file-trust posture as Lz4Text.
                let base = self.part[self.payload_off + offs_len..].as_ptr() as usize;
                let dst = &mut out.spare_capacity_mut()[..n];
                for (d, c) in dst.iter_mut().zip(p[lo * 4..(lo + n) * 4].chunks_exact(4)) {
                    let o = u32::from_le_bytes(c.try_into().unwrap()) as usize;
                    d.write(Datum::from_usize(base + o));
                }
                // SAFETY: dst (the first `n` spare slots) was fully written above.
                unsafe { out.set_len(out.len() + n) };
            }
            Encoding::Lz4Text => {
                let fo = self.granule(g).payload_off as usize;
                let raw_len = get_u32(p, fo) as usize;
                let comp_len = get_u32(p, fo + 4) as usize;
                let dst = arena_frame(arena, raw_len);
                decompress_frame_into(self.hdr.frame_codec(), &p[fo + 8..fo + 8 + comp_len], dst, raw_len);
                let base = dst.as_ptr() as usize;
                let dst = &mut out.spare_capacity_mut()[..n];
                for (d, c) in dst.iter_mut().zip(p[lo * 4..(lo + n) * 4].chunks_exact(4)) {
                    let o = u32::from_le_bytes(c.try_into().unwrap()) as usize;
                    d.write(Datum::from_usize(base + o));
                }
                // SAFETY: dst (the first `n` spare slots) was fully written above.
                unsafe { out.set_len(out.len() + n) };
            }
        }
    }
}

#[cfg(test)]
pub(crate) fn test_part(name: &str, rg_rows: &[u32], ncols: usize) -> String {
    let path = std::env::temp_dir().join(name);
    let path = path.to_str().unwrap().to_string();
    tests::write_part_v(&path, CB_HEADER_LEN, rg_rows, ncols, CB_VERSION, &vec![1; ncols], &[]);
    path
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn write_part_v(
        path: &str,
        footer_off: u64,
        rg_rows: &[u32],
        ncols: usize,
        version: u32,
        ndv: &[u64],
        sorted: &[u8],
    ) {
        let mut f = Vec::new();
        put_u32(&mut f, rg_rows.len() as u32);
        put_u32(&mut f, ncols as u32);
        if version >= CB_VERSION_V7 {
            // No length-stats columns: all-zero flags, empty stats section.
            f.extend_from_slice(&vec![0u8; ncols]);
        }
        for &n in rg_rows {
            put_u64(&mut f, 0);
            put_u32(&mut f, n);
            put_u32(&mut f, 1);
            put_u32(&mut f, 0);
            put_u32(&mut f, 0);
        }
        for _ in rg_rows {
            for _ in 0..ncols {
                put_u64(&mut f, 0);
                put_i64(&mut f, 0);
                put_i64(&mut f, 0);
            }
        }
        if version >= CB_VERSION_V2 {
            for c in 0..ncols {
                put_u64(&mut f, ndv.get(c).copied().unwrap_or(0));
            }
        }
        if version >= CB_VERSION_V4 {
            for _ in rg_rows {
                for _ in 0..ncols {
                    put_i128(&mut f, 0);
                }
            }
        }
        if version >= CB_VERSION_V5 {
            for c in 0..ncols {
                f.push(sorted.get(c).copied().unwrap_or(0));
            }
        }
        if version >= CB_VERSION_V6 {
            f.extend_from_slice(&[0u8; CB_CLUSTER_KEY_SECTION_LEN]);
        }
        if version >= CB_VERSION_V7 {
            // Union v7 tail: empty length-stats (all-zero flags above),
            // all-zero stitch section, all-zero zero-count section.
            f.resize(f.len() + ncols * 8 + rg_rows.len() * ncols * CB_STITCH_DIR_ENTRY_LEN, 0);
            for _ in rg_rows {
                for _ in 0..GRANULES_PER_RG * ncols {
                    put_u32(&mut f, 0);
                }
            }
        }
        if version >= CB_VERSION_V8 {
            // All-zero NDV-registers directory: no sketches.
            f.resize(f.len() + ncols * CB_NDVREGS_DIR_ENTRY_LEN, 0);
        }
        let crc = crc32c(&f);
        let flen = (f.len() + 16) as u64;
        put_u64(&mut f, flen);
        put_u32(&mut f, crc);
        put_u32(&mut f, CB_FOOTER_MAGIC);

        let mut hdr = Vec::new();
        put_u64(&mut hdr, CB_MAGIC);
        put_u32(&mut hdr, version);
        put_u32(&mut hdr, ncols as u32);
        put_u64(&mut hdr, footer_off);
        put_u64(&mut hdr, 0);
        hdr.resize(CB_HEADER_LEN as usize, 0);

        let mut bytes = hdr;
        bytes.resize(footer_off.max(CB_HEADER_LEN) as usize, 0);
        if footer_off != 0 {
            bytes.extend_from_slice(&f);
        }
        std::fs::write(path, bytes).unwrap();
    }

    fn write_part(path: &str, footer_off: u64, rg_rows: &[u32], ncols: usize) {
        write_part_v(path, footer_off, rg_rows, ncols, CB_VERSION_V1, &[], &[]);
    }

    fn tmp(name: &str) -> String {
        let p = std::env::temp_dir().join(format!("cbstore-footer-{}-{}", std::process::id(), name));
        p.to_str().unwrap().to_string()
    }

    // The sectioned lazy read (want_sums=false, the reader default) must
    // materialize exactly the sections the eager whole-body read does, at
    // every on-disk version.
    #[test]
    fn footer_lazy_sectioned_read_matches_eager() {
        for version in
            [CB_VERSION_V1, CB_VERSION_V2, CB_VERSION_V4, CB_VERSION_V5, CB_VERSION_V6, CB_VERSION]
        {
            let path = tmp(&format!("lazy-parity-v{version}"));
            let ndv = [7, 0, 12_345_678];
            let sorted = [1u8, 0, 1];
            write_part_v(&path, CB_HEADER_LEN, &[100, 200, 50], 3, version, &ndv, &sorted);
            let mut file = SegFile::open_rw(&path).unwrap();
            let (l_rgs, l_end, l_ndv, l_sorted, l_ckey, l_lenflags, l_stitch, l_ndvregs) =
                read_footer_rgs(&mut file, CB_HEADER_LEN, 3, version, false).unwrap();
            let (e_rgs, e_end, e_ndv, e_sorted, e_ckey, e_lenflags, e_stitch, e_ndvregs) =
                read_footer_rgs(&mut file, CB_HEADER_LEN, 3, version, true).unwrap();
            assert_eq!(l_end, e_end);
            assert_eq!(l_ndv, e_ndv);
            assert_eq!(l_sorted, e_sorted);
            assert_eq!(l_ckey, e_ckey);
            assert_eq!(l_lenflags, e_lenflags);
            assert_eq!(l_stitch.gndv, e_stitch.gndv);
            assert_eq!(l_ndvregs, e_ndvregs);
            assert_eq!(l_stitch.dir, e_stitch.dir);
            assert_eq!(l_rgs.len(), e_rgs.len());
            for (l, e) in l_rgs.iter().zip(&e_rgs) {
                assert_eq!(
                    (l.file_off, l.nrows, l.xmin, l.flags, &l.chunks),
                    (e.file_off, e.nrows, e.xmin, e.flags, &e.chunks)
                );
                // The lazy sections stay on disk (mmap consumers).
                assert!(l.sums.is_empty() && l.lenstats.is_empty() && l.zerocnt.is_empty());
            }
            std::fs::remove_file(&path).unwrap();
        }
    }

    // The lazy path skips the whole-body CRC but must still reject a torn
    // tail (magic or body-length word).
    #[test]
    fn footer_lazy_rejects_torn_tail() {
        let path = tmp("lazy-torn-tail");
        write_part_v(&path, CB_HEADER_LEN, &[100], 2, CB_VERSION, &[5, 9], &[1, 0]);
        let mut bytes = std::fs::read(&path).unwrap();
        let n = bytes.len();
        bytes[n - 1] ^= 0xff; // clobber the CBFT magic
        std::fs::write(&path, &bytes).unwrap();
        assert!(part_footer_rows(&path, 2).is_err());
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn footer_rows_sums_row_groups() {
        let path = tmp("sum");
        write_part(&path, CB_HEADER_LEN, &[1_048_576, 951_421, 3], 4);
        assert_eq!(part_footer_rows(&path, 4).unwrap(), Some(2_000_000));
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn footer_rows_none_before_publish() {
        let path = tmp("nofooter");
        write_part(&path, 0, &[], 4);
        assert_eq!(part_footer_rows(&path, 4).unwrap(), None);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn footer_rows_none_on_short_file() {
        let path = tmp("short");
        std::fs::write(&path, [0u8; 8]).unwrap();
        assert_eq!(part_footer_rows(&path, 4).unwrap(), None);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn footer_ndv_v2_roundtrip() {
        let path = tmp("ndv2");
        write_part_v(&path, CB_HEADER_LEN, &[100, 200], 3, CB_VERSION_V2, &[7, 0, 12_345_678], &[]);
        assert_eq!(part_footer_rows(&path, 3).unwrap(), Some(300));
        assert_eq!(part_footer_ndv(&path, 3).unwrap(), Some(vec![7, 0, 12_345_678]));
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn footer_current_reads_and_future_rejected() {
        let path = tmp("v3v4");
        write_part_v(&path, CB_HEADER_LEN, &[100], 2, CB_VERSION, &[5, 9], &[1, 0]);
        assert_eq!(part_footer_rows(&path, 2).unwrap(), Some(100));
        assert_eq!(part_footer_ndv(&path, 2).unwrap(), Some(vec![5, 9]));
        write_part_v(&path, CB_HEADER_LEN, &[100], 2, CB_VERSION + 1, &[5, 9], &[1, 0]);
        assert!(part_footer_rows(&path, 2).is_err());
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn footer_sums_v4_roundtrip() {
        let path = tmp("sums4");
        let mut f = Vec::new();
        put_u32(&mut f, 2);
        put_u32(&mut f, 2);
        for (n, flags) in [(100u32, RG_FLAG_SUMS), (50, 0)] {
            put_u64(&mut f, 0);
            put_u32(&mut f, n);
            put_u32(&mut f, 1);
            put_u32(&mut f, flags);
            put_u32(&mut f, 0);
        }
        for _ in 0..2 * 2 {
            put_u64(&mut f, 0);
            put_i64(&mut f, 0);
            put_i64(&mut f, 0);
        }
        for _ in 0..2 {
            put_u64(&mut f, 1);
        }
        let sums = [[-(1i128 << 70), 42], [0, 0]];
        for rg in &sums {
            for &s in rg {
                put_i128(&mut f, s);
            }
        }
        let crc = crc32c(&f);
        let flen = (f.len() + 16) as u64;
        put_u64(&mut f, flen);
        put_u32(&mut f, crc);
        put_u32(&mut f, CB_FOOTER_MAGIC);
        let mut bytes = vec![0u8; CB_HEADER_LEN as usize];
        bytes.extend_from_slice(&f);
        std::fs::write(&path, &bytes).unwrap();

        let mut file = SegFile::open_rw(&path).unwrap();
        let (rgs, ..) =
            read_footer_rgs(&mut file, CB_HEADER_LEN, 2, CB_VERSION_V4, true).unwrap();
        assert_eq!(rgs[0].flags & RG_FLAG_SUMS, RG_FLAG_SUMS);
        assert_eq!(rgs[0].sums, vec![-(1i128 << 70), 42]);
        assert_eq!(rgs[1].flags & RG_FLAG_SUMS, 0);
        assert_eq!(rgs[1].sums, vec![0, 0]);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn part_open_sums_stay_on_disk_and_rg_sum_reads_exact() {
        let path = tmp("lazysums");
        let mut f = Vec::new();
        put_u32(&mut f, 2);
        put_u32(&mut f, 2);
        // v7 prelude flags (header below stamps CB_VERSION): no length-stats
        // columns.
        f.extend_from_slice(&[0u8, 0u8]);
        for n in [100u32, 50] {
            put_u64(&mut f, 0);
            put_u32(&mut f, n);
            put_u32(&mut f, 1);
            put_u32(&mut f, RG_FLAG_FROZEN | RG_FLAG_SUMS);
            put_u32(&mut f, 0);
        }
        for _ in 0..2 * 2 {
            put_u64(&mut f, 0);
            put_i64(&mut f, 0);
            put_i64(&mut f, 0);
        }
        for _ in 0..2 {
            put_u64(&mut f, 1);
        }
        let sums = [[-(1i128 << 70), 42], [7, -9]];
        for rg in &sums {
            for &s in rg {
                put_i128(&mut f, s);
            }
        }
        // v5 sorted flags + v6 cluster-key + v7 zero-count sections (header
        // stamps CB_VERSION). The RGs carry no RG_FLAG_ZEROCNT — a
        // v<=6-preserved shape — so the zero entries are never consulted.
        f.extend_from_slice(&[0u8, 0u8]);
        f.extend_from_slice(&[0u8; CB_CLUSTER_KEY_SECTION_LEN]);
        // v7 sections: length stats are empty (no flagged columns above);
        // stitch = 2 x u64 gndv + 2 RGs x 2 cols x 12 B directory, then the
        // zero-count body (2 RGs x GRANULES_PER_RG x 2 cols u32) — all zero.
        f.resize(f.len() + 2 * 8 + 2 * 2 * CB_STITCH_DIR_ENTRY_LEN, 0);
        for _ in 0..2 * GRANULES_PER_RG * 2 {
            put_u32(&mut f, 0);
        }
        // v8 NDV-registers directory: all-zero (no sketches).
        f.resize(f.len() + 2 * CB_NDVREGS_DIR_ENTRY_LEN, 0);
        let crc = crc32c(&f);
        let flen = (f.len() + 16) as u64;
        put_u64(&mut f, flen);
        put_u32(&mut f, crc);
        put_u32(&mut f, CB_FOOTER_MAGIC);
        let mut hdr = Vec::new();
        put_u64(&mut hdr, CB_MAGIC);
        put_u32(&mut hdr, CB_VERSION);
        put_u32(&mut hdr, 2);
        put_u64(&mut hdr, CB_HEADER_LEN);
        put_u64(&mut hdr, 0);
        hdr.resize(CB_HEADER_LEN as usize, 0);
        let mut bytes = hdr;
        bytes.extend_from_slice(&f);
        std::fs::write(&path, &bytes).unwrap();

        let part = Part::open(&path, 2).unwrap().unwrap();
        for rg in 0..2 {
            assert!(part.rgs[rg].sums.is_empty());
            for col in 0..2 {
                assert_eq!(part.rg_sum(rg, col), sums[rg][col]);
            }
        }
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn footer_v3_reads_sums_unknown() {
        let path = tmp("sums3");
        write_part_v(&path, CB_HEADER_LEN, &[100, 200], 3, CB_VERSION_V3, &[1, 1, 1], &[]);
        let mut file = SegFile::open_rw(&path).unwrap();
        let (rgs, _, _, sorted, _, _, _, _) =
            read_footer_rgs(&mut file, CB_HEADER_LEN, 3, CB_VERSION_V3, true).unwrap();
        assert_eq!(sorted, vec![0, 0, 0]);
        for rg in &rgs {
            assert_eq!(rg.flags & RG_FLAG_SUMS, 0);
            assert!(rg.sums.is_empty());
        }
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn footer_v6_reads_lenstats_unknown() {
        // Feature detection: pre-v7 footers carry no length-stats flags or
        // entries; readers see all-unknown and fall back to the row path.
        let path = tmp("lens6");
        write_part_v(&path, CB_HEADER_LEN, &[100, 200], 3, CB_VERSION_V6, &[1, 1, 1], &[]);
        let mut file = SegFile::open_rw(&path).unwrap();
        let (rgs, _, _, _, _, lenflags, _, _) =
            read_footer_rgs(&mut file, CB_HEADER_LEN, 3, CB_VERSION_V6, true).unwrap();
        assert_eq!(lenflags, vec![0, 0, 0]);
        for rg in &rgs {
            assert_eq!(rg.flags & RG_FLAG_LENSTATS, 0);
            assert!(rg.lenstats.is_empty());
        }
        std::fs::remove_file(&path).unwrap();
    }

    // ---- v3 chunk sections: block zone maps + blooms ----

    fn int_chunk(vals: &[i64]) -> (Vec<u8>, u32) {
        let ngranules = vals.len().div_ceil(GRANULE_ROWS) as u32;
        let mut body = Vec::new();
        crate::writer::encode_int_chunk(&mut body, vals, ngranules, &crate::writer::test_codec_ctx());
        (body, vals.len() as u32)
    }

    #[test]
    fn block_zone_maps_roundtrip_and_boundaries() {
        // 1.5 granules of ascending values: every 1024-row block has an
        // exact tight range, last granule's unused block slots are empty.
        let n = GRANULE_ROWS + GRANULE_ROWS / 2;
        let vals: Vec<i64> = (0..n as i64).collect();
        let (body, nrows) = int_chunk(&vals);
        let cv = ChunkView::at(&body, 0, nrows);
        assert!(cv.has_block_zm());
        for g in 0..2 {
            for b in 0..BLOCKS_PER_GRANULE {
                let lo = (g * GRANULE_ROWS + b * BLOCK_ROWS) as i64;
                let (bmin, bmax) = cv.block_minmax(g, b);
                if lo >= n as i64 {
                    assert_eq!((bmin, bmax), (i64::MAX, i64::MIN));
                } else {
                    assert_eq!(bmin, lo);
                    assert_eq!(bmax, (lo + BLOCK_ROWS as i64 - 1).min(n as i64 - 1));
                }
            }
        }
        // Decode is unchanged by the new sections.
        let (mut out, mut dict, mut arena) = (Vec::new(), Vec::new(), Vec::new());
        cv.decode_granule(0, &mut out, &mut dict, &mut arena);
        assert_eq!(out.len(), GRANULE_ROWS);
        assert_eq!(out[0].as_i64(), 0);
        assert_eq!(out[GRANULE_ROWS - 1].as_i64(), GRANULE_ROWS as i64 - 1);
        cv.decode_granule(1, &mut out, &mut dict, &mut arena);
        assert_eq!(out.len(), GRANULE_ROWS / 2);
        assert_eq!(out[0].as_i64(), GRANULE_ROWS as i64);
    }

    #[test]
    fn const_chunk_has_no_sections() {
        let vals = vec![42i64; GRANULE_ROWS];
        let (body, nrows) = int_chunk(&vals);
        let cv = ChunkView::at(&body, 0, nrows);
        assert!(!cv.has_block_zm());
        assert!(!cv.has_bloom());
    }

    #[test]
    fn bloom_arming_policy() {
        // Sorted (clustered) high-NDV: granule zone maps are tight => no
        // bloom. Unclustered high-NDV: armed. Unclustered low-NDV: not.
        let sorted: Vec<i64> = (0..2 * GRANULE_ROWS as i64).collect();
        let cv_body = int_chunk(&sorted);
        assert!(!ChunkView::at(&cv_body.0, 0, cv_body.1).has_bloom());

        let unclustered: Vec<i64> = (0..2 * GRANULE_ROWS as u64)
            .map(|i| crate::hll::mix64(i) as i64)
            .collect();
        let (body, nrows) = int_chunk(&unclustered);
        let cv = ChunkView::at(&body, 0, nrows);
        assert!(cv.has_bloom());
        for (i, &v) in unclustered.iter().enumerate() {
            assert!(cv.bloom_may_contain(i / GRANULE_ROWS, v));
        }

        let lowndv: Vec<i64> =
            (0..2 * GRANULE_ROWS as u64).map(|i| (crate::hll::mix64(i) % 100) as i64).collect();
        let cv_body = int_chunk(&lowndv);
        assert!(!ChunkView::at(&cv_body.0, 0, cv_body.1).has_bloom());
    }

    #[test]
    fn bloom_prunes_absent_admits_present() {
        let vals: Vec<i64> = (0..GRANULE_ROWS as u64).map(|i| crate::hll::mix64(i) as i64).collect();
        let (body, nrows) = int_chunk(&vals);
        let cv = ChunkView::at(&body, 0, nrows);
        assert!(cv.has_bloom());
        // Values in-range for the zone map but absent: the bloom prunes the
        // overwhelming majority (fp ~5.7e-4).
        let mut pruned = 0;
        for i in 0..10_000u64 {
            if !cv.bloom_may_contain(0, crate::hll::mix64(u64::MAX - i) as i64) {
                pruned += 1;
            }
        }
        assert!(pruned >= 9_900, "pruned {pruned}/10000");
    }

    #[test]
    fn footer_ndv_none_on_v1() {
        let path = tmp("ndv1");
        write_part(&path, CB_HEADER_LEN, &[100], 2);
        assert_eq!(part_footer_rows(&path, 2).unwrap(), Some(100));
        assert_eq!(part_footer_ndv(&path, 2).unwrap(), None);
        std::fs::remove_file(&path).unwrap();
    }

    // ---- decode-kernel differential tests: chunk-edge, partial-final-granule
    // coverage for the FOR/Raw/dict-codes widen loops touched by simd-decode.
    // Expected values are computed independently of decode_granule's arms.

    fn view<'a>(part: &'a [u8], hdr: ChunkHeader, nrows: u32) -> ChunkView<'a> {
        ChunkView { part, hdr, gdir_off: 0, blockzm_off: 0, bloom_off: 0, payload_off: 0, nrows }
    }

    fn for_payload(nrows: usize, width: u8) -> (Vec<u8>, Vec<i64>) {
        let mut bytes = Vec::new();
        let mut expected = Vec::with_capacity(nrows);
        for i in 0..nrows {
            let v = ((i as u64).wrapping_mul(2654435761) & 0xFFFF_FFFF) as u64;
            match width {
                1 => {
                    let b = (v & 0xFF) as u8;
                    bytes.push(b);
                    expected.push(b as i64);
                }
                2 => {
                    let b = (v & 0xFFFF) as u16;
                    bytes.extend_from_slice(&b.to_le_bytes());
                    expected.push(b as i64);
                }
                4 => {
                    let b = (v & 0xFFFF_FFFF) as u32;
                    bytes.extend_from_slice(&b.to_le_bytes());
                    expected.push(b as i64);
                }
                w => panic!("bad width {w}"),
            }
        }
        (bytes, expected)
    }

    // Drives decode_granule across every granule of a chunk with `nrows`
    // rows (deliberately not a multiple of GRANULE_ROWS to cover the
    // partial-final-granule boundary), returning the concatenated i64s.
    fn decode_all_for(nrows: usize, base: i64, width: u8) -> (Vec<i64>, Vec<i64>) {
        let (payload, deltas) = for_payload(nrows, width);
        let hdr = ChunkHeader {
            encoding: Encoding::For,
            width,
            flags: 0,
            ngranules: (nrows.div_ceil(GRANULE_ROWS)) as u32,
            aux: base,
            payload_len: payload.len() as u64,
            codec: Codec::None,
        };
        let cv = view(&payload, hdr, nrows as u32);
        let mut out = Vec::new();
        let mut dict = Vec::new();
        let mut arena = Vec::new();
        let mut got = Vec::new();
        for g in 0..cv.hdr.ngranules as usize {
            cv.decode_granule(g, &mut out, &mut dict, &mut arena);
            got.extend(out.iter().map(|d| d.as_i64()));
        }
        let expected: Vec<i64> = deltas.iter().map(|d| base + d).collect();
        (got, expected)
    }

    #[test]
    fn for_widen_exact_granule_boundary() {
        for &width in &[1u8, 2, 4] {
            let (got, expected) = decode_all_for(GRANULE_ROWS * 2, 1_000, width);
            assert_eq!(got, expected, "FOR width {width} exact boundary");
        }
    }

    #[test]
    fn for_widen_partial_final_granule() {
        for &width in &[1u8, 2, 4] {
            let (got, expected) = decode_all_for(GRANULE_ROWS * 2 + 37, -500, width);
            assert_eq!(got, expected, "FOR width {width} partial final granule");
        }
    }

    #[test]
    fn for_widen_single_partial_granule() {
        for &width in &[1u8, 2, 4] {
            let (got, expected) = decode_all_for(1, 0, width);
            assert_eq!(got, expected, "FOR width {width} single row");
        }
    }

    #[test]
    fn raw_widen_matches_le_i64() {
        let nrows = GRANULE_ROWS + 5;
        let mut payload = Vec::new();
        let mut expected = Vec::with_capacity(nrows);
        for i in 0..nrows {
            let v = (i as i64) * 3 - 7;
            payload.extend_from_slice(&v.to_le_bytes());
            expected.push(v);
        }
        let hdr = ChunkHeader {
            encoding: Encoding::Raw,
            width: 8,
            flags: 0,
            ngranules: nrows.div_ceil(GRANULE_ROWS) as u32,
            aux: 0,
            payload_len: payload.len() as u64,
            codec: Codec::None,
        };
        let cv = view(&payload, hdr, nrows as u32);
        let mut out = Vec::new();
        let mut dict = Vec::new();
        let mut arena = Vec::new();
        let mut got = Vec::new();
        for g in 0..cv.hdr.ngranules as usize {
            cv.decode_granule(g, &mut out, &mut dict, &mut arena);
            got.extend(out.iter().map(|d| d.as_i64()));
        }
        assert_eq!(got, expected);
    }

    #[test]
    fn dict_codes_widen_matches_raw_codes() {
        for &width in &[1u8, 2, 4] {
            let nrows = GRANULE_ROWS * 2 + 11;
            let ndv = 5usize;
            let mut payload = Vec::new();
            let mut expected = Vec::with_capacity(nrows);
            for i in 0..nrows {
                let code = (i % ndv) as u32;
                match width {
                    1 => payload.push(code as u8),
                    2 => payload.extend_from_slice(&(code as u16).to_le_bytes()),
                    4 => payload.extend_from_slice(&code.to_le_bytes()),
                    w => panic!("bad width {w}"),
                }
                expected.push(code);
            }
            let codes_len = align4(nrows * width as usize);
            payload.resize(codes_len, 0);
            // Minimal valid off_tab + blob so build_dict doesn't fault; the
            // codes-only path never reads dict values.
            for _ in 0..ndv {
                put_u32(&mut payload, 0);
            }
            payload.extend_from_slice(&0u64.to_le_bytes());

            let hdr = ChunkHeader {
                encoding: Encoding::Dict,
                width,
                flags: 0,
                ngranules: nrows.div_ceil(GRANULE_ROWS) as u32,
                aux: ndv as i64,
                payload_len: payload.len() as u64,
                codec: Codec::None,
            };
            let cv = view(&payload, hdr, nrows as u32);
            let mut codes = Vec::new();
            let mut dict = Vec::new();
            let mut arena = Vec::new();
            let mut got = Vec::new();
            for g in 0..cv.hdr.ngranules as usize {
                let mut lazy = None;
                assert!(cv.decode_granule_codes(g, &mut codes, &mut dict, &mut arena, &mut lazy));
                got.extend_from_slice(&codes);
            }
            assert_eq!(got, expected, "dict codes width {width}");
        }
    }

    #[test]
    fn rawtext_offsets_walk_partial_final_granule() {
        let nrows = GRANULE_ROWS + 41;
        let mut payload = Vec::new();
        let mut offs = Vec::with_capacity(nrows);
        let mut o = 0u32;
        for i in 0..nrows {
            put_u32(&mut payload, o);
            offs.push(o);
            o += 8 + (i % 7) as u32;
        }
        payload.resize(payload.len() + o as usize, 0x42);
        let blob_base = payload[nrows * 4..].as_ptr() as usize;
        let hdr = ChunkHeader {
            encoding: Encoding::RawText,
            width: 4,
            flags: 0,
            ngranules: nrows.div_ceil(GRANULE_ROWS) as u32,
            aux: 0,
            payload_len: payload.len() as u64,
            codec: Codec::None,
        };
        let cv = ChunkView { part: &payload, hdr, gdir_off: 0, blockzm_off: 0, bloom_off: 0, payload_off: 0, nrows: nrows as u32 };
        let mut out = Vec::new();
        let mut dict = Vec::new();
        let mut arena = Vec::new();
        let mut got = Vec::new();
        for g in 0..cv.hdr.ngranules as usize {
            cv.decode_granule(g, &mut out, &mut dict, &mut arena);
            got.extend(out.iter().map(|d| d.as_usize()));
        }
        let expected: Vec<usize> = offs.iter().map(|&off| blob_base + off as usize).collect();
        assert_eq!(got, expected);
    }

    // coldio lane: process-shared SegMap registry — one live mapping per
    // (dev, ino, maplen) part state, across threads; distinct keys map
    // privately; the Weak registry never keeps a dead mapping alive.
    #[test]
    fn shared_segmap_identity() {
        #[cfg(not(target_family = "wasm"))]
use std::os::unix::fs::MetadataExt;
#[cfg(target_family = "wasm")]
use std::os::wasi::fs::MetadataExt;
        let path = test_part("cb_sharedmap_test.part", &[100, 100], 2);
        let md = std::fs::metadata(&path).unwrap();
        let a = SegMap::open_shared(&path, md.dev(), md.ino()).unwrap().unwrap();
        let b = SegMap::open_shared(&path, md.dev(), md.ino()).unwrap().unwrap();
        assert!(
            std::sync::Arc::ptr_eq(&a, &b),
            "same live part state must share one mapping"
        );
        // Cross-thread: a pool worker's open lands on the same mapping.
        let (path2, dev, ino) = (path.clone(), md.dev(), md.ino());
        let from_thread = std::thread::spawn(move || {
            SegMap::open_shared(&path2, dev, ino).unwrap().unwrap()
        })
        .join()
        .unwrap();
        assert!(std::sync::Arc::ptr_eq(&a, &from_thread));
        // A different identity (recreate stand-in) maps privately.
        let c = SegMap::open_shared(&path, md.dev(), md.ino() ^ 1).unwrap().unwrap();
        assert!(!std::sync::Arc::ptr_eq(&a, &c));
        // Weak registry: with every holder dropped the mapping dies and the
        // next open takes the upgrade-miss path (fresh map, still readable).
        drop((a, b, c, from_thread));
        let d = SegMap::open_shared(&path, md.dev(), md.ino()).unwrap().unwrap();
        assert!(!d.bytes().is_empty());
    }
}
