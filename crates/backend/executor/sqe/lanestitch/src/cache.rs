// The Tier-B stitched-body cache (production-plan §3 P1-3 remainder:
// "stitched bodies cached by shape fingerprint; prepared statements never
// re-stitch"). `get_or_stitch` returns a handle whose code pages came
// from the cache when an identical shape was stitched before, and are
// classified + emitted exactly once otherwise.
//
// # Identity and the structural-verify membrane (ruling Q4 2026-08-18)
//
// Hash-as-identity is legal only behind a structural-verify membrane; the
// payload here is EXECUTABLE CODE, so this cache goes further than the
// main engine's oracle-build verify: the KEY is the canonical serialized
// shape itself — a versioned byte encoding of (kind, target features,
// ncols/nouts, steps, consts, arrays, volatile) — and the 64-bit
// fingerprint is only an INDEX over it. Every hit compares the full
// canonical bytes in every build; two shapes that collide on the
// fingerprint occupy distinct bucket entries and can never share a body.
// The forged-collision gate in tests/cache.rs pins this.
//
// # Soundness law (why a process-global cache is legal at all)
//
// A stitched body is a pure function of the canonical key. Verified
// against spec.rs/stitch.rs/emit.rs: classification and emission read
// exactly (prog.steps, prog.consts, prog.arrays, prog.volatile, ncols,
// nouts, opts.simd, the resolved SIMD tier) — consts and SAOP arrays are
// baked BY VALUE into the instruction words (no pointer into any
// program-owned or statement-owned memory survives emission), the params
// layout is a compile-time constant of this crate, and the hardware
// probes (SVE2 HWCAPs, VL) are process-constant and folded into the key
// as the resolved tier. No bank, statement, snapshot, or session
// identity reaches the emitters, so identical keys yield byte-identical
// bodies (the golden-encoding gate) and any process may share them.
//
// Statement-LOCAL state stays on the handle, not the pages: the sticky
// `refused` rail is per-StitchedProgram/StitchedProjection, so one
// statement's data-error refusal never poisons another statement running
// the same cached body.
//
// # Body lifecycle
//
// Bodies live on per-body RX mappings (jit_deform::install_code_shared:
// writable only inside install, before the mapping is shared; immutable
// RX until munmap). The cache holds one Arc per entry and every handle
// holds another: LRU eviction under [`STITCH_CACHE_CAP_BYTES`] drops only
// the cache's Arc, so a body is unmapped strictly after the last handle
// drops — never mid-execution.
//
// # Invalidation
//
// None needed, by the purity law above. `bump_generation` exists as the
// hook for future Step-IR vocabulary migrations (a live-process encoding
// change would strand stale bodies): bumping retires every current entry
// (lazily — stale entries are skipped on probe and reaped on insert).
// The canon encoding also carries CANON_VERSION so on-disk reuse, if it
// ever exists, is self-versioning.
//
// Prepared-statement dispatch wiring is a later milestone; the consumers
// today are the parity suites, which route the fuzz gauntlets through
// `get_or_stitch` so every cached-hit body is oracle-verified.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::spec::{Program, Step};
use crate::{
    available, stitch, stitch_proj_words, stitch_qual_words, Body, PipelineFn, ProjMeta,
    ProjPipelineFn, QualMeta, StitchOpts, StitchedProgram, StitchedProjection,
};

/// Byte cap on cache-retained executable pages, counted in PAGE-ROUNDED
/// mapped bytes (per-body mappings: a ~200B body still holds one 16KiB
/// page on darwin/aarch64 — body count is the real currency, ~512 bodies
/// at this cap on 16KiB pages). cost_params-style constant: chosen so a
/// steady mixed workload's working set of qual/projection shapes stays
/// resident while a pathological shape churn cannot pin unbounded RX
/// memory. In-flight handles are NOT counted: eviction drops only the
/// cache's reference, so the cap bounds what the CACHE retains, not what
/// executing statements keep alive.
pub const STITCH_CACHE_CAP_BYTES: usize = 8 * 1024 * 1024;

/// Canonical-encoding version: bump on ANY change to the serialization
/// below (step tags, field order, target-feature section).
const CANON_VERSION: u8 = 1;

const KIND_QUAL: u8 = b'Q';
const KIND_PROJ: u8 = b'P';

// ---- canonical shape serialization ---------------------------------------

fn push_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn push_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn push_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

/// The resolved target-feature byte: the ONE hardware-dependent emission
/// input. `simd_tier` folds the SVE2 HWCAP probe and the caller's pin, so
/// A/B arms (`Sve2Pin::Off`/`Force`) key separately from `Auto` bodies on
/// the same host exactly when their emissions differ.
fn tier_byte(opts: StitchOpts) -> u8 {
    match stitch::simd_tier(opts.sve2) {
        stitch::SimdTier::Neon => 0,
        stitch::SimdTier::Sve2 { force: false } => 1,
        stitch::SimdTier::Sve2 { force: true } => 2,
    }
}

fn canon_header(out: &mut Vec<u8>, kind: u8, simd: bool, tier: u8) {
    out.push(kind);
    out.push(CANON_VERSION);
    out.push(core::mem::size_of::<usize>() as u8); // pointer width
    out.push(simd as u8);
    out.push(tier);
}

fn canon_program(out: &mut Vec<u8>, prog: &Program) {
    out.push(prog.volatile as u8);
    push_u32(out, prog.steps.len() as u32);
    for s in &prog.steps {
        match *s {
            Step::LoadLane { col, out: o } => {
                out.push(1);
                push_u16(out, col);
                out.push(o);
            }
            Step::LoadConst { k, out: o } => {
                out.push(2);
                push_u16(out, k);
                out.push(o);
            }
            Step::Cmp { op, a, b, out: o } => {
                out.push(3);
                out.push(op as u8);
                out.push(a);
                out.push(b);
                out.push(o);
            }
            Step::Arith { op, a, b, out: o } => {
                out.push(4);
                out.push(op as u8);
                out.push(a);
                out.push(b);
                out.push(o);
            }
            Step::NullTest { a, out: o, kind } => {
                out.push(5);
                out.push(kind as u8);
                out.push(a);
                out.push(o);
            }
            Step::BoolTest { a, out: o, kind } => {
                out.push(6);
                out.push(kind as u8);
                out.push(a);
                out.push(o);
            }
            Step::SaopAny { a, out: o, op, arr } => {
                out.push(7);
                out.push(op as u8);
                out.push(a);
                out.push(o);
                push_u16(out, arr);
            }
            Step::Qual { a } => {
                out.push(8);
                out.push(a);
            }
            Step::StoreOut { a, out: o } => {
                out.push(9);
                out.push(a);
                push_u16(out, o);
            }
        }
    }
    push_u32(out, prog.consts.len() as u32);
    for c in &prog.consts {
        push_u64(out, c.value.as_i64() as u64);
        out.push(c.isnull as u8);
    }
    push_u32(out, prog.arrays.len() as u32);
    for arr in &prog.arrays {
        push_u32(out, arr.len() as u32);
        for e in arr {
            push_u64(out, e.value.as_i64() as u64);
            out.push(e.isnull as u8);
        }
    }
}

fn canon_qual(prog: &Program, ncols: usize, opts: StitchOpts) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + prog.steps.len() * 6);
    canon_header(&mut out, KIND_QUAL, opts.simd, tier_byte(opts));
    push_u16(&mut out, ncols as u16);
    push_u16(&mut out, 0); // nouts (qual bodies have none)
    canon_program(&mut out, prog);
    out
}

fn canon_proj(prog: &Program, ncols: usize, nouts: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + prog.steps.len() * 6);
    // Projection emission has no SIMD tier today; encode the neutral
    // header so growing one later forces a keyed split via these bytes.
    canon_header(&mut out, KIND_PROJ, false, 0);
    push_u16(&mut out, ncols as u16);
    push_u16(&mut out, nouts as u16);
    canon_program(&mut out, prog);
    out
}

fn fingerprint(canon: &[u8]) -> u64 {
    let mut h = DefaultHasher::new();
    h.write(canon);
    h.finish()
}

// ---- the cache -----------------------------------------------------------

#[derive(Clone)]
enum Meta {
    Qual(QualMeta),
    Proj(ProjMeta),
}

struct Entry {
    /// The identity: full canonical bytes, compared on every probe.
    canon: Box<[u8]>,
    code: Arc<jit_deform::SharedCode>,
    meta: Meta,
    generation: u64,
    last_use: u64,
}

#[derive(Default)]
struct Inner {
    /// Fingerprint INDEX over canonical keys: colliding shapes coexist in
    /// one bucket, disambiguated by the full canon compare.
    index: HashMap<u64, Vec<Entry>>,
    /// Cache-retained page-rounded bytes (the cap currency).
    bytes: usize,
    entries: usize,
    tick: u64,
    hits: u64,
    misses: u64,
    evictions: u64,
}

/// Point-in-time counters (tests and observability).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub entries: usize,
    pub bytes: usize,
}

/// The stitched-body fragment cache. Process-global by soundness (see the
/// module header); `new` exists for capped/isolated instances in tests.
pub struct StitchCache {
    cap_bytes: usize,
    generation: AtomicU64,
    inner: Mutex<Inner>,
}

/// The process-global cache every production `get_or_stitch` shares.
pub fn stitch_cache() -> &'static StitchCache {
    static GLOBAL: OnceLock<StitchCache> = OnceLock::new();
    GLOBAL.get_or_init(|| StitchCache::new(STITCH_CACHE_CAP_BYTES))
}

impl StitchCache {
    pub fn new(cap_bytes: usize) -> StitchCache {
        StitchCache {
            cap_bytes,
            generation: AtomicU64::new(0),
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Get-or-stitch one qual body under default options.
    pub fn get_or_stitch(&self, prog: &Program, ncols: usize) -> Option<StitchedProgram> {
        self.get_or_stitch_with(prog, ncols, StitchOpts::default())
    }

    /// Get-or-stitch one qual body. A hit re-runs NO classification or
    /// emission (the "prepared statements never re-stitch" law): the
    /// handle wraps the cached pages with fresh per-handle rails. None =
    /// the shape refuses to compile (typed refusal at the caller) or the
    /// arch/install refuses — refusals are NOT cached (they cost no
    /// pages, and a refusal is re-derived cheaply by classification).
    pub fn get_or_stitch_with(
        &self,
        prog: &Program,
        ncols: usize,
        opts: StitchOpts,
    ) -> Option<StitchedProgram> {
        if !available() {
            return None;
        }
        let canon = canon_qual(prog, ncols, opts);
        let fp = fingerprint(&canon);
        self.qual_at(fp, canon, prog, ncols, opts)
    }

    /// Get-or-stitch one projection body (same laws as qual).
    pub fn get_or_stitch_project(
        &self,
        prog: &Program,
        ncols: usize,
        nouts: usize,
    ) -> Option<StitchedProjection> {
        if !available() {
            return None;
        }
        let canon = canon_proj(prog, ncols, nouts);
        let fp = fingerprint(&canon);
        let t0 = std::time::Instant::now();
        if let Some((code, Meta::Proj(meta))) = self.probe(fp, &canon) {
            return Some(proj_handle(code, meta, t0.elapsed().as_nanos() as u64));
        }
        // Miss: stitch OUTSIDE the lock (emission is the µs-class cost;
        // never serialize other threads' probes behind it).
        let (words, meta) = stitch_proj_words(prog, ncols, nouts)?;
        let code = Arc::new(jit_deform::install_code_shared(&words)?);
        let (code, meta) = match self.publish(fp, canon, code, Meta::Proj(meta)) {
            (code, Meta::Proj(meta)) => (code, meta),
            _ => unreachable!("qual entry under a proj canon key"),
        };
        Some(proj_handle(code, meta, t0.elapsed().as_nanos() as u64))
    }

    fn qual_at(
        &self,
        fp: u64,
        canon: Vec<u8>,
        prog: &Program,
        ncols: usize,
        opts: StitchOpts,
    ) -> Option<StitchedProgram> {
        let t0 = std::time::Instant::now();
        if let Some((code, Meta::Qual(meta))) = self.probe(fp, &canon) {
            return Some(qual_handle(code, meta, t0.elapsed().as_nanos() as u64));
        }
        let (words, meta) = stitch_qual_words(prog, ncols, opts)?;
        let code = Arc::new(jit_deform::install_code_shared(&words)?);
        let (code, meta) = match self.publish(fp, canon, code, Meta::Qual(meta)) {
            (code, Meta::Qual(meta)) => (code, meta),
            _ => unreachable!("proj entry under a qual canon key"),
        };
        Some(qual_handle(code, meta, t0.elapsed().as_nanos() as u64))
    }

    /// Probe the index. Hit = fingerprint match AND full canonical-byte
    /// equality AND current generation — the always-on structural-verify
    /// membrane: the fingerprint alone never grants a body.
    fn probe(&self, fp: u64, canon: &[u8]) -> Option<(Arc<jit_deform::SharedCode>, Meta)> {
        let gen = self.generation.load(Ordering::Acquire);
        let mut inner = self.inner.lock().unwrap();
        inner.tick += 1;
        let tick = inner.tick;
        let hit = inner.index.get_mut(&fp).and_then(|bucket| {
            bucket
                .iter_mut()
                .find(|e| e.generation == gen && *e.canon == *canon)
                .map(|e| {
                    e.last_use = tick;
                    (Arc::clone(&e.code), e.meta.clone())
                })
        });
        match hit {
            Some(found) => {
                inner.hits += 1;
                Some(found)
            }
            None => {
                inner.misses += 1;
                None
            }
        }
    }

    /// Insert a freshly stitched body, resolving the stitch/stitch race:
    /// if another thread published the same key while we emitted, adopt
    /// its entry (bodies are byte-identical by purity, but sharing ONE
    /// mapping keeps the cap accounting honest) and drop ours. Then evict
    /// LRU until the cap holds.
    fn publish(
        &self,
        fp: u64,
        canon: Vec<u8>,
        code: Arc<jit_deform::SharedCode>,
        meta: Meta,
    ) -> (Arc<jit_deform::SharedCode>, Meta) {
        let gen = self.generation.load(Ordering::Acquire);
        let mut inner = self.inner.lock().unwrap();
        inner.tick += 1;
        let tick = inner.tick;
        let bucket = inner.index.entry(fp).or_default();
        // Reap this bucket's stale-generation entries while we are here
        // (lazy generation retirement).
        let mut reaped_bytes = 0usize;
        let mut reaped = 0usize;
        bucket.retain(|e| {
            let keep = e.generation == gen;
            if !keep {
                reaped_bytes += e.code.mapped_bytes();
                reaped += 1;
            }
            keep
        });
        if let Some(e) = bucket.iter_mut().find(|e| *e.canon == *canon) {
            e.last_use = tick;
            let adopted = (Arc::clone(&e.code), e.meta.clone());
            inner.bytes -= reaped_bytes;
            inner.entries -= reaped;
            inner.evictions += reaped as u64;
            return adopted;
        }
        let ret = (Arc::clone(&code), meta.clone());
        let body_bytes = code.mapped_bytes();
        bucket.push(Entry {
            canon: canon.into_boxed_slice(),
            code,
            meta,
            generation: gen,
            last_use: tick,
        });
        inner.bytes = inner.bytes + body_bytes - reaped_bytes;
        inner.entries = inner.entries + 1 - reaped;
        inner.evictions += reaped as u64;
        // LRU eviction under the byte cap. Evicting only drops the
        // cache's Arc: any executing handle keeps its own, so pages are
        // never unmapped mid-execution. The just-inserted entry is
        // evictable too (a cap smaller than one body caches nothing —
        // the returned Arc above keeps the caller sound regardless).
        while inner.bytes > self.cap_bytes && inner.entries > 0 {
            let (&fp, idx) = inner
                .index
                .iter()
                .flat_map(|(fp, b)| b.iter().enumerate().map(move |(i, e)| (fp, i, e.last_use)))
                .min_by_key(|&(_, _, lu)| lu)
                .map(|(fp, i, _)| (fp, i))
                .expect("entries > 0");
            let bucket = inner.index.get_mut(&fp).expect("bucket exists");
            let victim = bucket.remove(idx);
            if bucket.is_empty() {
                inner.index.remove(&fp);
            }
            inner.bytes -= victim.code.mapped_bytes();
            inner.entries -= 1;
            inner.evictions += 1;
        }
        ret
    }

    /// Retire every current entry (future vocabulary-migration hook; see
    /// the module header). Stale entries stop hitting immediately and are
    /// reaped lazily on the buckets inserts touch, or eagerly here.
    pub fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
        let gen = self.generation.load(Ordering::Acquire);
        let mut inner = self.inner.lock().unwrap();
        let mut freed = 0usize;
        let mut reaped = 0usize;
        inner.index.retain(|_, bucket| {
            bucket.retain(|e| {
                let keep = e.generation == gen;
                if !keep {
                    freed += e.code.mapped_bytes();
                    reaped += 1;
                }
                keep
            });
            !bucket.is_empty()
        });
        inner.bytes -= freed;
        inner.entries -= reaped;
        inner.evictions += reaped as u64;
    }

    pub fn stats(&self) -> CacheStats {
        let inner = self.inner.lock().unwrap();
        CacheStats {
            hits: inner.hits,
            misses: inner.misses,
            evictions: inner.evictions,
            entries: inner.entries,
            bytes: inner.bytes,
        }
    }

    /// CI-only seam for the forged-collision gate (the Q4 pattern): mint
    /// a qual body under a CALLER-CHOSEN fingerprint, simulating a broken
    /// fingerprint function that maps different shapes to one index. The
    /// membrane (full canon compare) must keep the bodies distinct.
    #[cfg(feature = "oracle")]
    #[doc(hidden)]
    pub fn get_or_stitch_forced_fp(
        &self,
        fp: u64,
        prog: &Program,
        ncols: usize,
    ) -> Option<StitchedProgram> {
        if !available() {
            return None;
        }
        let opts = StitchOpts::default();
        let canon = canon_qual(prog, ncols, opts);
        self.qual_at(fp, canon, prog, ncols, opts)
    }
}

fn qual_handle(
    code: Arc<jit_deform::SharedCode>,
    meta: QualMeta,
    stitch_nanos: u64,
) -> StitchedProgram {
    // SAFETY: code holds a complete qual body starting at base, RX-mapped
    // and icache-flushed by install_code_shared; the Arc moved into Body
    // keeps it mapped for the handle's lifetime.
    let entry: PipelineFn = unsafe { core::mem::transmute(code.base()) };
    StitchedProgram::from_parts(Body::Shared(code), entry, meta, stitch_nanos)
}

fn proj_handle(
    code: Arc<jit_deform::SharedCode>,
    meta: ProjMeta,
    stitch_nanos: u64,
) -> StitchedProjection {
    // SAFETY: as qual_handle, for a projection body.
    let entry: ProjPipelineFn = unsafe { core::mem::transmute(code.base()) };
    StitchedProjection::from_parts(Body::Shared(code), entry, meta, stitch_nanos)
}
