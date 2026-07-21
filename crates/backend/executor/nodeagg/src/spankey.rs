//! spankey lane — copy-tax decomposition counters (MEASUREMENT ONLY).
//!
//! Charter step 1 (notes/spankey-lane.md): decompose the text-grouping
//! family's accept-side wall (the interned/dict-key top-n shapes @100M mt16) into
//!   (a) varlena datum materialization from the scan arena,
//!   (b) canonical-bytes / intern build,
//!   (c) hash/compare over the materialized bytes,
//! before building any span-feed mechanism. Staging is zero-copy by
//! construction (pgrcolumnar decode publishes pointers into the mmap blob /
//! per-granule decompress arena — reader.rs decode_granule RawText/Lz4Text),
//! so (a) is expected ~0 and the interesting shares are the intern resolves
//! (pack-loop Intern component), the accept-time canonical image + hash
//! (compact_extend_canon_hashes), the flush rematerialization
//! (sink_flush_table_canon), and the packed-table probe/fold bands.
//!
//! Everything here is OFF unless `PGRUST_SPANKEY_CTR=1`: one cached-bool
//! branch per BATCH on the hot paths (per-row counters only tick inside
//! the intern resolve, which is already off the packed hot loop). Counters
//! are process-global relaxed atomics summed across workers; the sink
//! drain's completion trace prints and resets them per engagement.

use std::sync::atomic::{AtomicU64, Ordering};

/// `PGRUST_SPANKEY_CTR=1` — cached once per process.
pub fn spankey_ctr_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_SPANKEY_CTR").is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("on"))
    })
}

/// STORE-ONCE canonical bytes (spankey step 2; coordinator-approved seam =
/// the canonical image lifecycle across accept/flush/combine). Default ON;
/// `PGRUST_RUNTIME_AGG_SPANKEY=0|off` kills it (exact incumbent paths:
/// accept-time build-hash-discard, flush pass-1 rebuild, combine-remainder
/// per-arrival rebuild). The canonical SPILL record path is untouched
/// either way (condition of record: spill bytes identical, the fail-closed
/// replay path unaware anything changed).
pub fn spankey_store_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !std::env::var("PGRUST_RUNTIME_AGG_SPANKEY")
            .is_ok_and(|v| v == "0" || v.eq_ignore_ascii_case("off"))
    })
}

macro_rules! ctrs {
    ($($name:ident),* $(,)?) => {
        #[derive(Default)]
        pub struct SpankeyCtrs {
            $(pub $name: AtomicU64,)*
        }
        impl SpankeyCtrs {
            fn report_reset(&self) -> String {
                let mut s = String::from("spankey copy-tax counters:");
                $(
                    s.push_str(&format!(
                        " {}={}",
                        stringify!($name),
                        self.$name.swap(0, Ordering::Relaxed)
                    ));
                )*
                s
            }
        }
    };
}

ctrs!(
    // scan_mk_batch pack pre-pass, Intern components: datum views +
    // code_ids identity cache + DictLazy ensures + intern resolves
    // (per-row on Raw windows — the interned-int-key class; per (identity, code) on
    // dict windows — the wide-vocabulary class).
    pack_intern_ns,
    // scan_mk_batch pack pre-pass, Int/Numeric components (the dict-int shape's int half).
    pack_word_ns,
    // agg_hash_compact_batch_mk1/mk2: packed-key batch probe + new-group
    // seeding. INCLUDES canon_accept_ns (nested) — subtract at analysis.
    probe_ns,
    // agg_fold_staged: whole-batch transition fold.
    fold_ns,
    // compact_extend_canon_hashes: accept-time canonical image build +
    // sink_hash_bytes per NEW group (the scratch image is discarded — only
    // the hash is retained; this is copy #2 of every distinct string).
    canon_accept_ns,
    canon_accept_rows,
    canon_accept_bytes,
    // sink_flush_table_canon: flush-time canonical image rebuild (copy #3)
    // + bucket-order permute (copy #4). Bytes = the run's key_bytes total.
    flush_canon_ns,
    flush_canon_rows,
    flush_canon_bytes,
    // agg_hash_compact_intern: resolves; new = first-arrival inserts (the
    // arena copy — copy #1 of every distinct string); bytes = inserted lens.
    intern_calls,
    intern_new,
    intern_new_bytes,
    // sink_combine_bucket: cross-worker canonical merge (claim-summed).
    // combine_ns covers the whole bucket merge; the two sub-bands below are
    // NESTED inside it (runs face = flushed/spilled runs replay; rem face =
    // live-table remainder, whose canonical images materialize HERE —
    // copy #3' for never-flushed groups).
    combine_ns,
    combine_runs_ns,
    combine_rem_ns,
    combine_rem_rows,
    combine_rem_bytes,
);

pub static SPANKEY_CTRS: SpankeyCtrs = SpankeyCtrs {
    pack_intern_ns: AtomicU64::new(0),
    pack_word_ns: AtomicU64::new(0),
    probe_ns: AtomicU64::new(0),
    fold_ns: AtomicU64::new(0),
    canon_accept_ns: AtomicU64::new(0),
    canon_accept_rows: AtomicU64::new(0),
    canon_accept_bytes: AtomicU64::new(0),
    flush_canon_ns: AtomicU64::new(0),
    flush_canon_rows: AtomicU64::new(0),
    flush_canon_bytes: AtomicU64::new(0),
    intern_calls: AtomicU64::new(0),
    intern_new: AtomicU64::new(0),
    intern_new_bytes: AtomicU64::new(0),
    combine_ns: AtomicU64::new(0),
    combine_runs_ns: AtomicU64::new(0),
    combine_rem_ns: AtomicU64::new(0),
    combine_rem_rows: AtomicU64::new(0),
    combine_rem_bytes: AtomicU64::new(0),
};

#[inline]
pub fn spankey_add(ctr: &AtomicU64, v: u64) {
    ctr.fetch_add(v, Ordering::Relaxed);
}

/// Timer start when the counters are armed (`None` = disarmed, zero cost
/// past the cached-bool load).
#[inline]
pub fn spankey_t0() -> Option<std::time::Instant> {
    spankey_ctr_enabled().then(std::time::Instant::now)
}

/// Accumulate an elapsed band.
#[inline]
pub fn spankey_lap(ctr: &AtomicU64, t0: Option<std::time::Instant>) {
    if let Some(t0) = t0 {
        ctr.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }
}

/// Print-and-reset, for the sink completion trace. `None` when disarmed.
pub fn spankey_report_reset() -> Option<String> {
    spankey_ctr_enabled().then(|| SPANKEY_CTRS.report_reset())
}
