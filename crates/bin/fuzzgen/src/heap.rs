//! Heapam ALT-PATH drain module (W5-HEAP): the still-drainable
//! backend/access/heap + toast chunk of the line-gap queue —
//! heap_update/heap_delete residue (all-visible VM-pin interplay,
//! locked-only/aborted-xmax overwrite arms, KEYS_UPDATED, toasted-row
//! update/delete), pruneheap HOT-chain walks, vacuumlazy (lazy_scan_prune,
//! lazy_scan_new_or_empty, lazy_truncate_heap + count_nondeletable_pages,
//! aggressive/FREEZE scans, INDEX_CLEANUP forcing, the VERBOSE
//! instrumentation block), freeze arms (heap_prepare_freeze_tuple,
//! heap_tuple_should_freeze via vacuum_freeze_min_age=0 brackets), the
//! toast write/read/delete surface (toast_save_datum, slice detoast,
//! heap_fetch_toast_slice, toast_tuple_init under toast_tuple_target),
//! heapam_relation_copy_for_cluster dead-tuple arms, TID scans
//! (heap_fetch), CREATE INDEX CONCURRENTLY validate scans, and the
//! serializable-read HeapCheckForSerializableConflictOut entries.
//!
//! Everything here is SINGLE-SESSION: the concurrency arms proper
//! (TM_Updated waits, EPQ) are CONCUR/Antithesis-owned. The single-session
//! routes into the "somebody else touched it" code are subtransactions:
//! a SAVEPOINT that locks/updates a row and is rolled back leaves an
//! aborted non-current xmax behind, which drives the aborted/locked-only
//! overwrite arms of heap_update/heap_delete and the multixact hint-bit
//! paths without a second backend.
//!
//! Determinism laws honored (same discipline as crate::spill):
//!   - every row-returning statement carries a TOTAL order ending in pk;
//!     everything else is aggregate-only with exact-typed aggregates. No
//!     float anything (B1).
//!   - tables stay <= 24000 rows total (exhaustive ANALYZE) and carry
//!     autovacuum_enabled = off reloptions (heap + toast), so every
//!     prune/vacuum/freeze transition is an EXPLICIT statement in the
//!     stream — identical on both differential sides.
//!   - every bracket SET has its RESET in the same group; VACUUM never
//!     appears inside a BEGIN bracket.
//!   - physical heap order is a ruled non-surface (COPY order ruling):
//!     TID-scan probes project count(*) only, so a placement divergence
//!     shows up as a countable row-set difference, not a row-order one.
//!   - toast payloads are pure functions of (table seed, pk): repeat()
//!     for the compressible/inline-compressed classes, md5 chains
//!     (string_agg ORDER BY g) for the incompressible/external classes.
//!
//! Data model: `fz_hp_N` tables, one live at a time.
//!   pk int4 PRIMARY KEY, k int4 (0..37, btree-indexed from birth — the
//!   non-HOT lever), v int4 (never indexed — the HOT lever), txt text
//!   (default EXTENDED storage: compressible payloads), ext text
//!   (STORAGE EXTERNAL: incompressible payloads, uncompressed toast),
//!   pad text (~24-38 chars: page-count ballast). Fillfactor 30/70/100
//!   and an optional toast_tuple_target=256 are per-table create picks;
//!   REPLICA IDENTITY FULL is applied to some tables (old-key extraction
//!   arms where the rig's wal_level allows). Toast payloads live on the
//!   low band pk <= TOAST_BAND only, so state probes stay cheap.

use crate::ddl::{DdlEvent, DdlEventKind};
use crate::stmt::{Gen, StmtKind};

/// One live table at a time (vacuum/cluster groups walk the whole heap).
const MAX_LIVE_TABLES: usize = 1;

/// Toast payloads are confined to pk <= TOAST_BAND.
const TOAST_BAND: u32 = 24;

const K_M: u64 = 37; // k in 0..37 (dup-heavy indexed key)

fn row_source(lo: i64, hi: i64) -> String {
    format!(
        "SELECT i, (i * 11) % {K_M}, 0, NULL::text, NULL::text, \
         'q' || repeat('z', 20 + (i % 15)::int) \
         FROM generate_series({lo}, {hi}) i"
    )
}

#[derive(Clone, Debug)]
pub struct HeapTable {
    pub name: String,
    pub live: bool,
    /// Committed base-load row count (pk 1..rows). Band picks stay inside
    /// it; the pk-shift and burst families use disjoint high ranges.
    pub rows: u32,
    /// The k index name (the CLUSTER alternate target).
    pub k_index: String,
}

/// Session-persistent heap-table model (swapped in and out of `Gen` by the
/// session loop exactly like `SpillState`).
#[derive(Clone, Debug, Default)]
pub struct HeapState {
    pub tables: Vec<HeapTable>,
    next_table: u32,
    next_index: u32,
    events: Vec<DdlEvent>,
}

impl HeapState {
    pub fn new() -> HeapState {
        HeapState::default()
    }

    pub fn take_events(&mut self) -> Vec<DdlEvent> {
        std::mem::take(&mut self.events)
    }

    fn live_tables(&self) -> Vec<usize> {
        self.tables
            .iter()
            .enumerate()
            .filter(|(_, t)| t.live)
            .map(|(i, _)| i)
            .collect()
    }
}

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_heap_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("heap");
    let action = g.weights.pick(
        g.rng,
        &[
            "heap:create",
            "heap:drop",
            "heap:hot",
            "heap:nonhot",
            "heap:abortchurn",
            "heap:lock",
            "heap:toastwrite",
            "heap:toastread",
            "heap:toastupd",
            "heap:vacuum",
            "heap:trunc",
            "heap:allvis",
            "heap:newpage",
            "heap:cic",
            "heap:cluster",
            "heap:tid",
            "heap:serial",
        ],
    );
    match action {
        "heap:create" => gen_create(g),
        "heap:drop" => gen_drop(g),
        "heap:hot" => gen_hot(g),
        "heap:nonhot" => gen_nonhot(g),
        "heap:abortchurn" => gen_abortchurn(g),
        "heap:lock" => gen_lock(g),
        "heap:toastwrite" => gen_toastwrite(g),
        "heap:toastread" => gen_toastread(g),
        "heap:toastupd" => gen_toastupd(g),
        "heap:vacuum" => gen_vacuum(g),
        "heap:trunc" => gen_trunc(g),
        "heap:allvis" => gen_allvis(g),
        "heap:newpage" => gen_newpage(g),
        "heap:cic" => gen_cic(g),
        "heap:cluster" => gen_cluster(g),
        "heap:tid" => gen_tid(g),
        _ => gen_serial(g),
    }
}

// ------------------------------------------------------------ brackets ----

/// SET/RESET bracket (RESETs reversed, same group) — the runner's GucPinned
/// wrapper re-applies the C-parity pin after RESETs on both sides.
fn bracket(gucs: &[(&str, &str)], body: Vec<StmtKind>) -> Vec<StmtKind> {
    let mut stmts: Vec<StmtKind> = gucs
        .iter()
        .map(|(n, v)| StmtKind::Raw(format!("SET {n} = {v};")))
        .collect();
    stmts.extend(body);
    for (n, _) in gucs.iter().rev() {
        stmts.push(StmtKind::Raw(format!("RESET {n};")));
    }
    stmts
}

// -------------------------------------------------------------- create ----

fn gen_create(g: &mut Gen) -> Vec<StmtKind> {
    if g.heap.live_tables().len() >= MAX_LIVE_TABLES {
        g.fire("heap:cap:tables");
        return gen_drop(g);
    }
    g.fire("heap:create");
    let name = format!("fz_hp_{}", g.heap.next_table);
    g.heap.next_table += 1;
    let ff = match g.weights.pick(g.rng, &["heap:ff:30", "heap:ff:70", "heap:ff:100"]) {
        "heap:ff:30" => 30,
        "heap:ff:70" => 70,
        _ => 100,
    };
    // Small tables: the heap arms live in tuple-state transitions, not
    // volume; vacuum/cluster groups then stay cheap enough to fire often.
    let rows = match g.weights.pick(g.rng, &["heap:rows:1500", "heap:rows:3000", "heap:rows:6000"]) {
        "heap:rows:1500" => 1500,
        "heap:rows:3000" => 3000,
        _ => 6000,
    };
    // toast_tuple_target=256 forces toast_tuple_init/heap_toast_insert_or_
    // update engagement on values far below the default 2kB threshold.
    let ttt = g.weights.pick(g.rng, &["heap:tt:default", "heap:tt:256"]) == "heap:tt:256";
    let reloptions = if ttt {
        format!("fillfactor = {ff}, toast_tuple_target = 256, autovacuum_enabled = off")
    } else {
        format!("fillfactor = {ff}, autovacuum_enabled = off")
    };
    let ki = format!("fz_hpi_{}", g.heap.next_index);
    g.heap.next_index += 1;
    let mut stmts = vec![
        StmtKind::Raw(format!(
            "CREATE TABLE {name} (pk int4 PRIMARY KEY, k int4, v int4, \
             txt text, ext text, pad text) WITH ({reloptions});"
        )),
        StmtKind::Raw(format!("ALTER TABLE {name} ALTER COLUMN ext SET STORAGE EXTERNAL;")),
    ];
    // Storage-class spread on txt: EXTENDED (default) vs MAIN vs PLAIN.
    match g.weights.pick(g.rng, &["heap:st:extended", "heap:st:main", "heap:st:plain"]) {
        "heap:st:main" => {
            g.fire("heap:st:main");
            stmts.push(StmtKind::Raw(format!(
                "ALTER TABLE {name} ALTER COLUMN txt SET STORAGE MAIN;"
            )));
        }
        "heap:st:plain" => {
            g.fire("heap:st:plain");
            stmts.push(StmtKind::Raw(format!(
                "ALTER TABLE {name} ALTER COLUMN txt SET STORAGE PLAIN;"
            )));
        }
        _ => {}
    }
    if g.weights.pick(g.rng, &["heap:ri:default", "heap:ri:full"]) == "heap:ri:full" {
        g.fire("heap:ri:full");
        stmts.push(StmtKind::Raw(format!("ALTER TABLE {name} REPLICA IDENTITY FULL;")));
    }
    stmts.push(StmtKind::Raw(format!("INSERT INTO {name} {};", row_source(1, rows as i64))));
    stmts.push(StmtKind::Raw(format!("CREATE INDEX {ki} ON {name} (k);")));
    stmts.push(StmtKind::Raw(format!("ANALYZE {name};")));
    g.heap.tables.push(HeapTable { name: name.clone(), live: true, rows, k_index: ki });
    g.heap
        .events
        .push(DdlEvent { table: name, pk: "pk".to_string(), kind: DdlEventKind::Created });
    stmts
}

fn gen_drop(g: &mut Gen) -> Vec<StmtKind> {
    let live = g.heap.live_tables();
    if live.is_empty() {
        // Never recurses: an empty population is below the cap by definition.
        g.fire("heap:fallback:create");
        return gen_create(g);
    }
    g.fire("heap:drop");
    let ti = live[g.rng.below_usize(live.len())];
    let name = g.heap.tables[ti].name.clone();
    g.heap.tables[ti].live = false;
    g.heap.events.push(DdlEvent {
        table: name.clone(),
        pk: "pk".to_string(),
        kind: DdlEventKind::Dropped,
    });
    vec![StmtKind::Raw(format!("DROP TABLE {};", name))]
}

fn pick_live(g: &mut Gen) -> Option<usize> {
    let live = g.heap.live_tables();
    if live.is_empty() {
        return None;
    }
    Some(live[g.rng.below_usize(live.len())])
}

macro_rules! need_table {
    ($g:expr) => {
        match pick_live($g) {
            Some(ti) => ti,
            None => {
                $g.fire("heap:fallback:create");
                return gen_create($g);
            }
        }
    };
}

/// A committed-band pick: `lo..=hi` inside pk 1..rows, width 20-170.
fn band(g: &mut Gen, rows: u32) -> (u32, u32) {
    let width = 20 + g.rng.below(150) as u32;
    let lo = 1 + g.rng.below(rows.saturating_sub(width).max(1) as u64) as u32;
    (lo, (lo + width).min(rows))
}

/// Ordered aggregate state probe over a band (exact-typed, no rows cross
/// the wire unordered).
fn probe(t: &str, lo: u32, hi: u32) -> StmtKind {
    StmtKind::Raw(format!(
        "SELECT count(*), sum(v::int8), sum(k::int8), min(pk), max(pk) \
         FROM {t} WHERE pk BETWEEN {lo} AND {hi};"
    ))
}

// ------------------------------------------------------------------ hot ---

/// HOT-chain fuel: repeated UPDATEs of the never-indexed v column over one
/// band. Low fillfactor keeps the chains on-page; the trailing seqscan
/// probe walks the chains (heap_hot_search_buffer) and — once the page's
/// prune xid horizon passes — fires heap_page_prune_opt/heap_prune_chain.
fn gen_hot(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("heap:hot");
    let t = g.heap.tables[ti].name.clone();
    let rows = g.heap.tables[ti].rows;
    let (lo, hi) = band(g, rows);
    let rounds = 2 + g.rng.below(3); // 2-4 chained updates
    let mut stmts = Vec::new();
    for r in 0..rounds {
        stmts.push(StmtKind::Raw(format!(
            "UPDATE {t} SET v = v + {} WHERE pk BETWEEN {lo} AND {hi};",
            r + 1
        )));
    }
    stmts.push(probe(&t, lo, hi));
    stmts
}

// --------------------------------------------------------------- nonhot ---

/// Non-HOT updates: touching the indexed k column (modified_attrs overlap
/// -> no HOT), or pk itself (HEAP_KEYS_UPDATED / TU_All index updates).
/// The pk-shift moves rows to a disjoint high range so committed-band
/// arithmetic stays valid; ON CONFLICT keeps later re-insert families
/// collision-proof.
fn gen_nonhot(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("heap:nonhot");
    let t = g.heap.tables[ti].name.clone();
    let rows = g.heap.tables[ti].rows;
    let (lo, hi) = band(g, rows);
    let shape = g.weights.pick(g.rng, &["heap:nonhot:key", "heap:nonhot:pk", "heap:nonhot:both"]);
    g.fire(shape);
    let mut stmts = Vec::new();
    match shape {
        "heap:nonhot:key" => {
            stmts.push(StmtKind::Raw(format!(
                "UPDATE {t} SET k = (k + 5) % {K_M} WHERE pk BETWEEN {lo} AND {hi};"
            )));
        }
        "heap:nonhot:pk" => {
            // Narrow: pk churn stays bounded (width <= 40).
            let hi = hi.min(lo + 40);
            stmts.push(StmtKind::Raw(format!(
                "UPDATE {t} SET pk = pk + 1000000 WHERE pk BETWEEN {lo} AND {hi};"
            )));
            // Bring them back: two full index-churn round trips per pick.
            stmts.push(StmtKind::Raw(format!(
                "UPDATE {t} SET pk = pk - 1000000 WHERE pk BETWEEN {} AND {};",
                lo + 1000000,
                hi + 1000000
            )));
        }
        _ => {
            stmts.push(StmtKind::Raw(format!(
                "UPDATE {t} SET k = (k + 9) % {K_M}, v = v + 1 WHERE pk BETWEEN {lo} AND {hi};"
            )));
            stmts.push(StmtKind::Raw(format!(
                "UPDATE {t} SET v = v + 1 WHERE pk BETWEEN {lo} AND {hi};"
            )));
        }
    }
    stmts.push(probe(&t, lo, hi.min(lo + 200)));
    stmts
}

// ----------------------------------------------------------- abortchurn ---

/// Aborted-write residue: rolled-back UPDATE/DELETE/INSERT leaves aborted
/// xmax/xmin tuples — the HeapTupleSatisfies* aborted arms, prune of
/// aborted chains, and the heap_update/heap_delete "previous xmax aborted"
/// overwrite arms. The savepoint variant aborts only a SUBxact, so the
/// follow-up committed write in the same transaction sees a non-current
/// aborted xmax (single-session route into the recheck arms).
fn gen_abortchurn(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("heap:abortchurn");
    let t = g.heap.tables[ti].name.clone();
    let rows = g.heap.tables[ti].rows;
    let (lo, hi) = band(g, rows);
    let shape = g.weights.pick(
        g.rng,
        &["heap:abort:rollback", "heap:abort:savepoint", "heap:abort:insabort"],
    );
    g.fire(shape);
    match shape {
        "heap:abort:rollback" => {
            let dml = if g.rng.chance(1, 2) {
                format!("UPDATE {t} SET v = v + 100 WHERE pk BETWEEN {lo} AND {hi};")
            } else {
                format!("DELETE FROM {t} WHERE pk BETWEEN {lo} AND {hi};")
            };
            vec![
                StmtKind::Raw("BEGIN;".to_string()),
                StmtKind::Raw(dml),
                StmtKind::Raw("ROLLBACK;".to_string()),
                probe(&t, lo, hi),
            ]
        }
        "heap:abort:savepoint" => {
            // Subxact-aborted xmax, then a committed re-write of the SAME
            // rows inside the same top-level transaction.
            let redo = if g.rng.chance(1, 3) {
                format!("DELETE FROM {t} WHERE pk BETWEEN {lo} AND {};", lo + 10)
            } else {
                format!("UPDATE {t} SET v = v + 3 WHERE pk BETWEEN {lo} AND {hi};")
            };
            vec![
                StmtKind::Raw("BEGIN;".to_string()),
                StmtKind::Raw(format!(
                    "UPDATE {t} SET v = v + 1 WHERE pk BETWEEN {lo} AND {hi};"
                )),
                StmtKind::Raw("SAVEPOINT sp1;".to_string()),
                StmtKind::Raw(format!(
                    "UPDATE {t} SET v = v + 7 WHERE pk BETWEEN {lo} AND {hi};"
                )),
                StmtKind::Raw("ROLLBACK TO SAVEPOINT sp1;".to_string()),
                StmtKind::Raw(redo),
                StmtKind::Raw("COMMIT;".to_string()),
                probe(&t, lo, hi),
            ]
        }
        _ => {
            // Aborted bulk insert into a disjoint high range: pages full of
            // all-dead never-committed tuples (+ possibly never-initialized
            // bulk-extend pages) for the next vacuum/prune group.
            let n = 1000 + g.rng.below(2000);
            vec![
                StmtKind::Raw("BEGIN;".to_string()),
                StmtKind::Raw(format!(
                    "INSERT INTO {t} {};",
                    row_source(2000000, 2000000 + n as i64)
                )),
                StmtKind::Raw("ROLLBACK;".to_string()),
                probe(&t, 1, 100),
            ]
        }
    }
}

// ----------------------------------------------------------------- lock ---

/// Single-session tuple locking: FOR KEY SHARE/SHARE/NO KEY UPDATE/UPDATE
/// row locks, lock upgrades across a SAVEPOINT (two distinct locker xids
/// on one tuple -> MultiXact + GetMultiXactIdHintBits on the follow-up
/// write), aborted-subxact locks, and lock-then-write in one transaction
/// (the locked-only overwrite arms of heap_delete/heap_update).
fn gen_lock(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("heap:lock");
    let t = g.heap.tables[ti].name.clone();
    let rows = g.heap.tables[ti].rows;
    let (lo, hi0) = band(g, rows);
    let hi = hi0.min(lo + 60);
    let mode = match g.weights.pick(
        g.rng,
        &["heap:lock:keyshare", "heap:lock:share", "heap:lock:nokeyupd", "heap:lock:update"],
    ) {
        "heap:lock:keyshare" => "KEY SHARE",
        "heap:lock:share" => "SHARE",
        "heap:lock:nokeyupd" => "NO KEY UPDATE",
        _ => "UPDATE",
    };
    let shape = g.weights.pick(
        g.rng,
        &["heap:lock:plain", "heap:lock:upgrade", "heap:lock:abortlock", "heap:lock:lockwrite"],
    );
    g.fire(shape);
    let sel = |m: &str, lo: u32, hi: u32| {
        StmtKind::Raw(format!(
            "SELECT count(*), sum(v::int8) FROM (SELECT v FROM {t} \
             WHERE pk BETWEEN {lo} AND {hi} FOR {m}) s;"
        ))
    };
    match shape {
        // Plain lock + commit: locked-only xmax left behind for later
        // prune/vacuum/write groups.
        "heap:lock:plain" => vec![
            StmtKind::Raw("BEGIN;".to_string()),
            sel(mode, lo, hi),
            StmtKind::Raw("COMMIT;".to_string()),
        ],
        // Weak lock, then a SAVEPOINT-scoped stronger lock: the tuple's
        // xmax becomes a MultiXact of two same-backend member xids.
        "heap:lock:upgrade" => vec![
            StmtKind::Raw("BEGIN;".to_string()),
            sel("KEY SHARE", lo, hi),
            StmtKind::Raw("SAVEPOINT sl1;".to_string()),
            sel(mode, lo, hi),
            StmtKind::Raw("RELEASE SAVEPOINT sl1;".to_string()),
            StmtKind::Raw(format!(
                "UPDATE {t} SET v = v + 1 WHERE pk BETWEEN {lo} AND {};",
                lo + 20
            )),
            StmtKind::Raw("COMMIT;".to_string()),
        ],
        // Lock inside an aborted savepoint, then write the same rows: the
        // write path sees a non-current ABORTED locker xmax.
        "heap:lock:abortlock" => {
            let redo = if g.rng.chance(1, 2) {
                format!("UPDATE {t} SET v = v + 2 WHERE pk BETWEEN {lo} AND {hi};")
            } else {
                format!("DELETE FROM {t} WHERE pk BETWEEN {lo} AND {};", lo + 8)
            };
            vec![
                StmtKind::Raw("BEGIN;".to_string()),
                StmtKind::Raw("SAVEPOINT sl2;".to_string()),
                sel(mode, lo, hi),
                StmtKind::Raw("ROLLBACK TO SAVEPOINT sl2;".to_string()),
                StmtKind::Raw(redo),
                StmtKind::Raw("COMMIT;".to_string()),
            ]
        }
        // Lock then write in one transaction: heap_update/heap_delete over
        // a self-locked tuple (locked-only overwrite, lock carry-forward
        // into the new tuple version).
        _ => {
            let write = if g.rng.chance(1, 3) {
                format!("DELETE FROM {t} WHERE pk BETWEEN {lo} AND {};", lo + 6)
            } else if g.rng.chance(1, 2) {
                format!("UPDATE {t} SET v = v + 5 WHERE pk BETWEEN {lo} AND {hi};")
            } else {
                // Key update over a self-locked tuple.
                format!(
                    "UPDATE {t} SET k = (k + 3) % {K_M} WHERE pk BETWEEN {lo} AND {};",
                    lo + 30
                )
            };
            vec![
                StmtKind::Raw("BEGIN;".to_string()),
                sel(mode, lo, hi),
                StmtKind::Raw(write),
                StmtKind::Raw("COMMIT;".to_string()),
            ]
        }
    }
}

// ------------------------------------------------------------ toastwrite --

/// Toast ingestion (toast_tuple_init, heap_toast_insert_or_update,
/// toast_save_datum) across the storage classes: compressible repeat()
/// payloads (EXTENDED: inline-compressed or compressed toast; MAIN: inline
/// pressure), incompressible md5 chains (compression fails -> EXTERNAL
/// path even on EXTENDED), and the STORAGE EXTERNAL column (uncompressed
/// chunks from birth). Payloads land on the low toast band only.
fn gen_toastwrite(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("heap:toastwrite");
    let t = g.heap.tables[ti].name.clone();
    let lo = 1 + g.rng.below((TOAST_BAND - 4) as u64) as u32;
    let hi = (lo + 2 + g.rng.below(5) as u32).min(TOAST_BAND);
    let shape = g.weights.pick(g.rng, &["heap:tw:comp", "heap:tw:incomp", "heap:tw:both"]);
    g.fire(shape);
    // Compressible: 4kB-56kB before compression.
    let comp_n = 2000 + g.rng.below(26000);
    // Incompressible: k md5 blocks of 32 chars => 2.5kB-46kB.
    let inc_k = 80 + g.rng.below(1350);
    let comp = format!("repeat('ab', {comp_n} + pk)");
    let incomp = format!(
        "(SELECT string_agg(md5((g * 1009 + pk)::text), '' ORDER BY g) \
         FROM generate_series(1, {inc_k}) g)"
    );
    let mut stmts = Vec::new();
    match shape {
        "heap:tw:comp" => stmts.push(StmtKind::Raw(format!(
            "UPDATE {t} SET txt = {comp} WHERE pk BETWEEN {lo} AND {hi};"
        ))),
        "heap:tw:incomp" => stmts.push(StmtKind::Raw(format!(
            "UPDATE {t} SET ext = {incomp} WHERE pk BETWEEN {lo} AND {hi};"
        ))),
        _ => stmts.push(StmtKind::Raw(format!(
            "UPDATE {t} SET txt = {comp}, ext = {incomp} WHERE pk BETWEEN {lo} AND {hi};"
        ))),
    }
    stmts.push(StmtKind::Raw(format!(
        "SELECT count(*), sum(length(txt)::int8), sum(length(ext)::int8) \
         FROM {t} WHERE pk <= {TOAST_BAND};"
    )));
    stmts
}

// ------------------------------------------------------------- toastread --

/// Detoast read arms: slice fetches (detoast_attr_slice +
/// heap_fetch_toast_slice — leading, interior and tail slices; sliced
/// decompression on the compressed class, direct chunk-window reads on
/// EXTERNAL), full detoast, length probes (toast_datum_size /
/// toast_raw_datum_size) and stored-size probes (pg_column_size).
fn gen_toastread(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("heap:toastread");
    let t = g.heap.tables[ti].name.clone();
    let shape = g.weights.pick(
        g.rng,
        &["heap:tr:slice", "heap:tr:len", "heap:tr:full", "heap:tr:size"],
    );
    g.fire(shape);
    let col = if g.rng.chance(1, 2) { "txt" } else { "ext" };
    let body = match shape {
        "heap:tr:slice" => {
            let off = 1 + g.rng.below(30000);
            let len = 1 + g.rng.below(4000);
            format!(
                "SELECT pk, length(substr({col}, {off}, {len})), \
                 md5(coalesce(substr({col}, {off}, {len}), '')) \
                 FROM {t} WHERE pk <= {TOAST_BAND} AND {col} IS NOT NULL ORDER BY pk;"
            )
        }
        "heap:tr:len" => format!(
            "SELECT pk, length({col}), octet_length({col}), left({col}, 4) \
             FROM {t} WHERE pk <= {TOAST_BAND} ORDER BY pk;"
        ),
        "heap:tr:full" => format!(
            "SELECT count(*), sum(length({col} || 'x')::int8) \
             FROM {t} WHERE pk <= {TOAST_BAND} AND {col} IS NOT NULL;"
        ),
        // Stored (post-compression) size: identical pglz on both sides is
        // an on-disk-parity surface — a divergence here is signal.
        _ => format!(
            "SELECT pk, pg_column_size({col}) FROM {t} \
             WHERE pk <= {TOAST_BAND} ORDER BY pk;"
        ),
    };
    vec![StmtKind::Raw(body)]
}

// -------------------------------------------------------------- toastupd --

/// Toast rewrite/delete arms: appending re-toasts (delete old chunks +
/// save new), NULLing deletes chunks, updating an untouched column carries
/// the toast pointer through heap_update without re-toasting, and DELETE
/// of toasted rows queues chunk reaping for the next vacuum group.
fn gen_toastupd(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("heap:toastupd");
    let t = g.heap.tables[ti].name.clone();
    let lo = 1 + g.rng.below((TOAST_BAND - 4) as u64) as u32;
    let hi = (lo + 3 + g.rng.below(6) as u32).min(TOAST_BAND);
    let shape = g.weights.pick(
        g.rng,
        &["heap:tu:append", "heap:tu:null", "heap:tu:plaincol", "heap:tu:delins"],
    );
    g.fire(shape);
    let mut stmts = Vec::new();
    match shape {
        "heap:tu:append" => stmts.push(StmtKind::Raw(format!(
            "UPDATE {t} SET txt = txt || md5(pk::text) WHERE pk BETWEEN {lo} AND {hi} \
             AND txt IS NOT NULL;"
        ))),
        "heap:tu:null" => stmts.push(StmtKind::Raw(format!(
            "UPDATE {t} SET txt = NULL, ext = NULL WHERE pk BETWEEN {lo} AND {hi};"
        ))),
        // v-only update of toasted rows: pointer carried, no re-toast.
        "heap:tu:plaincol" => stmts.push(StmtKind::Raw(format!(
            "UPDATE {t} SET v = v + 1 WHERE pk BETWEEN {lo} AND {hi} AND ext IS NOT NULL;"
        ))),
        _ => {
            stmts.push(StmtKind::Raw(format!(
                "DELETE FROM {t} WHERE pk BETWEEN {lo} AND {hi};"
            )));
            stmts.push(StmtKind::Raw(format!(
                "INSERT INTO {t} {} ON CONFLICT (pk) DO NOTHING;",
                row_source(lo as i64, hi as i64)
            )));
        }
    }
    stmts.push(StmtKind::Raw(format!(
        "SELECT count(*), count(txt), count(ext) FROM {t} WHERE pk <= {TOAST_BAND};"
    )));
    stmts
}

// ---------------------------------------------------------------- vacuum --

/// The vacuumlazy drain proper: churn (so there are dead tuples to prune/
/// reap) then a VACUUM variant — plain, FREEZE (aggressive scan +
/// heap_prepare_freeze_tuple), vacuum_freeze_min_age=0 brackets
/// (heap_tuple_should_freeze without the FREEZE keyword),
/// DISABLE_PAGE_SKIPPING, INDEX_CLEANUP forcing (both directions), and
/// VERBOSE (the whole instrumentation/report block; INFO output is not a
/// compare surface). Toast churn ahead of the vacuum reaps chunk rows too.
fn gen_vacuum(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("heap:vacuum");
    let t = g.heap.tables[ti].name.clone();
    let rows = g.heap.tables[ti].rows;
    let (lo, hi) = band(g, rows);
    let mut stmts = Vec::new();
    // Churn prelude: dead tuples for the scan to prune/reap.
    match g.rng.below(3) {
        0 => stmts.push(StmtKind::Raw(format!(
            "UPDATE {t} SET v = v + 1 WHERE pk BETWEEN {lo} AND {hi};"
        ))),
        1 => {
            stmts.push(StmtKind::Raw(format!(
                "DELETE FROM {t} WHERE pk BETWEEN {lo} AND {};",
                lo + 40
            )));
            stmts.push(StmtKind::Raw(format!(
                "INSERT INTO {t} {} ON CONFLICT (pk) DO NOTHING;",
                row_source(lo as i64, (lo + 40) as i64)
            )));
        }
        _ => stmts.push(StmtKind::Raw(format!(
            "UPDATE {t} SET txt = NULL WHERE pk BETWEEN 1 AND 8;"
        ))),
    }
    let shape = g.weights.pick(
        g.rng,
        &[
            "heap:vac:plain",
            "heap:vac:freeze",
            "heap:vac:minage",
            "heap:vac:skip",
            "heap:vac:indexoff",
            "heap:vac:indexon",
            "heap:vac:verbose",
        ],
    );
    g.fire(shape);
    match shape {
        "heap:vac:plain" => stmts.push(StmtKind::Raw(format!("VACUUM {t};"))),
        "heap:vac:freeze" => stmts.push(StmtKind::Raw(format!("VACUUM (FREEZE) {t};"))),
        "heap:vac:minage" => {
            // Freeze decisions via GUC horizons instead of the FREEZE
            // keyword: should_freeze / prepare_freeze under a normal
            // (then table-age-aggressive) scan.
            return bracket(
                &[("vacuum_freeze_min_age", "0"), ("vacuum_freeze_table_age", "0")],
                {
                    stmts.push(StmtKind::Raw(format!("VACUUM {t};")));
                    stmts
                },
            );
        }
        "heap:vac:skip" => {
            stmts.push(StmtKind::Raw(format!("VACUUM (DISABLE_PAGE_SKIPPING) {t};")))
        }
        "heap:vac:indexoff" => {
            stmts.push(StmtKind::Raw(format!("VACUUM (INDEX_CLEANUP OFF) {t};")))
        }
        "heap:vac:indexon" => stmts.push(StmtKind::Raw(format!("VACUUM (INDEX_CLEANUP ON) {t};"))),
        _ => stmts.push(StmtKind::Raw(format!("VACUUM (VERBOSE) {t};"))),
    }
    stmts.push(probe(&t, lo, hi));
    stmts
}

// ----------------------------------------------------------------- trunc --

/// Rel-truncation arms (lazy_truncate_heap + count_nondeletable_pages):
/// delete the pk tail, VACUUM (TRUNCATE ON) shrinks the rel, then reload
/// the tail so later bands stay populated. The all-dead tail pages also
/// walk the empty/all-removable page arms of lazy_scan_heap.
fn gen_trunc(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("heap:trunc");
    let t = g.heap.tables[ti].name.clone();
    let rows = g.heap.tables[ti].rows;
    let cut = rows - (rows / 4) - g.rng.below((rows / 4) as u64) as u32;
    vec![
        StmtKind::Raw(format!("DELETE FROM {t} WHERE pk > {cut};")),
        StmtKind::Raw(format!("VACUUM (TRUNCATE ON) {t};")),
        StmtKind::Raw(format!(
            "INSERT INTO {t} {} ON CONFLICT (pk) DO NOTHING;",
            row_source(cut as i64 + 1, rows as i64)
        )),
        StmtKind::Raw(format!("ANALYZE {t};")),
        probe(&t, cut.saturating_sub(20).max(1), (cut + 60).min(rows)),
    ]
}

// ---------------------------------------------------------------- allvis --

/// All-visible interplay: VACUUM sets the VM bits, then UPDATE/DELETE on
/// the all-visible pages takes the vmbuffer pin-and-recheck arms of
/// heap_update/heap_delete (and clears the bits under WAL). A second
/// VACUUM immediately after re-walks the mixed VM state.
fn gen_allvis(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("heap:allvis");
    let t = g.heap.tables[ti].name.clone();
    let rows = g.heap.tables[ti].rows;
    let (lo, hi) = band(g, rows);
    let write = if g.rng.chance(1, 3) {
        format!("DELETE FROM {t} WHERE pk BETWEEN {lo} AND {};", lo + 15)
    } else {
        format!("UPDATE {t} SET v = v + 1 WHERE pk BETWEEN {lo} AND {hi};")
    };
    let mut stmts = vec![StmtKind::Raw(format!("VACUUM {t};")), StmtKind::Raw(write)];
    if g.rng.chance(1, 2) {
        g.fire("heap:allvis:revac");
        stmts.push(StmtKind::Raw(format!("VACUUM {t};")));
    }
    stmts.push(probe(&t, lo, hi));
    stmts
}

// --------------------------------------------------------------- newpage --

/// Bulk-extend page-state arms (lazy_scan_new_or_empty): a large committed
/// burst into a high pk range multi-block-extends the rel (trailing
/// never-initialized pages when the extend overshoots), then deleting the
/// burst and vacuuming walks PageIsNew/PageIsEmpty/all-dead pages and
/// truncates them back off.
fn gen_newpage(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("heap:newpage");
    let t = g.heap.tables[ti].name.clone();
    let n = 3000 + g.rng.below(3000);
    vec![
        StmtKind::Raw(format!(
            "INSERT INTO {t} {};",
            row_source(3000000, 3000000 + n as i64)
        )),
        StmtKind::Raw(format!("DELETE FROM {t} WHERE pk >= 3000000;")),
        StmtKind::Raw(format!("VACUUM (TRUNCATE ON) {t};")),
        StmtKind::Raw(format!("SELECT count(*) FROM {t} WHERE pk >= 3000000;")),
    ]
}

// ------------------------------------------------------------------- cic --

/// CREATE INDEX CONCURRENTLY over a churned heap: the multi-snapshot
/// build + heapam_index_validate_scan (the validate pass always runs),
/// then DROP INDEX CONCURRENTLY. A plain expression CREATE INDEX variant
/// drives heapam_index_build_range_scan over dead/recently-dead tuples.
fn gen_cic(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("heap:cic");
    let t = g.heap.tables[ti].name.clone();
    let rows = g.heap.tables[ti].rows;
    let (lo, hi) = band(g, rows);
    let iname = format!("fz_hpi_{}", g.heap.next_index);
    g.heap.next_index += 1;
    let churn = StmtKind::Raw(format!(
        "UPDATE {t} SET v = v + 1 WHERE pk BETWEEN {lo} AND {hi};"
    ));
    if g.weights.pick(g.rng, &["heap:cic:conc", "heap:cic:plain"]) == "heap:cic:conc" {
        g.fire("heap:cic:conc");
        vec![
            churn,
            StmtKind::Raw(format!("CREATE INDEX CONCURRENTLY {iname} ON {t} (v);")),
            StmtKind::Raw(format!("DROP INDEX CONCURRENTLY {iname};")),
        ]
    } else {
        g.fire("heap:cic:plain");
        vec![
            churn,
            StmtKind::Raw(format!(
                "CREATE INDEX {iname} ON {t} (((v + k) % 1000)) WHERE pk < 4000;"
            )),
            StmtKind::Raw(format!("DROP INDEX {iname};")),
        ]
    }
}

// --------------------------------------------------------------- cluster --

/// Rewrite arms over a DIRTY heap: churn first so
/// heapam_relation_copy_for_cluster meets recently-dead/aborted tuples
/// (the spill module clusters clean tables; the dead-tuple arms need this
/// ordering). CLUSTER sorts via the pk or k index; VACUUM FULL takes the
/// no-order rewrite. ANALYZE re-pins stats after the rewrite.
fn gen_cluster(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("heap:cluster");
    let t = g.heap.tables[ti].name.clone();
    let rows = g.heap.tables[ti].rows;
    let kidx = g.heap.tables[ti].k_index.clone();
    let (lo, hi) = band(g, rows);
    let mut stmts = vec![
        StmtKind::Raw(format!(
            "UPDATE {t} SET v = v + 1 WHERE pk BETWEEN {lo} AND {hi};"
        )),
        StmtKind::Raw(format!(
            "DELETE FROM {t} WHERE pk BETWEEN {} AND {};",
            lo,
            lo + 12
        )),
    ];
    let shape = g.weights.pick(g.rng, &["heap:cluster:pk", "heap:cluster:k", "heap:cluster:full"]);
    g.fire(shape);
    match shape {
        "heap:cluster:pk" => stmts.push(StmtKind::Raw(format!("CLUSTER {t} USING {t}_pkey;"))),
        "heap:cluster:k" => stmts.push(StmtKind::Raw(format!("CLUSTER {t} USING {kidx};"))),
        _ => stmts.push(StmtKind::Raw(format!("VACUUM FULL {t};"))),
    }
    stmts.push(StmtKind::Raw(format!("ANALYZE {t};")));
    stmts.push(StmtKind::Raw(format!(
        "INSERT INTO {t} {} ON CONFLICT (pk) DO NOTHING;",
        row_source(lo as i64, (lo + 12) as i64)
    )));
    stmts
}

// ------------------------------------------------------------------- tid --

/// TID scans (heap_fetch / the TidRangeScan fetch loop). Physical
/// placement is a ruled non-surface, so probes project count(*) only —
/// identical single-session streams should still place identically, and a
/// count divergence is a countable row-set signal, triaged against the
/// COPY-order ruling.
fn gen_tid(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("heap:tid");
    let t = g.heap.tables[ti].name.clone();
    let shape = g.weights.pick(g.rng, &["heap:tid:eq", "heap:tid:range"]);
    g.fire(shape);
    let body = match shape {
        "heap:tid:eq" => {
            let b = g.rng.below(8);
            let i = 1 + g.rng.below(40);
            format!("SELECT count(*) FROM {t} WHERE ctid = '({b},{i})';")
        }
        _ => {
            let b = g.rng.below(6);
            format!(
                "SELECT count(*) FROM {t} WHERE ctid >= '({b},1)' AND ctid < '({},1)';",
                b + 2
            )
        }
    };
    vec![StmtKind::Raw(body)]
}

// ---------------------------------------------------------------- serial --

/// Serializable-read entries (HeapCheckForSerializableConflictOut runs on
/// every heap read under SSI, across the tuple-state switch): a
/// single-session SERIALIZABLE bracket reading churned/aborted tuple
/// states, with an optional small write before COMMIT.
fn gen_serial(g: &mut Gen) -> Vec<StmtKind> {
    let ti = need_table!(g);
    g.fire("heap:serial");
    let t = g.heap.tables[ti].name.clone();
    let rows = g.heap.tables[ti].rows;
    let (lo, hi) = band(g, rows);
    let mut stmts = vec![
        StmtKind::Raw("BEGIN ISOLATION LEVEL SERIALIZABLE;".to_string()),
        probe(&t, lo, hi),
    ];
    if g.rng.chance(1, 2) {
        g.fire("heap:serial:write");
        stmts.push(StmtKind::Raw(format!(
            "UPDATE {t} SET v = v + 1 WHERE pk BETWEEN {lo} AND {};",
            lo + 10
        )));
    }
    if g.rng.chance(1, 4) {
        g.fire("heap:serial:deferrable");
        stmts.push(StmtKind::Raw("ROLLBACK;".to_string()));
        stmts.push(StmtKind::Raw(
            "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY;".to_string(),
        ));
        stmts.push(probe(&t, lo, hi));
        stmts.push(StmtKind::Raw("COMMIT;".to_string()));
    } else {
        stmts.push(StmtKind::Raw("COMMIT;".to_string()));
    }
    stmts
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_actions(seed: u64, n: usize) -> Vec<Vec<StmtKind>> {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let weights = WeightTable::defaults();
        let mut rng = Rng::new(seed);
        let mut state = HeapState::new();
        let mut out = Vec::new();
        for _ in 0..n {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &weights, &mut prods, 4);
            std::mem::swap(&mut g.heap, &mut state);
            let stmts = gen_heap_module(&mut g);
            std::mem::swap(&mut g.heap, &mut state);
            out.push(stmts);
        }
        out
    }

    fn flat(seed: u64, n: usize) -> Vec<String> {
        gen_actions(seed, n)
            .into_iter()
            .flatten()
            .map(|k| k.to_sql())
            .collect()
    }

    #[test]
    fn deterministic_and_seed_sensitive() {
        assert_eq!(flat(5, 100), flat(5, 100));
        assert_ne!(flat(5, 100), flat(6, 100));
    }

    #[test]
    fn brackets_and_transactions_are_balanced() {
        for group in gen_actions(7, 600) {
            let mut open_sets: Vec<String> = Vec::new();
            let mut in_txn = false;
            let mut open_sps: Vec<()> = Vec::new();
            for k in &group {
                let sql = k.to_sql();
                if sql.starts_with("BEGIN") {
                    assert!(!in_txn, "nested BEGIN: {sql}");
                    in_txn = true;
                } else if sql == "COMMIT;" || sql == "ROLLBACK;" {
                    assert!(in_txn, "{sql} outside txn");
                    in_txn = false;
                    open_sps.clear();
                } else if sql.starts_with("SAVEPOINT ") {
                    assert!(in_txn, "SAVEPOINT outside txn: {sql}");
                    open_sps.push(());
                } else if let Some(rest) = sql.strip_prefix("SET ") {
                    let name = rest.split([' ', '=']).next().unwrap().to_string();
                    open_sets.push(name);
                } else if let Some(rest) = sql.strip_prefix("RESET ") {
                    let name = rest.trim_end_matches(';').to_string();
                    let pos = open_sets.iter().rposition(|n| *n == name);
                    assert!(pos.is_some(), "RESET {name} without SET in group");
                    open_sets.remove(pos.unwrap());
                }
            }
            assert!(open_sets.is_empty(), "unclosed SETs at group end: {open_sets:?}");
            assert!(!in_txn, "unclosed transaction bracket at group end: {:?}",
                group.iter().map(|k| k.to_sql()).collect::<Vec<_>>());
        }
    }

    #[test]
    fn vacuum_never_inside_txn_bracket() {
        for group in gen_actions(11, 600) {
            let mut in_txn = false;
            for k in &group {
                let sql = k.to_sql();
                if sql.starts_with("BEGIN") {
                    in_txn = true;
                } else if sql == "COMMIT;" || sql == "ROLLBACK;" {
                    in_txn = false;
                }
                if sql.starts_with("VACUUM") || sql.starts_with("CLUSTER")
                    || sql.contains("CONCURRENTLY")
                {
                    assert!(!in_txn, "utility statement inside txn bracket: {sql}");
                }
            }
        }
    }

    #[test]
    fn row_returning_selects_are_totally_ordered() {
        // Any SELECT projecting pk rows (not aggregate-wrapped) carries
        // ORDER BY pk.
        for sql in flat(13, 700) {
            if sql.starts_with("SELECT pk,") {
                assert!(sql.contains("ORDER BY pk"), "unordered row-returning SELECT: {sql}");
            }
        }
    }

    #[test]
    fn no_float_surface() {
        for sql in flat(17, 700) {
            for bad in ["avg(", "::float", "::double", "random("] {
                assert!(!sql.contains(bad), "float/nondeterministic surface: {sql}");
            }
        }
    }

    #[test]
    fn toast_band_is_bounded_and_payloads_deterministic() {
        for sql in flat(19, 700) {
            if sql.contains("string_agg(md5(") {
                assert!(sql.contains("ORDER BY g"), "unordered toast chain: {sql}");
            }
            // Toast writes stay on the low band.
            if (sql.contains("SET txt = repeat") || sql.contains("SET ext = (SELECT"))
                && sql.contains("BETWEEN")
            {
                let hi: u32 = sql
                    .rsplit(" AND ")
                    .next()
                    .unwrap()
                    .trim_end_matches(';')
                    .trim()
                    .parse()
                    .unwrap();
                assert!(hi <= TOAST_BAND, "toast write outside band: {sql}");
            }
        }
    }

    #[test]
    fn bulk_loads_respect_analyze_cap() {
        // Committed base loads stay <= 24000 rows (bursts are deleted in
        // the same group; base loads are <= 6000).
        for sql in flat(23, 500) {
            if sql.starts_with("INSERT INTO fz_hp_") && !sql.contains("ON CONFLICT") {
                let args = sql.split("generate_series(").nth(1).unwrap();
                let lo: i64 = args.split(',').next().unwrap().trim().parse().unwrap();
                let hi_s = args.split(',').nth(1).unwrap().trim();
                let digits: String = hi_s.chars().take_while(|c| c.is_ascii_digit()).collect();
                let hi: i64 = digits.parse().unwrap();
                assert!(hi - lo < 24000, "bulk load exceeds ANALYZE cap: {sql}");
            }
        }
    }

    #[test]
    fn tid_probes_project_count_only() {
        for sql in flat(29, 700) {
            if sql.contains("ctid") {
                assert!(
                    sql.starts_with("SELECT count(*) FROM"),
                    "TID probe must project count(*) only (heap order is a \
                     ruled non-surface): {sql}"
                );
            }
        }
    }

    #[test]
    fn cluster_targets_only_known_indexes_and_reanalyzes() {
        for group in gen_actions(31, 800) {
            let sqls: Vec<String> = group.iter().map(|k| k.to_sql()).collect();
            for (i, sql) in sqls.iter().enumerate() {
                if let Some(rest) = sql.strip_prefix("CLUSTER ") {
                    let mut it = rest.trim_end_matches(';').split(" USING ");
                    let table = it.next().unwrap();
                    let index = it.next().expect("CLUSTER without USING");
                    assert!(
                        index == format!("{table}_pkey") || index.starts_with("fz_hpi_"),
                        "CLUSTER on unknown index: {sql}"
                    );
                    assert!(
                        sqls[i + 1..].iter().any(|s| s.starts_with("ANALYZE ")),
                        "CLUSTER without trailing ANALYZE: {sqls:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn concurrent_index_lifecycles_are_paired() {
        for group in gen_actions(37, 800) {
            let sqls: Vec<String> = group.iter().map(|k| k.to_sql()).collect();
            let creates = sqls
                .iter()
                .filter(|s| s.starts_with("CREATE INDEX"))
                .count();
            let drops = sqls.iter().filter(|s| s.starts_with("DROP INDEX")).count();
            // Birth index (create group) has no drop; cic groups pair 1:1.
            if sqls.iter().any(|s| s.starts_with("CREATE TABLE")) {
                assert_eq!(drops, 0);
            } else {
                assert_eq!(creates, drops, "unpaired index lifecycle: {sqls:?}");
            }
        }
    }

    #[test]
    fn all_families_fire() {
        let stmts = flat(3, 2500).join("\n");
        for needle in [
            "CREATE TABLE fz_hp_",
            "fillfactor = 30",
            "fillfactor = 100",
            "toast_tuple_target = 256",
            "autovacuum_enabled = off",
            "SET STORAGE EXTERNAL",
            "SET STORAGE MAIN",
            "SET STORAGE PLAIN",
            "REPLICA IDENTITY FULL",
            "VACUUM fz_hp_",
            "VACUUM (FREEZE)",
            "VACUUM (VERBOSE)",
            "VACUUM (DISABLE_PAGE_SKIPPING)",
            "VACUUM (INDEX_CLEANUP OFF)",
            "VACUUM (INDEX_CLEANUP ON)",
            "VACUUM (TRUNCATE ON)",
            "VACUUM FULL fz_hp_",
            "SET vacuum_freeze_min_age = 0",
            "SET vacuum_freeze_table_age = 0",
            "CLUSTER fz_hp_",
            "CREATE INDEX CONCURRENTLY",
            "DROP INDEX CONCURRENTLY",
            "FOR KEY SHARE",
            "FOR SHARE",
            "FOR NO KEY UPDATE",
            "FOR UPDATE",
            "SAVEPOINT sl1;",
            "ROLLBACK TO SAVEPOINT",
            "RELEASE SAVEPOINT",
            "repeat('ab',",
            "string_agg(md5(",
            "substr(",
            "pg_column_size(",
            "SET txt = txt || md5(pk::text)",
            "SET txt = NULL",
            "ctid = '(",
            "ctid >= '(",
            "ISOLATION LEVEL SERIALIZABLE",
            "SERIALIZABLE READ ONLY",
            "SET pk = pk + 1000000",
            "ON CONFLICT (pk) DO NOTHING",
            "DELETE FROM fz_hp_",
        ] {
            assert!(stmts.contains(needle), "family never fired in 2500 groups: {needle}");
        }
    }
}
