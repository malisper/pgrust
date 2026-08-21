//! derived-key fold stencil (DerivedKeyFold): GROUP BY f(varlena col)
//! where f is a derivation law named by the plan's KeyExpr — resolved
//! ONCE PER DICT ENTRY, never per-row strings on dict parts (ENGINE-PLAN
//! identity tier: "derived key -> per-entry resolve + owned fold").
//!
//! Parametrized from the hot-shape elected kernel `domain_owned3_par`
//! (kernels.rs:2049, branch -hot-shape) — all three 96T-wall levers kept:
//!   1. unit grain skew law: dict parts are WHOLE-PART units (per-code
//!      counts via code_zone/count_into, then one entry-grain resolve);
//!      non-dict parts explode to GRANULE units (one worker grinding a
//!      part through a byte-key map was the measured wall);
//!   2. worker state (counts, direct-mapped pre-agg cache, radix buckets,
//!      side arenas, pass-2 tables) is rep-persistent in a process arena
//!      (page-fault floor law) — contents recomputed every call;
//!   3. PARALLEL worker-owned teardown (serial drop of the key Vecs +
//!      dict pins was a hidden ~145ms/rep tail).
//!
//! NO condition-cache legs: the only residue (`col <> ''`) evaluates at
//! DICT-ENTRY grain right next to the code count — a granule verdict
//! plane has nothing to remove (the planner ships an empty goal, and the
//! NeEmpty promotion deliberately excludes this family).
//!
//! Elections (functions over bank stats, all pre-rep):
//!   - partition count: planner::partition_count over the stats-face NDV
//!     estimate (floor's 256 is the clamp floor of the same law);
//!   - unit grain per part: dict publication (the skew law above);
//!   - thread claim: full pool width (bandwidth-bound fold; the node's
//!     BoundedWalk class tag is advisory for walks, not for folds —
//!     thread_claim in Params overrides when the planner sets it).

use crate::answer::{AnswerCol, AnswerSet, BytesBuild, ColData, Validity};
use crate::engine::SqeCtx;
use crate::fused::FusedCodes;
use crate::grouped::hash_bytes;
use crate::ir::{AggOp, KeyExpr, OrderBy, PlanNode};
use crate::kernels::referer_key;
use crate::planner::partition_count;
use crate::scan::{dict_handle, open_cursor, varlena_payload, CurCache, Scratch};
use crate::typmeta::TypMeta;
use std::collections::HashMap;
use std::sync::Mutex;

/// The derivation library, named by the plan (data, not query identity).
fn derive_fn(k: &KeyExpr) -> fn(&[u8]) -> &[u8] {
    match k {
        KeyExpr::HostRegex(_) => referer_key,
        other => panic!("derived_key_fold: no derivation law for {other:?}"),
    }
}

#[derive(Clone, Debug)]
struct Acc {
    c: u64,
    sum_len: u64,
    min_ref: Vec<u8>,
}

#[derive(Clone, Copy)]
struct Item {
    h: u64,
    kptr: usize,
    klen: u32,
    mptr: usize,
    mlen: u32,
    cnt: u64,
    sum_len: u64,
}
const ZI: Item = Item { h: 0, kptr: 0, klen: 0, mptr: 0, mlen: 0, cnt: 0, sum_len: 0 };

#[inline(always)]
unsafe fn sl<'x>(p: usize, l: u32) -> &'x [u8] {
    std::slice::from_raw_parts(p as *const u8, l as usize)
}

/// Direct-mapped pre-agg cache size (L2-sized; the floor's PRE).
const PRE: usize = 1 << 14;

/// Pass-1 worker state (rep-persistent via the arena — PLAIN HEAP only).
struct S1 {
    /// Per-engagement decode state [join-depot]: fetched fresh in init,
    /// parked/dropped at the pass-1 finish — NEVER at rest in the arena.
    /// The arena is keyed only by (col, p) with NO bank identity, so a
    /// parked CurCache (keyed by part index) could resurface another
    /// bank's stream on a same-column same-width query — the distinct.rs
    /// wrong-answer class.
    s: Option<Scratch>,
    codes: Vec<u32>,
    counts: Vec<u32>,
    pre: Vec<Item>,
    buckets: Vec<Vec<Item>>,
    dhs: Vec<pgrc2_read::dicthandle::DictHandle>,
    side: Vec<Vec<u8>>,
    local: HashMap<Vec<u8>, Acc>,
    nd_cur: Option<CurCache>,
}

/// Pass-2 owner workspace (rep-persistent; only occupancy re-zeroed).
struct S2 {
    th: Vec<u64>,
    tk: Vec<(usize, u32)>,
    tm: Vec<(usize, u32)>,
    tc: Vec<u64>,
    ts: Vec<u64>,
    out: Vec<(Vec<u8>, Acc)>,
}

/// Process arena: worker states keyed by slot (per-process scratch — the
/// PoC clear-choreography carried until P1-2 rehomes it into per-worker
/// pool arenas; risks.md §1 deviation note).
struct Arena {
    col: u32,
    p: usize,
    s1: Vec<Option<S1>>,
    s2: Vec<Option<S2>>,
}

static ARENA: Mutex<Option<Arena>> = Mutex::new(None);

pub fn run_derived_key_fold(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    let (bank, pool) = (ctx.bank, ctx.pool);
    let col = node.cols[0];
    assert!(
        node.params.key_exprs.len() == 1,
        "derived_key_fold: exactly one derived key"
    );
    let derive = derive_fn(&node.params.key_exprs[0]);
    // `col <> ''` residue: entry/payload grain (empty payload skipped) —
    // the family law; asserted present so a plan without it is caught.
    let drop_empty = node.params.ne_empty_cols.contains(&col);
    let having = node.params.having_min_count;

    // ---- elections (pre-rep; stats face is a standing face) --------------
    let sv = ctx.faces.stats(bank, col);
    let p = partition_count(
        sv.ndv_est_sum().max(1) as usize,
        node.params.slot_bytes,
        node.params.l2_bytes,
        ctx.pool.threads(),
    )
    .min(1 << 16);
    let pshift = 64 - p.trailing_zeros();
    // pool.run always engages every resident worker; arenas are sized to
    // the pool width (nw). The BoundedWalk class tag on this family is a
    // walk law, not a fold law — the fold is bandwidth-bound (Counting).
    let nw = pool.threads();

    // Unit list per the skew law: (part, granule or -1 = whole dict part).
    // [coldstart] the dict-ness census rides the part-parallel dict faces
    // (511 serial cursor opens were a cold-rep tax on the driving thread).
    let t_units = std::time::Instant::now();
    let dfs = ctx.faces.dicts_all(bank, col);
    ctx.faces.prewarm_payloads(&dfs);
    let units: Vec<(usize, i64)> = {
        let mut u = Vec::new();
        for pi in 0..bank.parts.len() {
            if dfs[pi].dh.is_some() {
                u.push((pi, -1i64));
            } else {
                let cur = open_cursor(bank, pi, col);
                for g in 0..cur.granule_count() {
                    u.push((pi, g as i64));
                }
            }
        }
        u
    };

    crate::engine::phn(node, "units", t_units);
    let t_p1 = std::time::Instant::now();
    // ---- arena checkout ---------------------------------------------------
    let (mut s1v, mut s2v) = {
        let mut a = ARENA.lock().unwrap();
        match a.take() {
            Some(ar) if ar.col == col && ar.p == p && ar.s1.len() == nw => (ar.s1, ar.s2),
            _ => (
                (0..nw).map(|_| None).collect::<Vec<Option<S1>>>(),
                (0..nw).map(|_| None).collect::<Vec<Option<S2>>>(),
            ),
        }
    };
    let s1slots: Vec<Mutex<Option<S1>>> = s1v.drain(..).map(Mutex::new).collect();
    let s2slots: Vec<Mutex<Option<S2>>> = s2v.drain(..).map(Mutex::new).collect();

    // ---- pass 1: per-unit fold -------------------------------------------
    let mut states = pool.run_finish(
        units.len(),
        |w| {
            let mut st = s1slots[w].lock().unwrap().take().unwrap_or_else(|| S1 {
                s: None,
                codes: vec![0; 8192],
                counts: Vec::new(),
                pre: vec![ZI; PRE],
                buckets: (0..p).map(|_| Vec::new()).collect(),
                dhs: Vec::new(),
                side: Vec::new(),
                local: HashMap::new(),
                nd_cur: None,
            });
            // Per-engagement decode state: depot scratch + fresh cursor
            // (never from the arena — see the S1 field doc).
            debug_assert!(
                st.s.is_none() && st.nd_cur.is_none(),
                "decode state must never rest in the arena"
            );
            st.s = Some(crate::scan::scratch_fetch());
            st.nd_cur = Some(CurCache::new(col));
            st
        },
        |st, ui| {
            let (pi, gsel) = units[ui];
            if gsel < 0 {
                // Whole dict part: per-code counts (const-code zones via
                // code_zone, else count_into / decode_codes), then ONE
                // entry-grain resolve + pre-agg + radix scatter.
                let dh = dict_handle(bank, pi, col);
                let n = dh.ncodes() as usize;
                if st.counts.len() < n {
                    st.counts.resize(n, 0);
                }
                st.counts[..n].fill(0);
                let counts = &mut st.counts;
                let fc = FusedCodes::open(bank, pi, col);
                let mut cur = open_cursor(bank, pi, col);
                for g in 0..cur.granule_count() {
                    let rows = cur.rows_in_granule(g) as usize;
                    match &fc {
                        Some(fc) => {
                            let (zbase, zw) = fc.code_zone(g);
                            if zw == 0 {
                                counts[zbase as usize] += rows as u32;
                            } else {
                                fc.count_into(g, rows, counts);
                            }
                        }
                        None => {
                            if st.codes.len() < rows {
                                st.codes.resize(rows, 0);
                            }
                            cur.decode_codes(g, &mut st.codes[..rows]).expect("codes");
                            for r in 0..rows {
                                counts[st.codes[r] as usize] += 1;
                            }
                        }
                    }
                }
                st.pre.fill(ZI);
                let census_n = st.counts[..n].iter().filter(|&&c| c != 0).count() as u64;
                crate::engine::census_entries(census_n);
                for c in 0..n {
                    if st.counts[c] == 0 {
                        continue;
                    }
                    let e = dh.entry(c as u32).expect("dict entry");
                    if drop_empty && e.bytes.is_empty() {
                        continue;
                    }
                    let k = derive(e.bytes);
                    let h = hash_bytes(k);
                    let cnt = st.counts[c] as u64;
                    let it = Item {
                        h,
                        kptr: k.as_ptr() as usize,
                        klen: k.len() as u32,
                        mptr: e.bytes.as_ptr() as usize,
                        mlen: e.bytes.len() as u32,
                        cnt,
                        sum_len: e.bytes.len() as u64 * cnt,
                    };
                    let slot = &mut st.pre[(h as usize) & (PRE - 1)];
                    if slot.cnt == 0 {
                        *slot = it;
                    } else if slot.h == h && unsafe { sl(slot.kptr, slot.klen) } == k {
                        slot.cnt += cnt;
                        slot.sum_len += it.sum_len;
                        if e.bytes < unsafe { sl(slot.mptr, slot.mlen) } {
                            slot.mptr = it.mptr;
                            slot.mlen = it.mlen;
                        }
                    } else {
                        let old = std::mem::replace(slot, it);
                        st.buckets[(old.h >> pshift) as usize].push(old);
                    }
                }
                for i in 0..PRE {
                    let it = st.pre[i];
                    if it.cnt != 0 {
                        st.buckets[(it.h >> pshift) as usize].push(it);
                    }
                }
                st.dhs.push(dh);
            } else {
                // Non-dict GRANULE unit: worker-local byte-key fold.
                let g = gsel as u32;
                let cur = st.nd_cur.as_mut().unwrap().get(bank, pi);
                let rows = cur.rows_in_granule(g) as usize;
                let d = st.s.as_mut().unwrap().decode_full(cur, g, rows);
                for &x in d {
                    let pbytes = unsafe { varlena_payload(x) };
                    if drop_empty && pbytes.is_empty() {
                        continue;
                    }
                    let k = derive(pbytes);
                    match st.local.get_mut(k) {
                        Some(gr) => {
                            gr.c += 1;
                            gr.sum_len += pbytes.len() as u64;
                            if pbytes < gr.min_ref.as_slice() {
                                gr.min_ref = pbytes.to_vec();
                            }
                        }
                        None => {
                            st.local.insert(
                                k.to_vec(),
                                Acc {
                                    c: 1,
                                    sum_len: pbytes.len() as u64,
                                    min_ref: pbytes.to_vec(),
                                },
                            );
                        }
                    }
                }
            }
        },
        // Worker-side finish: the scratch parks on THIS worker's depot,
        // the cursor drops — the arena stores plain heap only.
        |mut st| {
            if let Some(s) = st.s.take() {
                crate::scan::scratch_park(s);
            }
            st.nd_cur = None;
            st
        },
    );

    crate::engine::phn(node, "pass1", t_p1);
    let t_p2 = std::time::Instant::now();
    // ---- emit worker-local non-dict groups into own buckets (parallel) ----
    {
        let mstates: Vec<Mutex<&mut S1>> = states.iter_mut().map(Mutex::new).collect();
        pool.run(
            mstates.len(),
            |_| (),
            |_, w| {
                let mut s = mstates[w].lock().unwrap();
                let st: &mut S1 = &mut **s;
                let drained: Vec<(Vec<u8>, Acc)> = st.local.drain().collect();
                for (k, g) in drained {
                    let h = hash_bytes(&k);
                    let b = (h >> pshift) as usize;
                    st.side.push(k);
                    let kref = st.side.last().unwrap();
                    let (kptr, klen) = (kref.as_ptr() as usize, kref.len() as u32);
                    st.side.push(g.min_ref);
                    let mref2 = st.side.last().unwrap();
                    st.buckets[b].push(Item {
                        h,
                        kptr,
                        klen,
                        mptr: mref2.as_ptr() as usize,
                        mlen: mref2.len() as u32,
                        cnt: g.c,
                        sum_len: g.sum_len,
                    });
                }
            },
        );
    }

    // ---- pass 2: one owner per partition (open-addressed fold) -----------
    let wss = pool.run(
        p,
        |w| {
            s2slots
                .get(w)
                .and_then(|s| s.lock().unwrap().take())
                .unwrap_or_else(|| S2 {
                    th: Vec::new(),
                    tk: Vec::new(),
                    tm: Vec::new(),
                    tc: Vec::new(),
                    ts: Vec::new(),
                    out: Vec::new(),
                })
        },
        |ws, part| {
            let n: usize = states.iter().map(|s| s.buckets[part].len()).sum();
            let cap = (n * 2).next_power_of_two().max(16);
            let mask = cap - 1;
            if ws.th.len() < cap {
                ws.th.resize(cap, 0);
                ws.tk.resize(cap, (0, 0));
                ws.tm.resize(cap, (0, 0));
                ws.tc.resize(cap, 0);
                ws.ts.resize(cap, 0);
            }
            ws.tc[..cap].fill(0);
            for s in states.iter() {
                for &it in &s.buckets[part] {
                    let key = unsafe { sl(it.kptr, it.klen) };
                    let mut i = (it.h as usize) & mask;
                    loop {
                        if ws.tc[i] == 0 {
                            ws.th[i] = it.h;
                            ws.tk[i] = (it.kptr, it.klen);
                            ws.tm[i] = (it.mptr, it.mlen);
                            ws.tc[i] = it.cnt;
                            ws.ts[i] = it.sum_len;
                            break;
                        }
                        if ws.th[i] == it.h && unsafe { sl(ws.tk[i].0, ws.tk[i].1) } == key {
                            ws.tc[i] += it.cnt;
                            ws.ts[i] += it.sum_len;
                            let cand = unsafe { sl(it.mptr, it.mlen) };
                            if cand < unsafe { sl(ws.tm[i].0, ws.tm[i].1) } {
                                ws.tm[i] = (it.mptr, it.mlen);
                            }
                            break;
                        }
                        i = (i + 1) & mask;
                    }
                }
            }
            // HAVING applied per partition (exact: partitions split the
            // key space) — only survivors leave as owned rows.
            for i in 0..cap {
                if ws.tc[i] > having {
                    ws.out.push((
                        unsafe { sl(ws.tk[i].0, ws.tk[i].1) }.to_vec(),
                        Acc {
                            c: ws.tc[i],
                            sum_len: ws.ts[i],
                            min_ref: unsafe { sl(ws.tm[i].0, ws.tm[i].1) }.to_vec(),
                        },
                    ));
                }
            }
        },
    );

    let mut rows: Vec<(Vec<u8>, Acc)> = Vec::new();
    let mut s2back: Vec<Option<S2>> = Vec::with_capacity(p);
    for mut ws in wss {
        rows.append(&mut ws.out);
        s2back.push(Some(ws));
    }

    crate::engine::phn(node, "pass2", t_p2);
    let t_p3 = std::time::Instant::now();
    // ---- parallel worker-owned teardown (the hidden-tail lever) ----------
    {
        let mstates: Vec<Mutex<&mut S1>> = states.iter_mut().map(Mutex::new).collect();
        pool.run(
            mstates.len(),
            |_| (),
            |_, w| {
                let mut sg = mstates[w].lock().unwrap();
                let st: &mut S1 = &mut **sg;
                for b in st.buckets.iter_mut() {
                    b.clear();
                }
                st.side.clear();
                st.dhs.clear();
            },
        );
    }
    {
        let mut a = ARENA.lock().unwrap();
        *a = Some(Arena {
            col,
            p,
            s1: states.into_iter().map(Some).collect(),
            s2: s2back,
        });
    }

    crate::engine::phn(node, "teardown", t_p3);
    render(node, rows)
}

/// Generic render: line = esc(key) + one value per agg (plan order);
/// ORDER BY agg[idx] DESC with the exact cross-multiplied average law
/// (floor: q28_render, kernels.rs:1153), canonical key tie-break.
/// OrderBy::None (the server path: order/HAVING/slice apply shell-side)
/// emits the FULL group set in canonical key order — deterministic
/// across the parallel partition fold, no output-order obligation.
fn render(node: &PlanNode, mut rows: Vec<(Vec<u8>, Acc)>) -> AnswerSet {
    match node.params.order {
        OrderBy::AggDesc(idx) => {
            assert_eq!(
                node.agg[idx as usize].op,
                AggOp::AvgLen,
                "derived_key_fold: AggDesc order supported over AvgLen"
            );
            rows.sort_by(|a, b| {
                let l = (b.1.sum_len as u128 * a.1.c as u128)
                    .cmp(&(a.1.sum_len as u128 * b.1.c as u128));
                l.then_with(|| a.0.cmp(&b.0))
            });
        }
        OrderBy::None => rows.sort_by(|a, b| a.0.cmp(&b.0)),
        other => panic!("derived_key_fold: unsupported order {other:?}"),
    }
    let rows: Vec<(Vec<u8>, Acc)> = rows
        .into_iter()
        .skip(node.params.offset)
        .take(node.params.limit)
        .collect();
    // Typed emit: derived key bytes, then aggs in plan order.
    let mut kb = BytesBuild::new();
    for (k, _) in rows.iter() {
        kb.push(k);
    }
    let key_ty = node.ty_of(node.cols[0]);
    let mut cols: Vec<AnswerCol> = vec![kb.finish(key_ty)];
    for a in &node.agg {
        match a.op {
            AggOp::AvgLen => cols.push(AnswerCol {
                ty: a.out,
                data: ColData::Ratio {
                    pairs: rows.iter().map(|(_, g)| (g.sum_len as i128, g.c as i64)).collect(),
                    exact: false,
                },
                validity: Validity::AllValid,
            }),
            AggOp::CountStar => cols.push(AnswerCol::i64s(
                TypMeta::INT8,
                rows.iter().map(|(_, g)| g.c as i64).collect(),
            )),
            AggOp::MinBytes => {
                let mut b = BytesBuild::new();
                for (_, g) in rows.iter() {
                    b.push(&g.min_ref);
                }
                cols.push(b.finish(a.out));
            }
            other => panic!("derived_key_fold: unsupported agg {other:?}"),
        }
    }
    AnswerSet::from_cols(cols)
}
