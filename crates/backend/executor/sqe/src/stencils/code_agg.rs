//! famA dense-int two-level code agg (hot-shape lineage), routed from
//! `two_level::run_two_level_code_agg` when the group key is BYVAL:
//! level 1 folds the agg in the CODE domain (per-part dict entry-length
//! tables via byte_len_only — index-only, the varlena column never
//! hydrates); level 2 is the stats-bounded dense domain (vector-add,
//! chunk-parallel merge). Zone-constant fast path: a granule whose group
//! zone has min == max folds as ONE group without decoding the key lane.
//! Per-worker dense arrays are rep-persistent; the merge zeroes-as-it-
//! reads (the cid_zone2 lifecycle law).

use crate::answer::{AnswerCol, AnswerSet, ColData, Validity};
use crate::engine::SqeCtx;
use crate::ir::*;
use crate::scan::{CurCache, Scratch};
use crate::stencils::{col_width, sx};
use crate::typmeta::TypMeta;

static PREP_GEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// [ruling] per-query-run: invalidate the dense prep memo.
pub fn clear_prep() {
    PREP_GEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// [sqe-avglen] The entrylen dense authority: `Some((lo, slots))` iff the
/// key's EXACT stats domain fits this arm's measured dense-array budget
/// (u64 sum + u32 count per slot — its own cap, not the count-array law).
/// The family election, the server admission gate, and the prep below all
/// read THIS function (the one-authority law).
pub fn entrylen_dense_bounds(sv: &crate::statsview::StatsView) -> Option<(i64, usize)> {
    match sv.minmax_exact() {
        Some((lo, hi)) if (hi as i128 - lo as i128) < (1 << 22) => {
            Some((lo, (hi - lo + 1) as usize))
        }
        _ => None,
    }
}

pub fn dense_int_entrylen(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    let (bank, pool) = (ctx.bank, ctx.pool);
    let a_key = node.params.group_cols[0];
    let len_agg = node
        .agg
        .iter()
        .find(|a| a.op.is_avglen())
        .expect("dense_int code_agg: avg-length agg");
    // [q27-charlen] the one kernel split: AvgCharLen folds stored UTF-8
    // char counts (dict index char_len field / payload lead-byte walk),
    // AvgLen folds byte lengths. Everything else is shared verbatim.
    let charmode = len_agg.op == AggOp::AvgCharLen;
    let a_text = len_agg.col.expect("dense_int code_agg: avg-length input");
    let units = ctx.faces.walk(bank, a_key);
    let tp = pool.threads();

    // standing prep (honest-hot cached, the cid_zone2/condcache precedent):
    // entry-length tables, zone-constant keys, dense bounds.
    struct Prep {
        lens: Vec<Option<Vec<u32>>>,
        zc: Vec<i64>,
        dlo: i64,
        dn: usize,
    }
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, OnceLock};
    static PREP: OnceLock<Mutex<HashMap<u32, (u64, Arc<Prep>)>>> = OnceLock::new();
    // [ruling] per-query-run: a generation bump (clear_prep) invalidates.
    let gen = PREP_GEN.load(std::sync::atomic::Ordering::Relaxed);
    let prep = {
        let memo = PREP.get_or_init(|| Mutex::new(HashMap::new()));
        let mut m = memo.lock().unwrap();
        // [persist-rehome] eager purge: stale-generation entries used to
        // linger until their own q was re-hit (an unbounded leak across
        // queries); the per-query-run ruling now evicts them all here.
        m.retain(|_, e| e.0 == gen);
        m.entry(node.q)
            .or_insert_with(|| {
                (gen, {
                let lens_states = pool.run(
                    bank.parts.len(),
                    |_| Vec::new(),
                    |acc: &mut Vec<(usize, Option<Vec<u32>>)>, pi| {
                        let df = ctx.faces.dict(bank, pi, a_text);
                        let l = df.dh.as_ref().map(|dh| {
                            let n = df.ncodes as usize;
                            let mut lens = vec![0u32; n];
                            for ci in 0..n {
                                lens[ci] = if charmode {
                                    dh.lengths(ci as u32).expect("char_len").1
                                } else {
                                    dh.byte_len_only(ci as u32).expect("byte_len")
                                };
                            }
                            lens
                        });
                        acc.push((pi, l));
                    },
                );
                let mut lens: Vec<Option<Vec<u32>>> = (0..bank.parts.len()).map(|_| None).collect();
                for st in lens_states {
                    for (pi, l) in st {
                        lens[pi] = l;
                    }
                }
                let sv = ctx.faces.stats(bank, a_key);
                let (dlo, dn) = entrylen_dense_bounds(&sv).unwrap_or((0, 0));
                let zc: Vec<i64> = units
                    .iter()
                    .map(|&(pi, g, _, _)| {
                        sv.zone_exact(pi, g)
                            .filter(|(lo, hi)| lo == hi)
                            .map(|(lo, _)| lo)
                            .unwrap_or(i64::MIN)
                    })
                    .collect();
                Arc::new(Prep { lens, zc, dlo, dn })
                })
            })
            .1
            .clone()
    };
    let (dlo, dn) = (prep.dlo, prep.dn);
    assert!(dn != 0, "dense_int code_agg needs stats-elected dense bounds");

    struct S2 {
        su: Scratch,
        sc: Scratch,
        cu: CurCache,
        cc: CurCache,
        fc: crate::kernels_dec::FcCache,
        codes: Vec<u32>,
        dense_sl: Vec<u64>,
        dense_cnt: Vec<u32>,
    }
    // [persist-rehome] rep-persistent dense arrays + code buffers, parked
    // PLAIN-HEAP-ONLY under a byte cap (the former caller-TLS ARENA kept
    // whole S2 states — including CurCache/FcCache CURSORS — across
    // statements guarded only by (a_key, a_text): the distinct.rs
    // wrong-answer class, since a different bank with the same column
    // ids and dense width would resurface the old bank's streams).
    // Now: cursors are built per engagement and dropped at finish;
    // scratches ride the worker depot; only (codes, dense_sl, dense_cnt)
    // park, and the dense arrays park CLEAN (the merge zeroes-as-it-
    // reads; cancel/panic paths drop instead of parking).
    type CaPark = (Vec<u32>, Vec<u64>, Vec<u32>);
    static PARK_CA: crate::stencils::statepark::StatePark<CaPark> =
        crate::stencils::statepark::StatePark::new(256 << 20);
    let parked: std::sync::Mutex<Vec<CaPark>> =
        std::sync::Mutex::new(PARK_CA.fetch_up_to(tp));
    let p = &prep;
    let mut states = pool.run_finish(
        units.len(),
        |_| {
            let (codes, dense_sl, dense_cnt) = parked
                .lock()
                .unwrap()
                .pop()
                .filter(|(_, sl, _)| sl.len() == dn)
                .unwrap_or_else(|| (vec![0; 8192], vec![0u64; dn], vec![0u32; dn]));
            S2 {
                su: crate::scan::scratch_fetch(),
                sc: crate::scan::scratch_fetch(),
                cu: CurCache::new(a_text),
                cc: CurCache::new(a_key),
                fc: crate::kernels_dec::FcCache::new(a_text),
                codes,
                dense_sl,
                dense_cnt,
            }
        },
        |s, i| {
            let (pi, g, rows32, _) = units[i];
            let rows = rows32 as usize;
            let zcv = p.zc[i];
            if zcv != i64::MIN {
                if let Some(lens) = p.lens[pi].as_ref() {
                    if let Some(fc) = s.fc.get(bank, pi) {
                        let mut sum = 0u64;
                        let mut cnt = 0u32;
                        fc.fold(g, rows, |_, code| {
                            let len = lens[code as usize] as u64;
                            sum += len;
                            cnt += (len != 0) as u32;
                        });
                        let k = (zcv - dlo) as usize;
                        s.dense_sl[k] += sum;
                        s.dense_cnt[k] += cnt;
                        return;
                    }
                }
            }
            let cid = s.sc.decode_full(s.cc.get(bank, pi), g, rows);
            let cid: &[u64] = unsafe { std::slice::from_raw_parts(cid.as_ptr(), cid.len()) };
            match &p.lens[pi] {
                Some(lens) => {
                    let (sl, cn) = (&mut s.dense_sl, &mut s.dense_cnt);
                    match s.fc.get(bank, pi) {
                        Some(fc) => fc.fold(g, rows, |r, code| {
                            let len = lens[code as usize] as u64;
                            let k = (cid[r] as i64 - dlo) as usize;
                            sl[k] += len;
                            cn[k] += (len != 0) as u32;
                        }),
                        None => {
                            if s.codes.len() < rows {
                                s.codes.resize(rows, 0);
                            }
                            s.cu.get(bank, pi).decode_codes(g, &mut s.codes[..rows]).expect("codes");
                            for r in 0..rows {
                                let len = lens[s.codes[r] as usize] as u64;
                                let k = (cid[r] as i64 - dlo) as usize;
                                sl[k] += len;
                                cn[k] += (len != 0) as u32;
                            }
                        }
                    }
                }
                None => {
                    // hydrated fallback, 3VL-threaded: a NULL entry folds
                    // nothing (the `<> ''` law already excludes the row).
                    let cur = s.cu.get(bank, pi);
                    let gv = s.su.validity(cur, g, rows);
                    let all_valid = gv.all_valid();
                    let d = s.su.decode_full(cur, g, rows);
                    let d: &[u64] = unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) };
                    for (r, &x) in d.iter().enumerate() {
                        if !(all_valid || s.su.row_valid(r)) {
                            continue;
                        }
                        let pl = unsafe { crate::scan::varlena_payload(x) };
                        let len =
                            if charmode { crate::stencils::utf8_chars(pl) } else { pl.len() as u64 };
                        if len != 0 {
                            let k = (cid[r] as i64 - dlo) as usize;
                            s.dense_sl[k] += len;
                            s.dense_cnt[k] += 1;
                        }
                    }
                }
            }
        },
        // Worker-side finish: scratches to THIS worker's depot; cursors
        // drop here (never at rest); only plain heap crosses back.
        |s| {
            crate::scan::scratch_park(s.su);
            crate::scan::scratch_park(s.sc);
            (s.codes, s.dense_sl, s.dense_cnt)
        },
    );

    // chunk-parallel merge; zero-as-read leaves parked arrays clean.
    let mut sl = vec![0u64; dn];
    let mut cn = vec![0u64; dn];
    {
        struct SendPtr<T>(*mut T);
        unsafe impl<T> Send for SendPtr<T> {}
        unsafe impl<T> Sync for SendPtr<T> {}
        let slp = SendPtr(sl.as_mut_ptr());
        let cnp = SendPtr(cn.as_mut_ptr());
        let stp = SendPtr(states.as_mut_ptr());
        let nstates = states.len();
        const CHUNKS: usize = 256;
        let chunk = dn.div_ceil(CHUNKS);
        let (slp, cnp, stp) = (&slp, &cnp, &stp);
        pool.run(
            CHUNKS,
            |_| (),
            |_, c| {
                let lo = c * chunk;
                if lo >= dn {
                    return;
                }
                let hi = ((c + 1) * chunk).min(dn);
                // SAFETY: chunks own disjoint [lo, hi) ranges of the output
                // arrays and every state's dense slices.
                unsafe {
                    let sl = std::slice::from_raw_parts_mut(slp.0.add(lo), hi - lo);
                    let cn = std::slice::from_raw_parts_mut(cnp.0.add(lo), hi - lo);
                    for si in 0..nstates {
                        let st: &mut CaPark = &mut *stp.0.add(si);
                        for i in lo..hi {
                            sl[i - lo] += st.1[i];
                            cn[i - lo] += st.2[i] as u64;
                            st.1[i] = 0;
                            st.2[i] = 0;
                        }
                    }
                }
            },
        );
    }
    // Park under the byte cap: dense arrays are all-zero here (merge
    // zeroed-as-read), so nothing of this statement is at rest.
    for st in states.drain(..).chain(parked.into_inner().unwrap()) {
        let b = st.0.capacity() * 4 + st.1.capacity() * 8 + st.2.capacity() * 4;
        PARK_CA.park(st, b);
    }

    // render: HAVING count > v; rank per node order (AggDesc = exact
    // cross-mult avg compare), tie key ASC; aggs emit in plan order.
    let having = node.params.having_min_count;
    let mut rows: Vec<(usize, u64, u64)> = (0..dn)
        .filter(|&i| cn[i] > having && cn[i] > 0)
        .map(|i| (i, sl[i], cn[i]))
        .collect();
    match node.params.order {
        // No emission order: dense-index order (key ASC) is the
        // deterministic native walk for the UNBOUNDED answer.
        // [emitcap-audit] A NATIVE pushed bound with a (count DESC) key
        // (the server posture: the ORDER/LIMIT peel stays seam-side,
        // `order` arrives None, `params.topk` carries the contract) must
        // SELECT the top-n set under (count DESC, key ASC) BEFORE any
        // truncation — truncating the key-ordered walk kept the bottom
        // of the domain (the filed wrong-answer landmine). An O(G)
        // selection + head sort, the hash_plane native-bound precedent.
        // An EMPTY native spec keeps the walk: any n groups is the law.
        OrderBy::None => {
            if let Some(t) = &node.params.topk {
                if t.native && !t.keys.is_empty() {
                    let cmp = |a: &(usize, u64, u64), b: &(usize, u64, u64)| {
                        b.2.cmp(&a.2).then_with(|| a.0.cmp(&b.0))
                    };
                    if t.n > 0 && t.n < rows.len() {
                        rows.select_nth_unstable_by(t.n - 1, cmp);
                        rows.truncate(t.n);
                    }
                    rows.sort_unstable_by(cmp);
                }
            }
        }
        OrderBy::AggDesc(_) => rows.sort_unstable_by(|a, b| {
            (b.1 as u128 * a.2 as u128)
                .cmp(&(a.1 as u128 * b.2 as u128))
                .then_with(|| a.0.cmp(&b.0))
        }),
        OrderBy::CountDesc => {
            rows.sort_unstable_by(|a, b| b.2.cmp(&a.2).then_with(|| a.0.cmp(&b.0)))
        }
        other => panic!("dense_int code_agg: unsupported order {other:?}"),
    }
    let k = node.params.emit_cap();
    rows.truncate(k);
    let w = col_width(bank, a_key);
    let window: Vec<&(usize, u64, u64)> = rows.iter().skip(node.params.offset).collect();
    let keys: Vec<i64> = window.iter().map(|&&(i, _, _)| sx((i as i64 + dlo) as u64, w)).collect();
    let mut cols: Vec<AnswerCol> = vec![AnswerCol::i64s(node.ty_of(a_key), keys)];
    for a in &node.agg {
        match a.op {
            AggOp::AvgLen | AggOp::AvgCharLen => cols.push(AnswerCol {
                ty: a.out,
                data: ColData::Ratio {
                    pairs: window.iter().map(|&&(_, s, c)| (s as i128, c as i64)).collect(),
                    exact: false,
                },
                validity: Validity::AllValid,
            }),
            AggOp::CountStar => cols.push(AnswerCol::i64s(
                TypMeta::INT8,
                window.iter().map(|&&(_, _, c)| c as i64).collect(),
            )),
            other => panic!("dense_int code_agg: unsupported agg {other:?}"),
        }
    }
    AnswerSet::from_cols(cols)
}
