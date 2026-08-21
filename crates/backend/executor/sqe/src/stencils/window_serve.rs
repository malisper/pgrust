//! WindowServe stencil ([winserve v1]): SQL window functions (`OVER`)
//! over a served scan child, DEFAULT frame semantics only. The physical
//! law is the SortGrouped pass-A/pass-B substrate (its charter:
//! docs/design/sqe/sort-grouped-family.md §2.2/§2.3) instantiated for a
//! row-returning answer:
//!
//!   pass A (pool, part-grain claim): zone-skip int conjuncts, decode +
//!     3VL-evaluate the conjunction (NULL never passes), decode exactly
//!     the emit / partition-key / order-key / function-input cells of
//!     survivors, and scatter (radix_of(hash(PARTITION BY key)), cells,
//!     ingest ordinal) into per-worker per-partition buffers. Partition
//!     ownership is a pure function of the KEY BYTES; an empty
//!     PARTITION BY is the one-partition degenerate case (serial pass
//!     B, the SortGrouped §2.4 posture).
//!
//!   pass B (partition-owned, zero merge): per hash partition,
//!     concatenate the per-worker slices, sort by
//!         (partition key, window ORDER BY keys, ingest ordinal)
//!     — the ingest-ordinal tiebreak TOTALIZES the order, so answers
//!     are byte-identical at any pool width (the election-inputs law;
//!     on ties the engine's total order is one lawful member of PG's
//!     tie class — Michael's tie ruling 2026-08-18, and the window
//!     AGGREGATE values are peer-group functions, so they are
//!     PG-identical even on ties) — then one peer-group run walk emits
//!     ONE answer row per surviving input row.
//!
//! C ground truth (postgresql-18.6):
//!   row_number = position+1 (windowfuncs.c:84-91); rank = peer-head
//!   position+1, dense_rank = peer ordinal (windowfuncs.c:49-77,
//!   :138-151, :200-213 — rank_up's WinRowsArePeers boundary law).
//!   Aggregates under the DEFAULT frame (RANGE UNBOUNDED PRECEDING ..
//!   CURRENT ROW): the frame runs to the END of the current row's PEER
//!   GROUP — following peers are IN frame (row_is_in_frame,
//!   nodeWindowAgg.c:1441-1476: "following row that is not peer is out
//!   of frame") — so peer rows SHARE aggregate values; without a window
//!   ORDER BY every partition row is a peer and the value is the whole
//!   partition's. count(col)/sum/min/max/avg skip NULL inputs
//!   (strict transfns); count(*) counts every frame row; empty non-null
//!   input answers NULL (sum/min/max/avg) / 0 (counts).
//!
//! Answer: `emit` columns then function columns, rows in (hash
//! partition index, in-partition sort order) — data-pure; any SQL-level
//! ORDER BY obligation is applied by the seam's emitter sort exactly as
//! for ScanServe rows. Memory: witnessed byte-budget admission
//! (`planner::check_server_window`) — typed refusal, no spill, no OOM.
//!
//! The Cell/scatter machinery is deliberately a local twin of
//! sort_grouped.rs (the substrate lane may still move — unification is
//! a chartered follow-up, not a v1 coupling).

use std::cmp::Ordering;

use crate::answer::{AnswerCol, AnswerSet, BytesBuild, Validity};
use crate::bank::Face;
use crate::engine::SqeCtx;
use crate::grouped::{hash64, hash_bytes, radix_of, RADIX_P};
use crate::ir::{CalOff, FrameExclusion, FrameMode, FrameSpec, PlanNode, TopKKey, WinFuncSpec, WinOp};
use crate::scan::{varlena_payload, CurCache, Scratch};

// ---------------------------------------------------------------------------
// [winv3] Calendar month arithmetic — C's exact timestamp + interval
// month leg (timestamp.c timestamp_pl_interval / adt_timestamp
// interval.rs month_day_carry): decompose the usec timestamp into
// (julian date, time-of-day), add months on the j2date year/month with
// C's truncating normalization, clamp the day to the target month's
// end (Jan 31 + 1 mon = Feb 28/29), rebuild via date2j. Day + time
// interval legs are fixed usec for tz-free keys (date/timestamp) and
// ride the folded `CalOff::usecs`. Band admission (conservative month
// <= 31 days) keeps every input AND result inside the valid timestamp
// domain, so the julian math never leaves i32 range.
// ---------------------------------------------------------------------------

const USECS_PER_DAY: i128 = 86_400_000_000;
const POSTGRES_EPOCH_JDATE: i128 = 2_451_545;

/// datetime.c day_tab (adt_datetime calendar.rs DAY_TAB twin).
const DAY_TAB: [[i32; 12]; 2] = [
    [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
    [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
];

#[inline]
const fn isleap(y: i32) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

/// datetime.c date2j (band-admitted domain: no wrap reachable).
const fn date2j(mut year: i32, mut month: i32, day: i32) -> i32 {
    if month > 2 {
        month += 1;
        year += 4800;
    } else {
        month += 13;
        year += 4799;
    }
    let century = year / 100;
    let mut julian = year * 365 - 32167;
    julian += year / 4 - century + century / 4;
    julian += 7834 * month / 256 + day;
    julian
}

/// datetime.c j2date (band-admitted domain) -> (year, month, day).
fn j2date(jd: i32) -> (i32, i32, i32) {
    let mut julian = jd as u32;
    julian = julian.wrapping_add(32044);
    let mut quad = julian / 146097;
    let extra = (julian - quad * 146097) * 4 + 3;
    julian += 60 + quad * 3 + extra / 146097;
    quad = julian / 1461;
    julian -= quad * 1461;
    let mut y = (julian * 4 / 1461) as i32;
    julian = if y != 0 { (julian + 305) % 365 } else { (julian + 306) % 366 } + 123;
    y += (quad * 4) as i32;
    let year = y - 4800;
    quad = julian * 2141 / 65536;
    let day = (julian - 7834 * quad / 256) as i32;
    let month = ((quad + 10) % 12) as i32 + 1;
    (year, month, day)
}

/// C's timestamp +/- calendar interval on a tz-free usec timestamp:
/// month leg (calendar add, end-of-month clamp), then the folded
/// day+time usec leg. i128 out — band admission witnessed the result
/// in-domain, the wider type only keeps the arithmetic total.
fn ts_pl_cal(ts: i64, months: i64, usecs: i64) -> i128 {
    let mut ts = ts as i128;
    if months != 0 {
        let date = ts.div_euclid(USECS_PER_DAY);
        let time = ts.rem_euclid(USECS_PER_DAY);
        let (y, m, d) = j2date((date + POSTGRES_EPOCH_JDATE) as i32);
        // month_day_carry: C truncating / and % (timestamp.c).
        let mut y2 = y as i64;
        let mut m2 = m as i64 + months;
        if m2 > 12 {
            y2 += (m2 - 1) / 12;
            m2 = (m2 - 1) % 12 + 1;
        } else if m2 < 1 {
            y2 += m2 / 12 - 1;
            m2 = m2 % 12 + 12;
        }
        let dmax = DAY_TAB[isleap(y2 as i32) as usize][(m2 - 1) as usize];
        let d2 = if d > dmax { dmax } else { d };
        let njd = date2j(y2 as i32, m2 as i32, d2) as i128;
        ts = (njd - POSTGRES_EPOCH_JDATE) * USECS_PER_DAY + time;
    }
    ts + usecs as i128
}

/// One Range calendar bound target in the WALK domain (key embeds are
/// sign-flipped under desc). The interval's sign in the TRUE key
/// domain: walk-domain FOLLOWING adds, and desc flips it (C flips
/// in_range's sub the same way).
#[inline]
fn cal_target(cur: i128, desc: bool, following: bool, c: CalOff) -> i128 {
    let pos = following != desc;
    let t = if desc { -cur } else { cur };
    let (m, u) = if pos {
        (c.months as i64, c.usecs)
    } else {
        (-(c.months as i64), -c.usecs)
    };
    let r = ts_pl_cal(t as i64, m, u);
    if desc {
        -r
    } else {
        r
    }
}

/// One decoded survivor cell (None = SQL NULL). Word cells carry the
/// order-preserving AND value-preserving i64 embed; byte cells own
/// their payload copy.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Cell {
    I(Option<i64>),
    B(Option<Vec<u8>>),
}

/// NULL-absolute cell comparator (the scan_serve `cmp_cand` law: Bytes =
/// memcmp under the C-collation admission).
#[inline]
fn cmp_cell(a: &Cell, b: &Cell, desc: bool, nulls_first: bool) -> Ordering {
    let ord = match (a, b) {
        (Cell::I(None), Cell::I(None)) | (Cell::B(None), Cell::B(None)) => return Ordering::Equal,
        (Cell::I(None), _) | (Cell::B(None), _) => {
            return if nulls_first { Ordering::Less } else { Ordering::Greater }
        }
        (_, Cell::I(None)) | (_, Cell::B(None)) => {
            return if nulls_first { Ordering::Greater } else { Ordering::Less }
        }
        (Cell::I(Some(x)), Cell::I(Some(y))) => x.cmp(y),
        (Cell::B(Some(x)), Cell::B(Some(y))) => x.cmp(y),
        _ => unreachable!("cell class drift within one column (lowering bug)"),
    };
    if desc {
        ord.reverse()
    } else {
        ord
    }
}

/// One scattered survivor row: `cells` in the node's decode-set order,
/// `ord` = the ingest ordinal (unit index, row-in-granule).
struct SRow {
    cells: Vec<Cell>,
    ord: (u32, u16),
}

/// Partition ownership: a pure function of the PARTITION BY cells (NULL
/// keys hash to a fixed tag — window partitioning groups NULLs
/// together, the not-distinct law).
#[inline]
fn key_hash(cells: &[Cell], pdi: &[usize]) -> u64 {
    let mut h = 0x9E37_79B9_7F4A_7C15u64;
    for &i in pdi {
        h = match &cells[i] {
            Cell::I(None) | Cell::B(None) => hash64(h ^ 0xA5A5_A5A5),
            Cell::I(Some(v)) => hash64(h ^ hash64(*v as u64)),
            Cell::B(Some(b)) => hash64(h ^ hash_bytes(b)),
        };
    }
    h
}

/// One computed window-function value.
#[derive(Clone, Debug, PartialEq)]
enum FVal {
    /// Rank family + counts (always valid).
    N(i64),
    /// Exact i128 sum; None = all-NULL frame.
    S(Option<i128>),
    /// AVG {sum, count} pair (count 0 = NULL at the render seam).
    R(i128, i64),
    /// MIN/MAX word; None = all-NULL frame.
    W(Option<i64>),
    /// MIN/MAX bytes; None = all-NULL frame.
    B(Option<Vec<u8>>),
}

/// Compute per-row frame [head, tail) bounds over ONE sorted partition
/// (the C law: update_frameheadpos/update_frametailpos,
/// nodeWindowAgg.c:1540-2080). `gid`/`gb` = peer-group ids and (start,
/// end) bounds; `key1` = the scaled first-order-key embeds for Range
/// offsets (None cell = SQL NULL). Both arrays are nondecreasing in the
/// row position (offset bounds are monotone in the sorted key / group /
/// position domains) — the sliding-window consumers rely on that.
fn frame_bounds(
    n: usize,
    frame: &FrameSpec,
    gid: &[usize],
    gb: &[(usize, usize)],
    key1: Option<&[Option<i128>]>,
    desc: bool,
) -> (Vec<usize>, Vec<usize>) {
    use crate::ir::FrameBound as FB;
    let clamp = |v: i128| -> usize { v.clamp(0, n as i128) as usize };
    // The NULL region of the (single) Range order key: C's in_range
    // NULL law — a NULL current row's OFFSET bounds collapse to the
    // NULL region's edges; a non-null row's offset search never leaves
    // the non-null region (nodeWindowAgg.c:1704-1725/:1979-1994).
    let (null_s, null_e, nn_s, nn_e) = match key1 {
        Some(kv) => {
            let firstnull = kv.first().map(|c| c.is_none()).unwrap_or(false);
            if firstnull {
                let e = kv.partition_point(|c| c.is_none());
                (0usize, e, e, n)
            } else {
                let s = kv.partition_point(|c| c.is_some());
                (s, n, 0, s)
            }
        }
        None => (0, 0, 0, n),
    };
    let mut head = Vec::with_capacity(n);
    let mut tail = Vec::with_capacity(n);
    for i in 0..n {
        let g = gid[i];
        let h = match frame.start {
            FB::UnboundedPreceding => 0,
            FB::UnboundedFollowing => n, // fail-closed (parse error in C)
            FB::CurrentRow => match frame.mode {
                FrameMode::Rows => i,
                FrameMode::Range | FrameMode::Groups => gb[g].0,
            },
            FB::Preceding(o) | FB::Following(o) => {
                let fol = matches!(frame.start, FB::Following(_));
                let so = if fol { o } else { -o };
                match frame.mode {
                    FrameMode::Rows => clamp(i as i128 + so),
                    FrameMode::Groups => {
                        let tg = g as i128 + so;
                        if tg <= 0 {
                            0
                        } else if tg >= gb.len() as i128 {
                            n
                        } else {
                            gb[tg as usize].0
                        }
                    }
                    FrameMode::Range => {
                        let kv = key1.expect("range offsets need the key column");
                        match kv[i] {
                            None => null_s,
                            Some(cur) => {
                                // first row in the non-null region with
                                // key >= cur + so (in_range head law);
                                // [winv3] calendar offsets compute the
                                // target by C's exact ts +/- interval.
                                let target = match frame.cal_start {
                                    Some(c) => cal_target(cur, desc, fol, c),
                                    None => cur + so,
                                };
                                nn_s + kv[nn_s..nn_e]
                                    .partition_point(|c| c.expect("non-null region") < target)
                            }
                        }
                    }
                }
            }
        };
        let t = match frame.end {
            FB::UnboundedFollowing => n,
            FB::UnboundedPreceding => 0, // fail-closed (parse error in C)
            FB::CurrentRow => match frame.mode {
                FrameMode::Rows => i + 1,
                FrameMode::Range | FrameMode::Groups => gb[g].1,
            },
            FB::Preceding(o) | FB::Following(o) => {
                let fol = matches!(frame.end, FB::Following(_));
                let eo = if fol { o } else { -o };
                match frame.mode {
                    FrameMode::Rows => clamp(i as i128 + eo + 1),
                    FrameMode::Groups => {
                        let tg = g as i128 + eo;
                        if tg < 0 {
                            0
                        } else if tg >= gb.len() as i128 {
                            n
                        } else {
                            gb[tg as usize].1
                        }
                    }
                    FrameMode::Range => {
                        let kv = key1.expect("range offsets need the key column");
                        match kv[i] {
                            None => null_e,
                            Some(cur) => {
                                // one past the last non-null-region row
                                // with key <= cur + eo (in_range tail).
                                let target = match frame.cal_end {
                                    Some(c) => cal_target(cur, desc, fol, c),
                                    None => cur + eo,
                                };
                                nn_s + kv[nn_s..nn_e]
                                    .partition_point(|c| c.expect("non-null region") <= target)
                            }
                        }
                    }
                }
            }
        };
        head.push(h);
        tail.push(t);
    }
    (head, tail)
}

/// Compute every function column over ONE partition's SORTED row slice.
/// `odi` = the window ORDER BY keys' decode indexes (empty = no window
/// ORDER BY: every row is a peer); `keys` = the matching sort specs;
/// `fdi[i]` = function i's input decode index (usize::MAX for input-less
/// ops). Returns per-function value vectors aligned with `rows`.
///
/// The frame engine: per-row [head, tail) bounds (`frame_bounds`), then
/// sum/count/avg by prefix arrays (O(1) per row), min/max by a
/// monotonic deque riding the nondecreasing bounds (O(n) total), value
/// functions by positional reads — O(partition), never O(n x frame).
fn walk_partition(
    rows: &[&SRow],
    odi: &[usize],
    keys: &[crate::ir::TopKKey],
    frame: &FrameSpec,
    funcs: &[WinFuncSpec],
    fdi: &[usize],
) -> Vec<Vec<FVal>> {
    let n = rows.len();
    let mut out: Vec<Vec<FVal>> = funcs.iter().map(|_| Vec::with_capacity(n)).collect();
    if n == 0 {
        return out;
    }
    // Peer test: window ORDER BY cell equality (NULLs are peers — C's
    // are_peers/execTuplesMatch not-distinct law).
    let peers = |a: &SRow, b: &SRow| odi.iter().all(|&i| a.cells[i] == b.cells[i]);
    // Peer groups: gid per row + (start, end) bounds per group.
    let mut gid = vec![0usize; n];
    let mut gb: Vec<(usize, usize)> = Vec::new();
    {
        let mut s = 0usize;
        while s < n {
            let mut e = s + 1;
            while e < n && peers(rows[s], rows[e]) {
                e += 1;
            }
            for x in &mut gid[s..e] {
                *x = gb.len();
            }
            gb.push((s, e));
            s = e;
        }
    }
    // Scaled first-key embeds for Range offset bounds (desc keys are
    // sign-flipped so the walk domain is always ascending — C flips
    // in_range's sub/less the same way).
    let key1: Option<Vec<Option<i128>>> =
        (frame.mode == FrameMode::Range && frame.has_offsets() && !odi.is_empty()).then(|| {
            let desc = keys[0].desc;
            rows.iter()
                .map(|r| match &r.cells[odi[0]] {
                    Cell::I(Some(v)) => {
                        Some(if desc { -(*v as i128) } else { *v as i128 } * frame.scale)
                    }
                    Cell::I(None) => None,
                    Cell::B(_) => unreachable!("range-offset key face admitted as word"),
                })
                .collect()
        });
    let desc0 = keys.first().map(|k| k.desc).unwrap_or(false);
    let (head, tail) = frame_bounds(n, frame, &gid, &gb, key1.as_deref(), desc0);

    // [winv3] The EXCLUDE clause: per row, a (start, end, keep_current)
    // sub-range subtracted from [head, tail) — C row_is_in_frame's
    // exclusion leg (nodeWindowAgg.c:1511-1533). Group/Ties exclude the
    // current row's whole peer group (no ORDER BY = whole partition);
    // Ties re-admits the current row itself.
    let ex_of = |i: usize| -> Option<(usize, usize, bool)> {
        match frame.exclusion {
            FrameExclusion::None => None,
            FrameExclusion::CurrentRow => Some((i, i + 1, false)),
            FrameExclusion::Group => {
                let (s, e) = gb[gid[i]];
                Some((s, e, false))
            }
            FrameExclusion::Ties => {
                let (s, e) = gb[gid[i]];
                Some((s, e, true))
            }
        }
    };
    // Effective frame of row `i` as up to three ASCENDING [a, b)
    // segments: [head, ex.start), the kept current row (EXCLUDE TIES),
    // [ex.end, tail). Positional value functions read the same
    // concatenation C's WinGetFuncArgInFrame exclusion seek walks.
    let eff_segs = |i: usize| -> ([(usize, usize); 3], usize) {
        let (h, t) = (head[i], tail[i].max(head[i]));
        let mut out = [(0usize, 0usize); 3];
        let mut m = 0usize;
        match ex_of(i) {
            Some((es, ee, keep)) if es.max(h) < ee.min(t) => {
                let (es, ee) = (es.max(h), ee.min(t));
                if h < es {
                    out[m] = (h, es);
                    m += 1;
                }
                if keep && i >= h && i < t {
                    out[m] = (i, i + 1);
                    m += 1;
                }
                if ee < t {
                    out[m] = (ee, t);
                    m += 1;
                }
            }
            _ => {
                if h < t {
                    out[m] = (h, t);
                    m += 1;
                }
            }
        }
        (out, m)
    };

    // count(*)'s input-less probe cell (always-valid).
    const PROBE: Cell = Cell::I(Some(0));
    let cell_of = |x: usize, i: usize| -> &Cell {
        if fdi[x] == usize::MAX {
            &PROBE
        } else {
            &rows[i].cells[fdi[x]]
        }
    };
    let cell_val = |f: &WinFuncSpec, c: &Cell| -> FVal {
        if f.in_ty.map(|t| t.is_varlena()).unwrap_or(false) {
            match c {
                Cell::B(b) => FVal::B(b.clone()),
                Cell::I(_) => unreachable!("value-function cell class drift"),
            }
        } else {
            match c {
                Cell::I(v) => FVal::W(*v),
                Cell::B(_) => unreachable!("value-function cell class drift"),
            }
        }
    };
    let null_val = |f: &WinFuncSpec| -> FVal {
        if f.in_ty.map(|t| t.is_varlena()).unwrap_or(false) {
            FVal::B(None)
        } else {
            FVal::W(None)
        }
    };

    for (x, f) in funcs.iter().enumerate() {
        match f.op {
            WinOp::RowNumber => out[x].extend((0..n).map(|i| FVal::N(i as i64 + 1))),
            WinOp::Rank => {
                out[x].extend((0..n).map(|i| FVal::N(gb[gid[i]].0 as i64 + 1)));
            }
            WinOp::DenseRank => out[x].extend((0..n).map(|i| FVal::N(gid[i] as i64 + 1))),
            WinOp::CountStar => {
                out[x].extend((0..n).map(|i| {
                    let (sg, m) = eff_segs(i);
                    FVal::N(sg[..m].iter().map(|&(a, b)| (b - a) as i64).sum())
                }));
            }
            WinOp::Count | WinOp::Sum | WinOp::Avg => {
                // Prefix arrays: non-null count + exact i128 sum.
                // EXCLUDE composes directly: value = the segment sums
                // (frame minus the excluded sub-range).
                let mut pnn: Vec<i64> = Vec::with_capacity(n + 1);
                let mut psum: Vec<i128> = Vec::with_capacity(n + 1);
                pnn.push(0);
                psum.push(0);
                for i in 0..n {
                    let (dn, ds) = match cell_of(x, i) {
                        Cell::I(Some(v)) => (1, *v as i128),
                        Cell::B(Some(_)) => (1, 0),
                        Cell::I(None) | Cell::B(None) => (0, 0),
                    };
                    pnn.push(pnn[i] + dn);
                    psum.push(psum[i] + ds);
                }
                for i in 0..n {
                    let (sg, m) = eff_segs(i);
                    let mut nn = 0i64;
                    let mut sum = 0i128;
                    for &(a, b) in &sg[..m] {
                        nn += pnn[b] - pnn[a];
                        sum += psum[b] - psum[a];
                    }
                    out[x].push(match f.op {
                        WinOp::Count => FVal::N(nn),
                        WinOp::Sum => FVal::S((nn > 0).then_some(sum)),
                        _ => FVal::R(sum, nn),
                    });
                }
            }
            WinOp::Min | WinOp::Max if frame.exclusion != FrameExclusion::None => {
                // [winv3] EXCLUDE breaks the monotonic deque (the
                // excluded hole makes query ranges non-nested): a
                // sparse table of best-non-null-cell indexes answers
                // any [a, b) in O(1) after an O(n log n) build.
                let want_min = f.op == WinOp::Min;
                let better = |a: u32, b: u32| -> u32 {
                    if a == u32::MAX {
                        return b;
                    }
                    if b == u32::MAX {
                        return a;
                    }
                    let o = cmp_cell(cell_of(x, a as usize), cell_of(x, b as usize), false, false);
                    let a_wins = if want_min {
                        o != Ordering::Greater
                    } else {
                        o != Ordering::Less
                    };
                    if a_wins {
                        a
                    } else {
                        b
                    }
                };
                let mut lvl: Vec<Vec<u32>> = Vec::new();
                lvl.push(
                    (0..n)
                        .map(|i| {
                            if matches!(cell_of(x, i), Cell::I(None) | Cell::B(None)) {
                                u32::MAX
                            } else {
                                i as u32
                            }
                        })
                        .collect(),
                );
                let mut w = 1usize;
                while w * 2 <= n {
                    let prev = lvl.last().unwrap();
                    let next: Vec<u32> =
                        (0..=n - w * 2).map(|i| better(prev[i], prev[i + w])).collect();
                    lvl.push(next);
                    w *= 2;
                }
                // best over [a, b): two overlapping power-of-two spans.
                let query = |a: usize, b: usize| -> u32 {
                    debug_assert!(a < b && b <= n);
                    let k = usize::BITS - 1 - (b - a).leading_zeros();
                    let w = 1usize << k;
                    better(lvl[k as usize][a], lvl[k as usize][b - w])
                };
                for i in 0..n {
                    let (sg, m) = eff_segs(i);
                    let mut best = u32::MAX;
                    for &(a, b) in &sg[..m] {
                        best = better(best, query(a, b));
                    }
                    out[x].push(if best == u32::MAX {
                        null_val(f)
                    } else {
                        cell_val(f, cell_of(x, best as usize))
                    });
                }
            }
            WinOp::Min | WinOp::Max => {
                // Monotonic deque over non-null cells, window [head,
                // tail) — both bounds nondecreasing, so push-once /
                // pop-once: O(n).
                let want_min = f.op == WinOp::Min;
                let beats = |a: &Cell, b: &Cell| -> bool {
                    // a (new) evicts b (older) from the deque back.
                    let o = cmp_cell(a, b, false, false);
                    if want_min {
                        o != Ordering::Greater
                    } else {
                        o != Ordering::Less
                    }
                };
                let mut dq: std::collections::VecDeque<usize> = std::collections::VecDeque::new();
                let mut pushed = 0usize;
                for i in 0..n {
                    while pushed < tail[i] {
                        let c = cell_of(x, pushed);
                        if !matches!(c, Cell::I(None) | Cell::B(None)) {
                            while let Some(&b) = dq.back() {
                                if beats(c, cell_of(x, b)) {
                                    dq.pop_back();
                                } else {
                                    break;
                                }
                            }
                            dq.push_back(pushed);
                        }
                        pushed += 1;
                    }
                    while let Some(&fr) = dq.front() {
                        if fr < head[i] {
                            dq.pop_front();
                        } else {
                            break;
                        }
                    }
                    let best = (head[i] < tail[i])
                        .then(|| dq.front().copied())
                        .flatten()
                        .map(|j| cell_of(x, j));
                    out[x].push(match best {
                        Some(c) => cell_val(f, c),
                        None => null_val(f),
                    });
                }
            }
            WinOp::FirstValue | WinOp::LastValue | WinOp::NthValue => {
                // Positional reads over the effective frame — under
                // EXCLUDE that is the ascending segment concatenation
                // (C's WinGetFuncArgInFrame exclusion seek law).
                for i in 0..n {
                    let (sg, m) = eff_segs(i);
                    let j = match f.op {
                        WinOp::FirstValue => (m > 0).then(|| sg[0].0),
                        WinOp::LastValue => (m > 0).then(|| sg[m - 1].1 - 1),
                        _ => {
                            // nth_value: off - 1 positions into the
                            // concatenation (off >= 1 admitted).
                            let mut rel = f.off as i128 - 1;
                            let mut hit = None;
                            for &(a, b) in &sg[..m] {
                                let len = (b - a) as i128;
                                if rel < len {
                                    hit = Some(a + rel as usize);
                                    break;
                                }
                                rel -= len;
                            }
                            hit
                        }
                    };
                    out[x].push(match j {
                        Some(j) => cell_val(f, cell_of(x, j)),
                        None => null_val(f),
                    });
                }
            }
            WinOp::Lead | WinOp::Lag => {
                // Partition-positional, frame-independent (C's
                // WinGetFuncArgInPartition); out of partition = NULL.
                let sgn: i128 = if f.op == WinOp::Lead { 1 } else { -1 };
                for i in 0..n {
                    let j = i as i128 + sgn * f.off as i128;
                    out[x].push(if (0..n as i128).contains(&j) {
                        cell_val(f, cell_of(x, j as usize))
                    } else {
                        null_val(f)
                    });
                }
            }
        }
    }
    out
}

/// Render one function's per-row values into its typed answer column
/// (shared by the scan-lane emit and the [winv4] over-answer lane).
fn fvals_col<'a>(
    f: &WinFuncSpec,
    vals: impl Iterator<Item = &'a FVal>,
    nrows: usize,
) -> AnswerCol {
    match f.op {
        WinOp::RowNumber | WinOp::Rank | WinOp::DenseRank | WinOp::CountStar | WinOp::Count => {
            let v: Vec<i64> = vals
                .map(|fv| match fv {
                    FVal::N(n) => *n,
                    _ => unreachable!("rank/count value class drift"),
                })
                .collect();
            AnswerCol::i64s(f.out, v)
        }
        WinOp::Sum => {
            let mut v: Vec<i128> = Vec::with_capacity(nrows);
            let mut mask: Vec<bool> = Vec::with_capacity(nrows);
            for fv in vals {
                match fv {
                    FVal::S(s) => {
                        mask.push(s.is_some());
                        v.push(s.unwrap_or(0));
                    }
                    _ => unreachable!("sum value class drift"),
                }
            }
            let mut c = AnswerCol::i128s(f.out, v);
            if !mask.iter().all(|&m| m) {
                c.validity = Validity::Mask(mask);
            }
            c
        }
        WinOp::Avg => {
            let pairs: Vec<(i128, i64)> = vals
                .map(|fv| match fv {
                    FVal::R(s, n) => (*s, *n),
                    _ => unreachable!("avg value class drift"),
                })
                .collect();
            let exact = f.in_ty.map(|t| t.width == 8).unwrap_or(false);
            AnswerCol::ratios(f.out, pairs, exact)
        }
        WinOp::Min | WinOp::Max | WinOp::Lead | WinOp::Lag | WinOp::FirstValue
        | WinOp::LastValue | WinOp::NthValue => {
            if f.in_ty.map(|t| t.is_varlena()).unwrap_or(false) {
                let mut b = BytesBuild::new();
                let mut mask = Vec::with_capacity(nrows);
                for fv in vals {
                    match fv {
                        FVal::B(Some(x)) => {
                            b.push(x);
                            mask.push(true);
                        }
                        FVal::B(None) => {
                            b.push(b"");
                            mask.push(false);
                        }
                        _ => unreachable!("min/max value class drift"),
                    }
                }
                let mut c = b.finish(f.out);
                if !mask.iter().all(|&v| v) {
                    c.validity = Validity::Mask(mask);
                }
                c
            } else {
                let v: Vec<Option<i64>> = vals
                    .map(|fv| match fv {
                        FVal::W(x) => *x,
                        _ => unreachable!("min/max value class drift"),
                    })
                    .collect();
                AnswerCol::i64s_opt(f.out, v)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// [winv4] window-over-answer: the answer-as-input hop. A served child
// goal (an Agg) materialized its answer; the window walk consumes the
// ANSWER ROWS as input — `WindowSpec` columns index ANSWER columns, the
// emitted function columns align with the INPUT row order (the caller
// owns row-order obligations through its emitter sort; the walk sorts
// internally per the C law and scatters values back). Serial: grouped
// answers are group-count-sized — pool fan-out buys nothing here.
// ---------------------------------------------------------------------------

/// Run the window spec over materialized answer columns; returns ONE
/// function column per spec function, row-aligned with the input.
/// `run_conds`/`chain` are outside the over-answer charter (the seam
/// refuses those shapes before authoring) — fail closed here. `budget`
/// = the window byte budget (the memory law at engagement cadence).
pub fn run_window_over_answer(
    cols: &[AnswerCol],
    nrows: usize,
    w: &crate::ir::WindowSpec,
    budget: u64,
) -> Result<Vec<AnswerCol>, crate::refuse::Refuse> {
    use crate::answer::ColData;
    use crate::refuse::Refuse;
    if !w.run_conds.is_empty() || w.chain.is_some() {
        return Err(Refuse::WinUnsupported { what: "over-answer-shape" });
    }
    // ---- decode set over ANSWER columns -----------------------------------
    let mut dset: Vec<u32> = Vec::new();
    let mut stage = |c: u32| -> usize {
        match dset.iter().position(|&x| x == c) {
            Some(i) => i,
            None => {
                dset.push(c);
                dset.len() - 1
            }
        }
    };
    let pdi: Vec<usize> = w.part_cols.iter().map(|&c| stage(c)).collect();
    let odi: Vec<usize> = w.order.iter().map(|k| stage(k.col)).collect();
    let fdi: Vec<usize> = w
        .funcs
        .iter()
        .map(|f| f.col.map(&mut stage).unwrap_or(usize::MAX))
        .collect();
    // ---- the memory law (answer-sized; cells + values) --------------------
    let ncells = dset.len() + w.funcs.len();
    let mut est: u128 = nrows as u128 * (64 + 40 * ncells as u128);
    for &c in &dset {
        let col = cols.get(c as usize).ok_or(Refuse::WinUnsupported {
            what: "over-answer-col",
        })?;
        if let ColData::Bytes { arena, .. } = &col.data {
            est = est.saturating_add(arena.len() as u128);
        }
    }
    if est.min(u64::MAX as u128) as u64 > budget {
        return Err(Refuse::WinOverBudget {
            est: est.min(u64::MAX as u128) as u64,
            budget,
        });
    }
    // ---- cell extraction: word embeds / byte payloads only ----------------
    let cell_of = |c: u32, i: usize| -> Result<Cell, Refuse> {
        let col = &cols[c as usize];
        let valid = col.validity.is_valid(i);
        Ok(match &col.data {
            ColData::I64(v) => Cell::I(valid.then(|| v[i])),
            // Exact i128 sums (the grouped SUM answer class): the cell
            // plane is the i64 embed — every MATERIALIZED value is a
            // runtime witness, so an out-of-domain sum refuses typed
            // exactly here (never a silent wrap).
            ColData::I128(v) => {
                if valid && i64::try_from(v[i]).is_err() {
                    return Err(Refuse::WinUnsupported { what: "over-answer-i128-domain" });
                }
                Cell::I(valid.then(|| v[i] as i64))
            }
            ColData::Bytes { arena, offs } => Cell::B(valid.then(|| {
                arena[offs[i] as usize..offs[i + 1] as usize].to_vec()
            })),
            // Ratio / moments / float / nested classes have no cell
            // embed — the seam's lowering refuses them before
            // authoring; fail closed if one leaks through.
            _ => return Err(Refuse::WinUnsupported { what: "over-answer-col-class" }),
        })
    };
    let mut rows: Vec<SRow> = Vec::with_capacity(nrows);
    for i in 0..nrows {
        let mut cells = Vec::with_capacity(dset.len());
        for &c in &dset {
            cells.push(cell_of(c, i)?);
        }
        // The ingest ordinal: the answer row index (deterministic —
        // the child goal's answer order is engine-total).
        rows.push(SRow {
            cells,
            ord: ((i >> 16) as u32, (i & 0xFFFF) as u16),
        });
    }
    // ---- one serial pass B: total sort + partition-run walk ---------------
    let mut idx: Vec<&SRow> = rows.iter().collect();
    let pkey_cmp = |a: &SRow, b: &SRow| -> Ordering {
        for &i in &pdi {
            let o = cmp_cell(&a.cells[i], &b.cells[i], false, false);
            if o != Ordering::Equal {
                return o;
            }
        }
        Ordering::Equal
    };
    idx.sort_unstable_by(|a, b| {
        let o = pkey_cmp(a, b);
        if o != Ordering::Equal {
            return o;
        }
        for (ki, k) in w.order.iter().enumerate() {
            let o = cmp_cell(&a.cells[odi[ki]], &b.cells[odi[ki]], k.desc, k.nulls_first);
            if o != Ordering::Equal {
                return o;
            }
        }
        a.ord.cmp(&b.ord)
    });
    // Original row index of each sorted position (ord round-trip).
    let orig = |r: &SRow| -> usize { ((r.ord.0 as usize) << 16) | r.ord.1 as usize };
    let mut fvals: Vec<Vec<Option<FVal>>> =
        w.funcs.iter().map(|_| vec![None; nrows]).collect();
    let mut s = 0usize;
    while s < idx.len() {
        let mut e = s + 1;
        while e < idx.len() && pkey_cmp(idx[s], idx[e]) == Ordering::Equal {
            e += 1;
        }
        let run = walk_partition(&idx[s..e], &odi, &w.order, &w.frame, &w.funcs, &fdi);
        for (x, v) in run.into_iter().enumerate() {
            for (j, fv) in v.into_iter().enumerate() {
                fvals[x][orig(idx[s + j])] = Some(fv);
            }
        }
        s = e;
    }
    // ---- typed emit (input row order) -------------------------------------
    Ok(w
        .funcs
        .iter()
        .enumerate()
        .map(|(x, f)| {
            let vals: Vec<FVal> = fvals[x]
                .iter()
                .map(|v| v.clone().expect("every row walked"))
                .collect();
            fvals_col(f, vals.iter(), nrows)
        })
        .collect())
}

struct DecodeCol {
    face: Face,
    scr: Scratch,
    cc: CurCache,
    all_valid: bool,
}

impl DecodeCol {
    fn new(attno: u32, face: Face) -> DecodeCol {
        DecodeCol { face, scr: Scratch::new(), cc: CurCache::new(attno), all_valid: true }
    }
}

pub fn run_window_serve(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    let (bank, pool) = (ctx.bank, ctx.pool);
    let t_all = std::time::Instant::now();
    let w = node.params.window.as_ref().expect("WindowServe node without a window spec");
    assert!(node.agg.is_empty(), "window_serve: funcs ride the window spec, not node.agg");
    let grouped = !w.part_cols.is_empty();

    // ---- decode set: unique attnos of emit ++ part ++ order ++ inputs ----
    let mut dset: Vec<(u32, Face)> = Vec::new();
    let stage = |dset: &mut Vec<(u32, Face)>, a: u32| -> usize {
        match dset.iter().position(|&(c, _)| c == a) {
            Some(i) => i,
            None => {
                dset.push((a, bank.face(a)));
                dset.len() - 1
            }
        }
    };
    let edi: Vec<usize> = w.emit.iter().map(|&c| stage(&mut dset, c)).collect();
    let pdi: Vec<usize> = w.part_cols.iter().map(|&c| stage(&mut dset, c)).collect();
    let odi: Vec<usize> = w.order.iter().map(|k| stage(&mut dset, k.col)).collect();
    // [winv4] `fdi` spans the COMBINED function list (bottom spec then
    // chain): fdi[..n1] feeds the bottom walk, fdi[n1..] the chain's.
    let fdi: Vec<usize> = w
        .all_funcs()
        .map(|f| f.col.map(|c| stage(&mut dset, c)).unwrap_or(usize::MAX))
        .collect();
    let n1 = w.funcs.len();

    let iterms: Vec<crate::ir::PredTerm> =
        node.pred.iter().flat_map(|p| p.terms.iter().cloned()).collect();
    let vterms: Vec<crate::ir::VarPredTerm> =
        node.pred.iter().flat_map(|p| p.var_terms.iter().cloned()).collect();
    // Predicate columns join the decode set (their cells are decoded but
    // not scattered unless a consumer staged them above).
    let mut pset: Vec<(u32, Face)> = dset.clone();
    let pstage = |pset: &mut Vec<(u32, Face)>, a: u32| -> usize {
        match pset.iter().position(|&(c, _)| c == a) {
            Some(i) => i,
            None => {
                pset.push((a, bank.face(a)));
                pset.len() - 1
            }
        }
    };
    let iterm_di: Vec<usize> = iterms.iter().map(|t| pstage(&mut pset, t.col)).collect();
    let vterm_di: Vec<usize> = vterms.iter().map(|t| pstage(&mut pset, t.col)).collect();
    let ncells = dset.len();

    let units = ctx.faces.walk(bank, node.cols[0]);
    let smas: Vec<_> = iterms.iter().map(|t| ctx.faces.sma(bank, t.col)).collect();
    // [psma-consume] §8.2 candidate-slice faces per int conjunct (None
    // under the kill switch / uncovered column — consult degrades).
    let psmas: Vec<_> = iterms.iter().map(|t| ctx.faces.psma(bank, t.col)).collect();
    let mut part_units: Vec<(usize, usize)> = vec![(0, 0); bank.parts.len()];
    {
        let mut i = 0usize;
        while i < units.len() {
            let pi = units[i].0;
            let s = i;
            while i < units.len() && units[i].0 == pi {
                i += 1;
            }
            part_units[pi] = (s, i);
        }
    }

    // ---- pass A: filter -> decode survivor cells -> scatter by key hash ----
    struct W {
        cols: Vec<DecodeCol>,
        sel: Vec<u16>,
        parts: Vec<Vec<SRow>>,
    }
    let t_p1 = std::time::Instant::now();
    let states = pool.run(
        bank.parts.len(),
        |_| W {
            cols: pset.iter().map(|&(a, f)| DecodeCol::new(a, f)).collect(),
            sel: Vec::new(),
            parts: (0..RADIX_P).map(|_| Vec::new()).collect(),
        },
        |s: &mut W, pi| {
            let (u0, u1) = part_units[pi];
            for ui in u0..u1 {
                let (_, g, rows32, _) = units[ui];
                let rows = rows32 as usize;
                if rows == 0 {
                    continue;
                }
                if iterms
                    .iter()
                    .enumerate()
                    .any(|(ti, t)| !t.zone_may_pass(smas[ti].mins[ui], smas[ti].maxs[ui]))
                {
                    continue;
                }
                // [psma-consume] Zone said maybe: intersect the granule's
                // PSMA candidate slices (one probe per selective-class
                // conjunct, never per-row); an empty window skips the
                // granule before any decode.
                let (rlo, rhi) = iterms.iter().enumerate().fold((0usize, rows), |w, (ti, t)| {
                    crate::psmaface::narrow(
                        w,
                        psmas[ti].as_ref().and_then(|pf| {
                            pf.slice(pi, g, rows32, smas[ti].mins[ui], smas[ti].maxs[ui], t)
                        }),
                    )
                });
                if rlo >= rhi {
                    continue;
                }
                // Decode predicate columns, filter row-major (3VL).
                let mut decoded = vec![false; s.cols.len()];
                for di in iterm_di.iter().chain(vterm_di.iter()) {
                    if !decoded[*di] {
                        let c = &mut s.cols[*di];
                        let cur = c.cc.get(bank, pi);
                        c.all_valid = c.scr.validity(cur, g, rows).all_valid();
                        let cur = c.cc.get(bank, pi);
                        c.scr.decode_full(cur, g, rows);
                        decoded[*di] = true;
                    }
                }
                // [psma-consume, oracle] slice-complement emptiness: a
                // slice that hid a matching valid row panics loudly.
                #[cfg(feature = "oracle")]
                for (ti, t) in iterms.iter().enumerate() {
                    if let Some(sl) = psmas[ti].as_ref().and_then(|pf| {
                        pf.slice(pi, g, rows32, smas[ti].mins[ui], smas[ti].maxs[ui], t)
                    }) {
                        let c = &s.cols[iterm_di[ti]];
                        crate::psmaface::oracle_check_complement(
                            t,
                            sl.0 as usize,
                            (sl.1 as usize).min(rows),
                            rows,
                            |r| c.all_valid || c.scr.row_valid(r),
                            |r| c.face.word_key(c.scr.datums[r]),
                        );
                    }
                }
                s.sel.clear();
                'rows: for r in rlo..rhi {
                    for (ti, t) in iterms.iter().enumerate() {
                        let c = &s.cols[iterm_di[ti]];
                        let valid = c.all_valid || c.scr.row_valid(r);
                        if !valid || !t.eval(c.face.word_key(c.scr.datums[r])) {
                            continue 'rows;
                        }
                    }
                    for (ti, t) in vterms.iter().enumerate() {
                        let c = &s.cols[vterm_di[ti]];
                        let valid = c.all_valid || c.scr.row_valid(r);
                        if !valid || !t.eval(unsafe { varlena_payload(c.scr.datums[r]) }) {
                            continue 'rows;
                        }
                    }
                    s.sel.push(r as u16);
                }
                if s.sel.is_empty() {
                    continue;
                }
                // Late materialization: decode the scattered cells for
                // survivors only (decode_sel on partial granules).
                let full = s.sel.len() == rows;
                for di in 0..ncells {
                    if decoded[di] {
                        continue;
                    }
                    let c = &mut s.cols[di];
                    let cur = c.cc.get(bank, pi);
                    c.all_valid = c.scr.validity(cur, g, rows).all_valid();
                    let cur = c.cc.get(bank, pi);
                    if full {
                        c.scr.decode_full(cur, g, rows);
                    } else {
                        c.scr.decode_sel(cur, g, &s.sel);
                    }
                    decoded[di] = true;
                }
                for (i, &r) in s.sel.iter().enumerate() {
                    let cells: Vec<Cell> = (0..ncells)
                        .map(|di| {
                            let c = &s.cols[di];
                            let valid = c.all_valid || c.scr.row_valid(r as usize);
                            // Full decodes (whole granule / predicate
                            // columns) index by row; decode_sel by
                            // selection position (the scan_serve pass-2
                            // convention). Validity always by row.
                            let pred_col =
                                iterm_di.contains(&di) || vterm_di.contains(&di);
                            let d = if full || pred_col {
                                c.scr.datums[r as usize]
                            } else {
                                c.scr.datums[i]
                            };
                            if c.face == Face::Varlena {
                                Cell::B(valid.then(|| unsafe { varlena_payload(d) }.to_vec()))
                            } else {
                                Cell::I(valid.then(|| c.face.word_key(d)))
                            }
                        })
                        .collect();
                    let part = if grouped { radix_of(key_hash(&cells, &pdi)) } else { 0 };
                    s.parts[part].push(SRow { cells, ord: (ui as u32, r) });
                }
            }
        },
    );
    crate::engine::phn(node, "pass1", t_p1);

    // ---- pass B: partition-owned total sort + peer-group walk --------------
    let t_p2 = std::time::Instant::now();
    let pkey_cmp = |a: &SRow, b: &SRow| -> Ordering {
        for &i in &pdi {
            let o = cmp_cell(&a.cells[i], &b.cells[i], false, false);
            if o != Ordering::Equal {
                return o;
            }
        }
        Ordering::Equal
    };
    let row_cmp = |a: &SRow, b: &SRow, keys: &[TopKKey]| -> Ordering {
        let o = pkey_cmp(a, b);
        if o != Ordering::Equal {
            return o;
        }
        for (ki, k) in keys.iter().enumerate() {
            let o = cmp_cell(&a.cells[odi[ki]], &b.cells[odi[ki]], k.desc, k.nulls_first);
            if o != Ordering::Equal {
                return o;
            }
        }
        a.ord.cmp(&b.ord)
    };
    // Per hash partition: rows in final order + per-func values.
    struct PartOut<'r> {
        rows: Vec<&'r SRow>,
        fvals: Vec<Vec<FVal>>,
    }
    let owned: Vec<(usize, PartOut<'_>)> = pool
        .run(
            RADIX_P,
            |_| Vec::new(),
            |out: &mut Vec<(usize, PartOut<'_>)>, part| {
                let mut rows: Vec<&SRow> = states
                    .iter()
                    .flat_map(|s| s.parts[part].iter())
                    .collect();
                if rows.is_empty() {
                    return;
                }
                // A total order (ingest-ordinal tiebreak) makes stability
                // irrelevant: any comparison sort is admissible.
                rows.sort_unstable_by(|a, b| row_cmp(a, b, &w.order));
                // Walk each PARTITION BY run inside the hash partition.
                // [winv4] the chain's upper spec walks the SAME run
                // under its order prefix; runCondition legs truncate
                // the run's EMISSION at the first failing row (C's
                // STRICT pass-through / DONE law — per-partition; later
                // partitions restart in RUN mode).
                let nf = w.n_funcs();
                let mut fvals: Vec<Vec<FVal>> =
                    (0..nf).map(|_| Vec::with_capacity(rows.len())).collect();
                let mut out_rows: Vec<&SRow> = Vec::with_capacity(rows.len());
                let mut s = 0usize;
                while s < rows.len() {
                    let mut e = s + 1;
                    while e < rows.len() && pkey_cmp(rows[s], rows[e]) == Ordering::Equal {
                        e += 1;
                    }
                    let mut run =
                        walk_partition(&rows[s..e], &odi, &w.order, &w.frame, &w.funcs, &fdi[..n1]);
                    if let Some(c) = &w.chain {
                        run.extend(walk_partition(
                            &rows[s..e],
                            &odi[..c.n_ord],
                            &w.order[..c.n_ord],
                            &c.frame,
                            &c.funcs,
                            &fdi[n1..],
                        ));
                    }
                    // runCondition cut: the emitted prefix ends BEFORE
                    // the first row failing any leg (values checked in
                    // walk order, exactly ExecQual's per-row cadence).
                    let mut cut = e - s;
                    if !w.run_conds.is_empty() {
                        cut = (0..e - s)
                            .position(|i| {
                                w.run_conds.iter().any(|rc| match run[rc.func][i] {
                                    FVal::N(v) => !rc.op.eval(v, rc.val),
                                    _ => unreachable!("run-cond target class (planner law)"),
                                })
                            })
                            .unwrap_or(e - s);
                    }
                    for (x, v) in run.into_iter().enumerate() {
                        fvals[x].extend(v.into_iter().take(cut));
                    }
                    out_rows.extend(&rows[s..s + cut]);
                    s = e;
                }
                out.push((part, PartOut { rows: out_rows, fvals }));
            },
        )
        .into_iter()
        .flatten()
        .collect();
    let mut by_part = owned;
    by_part.sort_by_key(|(p, _)| *p);
    crate::engine::phn(node, "pass2", t_p2);

    // ---- typed emit: emit columns then function columns ---------------------
    let t_e = std::time::Instant::now();
    let nrows: usize = by_part.iter().map(|(_, p)| p.rows.len()).sum();
    let mut cols_out: Vec<AnswerCol> = Vec::new();
    for (ei, &ec) in w.emit.iter().enumerate() {
        let ty = node.ty_of(ec);
        if ty.is_varlena() {
            let mut b = BytesBuild::new();
            let mut mask = Vec::with_capacity(nrows);
            for (_, p) in &by_part {
                for r in &p.rows {
                    match &r.cells[edi[ei]] {
                        Cell::B(Some(x)) => {
                            b.push(x);
                            mask.push(true);
                        }
                        _ => {
                            b.push(b"");
                            mask.push(false);
                        }
                    }
                }
            }
            let mut c = b.finish(ty);
            if !mask.iter().all(|&v| v) {
                c.validity = Validity::Mask(mask);
            }
            cols_out.push(c);
        } else {
            let mut v: Vec<Option<i64>> = Vec::with_capacity(nrows);
            for (_, p) in &by_part {
                for r in &p.rows {
                    v.push(match &r.cells[edi[ei]] {
                        Cell::I(x) => *x,
                        Cell::B(_) => unreachable!("emit cell class drift"),
                    });
                }
            }
            cols_out.push(AnswerCol::i64s_opt(ty, v));
        }
    }
    for (x, f) in w.all_funcs().enumerate() {
        let vals = by_part.iter().flat_map(|(_, p)| p.fvals[x].iter());
        cols_out.push(fvals_col(f, vals, nrows));
    }
    // A zero-survivor statement still answers typed zero-row columns
    // (BytesBuild/vec builders above produce them naturally).
    let mut a = AnswerSet::from_cols(cols_out);
    if nrows == 0 && a.cols.is_empty() {
        a = AnswerSet::empty(
            w.emit
                .iter()
                .map(|&c| node.ty_of(c))
                .chain(w.all_funcs().map(|f| f.out))
                .collect(),
        );
    }
    crate::engine::phn(node, "epilogue", t_e);
    crate::engine::phn(node, "total", t_all);
    a
}

// ---------------------------------------------------------------------------
// Unit gates: the C walk laws over hand-built sorted partitions.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::typmeta::TypMeta;

    /// rows with (order-key cell, input cell) at decode indexes 0/1.
    fn rows(v: &[(Option<i64>, Option<i64>)]) -> Vec<SRow> {
        v.iter()
            .enumerate()
            .map(|(i, &(k, x))| SRow {
                cells: vec![Cell::I(k), Cell::I(x)],
                ord: (0, i as u16),
            })
            .collect()
    }
    fn refs(r: &[SRow]) -> Vec<&SRow> {
        r.iter().collect()
    }
    fn f(op: WinOp) -> WinFuncSpec {
        let col = !matches!(
            op,
            WinOp::RowNumber | WinOp::Rank | WinOp::DenseRank | WinOp::CountStar
        );
        let col = col.then_some(1u32);
        WinFuncSpec::new(op, col, col.map(|_| TypMeta::INT8))
    }
    fn key0() -> Vec<crate::ir::TopKKey> {
        vec![crate::ir::TopKKey { col: 0, desc: false, nulls_first: false, lo: None, trim: false }]
    }
    /// v1-shim: default frame over decode-index-0 order key.
    fn walk_default(
        rows: &[&SRow],
        odi: &[usize],
        funcs: &[WinFuncSpec],
        fdi: &[usize],
    ) -> Vec<Vec<FVal>> {
        let keys = if odi.is_empty() { vec![] } else { key0() };
        walk_partition(rows, odi, &keys, &FrameSpec::default(), funcs, fdi)
    }

    #[test]
    fn rank_family_peer_law() {
        // order keys 10,10,20,30,30,30 -> ranks 1,1,3,4,4,4; dense 1,1,2,3,3,3.
        let r = rows(&[
            (Some(10), Some(1)),
            (Some(10), Some(2)),
            (Some(20), Some(3)),
            (Some(30), Some(4)),
            (Some(30), Some(5)),
            (Some(30), Some(6)),
        ]);
        let out = walk_default(&refs(&r),
            &[0],
            &[f(WinOp::RowNumber), f(WinOp::Rank), f(WinOp::DenseRank)],
            &[usize::MAX, usize::MAX, usize::MAX],
        );
        let n = |v: &FVal| match v {
            FVal::N(n) => *n,
            _ => panic!(),
        };
        assert_eq!(out[0].iter().map(n).collect::<Vec<_>>(), vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(out[1].iter().map(n).collect::<Vec<_>>(), vec![1, 1, 3, 4, 4, 4]);
        assert_eq!(out[2].iter().map(n).collect::<Vec<_>>(), vec![1, 1, 2, 3, 3, 3]);
    }

    #[test]
    fn default_frame_peers_share_aggregates() {
        // sum(x) OVER (ORDER BY k): frame = start..peer-group end.
        // k: 1,2,2,3 / x: 5,10,20,40 -> sums 5,35,35,75; counts 1,3,3,4.
        let r = rows(&[
            (Some(1), Some(5)),
            (Some(2), Some(10)),
            (Some(2), Some(20)),
            (Some(3), Some(40)),
        ]);
        let out = walk_default(&refs(&r),
            &[0],
            &[f(WinOp::Sum), f(WinOp::Count), f(WinOp::CountStar)],
            &[1, 1, usize::MAX],
        );
        let s = |v: &FVal| match v {
            FVal::S(s) => *s,
            _ => panic!(),
        };
        assert_eq!(out[0].iter().map(s).collect::<Vec<_>>(),
            vec![Some(5), Some(35), Some(35), Some(75)]);
        let n = |v: &FVal| match v {
            FVal::N(n) => *n,
            _ => panic!(),
        };
        assert_eq!(out[1].iter().map(n).collect::<Vec<_>>(), vec![1, 3, 3, 4]);
        assert_eq!(out[2].iter().map(n).collect::<Vec<_>>(), vec![1, 3, 3, 4]);
    }

    #[test]
    fn no_order_whole_partition_frames() {
        // Without a window ORDER BY every row is a peer: aggregates are
        // partition totals; rank/dense_rank = 1; row_number still walks.
        let r = rows(&[(None, Some(5)), (None, None), (None, Some(7))]);
        let out = walk_default(&refs(&r),
            &[],
            &[f(WinOp::Sum), f(WinOp::Count), f(WinOp::CountStar), f(WinOp::Rank),
              f(WinOp::RowNumber)],
            &[1, 1, usize::MAX, usize::MAX, usize::MAX],
        );
        assert!(out[0].iter().all(|v| *v == FVal::S(Some(12))));
        assert!(out[1].iter().all(|v| *v == FVal::N(2)));
        assert!(out[2].iter().all(|v| *v == FVal::N(3)));
        assert!(out[3].iter().all(|v| *v == FVal::N(1)));
        assert_eq!(
            out[4].iter().map(|v| match v { FVal::N(n) => *n, _ => panic!() }).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn null_input_law() {
        // count skips NULL inputs; sum/min/max over an all-NULL frame
        // answer NULL; avg pairs carry count 0 (NULL at the render).
        let r = rows(&[(Some(1), None), (Some(2), None), (Some(3), Some(9))]);
        let out = walk_default(&refs(&r),
            &[0],
            &[f(WinOp::Sum), f(WinOp::Min), f(WinOp::Count), f(WinOp::Avg)],
            &[1, 1, 1, 1],
        );
        assert_eq!(out[0][0], FVal::S(None));
        assert_eq!(out[0][2], FVal::S(Some(9)));
        assert_eq!(out[1][1], FVal::W(None));
        assert_eq!(out[1][2], FVal::W(Some(9)));
        assert_eq!(out[2][1], FVal::N(0));
        assert_eq!(out[3][0], FVal::R(0, 0));
        assert_eq!(out[3][2], FVal::R(9, 1));
    }

    use crate::ir::{FrameBound as FB, FrameMode as FM};

    fn frame(mode: FM, start: FB, end: FB) -> FrameSpec {
        FrameSpec { mode, start, end, ..FrameSpec::default() }
    }
    fn walk_framed(
        rows: &[&SRow],
        fr: &FrameSpec,
        funcs: &[WinFuncSpec],
        fdi: &[usize],
    ) -> Vec<Vec<FVal>> {
        walk_partition(rows, &[0], &key0(), fr, funcs, fdi)
    }
    fn ns(out: &[FVal]) -> Vec<i64> {
        out.iter()
            .map(|v| match v {
                FVal::N(n) => *n,
                _ => panic!(),
            })
            .collect()
    }
    fn ws(out: &[FVal]) -> Vec<Option<i64>> {
        out.iter()
            .map(|v| match v {
                FVal::W(w) => *w,
                _ => panic!(),
            })
            .collect()
    }
    fn ss(out: &[FVal]) -> Vec<Option<i128>> {
        out.iter()
            .map(|v| match v {
                FVal::S(s) => *s,
                _ => panic!(),
            })
            .collect()
    }

    #[test]
    fn rows_frame_sliding_window() {
        // ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING over x = 1,2,3,4:
        // sums 3,6,9,7; min 1,1,2,3; count* 2,3,3,2.
        let r = rows(&[(Some(1), Some(1)), (Some(2), Some(2)), (Some(3), Some(3)), (Some(4), Some(4))]);
        let fr = frame(FM::Rows, FB::Preceding(1), FB::Following(1));
        let out = walk_framed(&refs(&r), &fr, &[f(WinOp::Sum), f(WinOp::Min), f(WinOp::CountStar)], &[1, 1, usize::MAX]);
        assert_eq!(ss(&out[0]), vec![Some(3), Some(6), Some(9), Some(7)]);
        assert_eq!(ws(&out[1]), vec![Some(1), Some(1), Some(2), Some(3)]);
        assert_eq!(ns(&out[2]), vec![2, 3, 3, 2]);
    }

    #[test]
    fn rows_empty_frame_law() {
        // ROWS BETWEEN 3 FOLLOWING AND 4 FOLLOWING near the end: empty
        // frames answer NULL (sum/min) / 0 (counts) — C's strict
        // transfn law over zero aggregated rows.
        let r = rows(&[(Some(1), Some(1)), (Some(2), Some(2)), (Some(3), Some(3))]);
        let fr = frame(FM::Rows, FB::Following(3), FB::Following(4));
        let out = walk_framed(&refs(&r), &fr, &[f(WinOp::Sum), f(WinOp::Max), f(WinOp::CountStar), f(WinOp::Count)], &[1, 1, usize::MAX, 1]);
        assert_eq!(ss(&out[0]), vec![None, None, None]);
        assert_eq!(ws(&out[1]), vec![None, None, None]);
        assert_eq!(ns(&out[2]), vec![0, 0, 0]);
        assert_eq!(ns(&out[3]), vec![0, 0, 0]);
    }

    #[test]
    fn range_offset_ties_at_boundary() {
        // RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING over k = 1,2,2,4:
        // every peer AT the boundary value is in frame (value law, not
        // position): sums 6(k<=2), 6,6, 4.
        let r = rows(&[(Some(1), Some(1)), (Some(2), Some(2)), (Some(2), Some(3)), (Some(4), Some(4))]);
        let fr = frame(FM::Range, FB::Preceding(1), FB::Following(1));
        let out = walk_framed(&refs(&r), &fr, &[f(WinOp::Sum)], &[1]);
        assert_eq!(ss(&out[0]), vec![Some(6), Some(6), Some(6), Some(4)]);
    }

    #[test]
    fn range_offset_null_region_law() {
        // NULL order keys are their OWN frame region under offset
        // bounds (nulls last here): non-null frames never see them,
        // a NULL current row's frame is exactly the NULL region.
        let r = rows(&[(Some(1), Some(1)), (Some(2), Some(2)), (None, Some(10)), (None, Some(20))]);
        let fr = frame(FM::Range, FB::Preceding(1), FB::Following(1));
        let out = walk_framed(&refs(&r), &fr, &[f(WinOp::Sum), f(WinOp::CountStar)], &[1, usize::MAX]);
        assert_eq!(ss(&out[0]), vec![Some(3), Some(3), Some(30), Some(30)]);
        assert_eq!(ns(&out[1]), vec![2, 2, 2, 2]);
    }

    #[test]
    fn range_desc_offset_flip() {
        // Descending key: PRECEDING walks toward LARGER values (C flips
        // in_range's sub/less). k desc = 4,2,1; RANGE 1 PRECEDING..CURRENT:
        // frames {4}, {2}, {2,1}.
        let r: Vec<SRow> = [(4, 4), (2, 2), (1, 1)]
            .iter()
            .enumerate()
            .map(|(i, &(k, x))| SRow { cells: vec![Cell::I(Some(k)), Cell::I(Some(x))], ord: (0, i as u16) })
            .collect();
        let keys = vec![crate::ir::TopKKey { col: 0, desc: true, nulls_first: false, lo: None, trim: false }];
        let fr = frame(FM::Range, FB::Preceding(1), FB::CurrentRow);
        let out = walk_partition(&refs(&r), &[0], &keys, &fr, &[f(WinOp::Sum)], &[1]);
        assert_eq!(ss(&out[0]), vec![Some(4), Some(2), Some(3)]);
    }

    #[test]
    fn groups_offset_law() {
        // GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW over k=1,1,2,3:
        // frames = current + previous peer group.
        let r = rows(&[(Some(1), Some(1)), (Some(1), Some(2)), (Some(2), Some(4)), (Some(3), Some(8))]);
        let fr = frame(FM::Groups, FB::Preceding(1), FB::CurrentRow);
        let out = walk_framed(&refs(&r), &fr, &[f(WinOp::Sum)], &[1]);
        assert_eq!(ss(&out[0]), vec![Some(3), Some(3), Some(7), Some(12)]);
    }

    #[test]
    fn value_functions_frame_and_partition_edges() {
        // first/last/nth respect the frame; lead/lag are positional and
        // NULL off the partition edges (C's WinGetFuncArgInPartition).
        let r = rows(&[(Some(1), Some(10)), (Some(2), Some(20)), (Some(3), Some(30))]);
        let fr = frame(FM::Rows, FB::Preceding(1), FB::CurrentRow);
        let out = walk_framed(
            &refs(&r),
            &fr,
            &[f(WinOp::FirstValue), f(WinOp::LastValue), f(WinOp::NthValue), f(WinOp::Lead), f(WinOp::Lag)],
            &[1, 1, 1, 1, 1],
        );
        assert_eq!(ws(&out[0]), vec![Some(10), Some(10), Some(20)]);
        assert_eq!(ws(&out[1]), vec![Some(10), Some(20), Some(30)]);
        // nth_value(x, 1) = first in frame.
        assert_eq!(ws(&out[2]), vec![Some(10), Some(10), Some(20)]);
        assert_eq!(ws(&out[3]), vec![Some(20), Some(30), None]);
        assert_eq!(ws(&out[4]), vec![None, Some(10), Some(20)]);
    }

    #[test]
    fn last_value_default_frame_peer_gotcha() {
        // The classic: last_value under the DEFAULT frame ends at the
        // current row's PEER GROUP end, not the partition end.
        let r = rows(&[(Some(1), Some(10)), (Some(2), Some(20)), (Some(2), Some(30)), (Some(3), Some(40))]);
        let out = walk_default(&refs(&r), &[0], &[f(WinOp::LastValue)], &[1]);
        assert_eq!(ws(&out[0]), vec![Some(10), Some(30), Some(30), Some(40)]);
    }

    // -- [winv3] EXCLUDE laws ------------------------------------------------

    fn framex(mode: FM, start: FB, end: FB, ex: crate::ir::FrameExclusion) -> FrameSpec {
        FrameSpec { mode, start, end, exclusion: ex, ..FrameSpec::default() }
    }
    use crate::ir::FrameExclusion as FX;

    #[test]
    fn exclude_current_row_rows_frame() {
        // ROWS 1 PRECEDING..1 FOLLOWING EXCLUDE CURRENT ROW over x =
        // 1,2,3,4: sums 2, 4, 6, 3; count* 1,2,2,1; min 2,1,2,3.
        let r = rows(&[(Some(1), Some(1)), (Some(2), Some(2)), (Some(3), Some(3)), (Some(4), Some(4))]);
        let fr = framex(FM::Rows, FB::Preceding(1), FB::Following(1), FX::CurrentRow);
        let out = walk_framed(&refs(&r), &fr, &[f(WinOp::Sum), f(WinOp::CountStar), f(WinOp::Min)], &[1, usize::MAX, 1]);
        assert_eq!(ss(&out[0]), vec![Some(2), Some(4), Some(6), Some(3)]);
        assert_eq!(ns(&out[1]), vec![1, 2, 2, 1]);
        assert_eq!(ws(&out[2]), vec![Some(2), Some(1), Some(2), Some(3)]);
    }

    #[test]
    fn exclude_group_and_ties_default_frame() {
        // Default frame (RANGE UNB..CURRENT) over k = 1,1,2 / x =
        // 10,20,40. GROUP: row0/1 exclude their whole peer group ->
        // empty; row2 keeps rows 0,1 -> 30. TIES: peers minus self ->
        // 10, 20, 70 (row2 has no peers).
        let r = rows(&[(Some(1), Some(10)), (Some(1), Some(20)), (Some(2), Some(40))]);
        let fg = framex(FM::Range, FB::UnboundedPreceding, FB::CurrentRow, FX::Group);
        let out = walk_framed(&refs(&r), &fg, &[f(WinOp::Sum), f(WinOp::CountStar)], &[1, usize::MAX]);
        assert_eq!(ss(&out[0]), vec![None, None, Some(30)]);
        assert_eq!(ns(&out[1]), vec![0, 0, 2]);
        let ft = framex(FM::Range, FB::UnboundedPreceding, FB::CurrentRow, FX::Ties);
        let out = walk_framed(&refs(&r), &ft, &[f(WinOp::Sum), f(WinOp::Min)], &[1, 1]);
        assert_eq!(ss(&out[0]), vec![Some(10), Some(20), Some(70)]);
        assert_eq!(ws(&out[1]), vec![Some(10), Some(20), Some(10)]);
    }

    #[test]
    fn exclude_no_order_by_peer_law() {
        // Without a window ORDER BY every partition row is a peer:
        // EXCLUDE GROUP empties every frame; EXCLUDE TIES keeps only
        // the current row itself.
        let r = rows(&[(None, Some(5)), (None, Some(7)), (None, None)]);
        let fg = framex(FM::Range, FB::UnboundedPreceding, FB::UnboundedFollowing, FX::Group);
        let out = walk_partition(&refs(&r), &[], &[], &fg, &[f(WinOp::Sum), f(WinOp::Count)], &[1, 1]);
        assert!(out[0].iter().all(|v| *v == FVal::S(None)));
        assert!(out[1].iter().all(|v| *v == FVal::N(0)));
        let ft = framex(FM::Range, FB::UnboundedPreceding, FB::UnboundedFollowing, FX::Ties);
        let out = walk_partition(&refs(&r), &[], &[], &ft, &[f(WinOp::Sum), f(WinOp::Max)], &[1, 1]);
        assert_eq!(ss(&out[0]), vec![Some(5), Some(7), None]);
        assert_eq!(ws(&out[1]), vec![Some(5), Some(7), None]);
    }

    #[test]
    fn exclude_minmax_sparse_table_nulls() {
        // The sparse-table min/max leg: NULL cells never win; an
        // all-excluded / all-NULL remainder answers NULL.
        let r = rows(&[
            (Some(1), Some(9)),
            (Some(2), None),
            (Some(3), Some(4)),
            (Some(4), Some(6)),
            (Some(5), None),
        ]);
        let fr = framex(FM::Rows, FB::Preceding(1), FB::Following(1), FX::CurrentRow);
        let out = walk_framed(&refs(&r), &fr, &[f(WinOp::Min), f(WinOp::Max)], &[1, 1]);
        assert_eq!(ws(&out[0]), vec![None, Some(4), Some(6), Some(4), Some(6)]);
        assert_eq!(ws(&out[1]), vec![None, Some(9), Some(6), Some(4), Some(6)]);
        // wider frame: min over [k-2, k+2] minus self.
        let fr = framex(FM::Rows, FB::Preceding(2), FB::Following(2), FX::CurrentRow);
        let out = walk_framed(&refs(&r), &fr, &[f(WinOp::Min)], &[1]);
        assert_eq!(ws(&out[0]), vec![Some(4), Some(4), Some(6), Some(4), Some(4)]);
    }

    #[test]
    fn exclude_value_functions_segments() {
        // first/last/nth read the ascending segment concatenation
        // (WinGetFuncArgInFrame's exclusion seek): ROWS UNB..UNB
        // EXCLUDE GROUP over tied k = 1,1,2,3.
        let r = rows(&[(Some(1), Some(10)), (Some(1), Some(20)), (Some(2), Some(40)), (Some(3), Some(80))]);
        let fr = framex(FM::Rows, FB::UnboundedPreceding, FB::UnboundedFollowing, FX::Group);
        let out = walk_framed(&refs(&r), &fr,
            &[f(WinOp::FirstValue), f(WinOp::LastValue), f(WinOp::NthValue)], &[1, 1, 1]);
        // row0/1 exclude rows {0,1}: remaining 40,80. row2: 10,20,80.
        assert_eq!(ws(&out[0]), vec![Some(40), Some(40), Some(10), Some(10)]);
        assert_eq!(ws(&out[1]), vec![Some(80), Some(80), Some(80), Some(40)]);
        // nth_value(x, 1) = first of the concatenation.
        assert_eq!(ws(&out[2]), vec![Some(40), Some(40), Some(10), Some(10)]);
        // EXCLUDE TIES keeps self in place: row0 sees 10,40,80.
        let ft = framex(FM::Rows, FB::UnboundedPreceding, FB::UnboundedFollowing, FX::Ties);
        let out = walk_framed(&refs(&r), &ft, &[f(WinOp::NthValue)], &[1]);
        // nth 1 = first included: row0 -> its own 10 (peer row1 gone).
        assert_eq!(ws(&out[0]), vec![Some(10), Some(20), Some(10), Some(10)]);
    }

    #[test]
    fn exclude_empty_after_exclusion() {
        // ROWS CURRENT ROW..CURRENT ROW EXCLUDE CURRENT ROW: every
        // frame empties -> NULL / 0 per agg class.
        let r = rows(&[(Some(1), Some(1)), (Some(2), Some(2))]);
        let fr = framex(FM::Rows, FB::CurrentRow, FB::CurrentRow, FX::CurrentRow);
        let out = walk_framed(&refs(&r), &fr,
            &[f(WinOp::Sum), f(WinOp::Min), f(WinOp::CountStar), f(WinOp::Count), f(WinOp::FirstValue)],
            &[1, 1, usize::MAX, 1, 1]);
        assert_eq!(ss(&out[0]), vec![None, None]);
        assert_eq!(ws(&out[1]), vec![None, None]);
        assert_eq!(ns(&out[2]), vec![0, 0]);
        assert_eq!(ns(&out[3]), vec![0, 0]);
        assert_eq!(ws(&out[4]), vec![None, None]);
    }

    // -- [winv3] calendar-month interval laws --------------------------------

    /// usec timestamp for a calendar date (midnight).
    fn ts(y: i32, m: i32, d: i32) -> i64 {
        ((date2j(y, m, d) as i128 - POSTGRES_EPOCH_JDATE) * USECS_PER_DAY) as i64
    }

    #[test]
    fn ts_pl_cal_month_clamp_law() {
        // C's timestamp_pl_interval month leg: end-of-month clamping.
        assert_eq!(ts_pl_cal(ts(2024, 1, 31), 1, 0), ts(2024, 2, 29) as i128); // leap
        assert_eq!(ts_pl_cal(ts(2023, 1, 31), 1, 0), ts(2023, 2, 28) as i128);
        assert_eq!(ts_pl_cal(ts(2024, 3, 31), -1, 0), ts(2024, 2, 29) as i128);
        assert_eq!(ts_pl_cal(ts(2024, 5, 31), 1, 0), ts(2024, 6, 30) as i128);
        // year boundary + negative normalization (C truncating / %).
        assert_eq!(ts_pl_cal(ts(2024, 1, 15), -1, 0), ts(2023, 12, 15) as i128);
        assert_eq!(ts_pl_cal(ts(2024, 12, 15), 1, 0), ts(2025, 1, 15) as i128);
        assert_eq!(ts_pl_cal(ts(2024, 2, 29), 12, 0), ts(2025, 2, 28) as i128);
        assert_eq!(ts_pl_cal(ts(2024, 2, 29), -12, 0), ts(2023, 2, 28) as i128);
        // mixed month + folded day/time usec leg (month first, C order).
        assert_eq!(
            ts_pl_cal(ts(2024, 1, 31), 1, USECS_PER_DAY as i64),
            ts(2024, 3, 1) as i128
        );
        // time-of-day rides through the month leg untouched.
        assert_eq!(
            ts_pl_cal(ts(2024, 1, 31) + 3_600_000_000, 1, 0),
            (ts(2024, 2, 29) + 3_600_000_000) as i128
        );
    }

    #[test]
    fn range_calendar_month_bounds() {
        // RANGE '1 month' PRECEDING .. CURRENT ROW over month-end
        // keys: the clamped subtraction pulls the previous month-end
        // into frame (2024-03-31 - 1 mon = 2024-02-29).
        let keys = [ts(2024, 1, 31), ts(2024, 2, 29), ts(2024, 3, 31)];
        let r: Vec<SRow> = keys
            .iter()
            .enumerate()
            .map(|(i, &k)| SRow {
                cells: vec![Cell::I(Some(k)), Cell::I(Some(1 << i))],
                ord: (0, i as u16),
            })
            .collect();
        let fr = FrameSpec {
            mode: FM::Range,
            start: FB::Preceding(31 * USECS_PER_DAY as i128),
            end: FB::CurrentRow,
            cal_start: Some(crate::ir::CalOff { months: 1, usecs: 0 }),
            ..FrameSpec::default()
        };
        let out = walk_partition(&refs(&r), &[0], &key0(), &fr, &[f(WinOp::Sum)], &[1]);
        // row0 {1}; row1: head >= 2024-01-29 -> {1,2}=3; row2: head >=
        // 2024-02-29 (clamped) -> {2,4}=6.
        assert_eq!(ss(&out[0]), vec![Some(1), Some(3), Some(6)]);
    }

    #[test]
    fn range_calendar_month_desc_flip() {
        // Descending key: PRECEDING walks toward LARGER values with
        // the interval sign flipped in the true domain.
        let keys = [ts(2024, 3, 31), ts(2024, 2, 29), ts(2024, 1, 31)];
        let r: Vec<SRow> = keys
            .iter()
            .enumerate()
            .map(|(i, &k)| SRow {
                cells: vec![Cell::I(Some(k)), Cell::I(Some(1 << i))],
                ord: (0, i as u16),
            })
            .collect();
        let dkeys = vec![crate::ir::TopKKey { col: 0, desc: true, nulls_first: false, lo: None, trim: false }];
        let fr = FrameSpec {
            mode: FM::Range,
            start: FB::Preceding(31 * USECS_PER_DAY as i128),
            end: FB::CurrentRow,
            cal_start: Some(crate::ir::CalOff { months: 1, usecs: 0 }),
            ..FrameSpec::default()
        };
        let out = walk_partition(&refs(&r), &[0], &dkeys, &fr, &[f(WinOp::Sum)], &[1]);
        // row0 {1}; row1: keys <= 2024-03-29 is FALSE for 2024-03-31,
        // head at 2024-02-29 + 1mon = 2024-03-29 -> {2}? No: head =
        // first walk row with true_key <= cur + 1mon; cur=2024-02-29 ->
        // 2024-03-29 excludes 2024-03-31 -> {2}=2. row2: cur=2024-01-31
        // + 1 mon = 2024-02-29 -> {2,4}=6.
        assert_eq!(ss(&out[0]), vec![Some(1), Some(2), Some(6)]);
    }

    #[test]
    fn null_order_keys_are_peers() {
        // NULL order-key cells form ONE peer group (are_peers'
        // not-distinct law): both NULL rows share rank and aggregates.
        let r = rows(&[(Some(1), Some(1)), (None, Some(2)), (None, Some(4))]);
        let out = walk_default(&refs(&r), &[0], &[f(WinOp::Rank), f(WinOp::Sum)], &[usize::MAX, 1]);
        assert_eq!(out[0][1], FVal::N(2));
        assert_eq!(out[0][2], FVal::N(2));
        assert_eq!(out[1][1], FVal::S(Some(7)));
        assert_eq!(out[1][2], FVal::S(Some(7)));
    }

    #[test]
    fn over_answer_walk_aligns_input_rows() {
        // [winv4] the answer-as-input hop: spec cols index ANSWER
        // columns; function values come back aligned with the INPUT
        // row order (the walk sorts internally, then scatters back).
        use crate::answer::{AnswerCol, ColData};
        let part = AnswerCol::i64s(TypMeta::INT4, vec![1, 0, 1, 0]);
        let key = AnswerCol::i64s(TypMeta::INT4, vec![2, 1, 1, 2]);
        let x = AnswerCol::i64s_opt(TypMeta::INT8, vec![Some(10), Some(1), None, Some(4)]);
        let spec = crate::ir::WindowSpec {
            part_cols: vec![0],
            order: vec![crate::ir::TopKKey { col: 1, desc: false, nulls_first: false, lo: None, trim: false }],
            funcs: vec![
                WinFuncSpec::new(WinOp::Sum, Some(2), Some(TypMeta::INT8)),
                WinFuncSpec::new(WinOp::RowNumber, None, None),
            ],
            emit: vec![],
            frame: FrameSpec::default(),
            run_conds: vec![],
            chain: None,
        };
        let out = run_window_over_answer(&[part, key, x], 4, &spec, u64::MAX).unwrap();
        // partition 0 by key: r1(1) r3(2) -> sums 1, 5; rownum 1, 2.
        // partition 1 by key: r2(NULL) r0(10) -> sums NULL, 10; 1, 2.
        let ColData::I128(sums) = &out[0].data else { panic!("sum class") };
        assert_eq!(sums, &vec![10, 1, 0, 5]);
        assert!(out[0].validity.is_valid(0) && !out[0].validity.is_valid(2));
        let ColData::I64(rn) = &out[1].data else { panic!("rownum class") };
        assert_eq!(rn, &vec![2, 1, 1, 2]);
    }

    #[test]
    fn over_answer_budget_refuses() {
        use crate::answer::AnswerCol;
        let part = AnswerCol::i64s(TypMeta::INT4, vec![0; 64]);
        let spec = crate::ir::WindowSpec {
            part_cols: vec![0],
            order: vec![],
            funcs: vec![WinFuncSpec::new(WinOp::CountStar, None, None)],
            emit: vec![],
            frame: FrameSpec::default(),
            run_conds: vec![],
            chain: None,
        };
        let r = run_window_over_answer(&[part], 64, &spec, 16);
        assert!(matches!(r, Err(crate::refuse::Refuse::WinOverBudget { .. })));
    }
}
