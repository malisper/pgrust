// partprune.c bound-matching kernel (get_matching_*_bounds + combine step),
// shared by the planner (plan-time pruning) and the executor (initial/exec
// pruning). Bound comparisons are caller-monomorphized closures so the
// planner's DatumImage bounds and the executor's partdesc bounds both fit.
#![allow(non_snake_case)]

use datum::Datum;
use mcx::Mcx;
use types_error::PgResult;
use types_nodes::Bitmapset;

pub const PARTITION_MAX_KEYS: usize = 32;

pub const InvalidStrategy: u16 = 0;
pub const BTLessStrategyNumber: u16 = 1;
pub const BTLessEqualStrategyNumber: u16 = 2;
pub const BTEqualStrategyNumber: u16 = 3;
pub const BTGreaterEqualStrategyNumber: u16 = 4;
pub const BTGreaterStrategyNumber: u16 = 5;
pub const BTMaxStrategyNumber: usize = 5;
pub const HTEqualStrategyNumber: u16 = 1;
pub const HTMaxStrategyNumber: usize = 1;

pub const KIND_MINVALUE: i8 = -1;
pub const KIND_VALUE: i8 = 0;
pub const KIND_MAXVALUE: i8 = 1;

/// Read-only view over a PartitionBoundInfo; `kind_at` returns KIND_VALUE for
/// list/hash strategies.
pub trait BoundInfo {
    fn strategy(&self) -> u8;
    fn ndatums(&self) -> i32;
    fn nindexes(&self) -> i32;
    fn index_at(&self, i: i32) -> i32;
    fn datum_at(&self, i: i32, j: i32) -> Datum;
    fn kind_at(&self, i: i32, j: i32) -> i8;
    fn null_index(&self) -> i32;
    fn default_index(&self) -> i32;
    fn has_default(&self) -> bool {
        self.default_index() != -1
    }
    fn accepts_nulls(&self) -> bool {
        self.null_index() != -1
    }
}

pub const HASH_PARTITION_SEED: u64 = 0x7A5B22367996DCFD;

#[inline]
pub fn hash_combine64(a: u64, b: u64) -> u64 {
    a ^ (b
        .wrapping_add(0x49a0f4dd15e5a8e3)
        .wrapping_add(a << 54)
        .wrapping_add(a >> 7))
}

impl BoundInfo for types_pathnodes::PartitionBoundInfoData<'_> {
    fn strategy(&self) -> u8 {
        self.strategy as u8
    }
    fn ndatums(&self) -> i32 {
        self.ndatums
    }
    fn nindexes(&self) -> i32 {
        self.nindexes
    }
    fn index_at(&self, i: i32) -> i32 {
        self.indexes[i as usize]
    }
    fn datum_at(&self, i: i32, j: i32) -> Datum {
        match &self.datums[i as usize][j as usize] {
            types_pathnodes::DatumImage::ByVal(w) => Datum::from_u64(*w),
            types_pathnodes::DatumImage::Bytes(b) => Datum::from_usize(b.as_ptr() as usize),
        }
    }
    fn kind_at(&self, i: i32, j: i32) -> i8 {
        match &self.kind {
            Some(k) => k[i as usize][j as usize],
            None => KIND_VALUE,
        }
    }
    fn null_index(&self) -> i32 {
        self.null_index
    }
    fn default_index(&self) -> i32 {
        self.default_index
    }
}

impl BoundInfo for partbounds::PartitionBoundInfoData<'_> {
    fn strategy(&self) -> u8 {
        self.strategy as u8
    }
    fn ndatums(&self) -> i32 {
        self.ndatums as i32
    }
    fn nindexes(&self) -> i32 {
        self.indexes.len() as i32
    }
    fn index_at(&self, i: i32) -> i32 {
        self.indexes[i as usize]
    }
    fn datum_at(&self, i: i32, j: i32) -> Datum {
        self.datum(i as usize, j as usize)
    }
    fn kind_at(&self, i: i32, j: i32) -> i8 {
        if self.kind.is_empty() {
            KIND_VALUE
        } else {
            partbounds::PartitionBoundInfoData::kind_at(self, i as usize, j as usize)
        }
    }
    fn null_index(&self) -> i32 {
        self.null_index
    }
    fn default_index(&self) -> i32 {
        self.default_index
    }
}

pub struct PruneStepResult<'mcx> {
    pub bound_offsets: Bitmapset<'mcx>,
    pub scan_default: bool,
    pub scan_null: bool,
}

impl<'mcx> PruneStepResult<'mcx> {
    pub fn empty() -> Self {
        PruneStepResult {
            bound_offsets: Bitmapset::empty(),
            scan_default: false,
            scan_null: false,
        }
    }
}

pub fn bms_add_range<'mcx>(
    mcx: Mcx<'mcx>,
    set: &mut Bitmapset<'mcx>,
    lo: i32,
    hi: i32,
) -> PgResult<()> {
    // C bms_add_range (bitmapset.c:1029) treats an inverted range as a no-op
    // before any validity check; partprune's callers rely on that for empty
    // pruning results.
    if hi < lo {
        return Ok(());
    }
    for x in lo..=hi {
        set.add_member(mcx, x)?;
    }
    Ok(())
}

// partition_list_bsearch (partbounds.c) over a caller cmp(bound, probe).
fn list_bsearch<B: BoundInfo>(
    boundinfo: &B,
    is_equal: &mut bool,
    mut cmp: impl FnMut(Datum) -> i32,
) -> i32 {
    let mut lo: i32 = -1;
    let mut hi: i32 = boundinfo.ndatums() - 1;
    while lo < hi {
        let mid = (lo + hi + 1) / 2;
        let cmpval = cmp(boundinfo.datum_at(mid, 0));
        if cmpval <= 0 {
            lo = mid;
            *is_equal = cmpval == 0;
            if *is_equal {
                break;
            }
        } else {
            hi = mid - 1;
        }
    }
    lo
}

// partition_rbound_datum_cmp (partbounds.c): cmp(col, bound_datum) compares
// the bound's column datum against the probe's column value.
fn rbound_datum_cmp<B: BoundInfo>(
    boundinfo: &B,
    row: i32,
    nvalues: i32,
    cmp: &mut impl FnMut(i32, Datum) -> i32,
) -> i32 {
    let mut cmpval = -1;
    for j in 0..nvalues {
        match boundinfo.kind_at(row, j) {
            KIND_MINVALUE => return -1,
            KIND_MAXVALUE => return 1,
            _ => {}
        }
        cmpval = cmp(j, boundinfo.datum_at(row, j));
        if cmpval != 0 {
            break;
        }
    }
    cmpval
}

// partition_range_datum_bsearch (partbounds.c).
fn range_datum_bsearch<B: BoundInfo>(
    boundinfo: &B,
    nvalues: i32,
    is_equal: &mut bool,
    cmp: &mut impl FnMut(i32, Datum) -> i32,
) -> i32 {
    let mut lo: i32 = -1;
    let mut hi: i32 = boundinfo.ndatums() - 1;
    while lo < hi {
        let mid = (lo + hi + 1) / 2;
        let cmpval = rbound_datum_cmp(boundinfo, mid, nvalues, cmp);
        if cmpval <= 0 {
            lo = mid;
            *is_equal = cmpval == 0;
            if *is_equal {
                break;
            }
        } else {
            hi = mid - 1;
        }
    }
    lo
}

/// get_matching_hash_bounds (partprune.c); `row_hash` computes
/// compute_partition_hash_value over the caller's resolved hash supfuncs.
pub fn get_matching_hash_bounds<'mcx, B: BoundInfo>(
    mcx: Mcx<'mcx>,
    boundinfo: &B,
    partnatts: i32,
    opstrategy: u16,
    nvalues: i32,
    nullkeys: &Bitmapset<'_>,
    row_hash: impl FnOnce() -> u64,
) -> PgResult<PruneStepResult<'mcx>> {
    let mut result = PruneStepResult::empty();
    if nvalues + nullkeys.num_members() == partnatts {
        debug_assert!(opstrategy == HTEqualStrategyNumber || nvalues == 0);
        let greatest_modulus = boundinfo.nindexes() as u64;
        let off = (row_hash() % greatest_modulus) as i32;
        if boundinfo.index_at(off) >= 0 {
            result.bound_offsets = Bitmapset::make_singleton(mcx, off)?;
        }
    } else {
        bms_add_range(mcx, &mut result.bound_offsets, 0, boundinfo.nindexes() - 1)?;
    }
    Ok(result)
}

/// get_matching_list_bounds (partprune.c); `cmp(bound)` compares a bound
/// datum against the probe value.
pub fn get_matching_list_bounds<'mcx, B: BoundInfo>(
    mcx: Mcx<'mcx>,
    boundinfo: &B,
    opstrategy: u16,
    nvalues: i32,
    nullkeys: &Bitmapset<'_>,
    mut cmp: impl FnMut(Datum) -> i32,
) -> PgResult<PruneStepResult<'mcx>> {
    let mut result = PruneStepResult::empty();

    if !nullkeys.is_empty() {
        if boundinfo.accepts_nulls() {
            result.scan_null = true;
        } else {
            result.scan_default = boundinfo.has_default();
        }
        return Ok(result);
    }

    if boundinfo.ndatums() == 0 {
        result.scan_default = boundinfo.has_default();
        return Ok(result);
    }

    let mut minoff = 0;
    let mut maxoff = boundinfo.ndatums() - 1;

    if nvalues == 0 {
        bms_add_range(mcx, &mut result.bound_offsets, 0, boundinfo.ndatums() - 1)?;
        result.scan_default = boundinfo.has_default();
        return Ok(result);
    }

    let mut is_equal = false;
    if opstrategy == InvalidStrategy {
        // <> clause: all bounds minus the matched datum, plus the default.
        bms_add_range(mcx, &mut result.bound_offsets, 0, boundinfo.ndatums() - 1)?;
        let off = list_bsearch(boundinfo, &mut is_equal, &mut cmp);
        if off >= 0 && is_equal {
            debug_assert!(boundinfo.index_at(off) >= 0);
            result.bound_offsets.del_member(off);
        }
        result.scan_default = boundinfo.has_default();
        return Ok(result);
    }

    if opstrategy != BTEqualStrategyNumber {
        result.scan_default = boundinfo.has_default();
    }

    let mut inclusive = false;
    match opstrategy {
        BTEqualStrategyNumber => {
            let off = list_bsearch(boundinfo, &mut is_equal, &mut cmp);
            if off >= 0 && is_equal {
                debug_assert!(boundinfo.index_at(off) >= 0);
                result.bound_offsets = Bitmapset::make_singleton(mcx, off)?;
            } else {
                result.scan_default = boundinfo.has_default();
            }
            return Ok(result);
        }
        BTGreaterEqualStrategyNumber | BTGreaterStrategyNumber => {
            if opstrategy == BTGreaterEqualStrategyNumber {
                inclusive = true;
            }
            let mut off = list_bsearch(boundinfo, &mut is_equal, &mut cmp);
            if off >= 0 {
                if !is_equal || !inclusive {
                    off += 1;
                }
            } else {
                off = 0;
            }
            if off > boundinfo.ndatums() - 1 {
                return Ok(result);
            }
            minoff = off;
        }
        BTLessEqualStrategyNumber | BTLessStrategyNumber => {
            if opstrategy == BTLessEqualStrategyNumber {
                inclusive = true;
            }
            let mut off = list_bsearch(boundinfo, &mut is_equal, &mut cmp);
            if off >= 0 && is_equal && !inclusive {
                off -= 1;
            }
            if off < 0 {
                return Ok(result);
            }
            maxoff = off;
        }
        other => panic!("invalid strategy number {other}"),
    }

    debug_assert!(minoff >= 0 && maxoff >= 0);
    bms_add_range(mcx, &mut result.bound_offsets, minoff, maxoff)?;
    Ok(result)
}

/// get_matching_range_bounds (partprune.c); `cmp(col, bound)` compares a
/// bound column datum against the probe's column value.
pub fn get_matching_range_bounds<'mcx, B: BoundInfo>(
    mcx: Mcx<'mcx>,
    boundinfo: &B,
    partnatts: i32,
    opstrategy: u16,
    nvalues: i32,
    nullkeys: &Bitmapset<'_>,
    cmp: &mut impl FnMut(i32, Datum) -> i32,
) -> PgResult<PruneStepResult<'mcx>> {
    let mut result = PruneStepResult::empty();
    debug_assert!(nvalues <= partnatts);

    if boundinfo.ndatums() == 0 || !nullkeys.is_empty() {
        result.scan_default = boundinfo.has_default();
        return Ok(result);
    }

    let mut minoff: i32 = 0;
    let mut maxoff: i32 = boundinfo.ndatums();

    if nvalues == 0 {
        if boundinfo.index_at(minoff) < 0 {
            minoff += 1;
        }
        if boundinfo.index_at(maxoff) < 0 {
            maxoff -= 1;
        }
        result.scan_default = boundinfo.has_default();
        debug_assert!(boundinfo.index_at(minoff) >= 0 && boundinfo.index_at(maxoff) >= 0);
        bms_add_range(mcx, &mut result.bound_offsets, minoff, maxoff)?;
        return Ok(result);
    }

    if nvalues < partnatts {
        result.scan_default = boundinfo.has_default();
    }

    let mut is_equal = false;
    let mut inclusive = false;
    match opstrategy {
        BTEqualStrategyNumber => {
            let mut off = range_datum_bsearch(boundinfo, nvalues, &mut is_equal, cmp);
            if off >= 0 && is_equal {
                if nvalues == partnatts {
                    result.bound_offsets = Bitmapset::make_singleton(mcx, off + 1)?;
                    return Ok(result);
                }
                let saved_off = off;
                while off >= 1 {
                    if rbound_datum_cmp(boundinfo, off - 1, nvalues, cmp) != 0 {
                        break;
                    }
                    off -= 1;
                }
                if boundinfo.kind_at(off, nvalues) == KIND_MINVALUE {
                    off += 1;
                }
                minoff = off;
                off = saved_off;
                while off < boundinfo.ndatums() - 1 {
                    if rbound_datum_cmp(boundinfo, off + 1, nvalues, cmp) != 0 {
                        break;
                    }
                    off += 1;
                }
                maxoff = off + 1;
                debug_assert!(minoff >= 0 && maxoff >= 0);
                bms_add_range(mcx, &mut result.bound_offsets, minoff, maxoff)?;
            } else {
                result.bound_offsets = Bitmapset::make_singleton(mcx, off + 1)?;
            }
            return Ok(result);
        }
        BTGreaterEqualStrategyNumber | BTGreaterStrategyNumber => {
            if opstrategy == BTGreaterEqualStrategyNumber {
                inclusive = true;
            }
            let mut off = range_datum_bsearch(boundinfo, nvalues, &mut is_equal, cmp);
            if off < 0 {
                minoff = 0;
            } else if is_equal && nvalues < partnatts {
                while off >= 1 && off < boundinfo.ndatums() - 1 {
                    let nextoff = if inclusive { off - 1 } else { off + 1 };
                    if rbound_datum_cmp(boundinfo, nextoff, nvalues, cmp) != 0 {
                        break;
                    }
                    off = nextoff;
                }
                minoff = if inclusive { off } else { off + 1 };
            } else {
                minoff = off + 1;
            }
        }
        BTLessEqualStrategyNumber | BTLessStrategyNumber => {
            if opstrategy == BTLessEqualStrategyNumber {
                inclusive = true;
            }
            let mut off = range_datum_bsearch(boundinfo, nvalues, &mut is_equal, cmp);
            if off >= 0 {
                if is_equal && nvalues < partnatts {
                    while off >= 1 && off < boundinfo.ndatums() - 1 {
                        let nextoff = if inclusive { off + 1 } else { off - 1 };
                        if rbound_datum_cmp(boundinfo, nextoff, nvalues, cmp) != 0 {
                            break;
                        }
                        off = nextoff;
                    }
                    maxoff = if inclusive { off + 1 } else { off };
                } else if !is_equal || inclusive {
                    maxoff = off + 1;
                } else {
                    maxoff = off;
                }
            } else {
                maxoff = off + 1;
            }
        }
        other => panic!("invalid strategy number {other}"),
    }

    debug_assert!(minoff >= 0 && minoff <= boundinfo.ndatums());
    debug_assert!(maxoff >= 0 && maxoff <= boundinfo.ndatums());

    if minoff < boundinfo.ndatums() && boundinfo.index_at(minoff) < 0 {
        let lastkey = nvalues - 1;
        if boundinfo.kind_at(minoff, lastkey) == KIND_MINVALUE {
            minoff += 1;
            debug_assert!(boundinfo.index_at(minoff) >= 0);
        }
    }
    if maxoff >= 1 && boundinfo.index_at(maxoff) < 0 {
        let lastkey = nvalues - 1;
        if boundinfo.kind_at(maxoff - 1, lastkey) == KIND_MAXVALUE {
            maxoff -= 1;
            debug_assert!(boundinfo.index_at(maxoff) >= 0);
        }
    }

    debug_assert!(minoff >= 0 && maxoff >= 0);
    if minoff <= maxoff {
        bms_add_range(mcx, &mut result.bound_offsets, minoff, maxoff)?;
    }
    Ok(result)
}

pub const PARTPRUNE_COMBINE_UNION: u32 = 0;
pub const PARTPRUNE_COMBINE_INTERSECT: u32 = 1;

/// perform_pruning_combine_step (partprune.c); `source_stepids` index into
/// `step_results` (None for the empty-source step's own slot).
pub fn perform_pruning_combine_step<'mcx, B: BoundInfo>(
    mcx: Mcx<'mcx>,
    boundinfo: &B,
    combine_op: u32,
    step_id: i32,
    source_stepids: impl Iterator<Item = i32> + Clone,
    step_results: &[Option<PruneStepResult<'mcx>>],
) -> PgResult<PruneStepResult<'mcx>> {
    let mut result = PruneStepResult::empty();

    if source_stepids.clone().next().is_none() {
        bms_add_range(mcx, &mut result.bound_offsets, 0, boundinfo.nindexes() - 1)?;
        result.scan_default = boundinfo.has_default();
        result.scan_null = boundinfo.accepts_nulls();
        return Ok(result);
    }

    match combine_op {
        PARTPRUNE_COMBINE_UNION => {
            for sid in source_stepids {
                assert!(sid < step_id, "invalid pruning combine step argument");
                let sr = step_results[sid as usize]
                    .as_ref()
                    .expect("source step evaluated before combine step");
                result.bound_offsets.add_members(mcx, &sr.bound_offsets)?;
                result.scan_null |= sr.scan_null;
                result.scan_default |= sr.scan_default;
            }
        }
        PARTPRUNE_COMBINE_INTERSECT => {
            let mut firststep = true;
            for sid in source_stepids {
                assert!(sid < step_id, "invalid pruning combine step argument");
                let sr = step_results[sid as usize]
                    .as_ref()
                    .expect("source step evaluated before combine step");
                if firststep {
                    result.bound_offsets = sr.bound_offsets.clone_in(mcx)?;
                    result.scan_null = sr.scan_null;
                    result.scan_default = sr.scan_default;
                    firststep = false;
                } else {
                    result.bound_offsets.int_members(&sr.bound_offsets);
                    result.scan_null &= sr.scan_null;
                    result.scan_default &= sr.scan_default;
                }
            }
        }
        other => panic!("invalid pruning combine op: {other}"),
    }
    Ok(result)
}

/// get_matching_partitions tail (partprune.c): map surviving bound offsets to
/// partition indexes, adding null/default partitions as flagged.
pub fn matching_bounds_to_partitions<'mcx, B: BoundInfo>(
    mcx: Mcx<'mcx>,
    boundinfo: &B,
    final_result: &PruneStepResult<'_>,
    strategy: u8,
) -> PgResult<Bitmapset<'mcx>> {
    let mut result = Bitmapset::empty();
    let mut scan_default = final_result.scan_default;
    let mut i = -1;
    loop {
        i = final_result.bound_offsets.next_member(i);
        if i < 0 {
            break;
        }
        debug_assert!(i < boundinfo.nindexes());
        let partindex = boundinfo.index_at(i);
        if partindex < 0 {
            scan_default |= boundinfo.has_default();
            continue;
        }
        result.add_member(mcx, partindex)?;
    }
    if final_result.scan_null {
        debug_assert!(strategy == b'l');
        debug_assert!(boundinfo.accepts_nulls());
        result.add_member(mcx, boundinfo.null_index())?;
    }
    if scan_default {
        debug_assert!(strategy == b'l' || strategy == b'r');
        debug_assert!(boundinfo.has_default());
        result.add_member(mcx, boundinfo.default_index())?;
    }
    Ok(result)
}

#[cfg(test)]
mod tests;
