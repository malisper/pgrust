// tablesample.c + bernoulli.c + system.c: the TSM dispatch enum. Builtins
// resolve by handler OID; extension methods (contrib tsm_system_rows /
// tsm_system_time) by the handler proc's C symbol — the IndexAmKind
// extension-arm pattern (amapi resolve_extension_handler).
#![allow(non_snake_case)]

use datum::Datum;
use mcx::Mcx;
use types_core::catalog::{FLOAT4OID, FLOAT8OID, INT8OID};
use types_core::{BlockNumber, Oid, OffsetNumber};
use types_error::{PgError, PgResult, ERRCODE_INVALID_TABLESAMPLE_ARGUMENT};
use types_nodes::{Node, NodeList};
use types_tuple::itemptr::{FirstOffsetNumber, InvalidOffsetNumber};

pub use tsm_system_rows::SystemRowsSampler;
pub use tsm_system_time::SystemTimeSampler;

pub const F_TSM_BERNOULLI_HANDLER: Oid = 3313;
pub const F_TSM_SYSTEM_HANDLER: Oid = 3314;

pub fn init_seams() {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tsm {
    Bernoulli,
    System,
    SystemRows,
    SystemTime,
}

impl Tsm {
    pub fn from_handler(tsmhandler: Oid) -> Option<Tsm> {
        match tsmhandler {
            F_TSM_BERNOULLI_HANDLER => Some(Tsm::Bernoulli),
            F_TSM_SYSTEM_HANDLER => Some(Tsm::System),
            _ => None,
        }
    }

    // Extension handler procs keyed by C symbol (what C dlopens and calls).
    fn from_symbol(prosrc: &[u8]) -> Option<Tsm> {
        if prosrc == tsm_system_rows::HANDLER_SYMBOL.as_bytes() {
            Some(Tsm::SystemRows)
        } else if prosrc == tsm_system_time::HANDLER_SYMBOL.as_bytes() {
            Some(Tsm::SystemTime)
        } else {
            None
        }
    }

    /// `GetTsmRoutine`; builtins stay catalog-free for bootstrap.
    pub fn get(mcx: Mcx<'_>, tsmhandler: Oid) -> PgResult<Tsm> {
        if let Some(tsm) = Tsm::from_handler(tsmhandler) {
            return Ok(tsm);
        }
        if let Some(prosrc) = syscache_seams::lookup_pg_proc_prosrc::call(mcx, tsmhandler)? {
            if let Some(tsm) = Tsm::from_symbol(prosrc.as_bytes()) {
                return Ok(tsm);
            }
        }
        Err(not_a_tsm_routine(tsmhandler))
    }

    pub fn parameter_types(self) -> &'static [Oid] {
        match self {
            Tsm::Bernoulli | Tsm::System => &[FLOAT4OID],
            Tsm::SystemRows => &[INT8OID],
            Tsm::SystemTime => &[FLOAT8OID],
        }
    }

    pub const fn repeatable_across_queries(self) -> bool {
        matches!(self, Tsm::Bernoulli | Tsm::System)
    }

    pub const fn repeatable_across_scans(self) -> bool {
        !matches!(self, Tsm::SystemTime)
    }

    pub const fn has_next_sample_block(self) -> bool {
        !matches!(self, Tsm::Bernoulli)
    }

    /// `*_samplescangetsamplesize`; returns (pages, tuples) for the baserel
    /// estimates. `spc_random_page_cost` feeds only SYSTEM_TIME's budget.
    pub fn sample_scan_get_sample_size<'mcx>(
        self,
        mcx: Mcx<'mcx>,
        paramexprs: &NodeList<'mcx>,
        baserel_pages: BlockNumber,
        baserel_tuples: f64,
        spc_random_page_cost: f64,
    ) -> PgResult<(BlockNumber, f64)> {
        let limitnode = paramexprs.iter().next().expect("TSM limit argument");
        let limitnode = clauses::fold::estimate_expression_value(mcx, limitnode)?;
        match self {
            Tsm::Bernoulli | Tsm::System => {
                let samplefract = extract_fraction(limitnode);
                let tuples = clamp_row_est(baserel_tuples * samplefract as f64);
                let pages = match self {
                    Tsm::Bernoulli => baserel_pages,
                    _ => clamp_row_est(baserel_pages as f64 * samplefract as f64) as BlockNumber,
                };
                Ok((pages, tuples))
            }
            Tsm::SystemRows => {
                let limit = match limitnode.as_const() {
                    Some(c) if !c.constisnull => Some(c.constvalue.as_i64()),
                    _ => None,
                };
                Ok(tsm_system_rows::sample_scan_get_sample_size(
                    limit,
                    baserel_pages,
                    baserel_tuples,
                ))
            }
            Tsm::SystemTime => {
                let limit = match limitnode.as_const() {
                    Some(c) if !c.constisnull => Some(c.constvalue.as_f64()),
                    _ => None,
                };
                Ok(tsm_system_time::sample_scan_get_sample_size(
                    limit,
                    spc_random_page_cost,
                    baserel_pages,
                    baserel_tuples,
                ))
            }
        }
    }

    pub fn init_state(self) -> TsmState {
        match self {
            Tsm::Bernoulli => TsmState::Bernoulli(BernoulliSampler::default()),
            Tsm::System => TsmState::System(SystemSampler::default()),
            Tsm::SystemRows => TsmState::SystemRows(SystemRowsSampler::default()),
            Tsm::SystemTime => TsmState::SystemTime(SystemTimeSampler::default()),
        }
    }
}

fn extract_fraction(pctnode: Node<'_>) -> f32 {
    match pctnode.as_const() {
        Some(c) if !c.constisnull => {
            let f = c.constvalue.as_f32();
            if (0.0..=100.0).contains(&f) && !f.is_nan() {
                f / 100.0f32
            } else {
                0.1
            }
        }
        _ => 0.1,
    }
}

// clamp_row_est (costsize.c); grounded here as in tableam (the costsize copy
// lives with the planner).
fn clamp_row_est(nrows: f64) -> f64 {
    const MAXIMUM_ROWCOUNT: f64 = 1e100;
    if nrows > MAXIMUM_ROWCOUNT || nrows.is_nan() {
        MAXIMUM_ROWCOUNT
    } else if nrows <= 1.0 {
        1.0
    } else {
        nrows.round_ties_even()
    }
}

#[derive(Default)]
pub struct BernoulliSampler {
    cutoff: u64,
    seed: u32,
    lt: OffsetNumber,
}

#[derive(Default)]
pub struct SystemSampler {
    cutoff: u64,
    seed: u32,
    nextblock: BlockNumber,
    lt: OffsetNumber,
}

pub enum TsmState {
    Bernoulli(BernoulliSampler),
    System(SystemSampler),
    SystemRows(SystemRowsSampler),
    SystemTime(SystemTimeSampler),
}

impl TsmState {
    /// `*_beginsamplescan`; returns (use_bulkread, use_pagemode).
    pub fn begin_sample_scan(&mut self, params: &[Datum], seed: u32) -> PgResult<(bool, bool)> {
        match self {
            TsmState::Bernoulli(s) => {
                let (cutoff, percent) = percent_cutoff(params[0])?;
                s.cutoff = cutoff;
                s.seed = seed;
                s.lt = InvalidOffsetNumber;
                // Pagemode only wins at larger fractions (C's 25% cutoff).
                Ok((true, percent >= 25.0))
            }
            TsmState::System(s) => {
                let (cutoff, percent) = percent_cutoff(params[0])?;
                s.cutoff = cutoff;
                s.seed = seed;
                s.nextblock = 0;
                s.lt = InvalidOffsetNumber;
                Ok((percent >= 1.0, true))
            }
            TsmState::SystemRows(s) => {
                s.begin_sample_scan(params[0].as_i64(), seed)?;
                Ok((true, true))
            }
            TsmState::SystemTime(s) => {
                s.begin_sample_scan(params[0].as_f64(), seed)?;
                Ok((true, true))
            }
        }
    }

    pub fn has_next_sample_block(&self) -> bool {
        !matches!(self, TsmState::Bernoulli(_))
    }

    /// `NextSampleBlock`; `donetuples` = scan's returned count. Bernoulli
    /// has none.
    pub fn next_sample_block(&mut self, nblocks: BlockNumber, donetuples: i64) -> BlockNumber {
        match self {
            TsmState::Bernoulli(_) => {
                panic!("NextSampleBlock called on a TSM without one (tsmapi.h)")
            }
            TsmState::System(s) => {
                let mut nextblock = s.nextblock;
                while nextblock < nblocks {
                    let hash = hash_u32s(&[nextblock, s.seed]);
                    if (hash as u64) < s.cutoff {
                        break;
                    }
                    nextblock += 1;
                }
                if nextblock < nblocks {
                    s.nextblock = nextblock + 1;
                    nextblock
                } else {
                    s.nextblock = 0;
                    types_core::InvalidBlockNumber
                }
            }
            TsmState::SystemRows(s) => s.next_sample_block(nblocks, donetuples),
            TsmState::SystemTime(s) => s.next_sample_block(nblocks),
        }
    }

    /// `NextSampleTuple`.
    pub fn next_sample_tuple(
        &mut self,
        blockno: BlockNumber,
        maxoffset: OffsetNumber,
        donetuples: i64,
    ) -> OffsetNumber {
        match self {
            TsmState::Bernoulli(s) => {
                let mut tupoffset =
                    if s.lt == InvalidOffsetNumber { FirstOffsetNumber } else { s.lt + 1 };
                while tupoffset <= maxoffset {
                    let hash = hash_u32s(&[blockno, tupoffset as u32, s.seed]);
                    if (hash as u64) < s.cutoff {
                        break;
                    }
                    tupoffset += 1;
                }
                if tupoffset > maxoffset {
                    tupoffset = InvalidOffsetNumber;
                }
                s.lt = tupoffset;
                tupoffset
            }
            TsmState::System(s) => {
                let mut tupoffset =
                    if s.lt == InvalidOffsetNumber { FirstOffsetNumber } else { s.lt + 1 };
                if tupoffset > maxoffset {
                    tupoffset = InvalidOffsetNumber;
                }
                s.lt = tupoffset;
                tupoffset
            }
            TsmState::SystemRows(s) => s.next_sample_tuple(maxoffset, donetuples),
            TsmState::SystemTime(s) => s.next_sample_tuple(maxoffset),
        }
    }
}

// The shared percent validation + cutoff of bernoulli/system beginsamplescan.
fn percent_cutoff(param: Datum) -> PgResult<(u64, f64)> {
    let percent = param.as_f32() as f64;
    if !(0.0..=100.0).contains(&percent) || percent.is_nan() {
        return Err(bad_percent());
    }
    let cutoff = (((u32::MAX as f64) + 1.0) * percent / 100.0).round_ties_even() as u64;
    Ok((cutoff, percent))
}

impl tableam_vocab::SampleScanDriver for TsmState {
    fn has_next_sample_block(&self) -> bool {
        TsmState::has_next_sample_block(self)
    }
    fn next_sample_block(&mut self, nblocks: BlockNumber, donetuples: i64) -> BlockNumber {
        TsmState::next_sample_block(self, nblocks, donetuples)
    }
    fn next_sample_tuple(
        &mut self,
        blockno: BlockNumber,
        maxoffset: OffsetNumber,
        donetuples: i64,
    ) -> OffsetNumber {
        TsmState::next_sample_tuple(self, blockno, maxoffset, donetuples)
    }
}

// hash_any over native-endian uint32 words, as C's hashinput arrays.
#[inline]
fn hash_u32s(words: &[u32]) -> u32 {
    let mut bytes = [0u8; 12];
    for (i, w) in words.iter().enumerate() {
        bytes[i * 4..i * 4 + 4].copy_from_slice(&w.to_ne_bytes());
    }
    hashfn::hash_bytes(&bytes[..words.len() * 4])
}

#[track_caller]
#[cold]
#[inline(never)]
fn bad_percent() -> Box<PgError> {
    Box::new(
        PgError::error("sample percentage must be between 0 and 100")
            .with_sqlstate(ERRCODE_INVALID_TABLESAMPLE_ARGUMENT),
    )
}

// GetTsmRoutine's elog for a handler that yields no TsmRoutine.
#[track_caller]
#[cold]
#[inline(never)]
fn not_a_tsm_routine(tsmhandler: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "tablesample handler function {tsmhandler} did not return a TsmRoutine struct"
    )))
}

mcx::forget_safe_nodrop!(Tsm);
mcx::forget_safe_struct!(
    BernoulliSampler { cutoff, seed, lt },
    SystemSampler { cutoff, seed, nextblock, lt },
);
mcx::forget_safe_nodrop!(TsmState);

#[cfg(test)]
mod tests;
