// tablesample.c + bernoulli.c + system.c: the in-core TSM methods as a
// closed enum; extension TSM handlers are a loud panic at resolve time.
#![allow(non_snake_case)]

use datum::Datum;
use mcx::Mcx;
use types_core::catalog::FLOAT4OID;
use types_core::{BlockNumber, Oid, OffsetNumber};
use types_error::{PgError, PgResult, ERRCODE_INVALID_TABLESAMPLE_ARGUMENT};
use types_nodes::{Node, NodeList};
use types_tuple::itemptr::{FirstOffsetNumber, InvalidOffsetNumber};

pub const F_TSM_BERNOULLI_HANDLER: Oid = 3313;
pub const F_TSM_SYSTEM_HANDLER: Oid = 3314;

pub fn init_seams() {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tsm {
    Bernoulli,
    System,
}

impl Tsm {
    pub fn from_handler(tsmhandler: Oid) -> Option<Tsm> {
        match tsmhandler {
            F_TSM_BERNOULLI_HANDLER => Some(Tsm::Bernoulli),
            F_TSM_SYSTEM_HANDLER => Some(Tsm::System),
            _ => None,
        }
    }

    /// `GetTsmRoutine`.
    pub fn get(tsmhandler: Oid) -> Tsm {
        Tsm::from_handler(tsmhandler).unwrap_or_else(|| {
            panic!(
                "GetTsmRoutine (tablesample.c): handler {tsmhandler} is not an \
                 in-core TSM; extension methods unported — \
                 unit backend-access-tablesample-core"
            )
        })
    }

    pub fn parameter_types(self) -> &'static [Oid] {
        &[FLOAT4OID]
    }

    pub const fn repeatable_across_queries(self) -> bool {
        true
    }

    pub const fn repeatable_across_scans(self) -> bool {
        true
    }

    pub const fn has_next_sample_block(self) -> bool {
        matches!(self, Tsm::System)
    }

    /// `bernoulli_samplescangetsamplesize` / `system_samplescangetsamplesize`;
    /// returns (pages, tuples) for the baserel estimates.
    pub fn sample_scan_get_sample_size<'mcx>(
        self,
        mcx: Mcx<'mcx>,
        paramexprs: &NodeList<'mcx>,
        baserel_pages: BlockNumber,
        baserel_tuples: f64,
    ) -> PgResult<(BlockNumber, f64)> {
        let pctnode = paramexprs.iter().next().expect("TSM percent argument");
        let pctnode = clauses::fold::estimate_expression_value(mcx, pctnode)?;
        let samplefract = extract_fraction(pctnode);
        let tuples = clamp_row_est(baserel_tuples * samplefract as f64);
        let pages = match self {
            Tsm::Bernoulli => baserel_pages,
            Tsm::System => clamp_row_est(baserel_pages as f64 * samplefract as f64) as BlockNumber,
        };
        Ok((pages, tuples))
    }

    pub fn init_state(self) -> TsmState {
        match self {
            Tsm::Bernoulli => TsmState::Bernoulli(BernoulliSampler::default()),
            Tsm::System => TsmState::System(SystemSampler::default()),
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
}

impl TsmState {
    /// `bernoulli_beginsamplescan` / `system_beginsamplescan`; returns
    /// (use_bulkread, use_pagemode).
    pub fn begin_sample_scan(&mut self, params: &[Datum], seed: u32) -> PgResult<(bool, bool)> {
        let percent = params[0].as_f32() as f64;
        if !(0.0..=100.0).contains(&percent) || percent.is_nan() {
            return Err(bad_percent());
        }
        let cutoff = (((u32::MAX as f64) + 1.0) * percent / 100.0).round_ties_even() as u64;
        match self {
            TsmState::Bernoulli(s) => {
                s.cutoff = cutoff;
                s.seed = seed;
                s.lt = InvalidOffsetNumber;
                // Pagemode visibility checking only wins at larger fractions
                // (C's experimentally chosen 25% cutoff).
                Ok((true, percent >= 25.0))
            }
            TsmState::System(s) => {
                s.cutoff = cutoff;
                s.seed = seed;
                s.nextblock = 0;
                s.lt = InvalidOffsetNumber;
                Ok((percent >= 1.0, true))
            }
        }
    }

    pub fn has_next_sample_block(&self) -> bool {
        matches!(self, TsmState::System(_))
    }

    /// `system_nextsampleblock`; bernoulli has no NextSampleBlock.
    pub fn next_sample_block(&mut self, nblocks: BlockNumber) -> BlockNumber {
        let TsmState::System(s) = self else {
            panic!("NextSampleBlock called on a TSM without one (tsmapi.h)")
        };
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

    /// `bernoulli_nextsampletuple` / `system_nextsampletuple`.
    pub fn next_sample_tuple(
        &mut self,
        blockno: BlockNumber,
        maxoffset: OffsetNumber,
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
        }
    }
}

impl tableam_vocab::SampleScanDriver for TsmState {
    fn has_next_sample_block(&self) -> bool {
        TsmState::has_next_sample_block(self)
    }
    fn next_sample_block(&mut self, nblocks: BlockNumber) -> BlockNumber {
        TsmState::next_sample_block(self, nblocks)
    }
    fn next_sample_tuple(&mut self, blockno: BlockNumber, maxoffset: OffsetNumber) -> OffsetNumber {
        TsmState::next_sample_tuple(self, blockno, maxoffset)
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

#[cold]
#[inline(never)]
fn bad_percent() -> Box<PgError> {
    Box::new(
        PgError::error("sample percentage must be between 0 and 100")
            .with_sqlstate(ERRCODE_INVALID_TABLESAMPLE_ARGUMENT),
    )
}

mcx::forget_safe_nodrop!(Tsm);
mcx::forget_safe_struct!(
    BernoulliSampler { cutoff, seed, lt },
    SystemSampler { cutoff, seed, nextblock, lt },
);
mcx::forget_safe_nodrop!(TsmState);

#[cfg(test)]
mod tests;
