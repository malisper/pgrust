// tsm_system_time.c: SYSTEM_TIME — tsm_system_rows' block walk bounded by
// elapsed ms; clock reads ride pg_clock::mono_ns (DST seam) as C INSTR_TIME.

use datum::Datum;
use pg_prng::PgPrng;
use types_core::{BlockNumber, InvalidBlockNumber, OffsetNumber};
use types_error::{PgError, PgResult, ERRCODE_INVALID_TABLESAMPLE_ARGUMENT};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_tuple::itemptr::{FirstOffsetNumber, InvalidOffsetNumber};

const LIBRARY: &str = "tsm_system_time";
/// pg_proc.prosrc of the extension's handler; the registry key.
pub const HANDLER_SYMBOL: &str = "tsm_system_time_handler";

/// `SystemTimeSamplerData` (palloc0; step == 0 = pattern not chosen yet).
#[derive(Default)]
pub struct SystemTimeSampler {
    seed: u32,
    millis: f64,
    start_time_ns: u64,
    lt: OffsetNumber,
    doneblocks: BlockNumber,
    lb: BlockNumber,
    nblocks: BlockNumber,
    firstblock: BlockNumber,
    step: BlockNumber,
}

impl SystemTimeSampler {
    /// `system_time_beginsamplescan`.
    pub fn begin_sample_scan(&mut self, millis: f64, seed: u32) -> PgResult<()> {
        if millis < 0.0 || millis.is_nan() {
            return Err(negative_time());
        }
        self.seed = seed;
        self.millis = millis;
        self.lt = InvalidOffsetNumber;
        self.doneblocks = 0;
        Ok(())
    }

    /// `system_time_nextsampleblock`.
    pub fn next_sample_block(&mut self, nblocks: BlockNumber) -> BlockNumber {
        if self.doneblocks == 0 {
            if self.step == 0 {
                if nblocks == 0 {
                    return InvalidBlockNumber;
                }
                let mut randstate = sampler_random_init_state(self.seed);
                self.nblocks = nblocks;
                self.firstblock =
                    (sampler_random_fract(&mut randstate) * self.nblocks as f64) as BlockNumber;
                self.step = random_relative_prime(self.nblocks, &mut randstate);
            }
            self.lb = self.firstblock;
            self.start_time_ns = pg_clock::mono_ns();
        }
        self.doneblocks += 1;
        if self.doneblocks > self.nblocks {
            return InvalidBlockNumber;
        }
        let elapsed_ms = pg_clock::mono_ns().wrapping_sub(self.start_time_ns) as f64 / 1e6;
        if elapsed_ms >= self.millis {
            return InvalidBlockNumber;
        }
        loop {
            self.lb = ((self.lb as u64 + self.step as u64) % self.nblocks as u64) as BlockNumber;
            if self.lb < nblocks {
                return self.lb;
            }
        }
    }

    /// `system_time_nextsampletuple`.
    pub fn next_sample_tuple(&mut self, maxoffset: OffsetNumber) -> OffsetNumber {
        let mut tupoffset = self.lt;
        if tupoffset == InvalidOffsetNumber {
            tupoffset = FirstOffsetNumber;
        } else {
            tupoffset += 1;
        }
        if tupoffset > maxoffset {
            tupoffset = InvalidOffsetNumber;
        }
        self.lt = tupoffset;
        tupoffset
    }
}

/// `system_time_samplescangetsamplesize` after the limit expr folding.
pub fn sample_scan_get_sample_size(
    limit: Option<f64>,
    spc_random_page_cost: f64,
    baserel_pages: BlockNumber,
    baserel_tuples: f64,
) -> (BlockNumber, f64) {
    let millis = match limit {
        Some(m) if m >= 0.0 && !m.is_nan() => m,
        _ => 1000.0,
    };

    let npages = if spc_random_page_cost > 0.0 { millis / spc_random_page_cost } else { millis };
    let npages = clamp_row_est((baserel_pages as f64).min(npages));

    let ntuples = if baserel_tuples > 0.0 && baserel_pages > 0 {
        let density = baserel_tuples / baserel_pages as f64;
        npages * density
    } else {
        npages
    };
    let ntuples = clamp_row_est(baserel_tuples.min(ntuples));
    (npages as BlockNumber, ntuples)
}

// sampler_random_* (utils/misc/sampling.c), grounded as in analyze.
fn sampler_random_init_state(seed: u32) -> PgPrng {
    PgPrng::seeded(seed as u64)
}

fn sampler_random_fract(randstate: &mut PgPrng) -> f64 {
    loop {
        let res = randstate.next_f64();
        if res != 0.0 {
            return res;
        }
    }
}

fn gcd(mut a: u32, mut b: u32) -> u32 {
    while a != 0 {
        let c = a;
        a = b % a;
        b = c;
    }
    b
}

/// Random value below and coprime to n (1 if n <= 1); ~2-3 draws. C's
/// just-in-case CHECK_FOR_INTERRUPTS is not carried (loop always ends).
pub fn random_relative_prime(n: u32, randstate: &mut PgPrng) -> u32 {
    if n <= 1 {
        return 1;
    }
    loop {
        let r = (sampler_random_fract(randstate) * n as f64) as u32;
        if r != 0 && gcd(r, n) == 1 {
            return r;
        }
    }
}

// clamp_row_est (costsize.c); grounded as in tablesample/tableam.
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

#[track_caller]
#[cold]
#[inline(never)]
fn negative_time() -> Box<PgError> {
    Box::new(
        PgError::error("sample collection time must not be negative")
            .with_sqlstate(ERRCODE_INVALID_TABLESAMPLE_ARGUMENT),
    )
}

// Never called: the registry resolves by prosrc (internal-typed argument).
fn fc_tsm_system_time_handler(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Err(Box::new(PgError::error(
        "tsm_system_time_handler cannot be called directly",
    )))
}

fn lookup(function: &str) -> Option<PGFunction> {
    match function {
        HANDLER_SYMBOL => Some(fc_tsm_system_time_handler),
        _ => None,
    }
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
}

mcx::forget_safe_struct!(
    SystemTimeSampler {
        seed, millis, start_time_ns, lt, doneblocks, lb, nblocks, firstblock, step,
    },
);

#[cfg(test)]
mod tests;
