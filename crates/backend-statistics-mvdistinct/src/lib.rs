//! Port of `backend/statistics/mvdistinct.c` — multivariate n-distinct
//! coefficients (the ndistinct slice of the combined `backend-statistics-core`
//! unit). Sibling of `backend-statistics-dependencies` (dependencies.c), ported
//! the same way.
//!
//! SELF-CONTAINED in this crate (100% of the C logic):
//!   * the `CombinationGenerator` n-choose-k enumeration (`n_choose_k`,
//!     `num_combinations`, `generator_init` / `_next` / `_free`,
//!     `generate_combinations` / `_recurse`),
//!   * `statext_ndistinct_build` control flow (the generator loop + `MVNDistinct`
//!     assembly; only the per-combination estimator kernel crosses the seam),
//!   * `statext_ndistinct_serialize` / `_deserialize` (exact C byte layout +
//!     every `elog(ERROR)` validity check, with allocation-safety bounds),
//!   * `statext_ndistinct_load` (the kind-not-built / null-bytea control; the
//!     syscache read itself is seamed),
//!   * `estimate_ndistinct` (the Duj1 estimator math),
//!   * `pg_ndistinct_in` / `_out` / `_recv` / `_send` (the SQL-callable I/O
//!     surface; `_out` formats the cstring in-crate, `_send` delegates to the
//!     ported `byteasend`).
//!
//! SEAMED to the not-yet-ported owner (`backend-statistics-core`, covering
//! `extended_stats.c` + the multi-sort support + vacuum's `VacAttrStats`):
//!   * `ndistinct_for_combination` — needs `multi_sort_init` /
//!     `multi_sort_add_dimension` / `multi_sort_compare` / `qsort_interruptible`
//!     over the per-column `VacAttrStats` matrix + `Datum`/`bool` value matrices
//!     inside the opaque `StatsBuildData`, plus `lookup_type_cache(...)->lt_opr`.
//!     Reached via `backend_statistics_core_seams::ndistinct_for_combination`,
//!     panics until owner lands. This mirrors how dependencies.c seamed
//!     `dependency_degree`.
//!   * `statext_ndistinct_load_bytea` — the `pg_statistic_ext_data` syscache read
//!     (`SearchSysCache2` + `SysCacheGetAttr` + `ReleaseSysCache`). The
//!     kind-not-built error text and behaviour stay in-crate.
//!   * `byteasend` is reached directly (the `backend-utils-adt-varlena` port is a
//!     real dependency, not a seam).
//!
//! This crate installs NO inward seams — its public functions are called only by
//! the (unported) `backend-statistics-core` dispatcher and the fmgr catalog.
//! See audits/backend-statistics-mvdistinct.md.

// The loop ladders mirror the C `for (i = 0; ...; i++)` control flow 1:1
// (parallel-vector indexing, fallible bodies); collapsing them would obscure the
// correspondence.
#![allow(clippy::needless_range_loop)]

use mcx::Mcx;
use types_core::{AttrNumber, Oid};
use types_error::{PgError, PgResult, ERRCODE_DATA_CORRUPTED, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_statistics::{
    MVNDistinct, MVNDistinctItem, StatsBuildData, STATS_EXT_NDISTINCT,
    STATS_MAX_DIMENSIONS, STATS_NDISTINCT_MAGIC, STATS_NDISTINCT_TYPE_BASIC,
};

use backend_statistics_core_seams as core_seam;

/* ---------------------------------------------------------------------------
 * Constants mirroring the C macros / catalog headers.
 * ------------------------------------------------------------------------- */

/// On-wire widths of the C `memcpy` fields (NOT Rust `size_of`): `sizeof(uint32)`,
/// `sizeof(int)`, `sizeof(double)`, `sizeof(AttrNumber)`.
const SIZE_U32: usize = 4;
const SIZE_I32: usize = 4;
const SIZE_F64: usize = 8;
const SIZE_ATTRNUMBER: usize = 2;

/// `VARHDRSZ` — the varlena length-header size.
const VARHDRSZ: usize = 4;

/// `STATS_EXT_NDISTINCT` ('d'), as a `char` for the kind-not-built message.
const STATS_EXT_NDISTINCT_CHAR: char = STATS_EXT_NDISTINCT as u8 as char;

/// `SizeOfHeader` — `(3 * sizeof(uint32))` (mvdistinct.c:45).
const SIZE_OF_HEADER: usize = 3 * SIZE_U32;

/// `SizeOfItem(natts)` — size of a serialized ndistinct item (coefficient, natts,
/// atts): `sizeof(double) + sizeof(int) + natts * sizeof(AttrNumber)`
/// (mvdistinct.c:48-49).
#[inline]
fn size_of_item(natts: usize) -> usize {
    SIZE_F64 + SIZE_I32 + natts * SIZE_ATTRNUMBER
}

/// `MinSizeOfItem` — minimal size of a ndistinct item (with two attributes)
/// (mvdistinct.c:52).
fn min_size_of_item() -> usize {
    size_of_item(2)
}

/// `MinSizeOfItems(nitems)` — minimal size of mvndistinct, when all items are
/// minimal (mvdistinct.c:55-56).
#[inline]
fn min_size_of_items(nitems: usize) -> usize {
    SIZE_OF_HEADER + nitems * min_size_of_item()
}

/// `InvalidAttrNumber`, the sentinel `AttributeNumberIsValid` tests against.
const INVALID_ATTR_NUMBER: AttrNumber = 0;

/// Allocation-safety bound (HARD RULE) on the data-derived `nitems` before any
/// reservation in the deserializer: a corrupt-yet-large bytea must not be able to
/// request an absurd allocation. The `minimum_size` check already proves
/// `nitems <= exhdr`; this is an additional absolute sanity cap.
const MAX_REASONABLE_NITEMS: usize = 1 << 24;

/* ---------------------------------------------------------------------------
 * CombinationGenerator — k-combinations of n columns (mvdistinct.c:61-68,
 * 588-699)
 *
 * Held as an owned Rust struct: C allocates it in a memory context and frees it
 * via `generator_free`, which here is plain ownership/`Drop`. The transient
 * working arrays use `try_reserve` (OOM -> `mcx.oom`) per the fallible-allocation
 * rule.
 * ------------------------------------------------------------------------- */

/// `CombinationGenerator` (mvdistinct.c:61-68).
struct CombinationGenerator {
    /// size of the combination
    k: i32,
    /// total number of elements
    n: i32,
    /// index of the next combination to return
    current: i32,
    /// number of combinations (size of array)
    ncombinations: i32,
    /// flat array of `k * ncombinations` pre-built combinations (C
    /// `state->combinations`).
    combinations: Vec<i32>,
}

impl CombinationGenerator {
    /// `generator_init` (mvdistinct.c:588-617).
    ///
    /// The generator produces combinations of K elements in the interval (0..N).
    /// We prebuild all the combinations here, which is simpler than generating
    /// them on the fly.
    fn init(mcx: Mcx<'_>, n: i32, k: i32) -> PgResult<CombinationGenerator> {
        debug_assert!((n >= k) && (k > 0)); // Assert((n >= k) && (k > 0))

        let ncombinations = n_choose_k(n, k);

        // pre-allocate space for all combinations:
        //   state->combinations = palloc(sizeof(int) * k * state->ncombinations)
        let total = (k * ncombinations) as usize;
        let mut combinations: Vec<i32> = Vec::new();
        combinations
            .try_reserve(total)
            .map_err(|_| mcx.oom(total * SIZE_I32))?;
        for _ in 0..total {
            combinations.push(0);
        }

        let mut state = CombinationGenerator {
            k,
            n,
            current: 0,
            ncombinations,
            combinations,
        };

        // now actually pre-generate all the combinations of K elements
        state.generate(mcx)?;

        // make sure we got the expected number of combinations
        debug_assert!(state.current == state.ncombinations);

        // reset the number, so we start with the first one
        state.current = 0;

        Ok(state)
    }

    /// `generate_combinations` (mvdistinct.c:691-699).
    fn generate(&mut self, mcx: Mcx<'_>) -> PgResult<()> {
        // int *current = palloc0(sizeof(int) * state->k);
        let kk = self.k as usize;
        let mut current: Vec<i32> = Vec::new();
        current
            .try_reserve(kk)
            .map_err(|_| mcx.oom(kk * SIZE_I32))?;
        for _ in 0..kk {
            current.push(0);
        }

        self.generate_recurse(0, 0, &mut current);
        // pfree(current) — Vec drop.
        Ok(())
    }

    /// `generate_combinations_recurse` (mvdistinct.c:656-685).
    ///
    /// Given a prefix (first few elements of the combination), generate following
    /// elements recursively, in lexicographic order (which eliminates
    /// permutations of the same combination).
    fn generate_recurse(&mut self, index: i32, start: i32, current: &mut [i32]) {
        // If we haven't filled all the elements, simply recurse.
        if index < self.k {
            // The values have to be in ascending order, so make sure we start
            // with the value passed by parameter.
            let mut i = start;
            while i < self.n {
                current[index as usize] = i;
                self.generate_recurse(index + 1, i + 1, current);
                i += 1;
            }
        } else {
            // we got a valid combination, add it to the array:
            //   memcpy(&state->combinations[state->k * state->current],
            //          current, state->k * sizeof(int));
            let dst = (self.k * self.current) as usize;
            let kk = self.k as usize;
            self.combinations[dst..dst + kk].copy_from_slice(&current[..kk]);
            self.current += 1;
        }
    }

    /// `generator_next` (mvdistinct.c:626-633): the start index of the next
    /// k-combination in `combinations`, or `None` (C returns `NULL`).
    fn next(&mut self) -> Option<usize> {
        if self.current == self.ncombinations {
            return None;
        }
        let idx = (self.k * self.current) as usize;
        self.current += 1;
        Some(idx)
    }

    /// Borrow the k-combination starting at `idx`.
    fn combination(&self, idx: usize) -> &[i32] {
        &self.combinations[idx..idx + self.k as usize]
    }
}
// generator_free (mvdistinct.c:641-646): the `combinations` Vec is reclaimed by
// `Drop`.

/* ---------------------------------------------------------------------------
 * statext_ndistinct_build (mvdistinct.c:87-141)
 * ------------------------------------------------------------------------- */

/// `statext_ndistinct_build` (mvdistinct.c:87-141).
///
/// Computes the ndistinct coefficient for the combination of attributes, using
/// the same estimator used in `analyze.c`. Generates all candidate combinations
/// (via the in-crate `CombinationGenerator`) and computes the estimate for each
/// (via the `ndistinct_for_combination` seam — the multi-sort kernel owned by the
/// unported extended-stats support).
///
/// `mcx` is the C `CurrentMemoryContext` (used for OOM error production on the
/// transient generator arrays). `data` is the opaque `StatsBuildData` handle;
/// `nattnums` is `data->nattnums` and `data_attnums` is `data->attnums` (the
/// build data cannot be dereferenced here — `data->attnums[...]` is mirrored by
/// the explicit `data_attnums` slice the owner supplies at call time, and the
/// kernel's `data->values`/`stats`/`nulls` reads happen owner-side behind the
/// seam).
pub fn statext_ndistinct_build<'mcx>(
    mcx: Mcx<'_>,
    totalrows: f64,
    data: &StatsBuildData<'mcx>,
    nattnums: i32,
    data_attnums: &[AttrNumber],
) -> PgResult<MVNDistinct> {
    let numattrs = nattnums; // int numattrs = data->nattnums;

    // num_combinations is `(1 << n) - (n + 1)`; the catalog never stores more
    // than STATS_MAX_DIMENSIONS columns, and a larger n would overflow the shift,
    // so reject it as a recoverable error (allocation-safety HARD RULE).
    if numattrs < 0 || numattrs as usize > STATS_MAX_DIMENSIONS {
        return Err(PgError::error(format!(
            "invalid number of attributes {numattrs} (expected 0..{STATS_MAX_DIMENSIONS})"
        )));
    }

    let numcombs = num_combinations(numattrs);

    // result = palloc(offsetof(MVNDistinct, items) + numcombs * ...);
    // The result (and each item's attributes) is the caller-owned value.
    let mut items: Vec<MVNDistinctItem> = Vec::new();
    items
        .try_reserve(numcombs.max(0) as usize)
        .map_err(|_| mcx.oom(numcombs.max(0) as usize * core::mem::size_of::<MVNDistinctItem>()))?;

    let mut itemcnt: usize = 0;
    let mut k: i32 = 2;
    while k <= numattrs {
        // generate combinations of K out of N elements
        let mut generator = CombinationGenerator::init(mcx, numattrs, k)?;

        while let Some(comb_idx) = generator.next() {
            // item->attributes = palloc(sizeof(AttrNumber) * k);
            // item->nattributes = k;
            let mut attributes: Vec<AttrNumber> = Vec::new();
            attributes
                .try_reserve(k as usize)
                .map_err(|_| mcx.oom(k as usize * SIZE_ATTRNUMBER))?;

            // The k-combination of zero-based column indexes into the stats obj.
            // Copy it out so the borrow of `generator` ends before the kernel call.
            let combination: Vec<i32> = generator.combination(comb_idx).to_vec();

            // translate the indexes to attnums:
            //   item->attributes[j] = data->attnums[combination[j]];
            for j in 0..k as usize {
                let attr = data_attnums[combination[j] as usize];
                debug_assert!(attr != INVALID_ATTR_NUMBER); // AttributeNumberIsValid
                attributes.push(attr);
            }

            // item->ndistinct =
            //     ndistinct_for_combination(totalrows, data, k, combination);
            let ndistinct =
                core_seam::ndistinct_for_combination::call(totalrows, data, k, &combination)?;

            items.push(MVNDistinctItem {
                ndistinct,
                attributes,
            });

            itemcnt += 1;
            debug_assert!(itemcnt <= numcombs as usize);
        }

        // generator_free(generator) — dropped at end of scope.
        k += 1;
    }

    // must consume exactly the whole output array
    debug_assert!(itemcnt == numcombs as usize);

    Ok(MVNDistinct {
        magic: STATS_NDISTINCT_MAGIC,
        r#type: STATS_NDISTINCT_TYPE_BASIC,
        items,
    })
}

/* ---------------------------------------------------------------------------
 * statext_ndistinct_load (mvdistinct.c:147-172)
 * ------------------------------------------------------------------------- */

/// `statext_ndistinct_load` (mvdistinct.c:147-172).
///
/// Load the ndistinct value for the indicated `pg_statistic_ext` tuple. The
/// syscache read (`SearchSysCache2` + `SysCacheGetAttr` + `ReleaseSysCache`)
/// crosses the seam, returning `Ok(Some(bytes))` for a present non-null bytea,
/// `Ok(None)` for the is-null attribute (so the kind-not-built message stays
/// in-crate), or `Err` for the missing-tuple cache-lookup failure.
pub fn statext_ndistinct_load(mvoid: Oid, inh: bool) -> PgResult<MVNDistinct> {
    let ndist = core_seam::statext_ndistinct_load_bytea::call(mvoid, inh)?;

    let ndist = match ndist {
        Some(body) => body,
        None => {
            // if (isnull) elog(ERROR, "requested statistics kind \"%c\" is not yet
            //     built for statistics object %u", STATS_EXT_NDISTINCT, mvoid);
            return Err(PgError::error(format!(
                "requested statistics kind \"{STATS_EXT_NDISTINCT_CHAR}\" is not yet built for statistics object {mvoid}"
            )));
        }
    };

    // result = statext_ndistinct_deserialize(DatumGetByteaPP(ndist));
    let result = statext_ndistinct_deserialize(Some(&ndist))?;

    // C dereferences the always-non-NULL syscache bytea; treat a None (NULL
    // datum) as a hard error to match the C crash contract without unsafe.
    result.ok_or_else(|| PgError::error("statext_ndistinct_load: null ndistinct bytea"))
}

/* ---------------------------------------------------------------------------
 * statext_ndistinct_serialize (mvdistinct.c:178-243)
 *
 * Produces the serialized bytea as an owned `Vec<u8>` whose first `VARHDRSZ`
 * bytes hold the varlena length header (`SET_VARSIZE`), exactly as the C `bytea`.
 * ------------------------------------------------------------------------- */

/// `statext_ndistinct_serialize` (mvdistinct.c:178-243).
pub fn statext_ndistinct_serialize(mcx: Mcx<'_>, ndistinct: &MVNDistinct) -> PgResult<Vec<u8>> {
    // Assert(ndistinct->magic == STATS_NDISTINCT_MAGIC);
    debug_assert!(ndistinct.magic == STATS_NDISTINCT_MAGIC);
    // Assert(ndistinct->type == STATS_NDISTINCT_TYPE_BASIC);
    debug_assert!(ndistinct.r#type == STATS_NDISTINCT_TYPE_BASIC);

    // Base size is size of scalar fields in the struct, plus one base struct for
    // each item, including number of items for each.
    let mut len: usize = VARHDRSZ + SIZE_OF_HEADER;

    // and also include space for the actual attribute numbers
    for i in 0..ndistinct.items.len() {
        let nmembers = ndistinct.items[i].attributes.len();
        // Assert(nmembers >= 2);
        debug_assert!(nmembers >= 2);
        len += size_of_item(nmembers);
    }

    // output = palloc(len); SET_VARSIZE(output, len);
    let mut output: Vec<u8> = Vec::new();
    output.try_reserve(len).map_err(|_| mcx.oom(len))?;
    output.resize(len, 0);
    set_varsize(&mut output, len);

    // tmp = VARDATA(output);  (offset VARHDRSZ)
    let mut tmp = VARHDRSZ;

    // Store the base struct values (magic, type, nitems)
    write_u32(&mut output, &mut tmp, ndistinct.magic);
    write_u32(&mut output, &mut tmp, ndistinct.r#type);
    write_u32(&mut output, &mut tmp, ndistinct.items.len() as u32);

    // store number of attributes and attribute numbers for each entry
    for i in 0..ndistinct.items.len() {
        let item = &ndistinct.items[i];
        let nmembers = item.attributes.len() as i32; // int

        // memcpy(tmp, &item.ndistinct, sizeof(double));
        write_f64(&mut output, &mut tmp, item.ndistinct);
        // memcpy(tmp, &nmembers, sizeof(int));
        write_i32(&mut output, &mut tmp, nmembers);

        // memcpy(tmp, item.attributes, sizeof(AttrNumber) * nmembers);
        for a in 0..item.attributes.len() {
            write_attnum(&mut output, &mut tmp, item.attributes[a]);
        }

        // Assert(tmp <= ((char *) output + len));
        debug_assert!(tmp <= len);
    }

    // check we used exactly the expected space
    debug_assert!(tmp == len);

    Ok(output)
}

/* ---------------------------------------------------------------------------
 * statext_ndistinct_deserialize (mvdistinct.c:249-329)
 * ------------------------------------------------------------------------- */

/// `statext_ndistinct_deserialize` (mvdistinct.c:249-329).
///
/// `data` is the full bytea (varlena length header + payload), or `None` for the
/// C `data == NULL` case.
pub fn statext_ndistinct_deserialize(data: Option<&[u8]>) -> PgResult<Option<MVNDistinct>> {
    // if (data == NULL) return NULL;
    let bytes = match data {
        None => return Ok(None),
        Some(b) => b,
    };

    // Treat the bytea body as a flat byte slice (header + payload).
    //
    // `varsize_any` decodes the 4-byte length header. A malformed bytea can carry
    // a length word smaller than the header (so `total - VARHDRSZ` would
    // underflow) or larger than the buffer (so reads would over-run); validate
    // before deriving the exhdr length (Allocation-Safety HARD RULE).
    let total = varsize_any(bytes)?;
    let hdrsz = varhdrsz_any(bytes); // 1 (short) or VARHDRSZ (long)
    if total < hdrsz || total > bytes.len() {
        return Err(PgError::error("invalid ndistinct varlena length")
            .with_sqlstate(ERRCODE_DATA_CORRUPTED));
    }
    let exhdr = total - hdrsz; // VARSIZE_ANY_EXHDR(data)

    // we expect at least the basic fields of MVNDistinct struct
    if exhdr < SIZE_OF_HEADER {
        return Err(PgError::error(format!(
            "invalid MVNDistinct size {exhdr} (expected at least {SIZE_OF_HEADER})"
        )));
    }

    // initialize pointer to the data part (skip the varlena header)
    let mut tmp = hdrsz;

    // read the header fields and perform basic sanity checks
    let magic = read_u32(bytes, &mut tmp)?;
    let r#type = read_u32(bytes, &mut tmp)?;
    let nitems = read_u32(bytes, &mut tmp)?;

    if magic != STATS_NDISTINCT_MAGIC {
        // elog(ERROR, "invalid ndistinct magic %08x (expected %08x)", ...);
        return Err(PgError::error(format!(
            "invalid ndistinct magic {magic:08x} (expected {STATS_NDISTINCT_MAGIC:08x})"
        )));
    }
    if r#type != STATS_NDISTINCT_TYPE_BASIC {
        // C prints both via %d (mvdistinct.c:281).
        return Err(PgError::error(format!(
            "invalid ndistinct type {} (expected {})",
            r#type as i32, STATS_NDISTINCT_TYPE_BASIC as i32
        )));
    }
    if nitems == 0 {
        return Err(PgError::error(
            "invalid zero-length item array in MVNDistinct",
        ));
    }

    // what minimum bytea size do we expect for those parameters
    let minimum_size = min_size_of_items(nitems as usize);
    if exhdr < minimum_size {
        return Err(PgError::error(format!(
            "invalid MVNDistinct size {exhdr} (expected at least {minimum_size})"
        )));
    }

    // Allocation-safety HARD RULE: bound the data-derived `nitems`. The
    // minimum_size check already proves nitems <= exhdr (each item costs at least
    // MinSizeOfItem bytes); this is an additional absolute cap.
    let nitems = nitems as usize;
    if nitems > MAX_REASONABLE_NITEMS {
        return Err(PgError::error(format!(
            "invalid ndistinct item count {nitems} (exceeds {MAX_REASONABLE_NITEMS})"
        )));
    }

    // Allocate space for the ndistinct items.
    let mut items: Vec<MVNDistinctItem> = Vec::new();
    items
        .try_reserve(nitems)
        .map_err(|_| PgError::error("out of memory"))?;

    for _ in 0..nitems {
        // ndistinct value
        let ndistinct = read_f64(bytes, &mut tmp)?;

        // number of attributes
        let nattributes = read_i32(bytes, &mut tmp)?;
        // Assert((nattributes >= 2) && (nattributes <= STATS_MAX_DIMENSIONS));
        // Untrusted on-disk field bounding a heap alloc -> hard recoverable check.
        if !(2..=STATS_MAX_DIMENSIONS as i32).contains(&nattributes) {
            return Err(PgError::error(format!(
                "invalid ndistinct item attribute count {nattributes} (expected 2..{STATS_MAX_DIMENSIONS})"
            )));
        }

        // item->attributes = palloc(item->nattributes * sizeof(AttrNumber));
        let mut attributes: Vec<AttrNumber> = Vec::new();
        attributes
            .try_reserve(nattributes as usize)
            .map_err(|_| PgError::error("out of memory"))?;
        for _ in 0..nattributes {
            attributes.push(read_attnum(bytes, &mut tmp)?);
        }

        items.push(MVNDistinctItem {
            ndistinct,
            attributes,
        });

        // still within the bytea
        debug_assert!(tmp <= total);
    }

    // we should have consumed the whole bytea exactly
    debug_assert!(tmp == total);

    Ok(Some(MVNDistinct {
        magic,
        r#type,
        items,
    }))
}

/* ---------------------------------------------------------------------------
 * pg_ndistinct_in / _out / _recv / _send (mvdistinct.c:338-411)
 * ------------------------------------------------------------------------- */

/// The `ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg(...)))`
/// shared by `pg_ndistinct_in` and `pg_ndistinct_recv`
/// (mvdistinct.c:341-343 / 394-396).
fn cannot_accept_value() -> PgError {
    PgError::error(format!("cannot accept a value of type {}", "pg_ndistinct"))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

/// `pg_ndistinct_in` (mvdistinct.c:338-346) — text input is disallowed.
pub fn pg_ndistinct_in() -> PgResult<()> {
    Err(cannot_accept_value())
}

/// `pg_ndistinct_recv` (mvdistinct.c:391-399) — binary input disallowed.
pub fn pg_ndistinct_recv() -> PgResult<()> {
    Err(cannot_accept_value())
}

/// `pg_ndistinct_out` (mvdistinct.c:354-385).
///
/// `data` is the detoasted bytea (the fmgr `PG_GETARG_BYTEA_PP(0)`). Returns the
/// NUL-terminated cstring bytes (the fmgr `PG_RETURN_CSTRING` payload). The
/// `StringInfo` formatting is done in-crate: `%d` is the decimal attnum and
/// `(int) item.ndistinct`.
pub fn pg_ndistinct_out(data: &[u8]) -> PgResult<Vec<u8>> {
    // MVNDistinct *ndist = statext_ndistinct_deserialize(data);
    let ndist = statext_ndistinct_deserialize(Some(data))?
        // C unconditionally dereferences the result; a non-NULL detoasted arg
        // always deserializes to Some. Treat None (NULL datum) as a hard error.
        .ok_or_else(|| PgError::error("pg_ndistinct_out: null ndistinct bytea"))?;

    // initStringInfo(&str); appendStringInfoChar(&str, '{');
    let mut str: Vec<u8> = Vec::new();
    str.push(b'{');

    for i in 0..ndist.items.len() {
        let item = &ndist.items[i];

        if i > 0 {
            str.extend_from_slice(b", ");
        }

        for j in 0..item.attributes.len() {
            let attnum = item.attributes[j];
            // appendStringInfo(&str, "%s%d", (j == 0) ? "\"" : ", ", attnum);
            let prefix: &[u8] = if j == 0 { b"\"" } else { b", " };
            str.extend_from_slice(prefix);
            str.extend_from_slice(format!("{attnum}").as_bytes());
        }
        // appendStringInfo(&str, "\": %d", (int) item.ndistinct);
        str.extend_from_slice(b"\": ");
        str.extend_from_slice(format!("{}", item.ndistinct as i32).as_bytes());
    }

    // appendStringInfoChar(&str, '}');
    str.push(b'}');
    // PG_RETURN_CSTRING(str.data): NUL-terminate the cstring payload.
    str.push(0);

    Ok(str)
}

/// `pg_ndistinct_send` (mvdistinct.c:407-411) — `return byteasend(fcinfo)`.
///
/// n-distinct is serialized into a bytea value, so the binary-output routine just
/// sends those bytes. Delegates to the ported `byteasend`.
pub fn pg_ndistinct_send<'mcx>(mcx: Mcx<'mcx>, data: &[u8]) -> PgResult<mcx::PgVec<'mcx, u8>> {
    backend_utils_adt_varlena::bytea::byteasend(mcx, data)
}

/* ---------------------------------------------------------------------------
 * estimate_ndistinct (mvdistinct.c:520-542)
 * ------------------------------------------------------------------------- */

/// The Duj1 estimator (already used in `analyze.c`): `n*d / (n - f1 + f1*n/N)`.
///
/// Kept `pub` so the seamed `ndistinct_for_combination` owner reuses the exact
/// estimator arithmetic.
pub fn estimate_ndistinct(totalrows: f64, numrows: i32, d: i32, f1: i32) -> f64 {
    // numer = (double) numrows * (double) d;
    let numer = numrows as f64 * d as f64;

    // denom = (double) (numrows - f1) + (double) f1 * (double) numrows / totalrows;
    let denom = (numrows - f1) as f64 + f1 as f64 * numrows as f64 / totalrows;

    let mut ndistinct = numer / denom;

    // Clamp to sane range in case of roundoff error
    if ndistinct < d as f64 {
        ndistinct = d as f64;
    }
    if ndistinct > totalrows {
        ndistinct = totalrows;
    }

    // return floor(ndistinct + 0.5);
    (ndistinct + 0.5).floor()
}

/* ---------------------------------------------------------------------------
 * n_choose_k / num_combinations (mvdistinct.c:549-578)
 * ------------------------------------------------------------------------- */

/// `n_choose_k` (mvdistinct.c:549-568) — binomial coefficients via an algorithm
/// that is both efficient and prevents overflows.
fn n_choose_k(n: i32, k: i32) -> i32 {
    // Assert((k > 0) && (n >= k));
    debug_assert!((k > 0) && (n >= k));

    // use symmetry of the binomial coefficients: k = Min(k, n - k);
    let k = k.min(n - k);

    let mut n = n;
    let mut r: i32 = 1;
    let mut d = 1;
    while d <= k {
        // r *= n--;  r /= d;
        r *= n;
        n -= 1;
        r /= d;
        d += 1;
    }

    r
}

/// `num_combinations` (mvdistinct.c:574-578) — number of combinations, excluding
/// single-value combinations: `(1 << n) - (n + 1)`.
fn num_combinations(n: i32) -> i32 {
    (1 << n) - (n + 1)
}

/* ---------------------------------------------------------------------------
 * Varlena byte accessors (the C `memcpy`/`SET_VARSIZE`/`VARSIZE_ANY` chain).
 * ------------------------------------------------------------------------- */

/// `SET_VARSIZE(ptr, len)` — store the 4-byte varlena length (long format; the
/// on-disk serialized bytea is never short/compressed). The header word holds
/// `len << 2`, native byte order, with the two low (tag) bits clear.
fn set_varsize(buf: &mut [u8], len: usize) {
    let header = (len as u32) << 2;
    buf[0..4].copy_from_slice(&header.to_ne_bytes());
}

/// `VARHDRSZ_SHORT` — the 1-byte short varlena header size.
const VARHDRSZ_SHORT: usize = 1;

/// `VARHDRSZ_ANY(ptr)` — the on-disk varlena length-header size: 1 byte for a
/// short (low-bit-set) header, else `VARHDRSZ` (4). A stored `pg_ndistinct`
/// value arrives short-headed once `SHORT_VARLENA_PACKING` is on; the
/// deserializer must skip the real header, not a fixed 4. No-op while the flag
/// is off (every stored value is 4B).
#[inline]
fn varhdrsz_any(bytes: &[u8]) -> usize {
    match bytes.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => VARHDRSZ_SHORT,
        _ => VARHDRSZ,
    }
}

/// `VARSIZE_ANY(ptr)` — total varlena size in bytes, handling both the 1-byte
/// short and 4-byte long uncompressed header forms.
fn varsize_any(bytes: &[u8]) -> PgResult<usize> {
    match bytes.first() {
        // Short (1-byte) header: bits 7..1 hold the total length (incl. header).
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => Ok((h >> 1) as usize),
        _ => {
            if bytes.len() < VARHDRSZ {
                return Err(PgError::error(format!(
                    "invalid MVNDistinct size {} (expected at least {SIZE_OF_HEADER})",
                    bytes.len()
                )));
            }
            let header = u32::from_ne_bytes(bytes[0..4].try_into().unwrap());
            Ok((header >> 2) as usize)
        }
    }
}

/// Read a native `u32` at `*tmp`, advancing `*tmp`.
fn read_u32(bytes: &[u8], tmp: &mut usize) -> PgResult<u32> {
    Ok(u32::from_ne_bytes(read_array(bytes, tmp)?))
}

/// Read an `i32` at `*tmp`, advancing `*tmp`.
fn read_i32(bytes: &[u8], tmp: &mut usize) -> PgResult<i32> {
    Ok(i32::from_ne_bytes(read_array(bytes, tmp)?))
}

/// Read an `f64` at `*tmp`, advancing `*tmp`.
fn read_f64(bytes: &[u8], tmp: &mut usize) -> PgResult<f64> {
    Ok(f64::from_ne_bytes(read_array(bytes, tmp)?))
}

/// Read an `AttrNumber` (`int16`) at `*tmp`, advancing `*tmp`.
fn read_attnum(bytes: &[u8], tmp: &mut usize) -> PgResult<AttrNumber> {
    Ok(AttrNumber::from_ne_bytes(read_array(bytes, tmp)?))
}

/// Read a fixed-size `[u8; N]` at `*tmp`, advancing `*tmp`; errors (rather than
/// panicking) if the slice would be read past its end.
fn read_array<const N: usize>(bytes: &[u8], tmp: &mut usize) -> PgResult<[u8; N]> {
    let end = tmp.checked_add(N).filter(|&e| e <= bytes.len());
    match end {
        Some(end) => {
            let arr: [u8; N] = bytes[*tmp..end].try_into().unwrap();
            *tmp = end;
            Ok(arr)
        }
        None => Err(PgError::error(format!(
            "invalid MVNDistinct size {} (truncated item data)",
            bytes.len()
        ))),
    }
}

/// Write a native `u32` at `*tmp`, advancing `*tmp`.
fn write_u32(buf: &mut [u8], tmp: &mut usize, v: u32) {
    buf[*tmp..*tmp + 4].copy_from_slice(&v.to_ne_bytes());
    *tmp += 4;
}

/// Write an `i32` at `*tmp`, advancing `*tmp`.
fn write_i32(buf: &mut [u8], tmp: &mut usize, v: i32) {
    buf[*tmp..*tmp + 4].copy_from_slice(&v.to_ne_bytes());
    *tmp += 4;
}

/// Write an `f64` at `*tmp`, advancing `*tmp`.
fn write_f64(buf: &mut [u8], tmp: &mut usize, v: f64) {
    buf[*tmp..*tmp + 8].copy_from_slice(&v.to_ne_bytes());
    *tmp += 8;
}

/// Write an `AttrNumber` (`int16`) at `*tmp`, advancing `*tmp`.
fn write_attnum(buf: &mut [u8], tmp: &mut usize, v: AttrNumber) {
    buf[*tmp..*tmp + SIZE_ATTRNUMBER].copy_from_slice(&v.to_ne_bytes());
    *tmp += SIZE_ATTRNUMBER;
}

pub mod fmgr_builtins;

/// Registers the `pg_ndistinct` I/O builtins into the fmgr-core builtin table
/// (C: `fmgr_builtins[]`), so by-OID dispatch resolves them. The rest of this
/// crate's functions are called by the (unported) `backend-statistics-core`
/// dispatcher.
pub fn init_seams() {
    fmgr_builtins::register_mvdistinct_builtins();
}

#[cfg(test)]
mod tests;
