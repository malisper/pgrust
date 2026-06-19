//! Port of `backend/statistics/dependencies.c` — multivariate functional
//! dependencies (the functional-dependency slice of the combined
//! `backend-statistics-core` unit).
//!
//! SELF-CONTAINED in this crate (100% of the C logic):
//!   * the `DependencyGenerator` k-permutation generator (init / generate /
//!     generate_recurse / next / free),
//!   * `statext_dependencies_build` control flow (the generator loop +
//!     `MVDependencies` assembly; only the per-tuple `dependency_degree`
//!     validation kernel crosses the seam),
//!   * `statext_dependencies_serialize` / `_deserialize` (exact C byte layout +
//!     every `elog(ERROR)` validity check, with allocation-safety bounds),
//!   * `dependency_is_fully_matched`, `find_strongest_dependency`,
//!     `clauselist_apply_dependencies`' selectivity arithmetic,
//!   * `clamp_probability` + the serialization size helpers,
//!   * `pg_dependencies_in` / `_out` / `_recv` / `_send` (the SQL-callable I/O
//!     surface; `_out` formats the cstring in-crate, `_send` delegates to the
//!     ported `byteasend`).
//!
//! SEAMED to the not-yet-ported owner (`backend-statistics-core`, covering
//! `extended_stats.c` + the multi-sort support + vacuum's `VacAttrStats`):
//!   * `dependency_degree` — needs `multi_sort_init` / `build_sorted_items` /
//!     `multi_sort_compare_dim(s)` over the opaque `StatsBuildData`. Reached via
//!     `backend_statistics_core_seams::dependency_degree`, panics until owner
//!     lands.
//!
//! NOT PORTED HERE (deferred to `backend-statistics-core`): the planner-arena
//! selectivity dispatch `dependencies_clauselist_selectivity` and its clause-
//! compatibility helpers (`dependency_is_compatible_clause` / `_expression`),
//! plus `statext_dependencies_load`. Those inspect planner `Node` structs
//! (`Var`/`OpExpr`/`RestrictInfo`/…) over the planner arena and read the
//! `pg_statistic_ext_data` syscache; their only caller in C is the unported
//! `statext_clauselist_selectivity` dispatcher. They land with the owner. The
//! selectivity *arithmetic* (`clauselist_apply_dependencies`) IS ported here so
//! the owner reuses it. See audits/backend-statistics-dependencies.md.

// The clause/loop ladders mirror the C `for (i = 0; ...; i++)` control flow 1:1
// (parallel-vector indexing, fallible bodies); collapsing them would obscure
// the correspondence.
#![allow(clippy::needless_range_loop)]

use mcx::Mcx;
use types_core::AttrNumber;
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_statistics::{
    MVDependencies, MVDependency, StatsBuildData, STATS_DEPS_MAGIC, STATS_DEPS_TYPE_BASIC,
    STATS_MAX_DIMENSIONS,
};

use backend_statistics_core_seams as core_seam;

/* ---------------------------------------------------------------------------
 * Constants mirroring the C macros / catalog headers.
 * ------------------------------------------------------------------------- */

/// On-wire widths of the C `memcpy` fields (NOT Rust `size_of`): `sizeof(uint32)`,
/// `sizeof(double)`, `sizeof(AttrNumber)`.
const SIZE_U32: usize = 4;
const SIZE_F64: usize = 8;
const SIZE_ATTRNUMBER: usize = 2;

/// `SizeOfHeader` — `(3 * sizeof(uint32))` (dependencies.c:38).
const SIZE_OF_HEADER: usize = 3 * SIZE_U32;

/// `SizeOfItem(natts)` — `(sizeof(double) + sizeof(AttrNumber) * (1 + natts))`
/// (dependencies.c:41-42).
#[inline]
fn size_of_item(natts: usize) -> usize {
    SIZE_F64 + SIZE_ATTRNUMBER * (1 + natts)
}

/// Allocation-safety bound (HARD RULE) on the data-derived `ndeps` before any
/// reservation in the deserializer: a corrupt-yet-large bytea must not be able
/// to request an absurd allocation. The `min_expected_size` check already proves
/// `ndeps <= exhdr`; this is an additional absolute sanity cap.
const MAX_REASONABLE_NDEPS: usize = 1 << 24;

/// `CLAMP_PROBABILITY(p)` (`utils/selfuncs.h`): `if (p < 0) p = 0; else if (p >
/// 1) p = 1;`. Mirrors the macro's exact branch structure (NOT `f64::clamp`,
/// which differs on NaN).
#[inline]
fn clamp_probability(p: f64) -> f64 {
    if p < 0.0 {
        0.0
    } else if p > 1.0 {
        1.0
    } else {
        p
    }
}

/* ---------------------------------------------------------------------------
 * DependencyGenerator — k-permutations of n columns (dependencies.c:56-210)
 *
 * Held as an owned Rust struct: C allocates it in a memory context and frees it
 * via `DependencyGenerator_free`, which here is plain ownership/`Drop`. The
 * transient working arrays use `try_reserve` (OOM -> `mcx.oom`) per the
 * fallible-allocation rule.
 * ------------------------------------------------------------------------- */

/// `DependencyGeneratorData` (dependencies.c:56-63).
struct DependencyGenerator {
    k: i32,
    n: i32,
    current: AttrNumber,
    ndependencies: AttrNumber,
    /// flat array of `k * ndependencies` AttrNumbers (C `state->dependencies`).
    dependencies: Vec<AttrNumber>,
}

impl DependencyGenerator {
    /// `DependencyGenerator_init` (dependencies.c:172-192).
    fn init(mcx: Mcx<'_>, n: i32, k: i32) -> PgResult<DependencyGenerator> {
        debug_assert!((n >= k) && (k > 0)); // Assert((n >= k) && (k > 0))

        // state->dependencies = palloc(k * sizeof(AttrNumber)) — initial capacity.
        let mut dependencies: Vec<AttrNumber> = Vec::new();
        dependencies
            .try_reserve(k as usize)
            .map_err(|_| mcx.oom(k as usize * SIZE_ATTRNUMBER))?;

        let mut state = DependencyGenerator {
            k,
            n,
            current: 0,
            ndependencies: 0,
            dependencies,
        };

        // now actually pre-generate all the variations
        state.generate(mcx)?;

        Ok(state)
    }

    /// `generate_dependencies` (dependencies.c:156-164).
    fn generate(&mut self, mcx: Mcx<'_>) -> PgResult<()> {
        // current = palloc0(sizeof(AttrNumber) * k)
        let mut current: Vec<AttrNumber> = Vec::new();
        current
            .try_reserve(self.k as usize)
            .map_err(|_| mcx.oom(self.k as usize * SIZE_ATTRNUMBER))?;
        for _ in 0..self.k as usize {
            current.push(0);
        }
        self.generate_recurse(mcx, 0, 0, &mut current)
        // pfree(current) — Vec drop.
    }

    /// `generate_dependencies_recurse` (dependencies.c:90-153).
    fn generate_recurse(
        &mut self,
        mcx: Mcx<'_>,
        index: i32,
        start: AttrNumber,
        current: &mut Vec<AttrNumber>,
    ) -> PgResult<()> {
        if index < (self.k - 1) {
            // The first (k-1) values have to be in ascending order, generated
            // recursively.
            let mut i: AttrNumber = start;
            while (i as i32) < self.n {
                current[index as usize] = i;
                self.generate_recurse(mcx, index + 1, i + 1, current)?;
                i += 1;
            }
        } else {
            // The last element is the implied value, which does not respect the
            // ascending order; we just check that the value is not in the first
            // (k-1) elements.
            let mut i: i32 = 0;
            while i < self.n {
                let mut matched = false;

                current[index as usize] = i as AttrNumber;

                let mut j: i32 = 0;
                while j < index {
                    if current[j as usize] as i32 == i {
                        matched = true;
                        break;
                    }
                    j += 1;
                }

                // If the value is not found in the first part of the dependency,
                // we're done.
                if !matched {
                    // repalloc state->dependencies to k * (ndependencies + 1)
                    // then memcpy(&dependencies[k*ndependencies], current, k).
                    self.dependencies
                        .try_reserve(self.k as usize)
                        .map_err(|_| mcx.oom(self.k as usize * SIZE_ATTRNUMBER))?;
                    for c in 0..self.k as usize {
                        self.dependencies.push(current[c]);
                    }
                    self.ndependencies += 1;
                }
                i += 1;
            }
        }
        Ok(())
    }

    /// `DependencyGenerator_next` (dependencies.c:203-210): the start index of
    /// the next k-tuple in `dependencies`, or `None`.
    fn next(&mut self) -> Option<usize> {
        if self.current == self.ndependencies {
            return None;
        }
        let idx = (self.k * self.current as i32) as usize;
        self.current += 1;
        Some(idx)
    }

    /// Borrow the k-tuple starting at `idx`.
    fn tuple(&self, idx: usize) -> &[AttrNumber] {
        &self.dependencies[idx..idx + self.k as usize]
    }
}
// DependencyGenerator_free (dependencies.c:198-201): the `dependencies` Vec is
// reclaimed by `Drop`.

/* ---------------------------------------------------------------------------
 * statext_dependencies_build (dependencies.c:347-437)
 * ------------------------------------------------------------------------- */

/// `statext_dependencies_build` (dependencies.c:347-437).
///
/// Generates all candidate variations (via the in-crate `DependencyGenerator`)
/// and computes the degree of validity for each (via the `dependency_degree`
/// seam — the per-column sort/group validation owned by the unported multi-sort
/// support). Returns `None` when no dependency had a non-zero degree (C returns
/// `NULL`).
///
/// `mcx` is the C `CurrentMemoryContext` (used for OOM error production on the
/// transient generator arrays). `data` is the opaque `StatsBuildData` handle;
/// `nattnums` is `data->nattnums` and `data_attnums` is `data->attnums` (the
/// build data cannot be dereferenced here — `data->attnums[...]` is mirrored by
/// the explicit `data_attnums` slice the owner supplies at call time).
pub fn statext_dependencies_build<'mcx>(
    mcx: Mcx<'_>,
    data: &StatsBuildData<'mcx>,
    nattnums: i32,
    data_attnums: &[AttrNumber],
) -> PgResult<Option<MVDependencies>> {
    debug_assert!(nattnums >= 2); // Assert(data->nattnums >= 2)

    // result
    let mut dependencies: Option<MVDependencies> = None;

    // Build dependencies from smallest (2 columns) to largest.
    let mut k: i32 = 2;
    while k <= nattnums {
        // prepare a DependencyGenerator of variation
        let mut generator = DependencyGenerator::init(mcx, nattnums, k)?;

        // generate all possible variations of k values (out of n)
        loop {
            let dep_idx = match generator.next() {
                Some(idx) => idx,
                None => break,
            };

            // The k-tuple of zero-based column indexes into the stats object.
            let dependency = generator.tuple(dep_idx);

            // compute how valid the dependency seems (the multi-sort kernel).
            let degree = core_seam::dependency_degree::call(data, k, dependency)?;

            // if the dependency seems entirely invalid, don't store it
            if degree == 0.0 {
                continue;
            }

            // copy the dependency (keep the indexes into stxkeys translated to
            // attnums): d->attributes[i] = data->attnums[dependency[i]].
            let mut attributes: Vec<AttrNumber> = Vec::new();
            attributes
                .try_reserve(k as usize)
                .map_err(|_| mcx.oom(k as usize * SIZE_ATTRNUMBER))?;
            for i in 0..k as usize {
                attributes.push(data_attnums[dependency[i] as usize]);
            }

            let d = MVDependency {
                degree,
                nattributes: k as AttrNumber,
                attributes,
            };

            // initialize the list of dependencies
            let deps = dependencies.get_or_insert_with(|| MVDependencies {
                magic: STATS_DEPS_MAGIC,
                r#type: STATS_DEPS_TYPE_BASIC,
                ndeps: 0,
                deps: Vec::new(),
            });

            deps.ndeps += 1;
            deps.deps
                .try_reserve(1)
                .map_err(|_| mcx.oom(core::mem::size_of::<usize>()))?;
            deps.deps.push(Box::new(d));
        }

        // DependencyGenerator_free — generator dropped at end of scope.
        k += 1;
    }

    Ok(dependencies)
}

/* ---------------------------------------------------------------------------
 * statext_dependencies_serialize (dependencies.c:443-493)
 *
 * Produces the serialized payload as an owned `Vec<u8>` whose bytes match the C
 * `VARDATA(output)` body exactly (the varlena framing is added by the storage
 * layer).
 * ------------------------------------------------------------------------- */

/// `statext_dependencies_serialize` (dependencies.c:443-493).
pub fn statext_dependencies_serialize(
    mcx: Mcx<'_>,
    dependencies: &MVDependencies,
) -> PgResult<Vec<u8>> {
    // we need to store ndeps, with a number of attributes for each one
    let mut len: usize = SIZE_OF_HEADER;

    // and also include space for the actual attribute numbers and degrees
    for i in 0..dependencies.ndeps as usize {
        len += size_of_item(dependencies.deps[i].nattributes as usize);
    }

    // output = palloc0(len)
    let mut out: Vec<u8> = Vec::new();
    out.try_reserve(len).map_err(|_| mcx.oom(len))?;

    // Store the base struct values (magic, type, ndeps).
    out.extend_from_slice(&dependencies.magic.to_ne_bytes());
    out.extend_from_slice(&dependencies.r#type.to_ne_bytes());
    out.extend_from_slice(&dependencies.ndeps.to_ne_bytes());

    // store number of attributes and attribute numbers for each dependency
    for i in 0..dependencies.ndeps as usize {
        let d = &dependencies.deps[i];

        out.extend_from_slice(&d.degree.to_ne_bytes());
        out.extend_from_slice(&d.nattributes.to_ne_bytes());

        for a in 0..d.nattributes as usize {
            out.extend_from_slice(&d.attributes[a].to_ne_bytes());
        }

        debug_assert!(out.len() <= len); // Assert(tmp <= output + len)
    }

    debug_assert!(out.len() == len); // Assert(tmp == output + len)

    Ok(out)
}

/* ---------------------------------------------------------------------------
 * statext_dependencies_deserialize (dependencies.c:498-587)
 * ------------------------------------------------------------------------- */

/// Little cursor over the bytea body (mirrors the C `tmp` walking pointer).
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Cursor { buf, pos: 0 }
    }
    fn read_u32(&mut self) -> u32 {
        let mut b = [0u8; 4];
        b.copy_from_slice(&self.buf[self.pos..self.pos + 4]);
        self.pos += 4;
        u32::from_ne_bytes(b)
    }
    fn read_f64(&mut self) -> f64 {
        let mut b = [0u8; 8];
        b.copy_from_slice(&self.buf[self.pos..self.pos + 8]);
        self.pos += 8;
        f64::from_ne_bytes(b)
    }
    fn read_i16(&mut self) -> i16 {
        let mut b = [0u8; 2];
        b.copy_from_slice(&self.buf[self.pos..self.pos + 2]);
        self.pos += 2;
        i16::from_ne_bytes(b)
    }
    fn offset(&self) -> usize {
        self.pos
    }
}

/// `statext_dependencies_deserialize` (dependencies.c:498-587).
///
/// `data` is the bytea *payload* (the `VARSIZE_ANY_EXHDR` body, varlena framing
/// already stripped). Returns `None` for a NULL datum (C returns `NULL`).
pub fn statext_dependencies_deserialize(
    mcx: Mcx<'_>,
    data: Option<&[u8]>,
) -> PgResult<Option<MVDependencies>> {
    let data = match data {
        None => return Ok(None),
        Some(d) => d,
    };

    let exhdr = data.len();

    // VARSIZE_ANY_EXHDR(data) < SizeOfHeader
    if exhdr < SIZE_OF_HEADER {
        return Err(PgError::error(format!(
            "invalid MVDependencies size {exhdr} (expected at least {SIZE_OF_HEADER})"
        )));
    }

    // read the header fields and perform basic sanity checks
    let mut cur = Cursor::new(data);
    let magic = cur.read_u32();
    let r#type = cur.read_u32();
    let ndeps_raw = cur.read_u32();

    // C prints both uint32 operands via %d (dependencies.c:528/532) — match by
    // reinterpreting as signed.
    if magic != STATS_DEPS_MAGIC {
        return Err(PgError::error(format!(
            "invalid dependency magic {} (expected {})",
            magic as i32, STATS_DEPS_MAGIC as i32
        )));
    }

    if r#type != STATS_DEPS_TYPE_BASIC {
        return Err(PgError::error(format!(
            "invalid dependency type {} (expected {})",
            r#type as i32, STATS_DEPS_TYPE_BASIC as i32
        )));
    }

    if ndeps_raw == 0 {
        return Err(PgError::error(
            "invalid zero-length item array in MVDependencies",
        ));
    }

    let ndeps = ndeps_raw as usize;

    // what minimum bytea size do we expect for those parameters
    let min_expected_size = size_of_item(ndeps);

    if exhdr < min_expected_size {
        return Err(PgError::error(format!(
            "invalid dependencies size {exhdr} (expected at least {min_expected_size})"
        )));
    }

    // Allocation-safety HARD RULE: bound the data-derived `ndeps` before reserve.
    if ndeps > MAX_REASONABLE_NDEPS {
        return Err(PgError::error(format!(
            "invalid dependencies count {ndeps} (exceeds {MAX_REASONABLE_NDEPS})"
        )));
    }

    let mut deps: Vec<Box<MVDependency>> = Vec::new();
    deps.try_reserve(ndeps)
        .map_err(|_| mcx.oom(ndeps * core::mem::size_of::<usize>()))?;

    for _ in 0..ndeps {
        // degree of validity
        let degree = cur.read_f64();

        // number of attributes
        let kk = cur.read_i16();

        // is the number of attributes valid?
        debug_assert!((kk >= 2) && (kk as usize <= STATS_MAX_DIMENSIONS));

        // now that we know the number of attributes, allocate the dependency
        let mut attributes: Vec<AttrNumber> = Vec::new();
        attributes
            .try_reserve(kk as usize)
            .map_err(|_| mcx.oom(kk as usize * SIZE_ATTRNUMBER))?;

        // copy attribute numbers
        for _ in 0..kk as usize {
            attributes.push(cur.read_i16());
        }

        deps.push(Box::new(MVDependency {
            degree,
            nattributes: kk,
            attributes,
        }));

        debug_assert!(cur.offset() <= exhdr); // still within the bytea
    }

    debug_assert!(cur.offset() == exhdr); // consumed the whole bytea exactly

    Ok(Some(MVDependencies {
        magic,
        r#type,
        ndeps: ndeps_raw,
        deps,
    }))
}

/* ---------------------------------------------------------------------------
 * dependency_is_fully_matched (dependencies.c:594-612)
 * ------------------------------------------------------------------------- */

/// `dependency_is_fully_matched` (dependencies.c:594-612).
///
/// `attnums` is the clause-attribute bitmapset (the C `Bitmapset *attnums`),
/// represented here as a sorted/searchable slice of member attnums — the
/// callers (`find_strongest_dependency` and the planner-arena selectivity
/// driver) own the bitmapset and pass its members.
fn dependency_is_fully_matched(dependency: &MVDependency, attnums: &[i32]) -> bool {
    // Check that the dependency actually is fully covered by clauses.
    for j in 0..dependency.nattributes as usize {
        let attnum = dependency.attributes[j] as i32;
        if !attnums.contains(&attnum) {
            return false;
        }
    }
    true
}

/* ---------------------------------------------------------------------------
 * find_strongest_dependency (dependencies.c:928-980)
 * ------------------------------------------------------------------------- */

/// `find_strongest_dependency` (dependencies.c:928-980).
///
/// Returns the `(stat_index, dep_index)` of the strongest fully-matched
/// dependency, or `None`. Indices rather than a borrow because the caller keeps
/// the dependency collections mutable across the surrounding loop. `attnums` is
/// the clause-attribute member set (the C `Bitmapset *attnums`).
pub fn find_strongest_dependency(
    dependencies: &[MVDependencies],
    attnums: &[i32],
) -> Option<(usize, usize)> {
    let mut strongest: Option<(usize, usize)> = None;
    // The strongest's (nattributes, degree) for the cheap comparisons.
    let mut strongest_natts: AttrNumber = 0;
    let mut strongest_degree: f64 = 0.0;

    // number of attnums in clauses
    let nattnums = attnums.len() as i32; // bms_num_members(attnums)

    for i in 0..dependencies.len() {
        let deps = &dependencies[i];
        for j in 0..deps.ndeps as usize {
            let dependency = &deps.deps[j];

            // Skip dependencies referencing more attributes than available.
            if dependency.nattributes as i32 > nattnums {
                continue;
            }

            if strongest.is_some() {
                // skip dependencies on fewer attributes than the strongest.
                if dependency.nattributes < strongest_natts {
                    continue;
                }

                // also skip weaker dependencies when attribute count matches
                if strongest_natts == dependency.nattributes
                    && strongest_degree > dependency.degree
                {
                    continue;
                }
            }

            // check it's fully matched to these attnums (last, most expensive).
            if dependency_is_fully_matched(dependency, attnums) {
                strongest = Some((i, j)); // save new best match
                strongest_natts = dependency.nattributes;
                strongest_degree = dependency.degree;
            }
        }
    }

    strongest
}

/* ---------------------------------------------------------------------------
 * clauselist_apply_dependencies — selectivity arithmetic (dependencies.c:1013-1157)
 *
 * The per-column selectivity estimation (`clauselist_selectivity_ext`), the
 * clause-to-attribute bitmap marking, and the bitmapset operations live in the
 * planner-arena driver (`dependencies_clauselist_selectivity`, deferred to the
 * owner). The conditional-probability COMBINATION arithmetic — the reusable
 * kernel — is ported here.
 * ------------------------------------------------------------------------- */

/// The selectivity-combination kernel of `clauselist_apply_dependencies`
/// (dependencies.c:1080-1151): given the per-attribute simple selectivities
/// (`attr_sel`, indexed in bitmapset-member order), the selected dependencies
/// (each as a list of member-index positions into `attr_sel`, implying-first /
/// implied-last, plus the degree `f`), combine them backwards into the overall
/// clamped selectivity.
///
/// The caller (`dependencies_clauselist_selectivity`, owner-side) does the
/// attnum extraction (dependencies.c:1037-1046), the per-column
/// `clauselist_selectivity_ext` estimation (1048-1078), and the bms_member_index
/// lookups (1116/1122) that produce `dep_member_indexes`. This function is the
/// pure arithmetic at 1104-1151.
pub fn combine_dependency_selectivities(
    attr_sel: &mut [f64],
    dep_member_indexes: &[Vec<usize>],
    dep_degrees: &[f64],
) -> f64 {
    let ndependencies = dep_member_indexes.len();

    // Combine selectivities using the dependency information, backwards, so that
    // chains a -> b -> c compute in the right order.
    let mut i: i32 = ndependencies as i32 - 1;
    while i >= 0 {
        let members = &dep_member_indexes[i as usize];
        let f = dep_degrees[i as usize];

        // Selectivity of all the implying attributes (all but the last member).
        let mut s1: f64 = 1.0;
        let nimplying = members.len() - 1;
        for j in 0..nimplying {
            s1 *= attr_sel[members[j]];
        }

        // Original selectivity of the implied attribute (the last member).
        let attidx = members[nimplying];
        let s2 = attr_sel[attidx];

        // Replace s2 with the conditional probability s2 given s1:
        //   P(b|a) = f * Min(P(a),P(b)) / P(a) + (1-f) * P(b)
        if s1 <= s2 {
            attr_sel[attidx] = f + (1.0 - f) * s2;
        } else {
            attr_sel[attidx] = f * s2 / s1 + (1.0 - f) * s2;
        }

        i -= 1;
    }

    // Overall selectivity is the product of all per-attribute selectivities.
    let mut s1: f64 = 1.0;
    for idx in 0..attr_sel.len() {
        s1 *= attr_sel[idx];
    }

    clamp_probability(s1)
}

/* ---------------------------------------------------------------------------
 * pg_dependencies_in / _out / _recv / _send (dependencies.c:652-729)
 * ------------------------------------------------------------------------- */

/// The `ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg(...)))`
/// shared by `pg_dependencies_in` and `pg_dependencies_recv`
/// (dependencies.c:659-661 / 712-714).
fn cannot_accept_value() -> PgError {
    PgError::error(format!("cannot accept a value of type {}", "pg_dependencies"))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

/// `pg_dependencies_in` (dependencies.c:652-664) — text input is disallowed.
pub fn pg_dependencies_in() -> PgResult<()> {
    Err(cannot_accept_value())
}

/// `pg_dependencies_recv` (dependencies.c:709-717) — binary input disallowed.
pub fn pg_dependencies_recv() -> PgResult<()> {
    Err(cannot_accept_value())
}

/// `pg_dependencies_out` (dependencies.c:669-704).
///
/// `data` is the detoasted bytea payload (the fmgr `PG_GETARG_BYTEA_PP(0)` body).
/// Returns the NUL-terminated cstring bytes (the fmgr `PG_RETURN_CSTRING`
/// payload). The `StringInfo` formatting is done in-crate: `%d` is the decimal
/// attnum and `%f` is C `printf` default 6-fractional-digit float formatting.
pub fn pg_dependencies_out(mcx: Mcx<'_>, data: &[u8]) -> PgResult<Vec<u8>> {
    // MVDependencies *dependencies = statext_dependencies_deserialize(data);
    let dependencies = statext_dependencies_deserialize(mcx, Some(data))?;
    // C unconditionally dereferences the result; a non-NULL detoasted arg always
    // deserializes to Some. Treat None (NULL datum) as a hard error to match the
    // C crash contract without an unsafe deref.
    let dependencies = dependencies
        .ok_or_else(|| PgError::error("pg_dependencies_out: NULL dependencies"))?;

    // initStringInfo(&str); appendStringInfoChar(&str, '{');
    let mut str: Vec<u8> = Vec::new();
    str.push(b'{');

    let ndeps = dependencies.ndeps as i32;
    for i in 0..ndeps {
        let dependency = &dependencies.deps[i as usize];

        if i > 0 {
            str.extend_from_slice(b", ");
        }

        str.push(b'"');

        let nattributes = dependency.nattributes as i32;
        for j in 0..nattributes {
            if j == nattributes - 1 {
                str.extend_from_slice(b" => ");
            } else if j > 0 {
                str.extend_from_slice(b", ");
            }

            // appendStringInfo(&str, "%d", dependency->attributes[j]);
            str.extend_from_slice(format!("{}", dependency.attributes[j as usize]).as_bytes());
        }
        // appendStringInfo(&str, "\": %f", dependency->degree);
        // C printf "%f" is fixed 6 fractional digits (no exponent).
        str.extend_from_slice(b"\": ");
        str.extend_from_slice(format!("{:.6}", dependency.degree).as_bytes());
    }

    str.push(b'}');
    // PG_RETURN_CSTRING(str.data): NUL-terminate the cstring payload.
    str.push(0);

    Ok(str)
}

/// `pg_dependencies_send` (dependencies.c:725-729) — `return byteasend(fcinfo)`.
///
/// Functional dependencies are stored as a bytea, so the binary-output routine
/// just sends those bytes. Delegates to the ported `byteasend`.
pub fn pg_dependencies_send<'mcx>(
    mcx: Mcx<'mcx>,
    v: &[u8],
) -> PgResult<mcx::PgVec<'mcx, u8>> {
    backend_utils_adt_varlena::bytea::byteasend(mcx, v)
}

/// This crate installs NO inward seams — its public functions are called only by
/// the (unported) `backend-statistics-core` dispatcher and the fmgr catalog,
/// neither of which is in-repo yet. Present so the aggregator can invoke it
/// uniformly if it ever needs to; the recurrence guard only requires wiring for
/// crates that actually install a seam.
pub mod fmgr_builtins;

/// Registers the `pg_dependencies` I/O builtins into the fmgr-core builtin table
/// (C: `fmgr_builtins[]`), so by-OID dispatch resolves them. The rest of this
/// crate's functions are called by the (unported) `backend-statistics-core`
/// dispatcher.
pub fn init_seams() {
    fmgr_builtins::register_dependencies_builtins();
}

#[cfg(test)]
mod tests;
