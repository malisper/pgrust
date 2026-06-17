//! `opclass` family — `lsyscache.c` lookups keyed on `pg_opclass` /
//! `pg_opfamily` / `pg_amproc` (operator-class and operator-family metadata).
//!
//! C entry points covered here: `get_opclass_input_type`, `get_opclass_family`,
//! `get_opfamily_method`, `get_opfamily_proc`, `get_opfamily_name`.
//!
//! The `SearchSysCache*` probes route through the `backend-utils-cache-syscache`
//! owner's seam (a loud panic until syscache installs them). The scalar-only
//! entry points (`get_opclass_input_type` / `get_opclass_family` /
//! `get_opfamily_method` / `get_opfamily_proc`) own no `Mcx` argument — C reads
//! the scalar straight out of the cached tuple and `ReleaseSysCache`s — so they
//! spin a short-lived scratch context for the projected-row copy (mirroring the
//! syscache projections' own pattern) and return the `Copy` scalar; the scratch
//! drops at end of call. `get_opfamily_name` copies the name into the caller's
//! `Mcx` (C: `pstrdup`).
//!
//! `GetDefaultOpClass` is NOT a `lsyscache.c` function — it lives in
//! `commands/indexcmds.c` and is owned by `backend-commands-indexcmds`, which
//! installs the `get_default_opclass` seam declared on this crate's seam unit.

use backend_utils_cache_syscache_seams as syscache_seams;
use mcx::{Mcx, MemoryContext, PgString};
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult};

/// `get_opclass_input_type(opclass)` (lsyscache.c): the opclass's `opcintype`.
///
/// ```c
/// Oid
/// get_opclass_input_type(Oid opclass)
/// {
///     HeapTuple   tp;
///     Form_pg_opclass cla_tup;
///     Oid         result;
///
///     tp = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
///     if (!HeapTupleIsValid(tp))
///         elog(ERROR, "cache lookup failed for opclass %u", opclass);
///     cla_tup = (Form_pg_opclass) GETSTRUCT(tp);
///     result = cla_tup->opcintype;
///     ReleaseSysCache(tp);
///     return result;
/// }
/// ```
pub fn get_opclass_input_type(opclass: Oid) -> PgResult<Oid> {
    let scratch = MemoryContext::new("get_opclass_input_type");
    let tp = syscache_seams::search_opclass::call(scratch.mcx(), opclass)?;
    match tp {
        Some(cla_tup) => Ok(cla_tup.opcintype),
        None => Err(PgError::error(format!(
            "cache lookup failed for opclass {}",
            opclass
        ))),
    }
}

/// `get_opclass_family(opclass)` (lsyscache.c): the opclass's `opcfamily`.
///
/// ```c
/// Oid
/// get_opclass_family(Oid opclass)
/// {
///     HeapTuple   tp;
///     Form_pg_opclass cla_tup;
///     Oid         result;
///
///     tp = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
///     if (!HeapTupleIsValid(tp))
///         elog(ERROR, "cache lookup failed for opclass %u", opclass);
///     cla_tup = (Form_pg_opclass) GETSTRUCT(tp);
///     result = cla_tup->opcfamily;
///     ReleaseSysCache(tp);
///     return result;
/// }
/// ```
pub fn get_opclass_family(opclass: Oid) -> PgResult<Oid> {
    let scratch = MemoryContext::new("get_opclass_family");
    let tp = syscache_seams::search_opclass::call(scratch.mcx(), opclass)?;
    match tp {
        Some(cla_tup) => Ok(cla_tup.opcfamily),
        None => Err(PgError::error(format!(
            "cache lookup failed for opclass {}",
            opclass
        ))),
    }
}

/// `get_opfamily_method(opfid)` (lsyscache.c): the access-method OID
/// (`opfmethod`) of the opfamily.
///
/// ```c
/// Oid
/// get_opfamily_method(Oid opfid)
/// {
///     HeapTuple   tp;
///     Form_pg_opfamily opf_tup;
///     Oid         result;
///
///     tp = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(opfid));
///     if (!HeapTupleIsValid(tp))
///         elog(ERROR, "cache lookup failed for operator family %u", opfid);
///     opf_tup = (Form_pg_opfamily) GETSTRUCT(tp);
///     result = opf_tup->opfmethod;
///     ReleaseSysCache(tp);
///     return result;
/// }
/// ```
///
/// The owned syscache probe projects the `OPFAMILYOID` row to
/// `(opfnamespace, opfmethod, opfname)`; only `opfmethod` (the second field) is
/// read here.
pub fn get_opfamily_method(opfid: Oid) -> PgResult<Oid> {
    let scratch = MemoryContext::new("get_opfamily_method");
    let tp = syscache_seams::opfamily_namespace_method_name::call(scratch.mcx(), opfid)?;
    match tp {
        Some((_opfnamespace, opfmethod, _opfname)) => Ok(opfmethod),
        None => Err(PgError::error(format!(
            "cache lookup failed for operator family {}",
            opfid
        ))),
    }
}

/// `get_opfamily_proc(opfamily, lefttype, righttype, procnum)` (lsyscache.c):
/// the support function OID registered for the procnum/type pair, or
/// `InvalidOid` if none.
///
/// ```c
/// RegProcedure
/// get_opfamily_proc(Oid opfamily, Oid lefttype, Oid righttype, int16 procnum)
/// {
///     HeapTuple   tp;
///     Form_pg_amproc amproc_tup;
///     RegProcedure result;
///
///     tp = SearchSysCache4(AMPROCNUM,
///                          ObjectIdGetDatum(opfamily),
///                          ObjectIdGetDatum(lefttype),
///                          ObjectIdGetDatum(righttype),
///                          Int16GetDatum(procnum));
///     if (!HeapTupleIsValid(tp))
///         return InvalidOid;
///     amproc_tup = (Form_pg_amproc) GETSTRUCT(tp);
///     result = amproc_tup->amproc;
///     ReleaseSysCache(tp);
///     return result;
/// }
/// ```
///
/// `AMPROCNUM` is unique on `(amprocfamily, amproclefttype, amprocrighttype,
/// amprocnum)`, so the C `SearchSysCache4` point lookup is reproduced by
/// scanning the owned `AMPROCNUM` cat-list for the opfamily and selecting the
/// row whose `(amproclefttype, amprocrighttype, amprocnum)` match; the unique
/// key guarantees at most one hit. A miss is the C `InvalidOid`.
pub fn get_opfamily_proc(
    opfamily: Oid,
    lefttype: Oid,
    righttype: Oid,
    procnum: i16,
) -> PgResult<Oid> {
    let scratch = MemoryContext::new("get_opfamily_proc");
    let proclist = syscache_seams::search_amproc_list::call(scratch.mcx(), opfamily)?;
    for amproc_tup in proclist.iter() {
        if amproc_tup.amproclefttype == lefttype
            && amproc_tup.amprocrighttype == righttype
            && amproc_tup.amprocnum == procnum
        {
            return Ok(amproc_tup.amproc);
        }
    }
    Ok(InvalidOid)
}

/// `get_opfamily_name(opfid, missing_ok)` (lsyscache.c): the opfamily's name,
/// copied into `mcx` (C: `pstrdup`).
///
/// ```c
/// char *
/// get_opfamily_name(Oid opfid, bool missing_ok)
/// {
///     HeapTuple   tup;
///     char       *result;
///     Form_pg_opfamily opfform;
///
///     tup = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(opfid));
///     if (!HeapTupleIsValid(tup))
///     {
///         if (!missing_ok)
///             elog(ERROR, "cache lookup failed for operator family %u", opfid);
///         return NULL;
///     }
///     opfform = (Form_pg_opfamily) GETSTRUCT(tup);
///     result = pstrdup(NameStr(opfform->opfname));
///     ReleaseSysCache(tup);
///     return result;
/// }
/// ```
///
/// The owned syscache probe already copies `NameStr(opfform->opfname)` into the
/// supplied `mcx`, so the projected name string is the `pstrdup` result.
pub fn get_opfamily_name<'mcx>(
    mcx: Mcx<'mcx>,
    opfid: Oid,
    missing_ok: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    let tup = syscache_seams::opfamily_namespace_method_name::call(mcx, opfid)?;
    match tup {
        Some((_opfnamespace, _opfmethod, opfname)) => Ok(Some(opfname)),
        None => {
            if !missing_ok {
                return Err(PgError::error(format!(
                    "cache lookup failed for operator family {}",
                    opfid
                )));
            }
            Ok(None)
        }
    }
}

/// `get_opclass_opfamily_and_input_type(opclass, &opfamily, &opcintype)`
/// (lsyscache.c).
///
/// ```c
/// tp = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
/// if (!HeapTupleIsValid(tp)) return false;
/// cla_tup = (Form_pg_opclass) GETSTRUCT(tp);
/// *opfamily = cla_tup->opcfamily;
/// *opcintype = cla_tup->opcintype;
/// ReleaseSysCache(tp);
/// return true;
/// ```
pub fn get_opclass_opfamily_and_input_type(opclass: Oid) -> PgResult<Option<(Oid, Oid)>> {
    match syscache_seams::pg_opclass_form::call(opclass)? {
        Some((opcfamily, opcintype, _opcmethod)) => Ok(Some((opcfamily, opcintype))),
        None => Ok(None),
    }
}

/// `get_opclass_method(opclass)` (lsyscache.c): the index AM OID
/// (`opcmethod`); a missing opclass is the C `elog(ERROR, "cache lookup failed
/// for opclass %u")`.
pub fn get_opclass_method(opclass: Oid) -> PgResult<Oid> {
    match syscache_seams::pg_opclass_form::call(opclass)? {
        Some((_opcfamily, _opcintype, opcmethod)) => Ok(opcmethod),
        None => Err(PgError::error(format!(
            "cache lookup failed for opclass {opclass}"
        ))),
    }
}
