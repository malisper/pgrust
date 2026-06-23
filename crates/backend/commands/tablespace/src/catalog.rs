//! The `pg_tablespace` catalog primitives that `tablespace.c` inlines:
//! `table_open`/`table_close`, the `table_beginscan_catalog` + `heap_getnext`
//! scans (by name, by oid, full), the `transformRelOptions` +
//! `tablespace_reloptions` options build, and the `heap_form_tuple` /
//! `heap_modify_tuple` + `CatalogTupleInsert`/`Update`/`Delete` row writers.
//!
//! There is no separate `pg_tablespace.c` translation unit; these are the
//! catalog operations inlined into tablespace.c, so this unit owns and installs
//! the `backend-catalog-pg-tablespace-seams` declarations.

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use ::mcx::{Mcx, MemoryContext, PgString};
use ::types_core::primitive::Oid;
use ::utils_error::ereport;
use ::types_error::{ERROR, PgError, PgResult};
use ::types_error::{ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_SYNTAX_ERROR};
use ::types_catalog::catalog::TABLE_SPACE_RELATION_ID;
use ::nodes::ddlnodes::DefElem;
use ::nodes::nodes::ntag;
use ::rel::Relation;
use ::types_scan::scankey::{ScanKeyData, StrategyNumber};
use ::types_scan::sdir::ScanDirection;
use ::types_storage::lock::LOCKMODE;
use ::types_tableam::relscan::TableScanDesc;
use ::types_tuple::heaptuple::{Datum, FormedTuple};
use ::types_tuple::heaptuple::ItemPointerData;

use ::pg_tablespace_seams::{self as cat, TablespaceTuple};
use ::heaptuple::{
    heap_copytuple, heap_form_tuple, heap_getattr, heap_modify_tuple,
};
use ::scankey::ScanKeyInit;
use table_seams as table_seam;
use ::table_tableam::{table_beginscan_catalog, table_endscan};
use ::heapam::scan::heap_getnext;
use ::indexing::keystone::{
    CatalogTupleDelete, CatalogTupleInsert, CatalogTupleUpdate,
};
use reloptions_seams as reloptions_seam;

// pg_tablespace attribute numbers (pg_tablespace_d.h):
//   oid = 1, spcname = 2, spcowner = 3, spcacl = 4, spcoptions = 5; Natts = 5.
const ANUM_OID: i32 = 1;
const ANUM_SPCNAME: i32 = 2;
const ANUM_SPCOWNER: i32 = 3;
const ANUM_SPCACL: i32 = 4;
const ANUM_SPCOPTIONS: i32 = 5;
const NATTS: usize = 5;

// `stratnum.h` BTEqualStrategyNumber.
const BT_EQUAL_STRATEGY_NUMBER: StrategyNumber = 3;

/// `CStringGetDatum(name)` scan-key arg for `F_NAMEEQ`. The `nameeq` comparator
/// reads BOTH args off the by-ref varlena lane (`PG_GETARG_NAME`), so the key
/// must cross as a `Datum::ByRef` `NameData` image (a NUL-terminated byte
/// image), not a `Datum::Cstring` — matching the proven `pg_database` name-key
/// path.
fn name_key_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    let mut bytes: ::mcx::PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, s.len() + 1)?;
    for &b in s.as_bytes() {
        bytes.push(b);
    }
    bytes.push(0);
    Ok(Datum::ByRef(bytes))
}

/// `namein(s)` image — a `NAMEDATALEN`-byte NUL-padded `NameData` Datum.
fn name_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    use ::types_core::fmgr::NAMEDATALEN;
    let len = NAMEDATALEN as usize;
    let mut image: ::mcx::PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, len)?;
    let src = s.as_bytes();
    let take = core::cmp::min(src.len(), len - 1);
    for &b in &src[..take] {
        image.push(b);
    }
    while image.len() < len {
        image.push(0);
    }
    Ok(Datum::ByRef(image))
}

/// `NameStr(name)` — read a NUL-padded `NameData` image as a `&str`.
fn name_str(bytes: &[u8]) -> &str {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    core::str::from_utf8(&bytes[..end]).unwrap_or("")
}

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, proc, arg)`.
fn eq_key<'mcx>(
    attno: i32,
    proc: ::types_core::primitive::RegProcedure,
    arg: Datum<'mcx>,
) -> PgResult<ScanKeyData<'mcx>> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        attno as ::types_core::AttrNumber,
        BT_EQUAL_STRATEGY_NUMBER,
        proc,
        arg,
    )?;
    Ok(key)
}

/// `GETSTRUCT(tuple)->oid` — deform and read the `oid` column.
fn form_oid<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tup: &FormedTuple<'mcx>,
) -> PgResult<Oid> {
    let col = heap_getattr(mcx, tup, ANUM_OID, &rel.rd_att)?;
    Ok(col.0.as_oid())
}

// ===========================================================================
// table_open / table_close
// ===========================================================================

/// `table_open(TableSpaceRelationId, lockmode)`.
fn tablespace_table_open<'mcx>(mcx: Mcx<'mcx>, lockmode: LOCKMODE) -> PgResult<Relation<'mcx>> {
    table_seam::table_open::call(mcx, TABLE_SPACE_RELATION_ID, lockmode)
}

/// `table_close(rel, lockmode)`.
fn tablespace_table_close<'mcx>(rel: Relation<'mcx>, lockmode: LOCKMODE) -> PgResult<()> {
    rel.close(lockmode)
}

// ===========================================================================
// scans
// ===========================================================================

/// `table_beginscan_catalog(rel, 1, [spcname == name]) + heap_getnext`
/// (`F_NAMEEQ`); returns the matched row's `oid` + `t_self` identity or `None`.
fn scan_tablespace_by_name<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    name: &str,
) -> PgResult<Option<TablespaceTuple>> {
    use ::types_core::fmgr::F_NAMEEQ;
    let key = eq_key(ANUM_SPCNAME, F_NAMEEQ, name_key_datum(mcx, name)?)?;
    let mut keys: ::mcx::PgVec<'mcx, ScanKeyData<'mcx>> = ::mcx::vec_with_capacity_in(mcx, 1)?;
    keys.push(key);

    let mut scan: TableScanDesc<'mcx> = table_beginscan_catalog(mcx, rel, 1, keys)?;

    let result = match heap_getnext(mcx, &mut scan, ScanDirection::ForwardScanDirection)? {
        Some(tup) => {
            let oid = form_oid(mcx, rel, tup)?;
            Some(TablespaceTuple {
                oid,
                handle: tup.tuple.t_self,
            })
        }
        None => None,
    };

    table_endscan(scan)?;
    Ok(result)
}

/// `table_beginscan_catalog(rel, 0, NULL) + heap_getnext` loop returning every
/// row's `oid` in scan order.
fn scan_all_tablespace_oids<'mcx>(mcx: Mcx<'mcx>, rel: &Relation<'mcx>) -> PgResult<Vec<Oid>> {
    let keys: ::mcx::PgVec<'mcx, ScanKeyData<'mcx>> = ::mcx::PgVec::new_in(mcx);
    let mut scan: TableScanDesc<'mcx> = table_beginscan_catalog(mcx, rel, 0, keys)?;

    let mut out: Vec<Oid> = Vec::new();
    while let Some(tup) = heap_getnext(mcx, &mut scan, ScanDirection::ForwardScanDirection)? {
        out.push(form_oid(mcx, rel, tup)?);
    }

    table_endscan(scan)?;
    Ok(out)
}

/// `table_beginscan_catalog(rel, 1, [oid == spc_oid]) + heap_getnext`
/// (`F_OIDEQ`); returns `pstrdup(NameStr(spcname))` (in `mcx`) of the match or
/// `None`.
fn scan_tablespace_name_by_oid<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    spc_oid: Oid,
) -> PgResult<Option<PgString<'mcx>>> {
    use ::types_core::fmgr::F_OIDEQ;
    let key = eq_key(ANUM_OID, F_OIDEQ, Datum::from_oid(spc_oid))?;
    let mut keys: ::mcx::PgVec<'mcx, ScanKeyData<'mcx>> = ::mcx::vec_with_capacity_in(mcx, 1)?;
    keys.push(key);

    let mut scan: TableScanDesc<'mcx> = table_beginscan_catalog(mcx, rel, 1, keys)?;

    let result = match heap_getnext(mcx, &mut scan, ScanDirection::ForwardScanDirection)? {
        Some(tup) => {
            let col = heap_getattr(mcx, tup, ANUM_SPCNAME, &rel.rd_att)?;
            let name = name_str(col.0.as_ref_bytes()).to_string();
            Some(PgString::from_str_in(&name, mcx)?)
        }
        None => None,
    };

    table_endscan(scan)?;
    Ok(result)
}

// ===========================================================================
// options build (transformRelOptions + tablespace_reloptions validation)
// ===========================================================================

/// `def_get_string` projection of a `DefElem`'s value node (define.c).
fn defel_arg(def: &DefElem<'_>) -> PgResult<Option<::define_seams::DefElemArg>> {
    use ::define_seams::DefElemArg;
    let Some(node) = def.arg.as_deref() else {
        return Ok(None);
    };
    // Mirror `defGetString`'s full node switch (define.c): a bare-identifier
    // value arrives as a `T_TypeName` and a qualified name as a `T_List`; both
    // render to text. A `_ => AStar` catch-all would collapse those to `"*"`.
    Ok(Some(match node.node_tag() {
        ntag::T_Integer => DefElemArg::Integer(node.expect_integer().ival as i64),
        ntag::T_Float => DefElemArg::Float(node.expect_float().fval.as_str().to_string()),
        ntag::T_Boolean => DefElemArg::Boolean(node.expect_boolean().boolval),
        ntag::T_String => DefElemArg::String(node.expect_string().sval.as_str().to_string()),
        ntag::T_TypeName => DefElemArg::TypeName(defel_type_name_to_string(node.expect_typename())?),
        ntag::T_List => DefElemArg::List(defel_name_list_to_string(node.expect_list())?),
        ntag::T_A_Star => DefElemArg::AStar,
        other => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("unrecognized node type: {}", other))
                .into_error())
        }
    }))
}

/// `TypeNameToString(typeName)` for the `defGetString` `T_TypeName` case.
fn defel_type_name_to_string(tn: &::nodes::rawnodes::TypeName<'_>) -> PgResult<String> {
    if tn.names.is_empty() {
        return Err(ereport(ERROR)
            .errmsg_internal("DefElem TypeName carries no name")
            .into_error());
    }
    let mut out = String::new();
    for (i, name) in tn.names.iter().enumerate() {
        if i != 0 {
            out.push('.');
        }
        let node: &::nodes::nodes::Node = name;
        match node.node_tag() {
            ntag::T_String => out.push_str(node.expect_string().sval.as_str()),
            other => {
                return Err(ereport(ERROR)
                    .errmsg_internal(format!("unrecognized node type: {}", other))
                    .into_error())
            }
        }
    }
    if tn.pct_type {
        out.push_str("%TYPE");
    }
    if !tn.arrayBounds.is_empty() {
        out.push_str("[]");
    }
    Ok(out)
}

/// `NameListToString(names)` (namespace.c) for the `defGetString` `T_List` case.
fn defel_name_list_to_string(names: &[::nodes::nodes::NodePtr<'_>]) -> PgResult<String> {
    let mut out = String::new();
    for (i, name) in names.iter().enumerate() {
        if i != 0 {
            out.push('.');
        }
        let node: &::nodes::nodes::Node = name;
        match node.node_tag() {
            ntag::T_String => out.push_str(node.expect_string().sval.as_str()),
            ntag::T_A_Star => out.push('*'),
            other => {
                return Err(ereport(ERROR)
                    .errmsg_internal(format!("unrecognized node type: {}", other))
                    .into_error())
            }
        }
    }
    Ok(out)
}

/// `newOptions = transformRelOptions((Datum) 0, options, NULL, NULL, false,
/// false); (void) tablespace_reloptions(newOptions, true);` (tablespace.c).
/// Returns the serialized `spcoptions` `text[]` image (`None` ⇒
/// `nulls[spcoptions]`), validating the options as a side effect.
///
/// Inlined create-time `transformRelOptions` (the reloptions owner's
/// `transformRelOptions` takes its own `DefElem` view + `datum` Datum, a
/// different type model than this seam carries — the established codebase
/// pattern, mirrored from tablecmds.c's `transform_and_check_reloptions`, is to
/// flatten the def-list here and validate via the relkind reloptions function).
fn build_create_options<'mcx>(
    mcx: Mcx<'mcx>,
    options: &[DefElem<'mcx>],
) -> PgResult<Option<Vec<u8>>> {
    transform_options_bytes(mcx, None, options, false)
}

// ===========================================================================
// row writers (insert / update / delete)
// ===========================================================================

/// `heap_form_tuple(rel->rd_att, values, nulls)` + `CatalogTupleInsert` +
/// `heap_freetuple`.
fn insert_tablespace_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tablespaceoid: Oid,
    tablespacename: &str,
    owner_id: Oid,
    new_options: Option<Vec<u8>>,
) -> PgResult<()> {
    let mut values: [Datum<'mcx>; NATTS] = core::array::from_fn(|_| Datum::null());
    let mut nulls = [false; NATTS];
    let idx = |attno: i32| (attno - 1) as usize;

    values[idx(ANUM_OID)] = Datum::from_oid(tablespaceoid);
    values[idx(ANUM_SPCNAME)] = name_datum(mcx, tablespacename)?;
    values[idx(ANUM_SPCOWNER)] = Datum::from_oid(owner_id);
    nulls[idx(ANUM_SPCACL)] = true;
    match new_options {
        Some(bytes) => {
            let mut buf: ::mcx::PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, bytes.len())?;
            for b in bytes {
                buf.push(b);
            }
            values[idx(ANUM_SPCOPTIONS)] = Datum::ByRef(buf);
        }
        None => nulls[idx(ANUM_SPCOPTIONS)] = true,
    }

    let mut tup = heap_form_tuple(mcx, &rel.rd_att, &values, &nulls)
        .map_err(|e| PgError::error(format!("heap_form_tuple failed: {e:?}")))?;

    CatalogTupleInsert(mcx, rel, &mut tup)
}

/// `heap_copytuple` + `namestrcpy(&newform->spcname, newname)` +
/// `CatalogTupleUpdate(rel, &newtuple->t_self, newtuple)`.
///
/// The scanned tuple identity is given by `handle`; we re-read the row by its
/// `t_self`, deform it, replace the `spcname` column with the new name, reform,
/// and update.  (The C path holds the live scanned `HeapTuple`; the seam carries
/// only its `t_self`, so we re-fetch the row through a one-shot name-keyed
/// re-scan is unnecessary — the caller passes the identity and we rebuild the
/// row from the existing tuple read back via the rel's tuple descriptor.)
fn update_tablespace_name<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    handle: ItemPointerData,
    newname: &str,
) -> PgResult<()> {
    let tup = fetch_tuple_by_tid(mcx, rel, handle)?;
    let newtuple =
        heap_copytuple(mcx, Some(&tup))?.expect("heap_copytuple of a valid tuple");

    // namestrcpy(&newform->spcname, newname): replace the spcname column.
    let mut repl_val: [Datum<'mcx>; NATTS] = core::array::from_fn(|_| Datum::null());
    let repl_null = [false; NATTS];
    let mut repl_repl = [false; NATTS];
    let idx = |attno: i32| (attno - 1) as usize;
    repl_val[idx(ANUM_SPCNAME)] = name_datum(mcx, newname)?;
    repl_repl[idx(ANUM_SPCNAME)] = true;

    let mut modified = heap_modify_tuple(mcx, &newtuple, &rel.rd_att, &repl_val, &repl_null, &repl_repl)
        .map_err(|e| PgError::error(format!("heap_modify_tuple failed: {e:?}")))?;

    CatalogTupleUpdate(mcx, rel, modified.tuple.t_self, &mut modified)
}

/// `AlterTableSpaceOptions` options-update leg: read the existing `spcoptions`
/// (`heap_getattr`), `transformRelOptions(old, options, …, is_reset)`, validate
/// via `tablespace_reloptions`, `heap_modify_tuple` (replacing `spcoptions`) +
/// `CatalogTupleUpdate`.
fn update_tablespace_options<'mcx, 'a>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'a>,
    handle: ItemPointerData,
    options: &[DefElem<'a>],
    is_reset: bool,
) -> PgResult<()> {
    let tup = fetch_tuple_by_tid(mcx, rel, handle)?;

    // datum = heap_getattr(tup, Anum_pg_tablespace_spcoptions, desc, &isnull);
    let (old_datum, isnull) = heap_getattr(mcx, &tup, ANUM_SPCOPTIONS, &rel.rd_att)?;
    let old_bytes: Option<&[u8]> = if isnull {
        None
    } else {
        Some(old_datum.as_ref_bytes())
    };

    // newOptions = transformRelOptions(old, options, NULL, NULL, false, isReset)
    // + (void) tablespace_reloptions(newOptions, true) (validation inside).
    let new_bytes = transform_options_bytes(mcx, old_bytes, options, is_reset)?;

    // Build new tuple replacing spcoptions.
    let mut repl_val: [Datum<'mcx>; NATTS] = core::array::from_fn(|_| Datum::null());
    let mut repl_null = [false; NATTS];
    let mut repl_repl = [false; NATTS];
    let idx = |attno: i32| (attno - 1) as usize;
    match &new_bytes {
        Some(bytes) => {
            let mut buf: ::mcx::PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, bytes.len())?;
            for &b in bytes.iter() {
                buf.push(b);
            }
            repl_val[idx(ANUM_SPCOPTIONS)] = Datum::ByRef(buf);
        }
        None => repl_null[idx(ANUM_SPCOPTIONS)] = true,
    }
    repl_repl[idx(ANUM_SPCOPTIONS)] = true;

    let mut newtuple = heap_modify_tuple(mcx, &tup, &rel.rd_att, &repl_val, &repl_null, &repl_repl)
        .map_err(|e| PgError::error(format!("heap_modify_tuple failed: {e:?}")))?;

    CatalogTupleUpdate(mcx, rel, newtuple.tuple.t_self, &mut newtuple)
}

/// `CatalogTupleDelete(rel, &tuple->t_self)`.
fn delete_tablespace_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    handle: ItemPointerData,
) -> PgResult<()> {
    CatalogTupleDelete(mcx, rel, handle)
}

// ---------------------------------------------------------------------------

/// `transformRelOptions(old, defList, NULL, NULL, false, isReset)` (inlined; see
/// `build_create_options`) + `(void) tablespace_reloptions(newOptions, true)`.
/// Copies any old options not being replaced/reset, applies/removes the
/// `defList` options, then validates. Returns the serialized `text[]` image
/// (`None` ⇒ `(Datum) 0`).
fn transform_options_bytes<'mcx, 'a>(
    mcx: Mcx<'mcx>,
    old_options: Option<&[u8]>,
    def_list: &[DefElem<'a>],
    is_reset: bool,
) -> PgResult<Option<Vec<u8>>> {
    // Start with the old options as "name=value" strings (those not replaced).
    let mut astate: Vec<String> = Vec::new();

    if let Some(old) = old_options {
        let olds = arrayfuncs_seams::deconstruct_text_array::call(mcx, old)?;
        for opt in olds.iter() {
            let s = opt.as_str();
            // keep an old option only if def_list does not mention its name.
            let kw = s.split('=').next().unwrap_or("");
            let replaced = def_list
                .iter()
                .any(|d| d.defnamespace.is_none() && d.defname.as_deref() == Some(kw));
            if !replaced {
                astate.push(s.to_string());
            }
        }
    }

    // Apply the def_list: on reset, options are only removed (already skipped
    // above); otherwise add "name=value".  If RESET, just check that the user
    // didn't say RESET (option=val) — the grammar doesn't enforce it.
    if is_reset {
        for def in def_list {
            if def.arg.is_some() {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg("RESET must not include values for parameters")
                    .into_error());
            }
        }
    } else {
        for def in def_list {
            // namspace == NULL: a qualified option belongs to another pass.
            if def.defnamespace.is_some() {
                continue;
            }
            let defname = def.defname.as_deref().unwrap_or("");
            // bare "name" means "name=true".
            let value: String = if def.arg.is_some() {
                ::define_seams::def_get_string::call(
                    mcx,
                    defname.to_string(),
                    defel_arg(def)?,
                )?
                .as_str()
                .to_string()
            } else {
                "true".to_string()
            };
            // Insist that name not contain "=", else "a=b=c" is ambiguous.
            if defname.contains('=') {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!(
                        "invalid option name \"{defname}\": must not contain \"=\""
                    ))
                    .into_error());
            }
            astate.push(format!("{defname}={value}"));
        }
    }

    // makeArrayResult / (Datum) 0: no elements ⇒ a NULL spcoptions token. C's
    // `(void) tablespace_reloptions((Datum) 0, true)` validates nothing.
    if astate.is_empty() {
        return Ok(None);
    }

    // Assemble the on-disk text[] varlena image (the C makeArrayResult).
    let elems: Vec<Option<&[u8]>> = astate.iter().map(|s| Some(s.as_bytes())).collect();
    let bytes: Vec<u8> =
        arrayfuncs_seams::build_text_array_nullable::call(mcx, &elems)?
            .iter()
            .copied()
            .collect();

    // (void) tablespace_reloptions(newOptions, true): validate the options.
    let _ = reloptions_seam::tablespace_reloptions::call(&bytes, true)?;

    Ok(Some(bytes))
}

/// Re-read a `pg_tablespace` row by its `t_self` identity: a one-key scan on the
/// oid column would be cheaper, but the scanned identity is a TID, so do a full
/// scan and match `t_self`. (tablespace.c keeps the live scanned `HeapTuple`;
/// the seam carries only its `t_self`, so the owner re-fetches the row here.)
fn fetch_tuple_by_tid<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    handle: ItemPointerData,
) -> PgResult<FormedTuple<'mcx>> {
    let keys: ::mcx::PgVec<'mcx, ScanKeyData<'mcx>> = ::mcx::PgVec::new_in(mcx);
    let mut scan: TableScanDesc<'mcx> = table_beginscan_catalog(mcx, rel, 0, keys)?;
    let mut found: Option<FormedTuple<'mcx>> = None;
    while let Some(tup) = heap_getnext(mcx, &mut scan, ScanDirection::ForwardScanDirection)? {
        if tup.tuple.t_self == handle {
            found = Some(tup.clone_in(mcx)?);
            break;
        }
    }
    table_endscan(scan)?;
    found.ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal("pg_tablespace row vanished during catalog update")
            .into_error()
    })
}

// ===========================================================================
// install
// ===========================================================================

pub fn install() {
    cat::tablespace_table_open::set(tablespace_table_open);
    cat::tablespace_table_close::set(tablespace_table_close);
    // The scan/write seams below carry no `mcx` (their results are owned `Copy`
    // values / `Vec<Oid>` / `()`); the scan descriptor + per-row deform are
    // internal, so each runs in a private throwaway context — the C
    // `CurrentMemoryContext` allocation of the scan state that is freed at
    // `table_endscan` (the returned identity/oids outlive it).
    cat::scan_tablespace_by_name::set(|rel, name| {
        let ctx = MemoryContext::new("scan_tablespace_by_name");
        scan_tablespace_by_name(ctx.mcx(), rel, name)
    });
    cat::scan_all_tablespace_oids::set(|rel| {
        let ctx = MemoryContext::new("scan_all_tablespace_oids");
        scan_all_tablespace_oids(ctx.mcx(), rel)
    });
    cat::scan_tablespace_name_by_oid::set(scan_tablespace_name_by_oid);
    cat::build_create_options::set(build_create_options);
    cat::insert_tablespace_tuple::set(|rel, oid, name, owner, opts| {
        let ctx = MemoryContext::new("insert_tablespace_tuple");
        insert_tablespace_tuple(ctx.mcx(), rel, oid, name, owner, opts)
    });
    cat::update_tablespace_name::set(|rel, handle, newname| {
        let ctx = MemoryContext::new("update_tablespace_name");
        update_tablespace_name(ctx.mcx(), rel, handle, newname)
    });
    cat::update_tablespace_options::set(|rel, handle, options, is_reset| {
        let ctx = MemoryContext::new("update_tablespace_options");
        update_tablespace_options(ctx.mcx(), rel, handle, options, is_reset)
    });
    cat::delete_tablespace_tuple::set(|rel, handle| {
        let ctx = MemoryContext::new("delete_tablespace_tuple");
        delete_tablespace_tuple(ctx.mcx(), rel, handle)
    });
}
