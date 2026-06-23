//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! object-introspection functions in `objectaddress.c` whose argument/result
//! types are expressible at the current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word /
//! by-reference payload. [`register_objectaddress_builtins`] registers every row
//! into the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch
//! resolves them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat`.

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};
use types_tuple::Datum as DatumV;

use types_core::Oid;

const OIDOID: Oid = 26;
const INT4OID: Oid = 23;
const TEXTOID: Oid = 25;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_OID(i)` → `DatumGetObjectId`: the low 32 bits of arg `i`'s word.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo
        .arg(i)
        .expect("objectaddress fn: missing arg")
        .value
        .as_oid()
}

/// `PG_GETARG_INT32(i)` → `DatumGetInt32`.
#[inline]
fn arg_i32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo
        .arg(i)
        .expect("objectaddress fn: missing arg")
        .value
        .as_i32()
}

/// Set a `text` result on the by-ref lane (the owner of the `VARHDRSZ` framing
/// is the boundary, so the payload is the bare text bytes). C: `PG_RETURN_TEXT_P`.
#[inline]
fn ret_text(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    // `cstring_to_text`: build a header-ful `text` image (4-byte length word).
    let total = bytes.len() + 4;
    let mut img = Vec::with_capacity(total);
    img.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    img.extend_from_slice(&bytes);
    fcinfo.set_ref_result(RefPayload::Varlena(img));
    Datum::from_usize(0)
}

/// `PG_RETURN_NULL()`.
#[inline]
fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.set_result_null(true);
    Datum::from_usize(0)
}

/// `PG_GETARG_TEXT_PP(i)` → `text_to_cstring`: the `VARDATA_ANY` payload of a
/// `text` arg as a UTF-8 `&str`. The payload must be reached via the size-aware
/// `VARDATA_ANY` (1-byte header for a short value, else `VARHDRSZ`), not a fixed
/// 4-byte strip: under `SHORT_VARLENA_PACKING` a `text` argument (e.g. the
/// `objects.type` column read by `pg_get_object_address`) arrives short-packed,
/// and skipping a fixed `VARHDRSZ` drops 3 payload bytes ("table" -> "le" ->
/// `unrecognized object type`). Behavior-preserving while packing is OFF (the arg
/// is a plain 4-byte varlena). `text` args here are small literals/columns, never
/// compressed/external, so the only header forms are short and 4-byte.
#[inline]
fn arg_text_str<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("objectaddress fn: text arg missing from by-ref lane");
    // VARDATA_ANY: a short (1-byte) header has its low bit set and is not the
    // external sentinel 0x01; otherwise it is a 4-byte (VARHDRSZ) header.
    let off = match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => 1,
        _ => types_datum::varlena::VARHDRSZ,
    };
    let payload = &image[off..];
    std::str::from_utf8(payload).expect("objectaddress fn: text arg not valid UTF-8")
}

/// `PG_GETARG_ARRAYTYPE_P(i)`: the verbatim header-ful on-disk `ArrayType` image
/// of an array (`text[]`) arg, as the array-deconstruction cores consume it
/// (they detoast the header-bearing image themselves).
#[inline]
fn arg_array_image<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("objectaddress fn: array arg missing from by-ref lane")
}

/// Carry a composite-record `Datum` (built by `record_from_values`) onto the
/// fmgr frame's by-reference `Composite` lane, returning the `(Datum) 0`
/// placeholder word.
#[inline]
fn ret_record(fcinfo: &mut FunctionCallInfoBaseData, built: DatumV<'_>) -> Datum {
    match built {
        DatumV::ByRef(bytes) => {
            fcinfo.set_ref_result(RefPayload::Composite(bytes.as_slice().to_vec()));
            Datum::from_usize(0)
        }
        DatumV::Composite(t) => {
            fcinfo.set_ref_result(RefPayload::Composite(t.to_datum_image()));
            Datum::from_usize(0)
        }
        _ => panic!("objectaddress record fmgr: record_from_values produced a non-composite Datum"),
    }
}

/// A scratch context for cores that allocate their result through `Mcx`.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("objectaddress fmgr scratch")
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `pg_describe_object(classid oid, objid oid, objsubid int4) -> text`
/// (objectaddress.c 4220). The C wraps `getObjectDescription`'s cstring into a
/// `text` varlena, or returns NULL for "pinned" pg_depend items.
fn fc_pg_describe_object(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let classid = arg_oid(fcinfo, 0);
    let objid = arg_oid(fcinfo, 1);
    let objsubid = arg_i32(fcinfo, 2);

    let m = scratch_mcx();
    // Lower the borrowed `PgString` to an owned byte image while `m` is still
    // alive, then write to the frame (the `text` framing is the boundary's).
    let bytes: Option<Vec<u8>> =
        match crate::fmgr_sql::pg_describe_object(m.mcx(), classid, objid, objsubid)? {
            Some(s) => Some(s.as_bytes().to_vec()),
            None => None,
        };
    match bytes {
        Some(b) => Ok(ret_text(fcinfo, b)),
        None => Ok(ret_null(fcinfo)),
    }
}

/// `pg_get_object_address(type text, object_names text[], object_args text[])
/// -> record(classid oid, objid oid, objsubid int4)` (objectaddress.c 2109).
/// Resolve the name/args to an `ObjectAddress` and return the 3-column record.
fn fc_pg_get_object_address(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    // Own-copy the args so the immutable arg borrow is released before forming
    // the result onto the mutable frame.
    let type_name = arg_text_str(fcinfo, 0).to_string();
    let name_arr = arg_array_image(fcinfo, 1).to_vec();
    let args_arr = arg_array_image(fcinfo, 2).to_vec();

    let addr =
        crate::fmgr_sql::pg_get_object_address(m.mcx(), &type_name, &name_arr, &args_arr)?;

    // C: values[0]=ObjectIdGetDatum(address.classId);
    //    values[1]=ObjectIdGetDatum(address.objectId);
    //    values[2]=Int32GetDatum(address.objectSubId);
    let coltypes = [OIDOID, OIDOID, INT4OID];
    let values = [
        DatumV::from_oid(addr.classId),
        DatumV::from_oid(addr.objectId),
        DatumV::from_i32(addr.objectSubId),
    ];
    let nulls = [false, false, false];
    let rec = backend_utils_fmgr_funcapi_seams::record_from_values::call(
        m.mcx(),
        &coltypes,
        &values,
        &nulls,
    )?;
    Ok(ret_record(fcinfo, rec))
}

/// Build a `text` value `DatumV` (header-ful varlena image) from an optional
/// string column, or a NULL column. Returns `(value, isnull)`.
fn text_col<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    s: Option<&str>,
) -> types_error::PgResult<(DatumV<'mcx>, bool)> {
    match s {
        None => Ok((DatumV::null(), true)),
        Some(s) => {
            let total = s.len() + 4;
            let mut img = Vec::with_capacity(total);
            img.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
            img.extend_from_slice(s.as_bytes());
            Ok((DatumV::from_byref_bytes_in(mcx, &img)?, false))
        }
    }
}

/// `pg_identify_object(classid oid, objid oid, objsubid int4)
/// -> record(type text, schema text, name text, identity text)`
/// (objectaddress.c 4248).
fn fc_pg_identify_object(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let classid = arg_oid(fcinfo, 0);
    let objid = arg_oid(fcinfo, 1);
    let objsubid = arg_i32(fcinfo, 2);

    let m = scratch_mcx();
    let row = crate::fmgr_sql::pg_identify_object(m.mcx(), classid, objid, objsubid)?;

    let coltypes = [TEXTOID, TEXTOID, TEXTOID, TEXTOID];
    let (c0, n0) = text_col(m.mcx(), row.type_.as_deref())?;
    let (c1, n1) = text_col(m.mcx(), row.schema.as_deref())?;
    let (c2, n2) = text_col(m.mcx(), row.name.as_deref())?;
    let (c3, n3) = text_col(m.mcx(), row.identity.as_deref())?;
    let values = [c0, c1, c2, c3];
    let nulls = [n0, n1, n2, n3];
    let rec = backend_utils_fmgr_funcapi_seams::record_from_values::call(
        m.mcx(),
        &coltypes,
        &values,
        &nulls,
    )?;
    Ok(ret_record(fcinfo, rec))
}

/// `pg_identify_object_as_address(classid oid, objid oid, objsubid int4)
/// -> record(type text, object_names text[], object_args text[])`
/// (objectaddress.c 4365).
fn fc_pg_identify_object_as_address(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    /// `_text` (text[]) element-type-array OID.
    const TEXTARRAYOID: Oid = 1009;

    let classid = arg_oid(fcinfo, 0);
    let objid = arg_oid(fcinfo, 1);
    let objsubid = arg_i32(fcinfo, 2);

    let m = scratch_mcx();
    let row =
        crate::fmgr_sql::pg_identify_object_as_address(m.mcx(), classid, objid, objsubid)?;

    // type (text, never NULL)
    let (c0, n0) = text_col(m.mcx(), row.type_.as_deref())?;
    // object_names / object_args (text[]; strlist_to_textarray, or empty array;
    // SQL NULL when `getObjectIdentityParts` returned NULL).
    let build_array = |elems: &Option<Vec<Option<String>>>| -> types_error::PgResult<(DatumV, bool)> {
        match elems {
            None => Ok((DatumV::null(), true)),
            Some(v) => {
                let e: Vec<Option<&[u8]>> =
                    v.iter().map(|o| o.as_deref().map(|s| s.as_bytes())).collect();
                Ok((
                    DatumV::ByRef(
                        backend_utils_adt_arrayfuncs_seams::build_text_array_nullable::call(
                            m.mcx(),
                            &e,
                        )?,
                    ),
                    false,
                ))
            }
        }
    };
    let (c1, n1) = build_array(&row.object_names)?;
    let (c2, n2) = build_array(&row.object_args)?;

    let coltypes = [TEXTOID, TEXTARRAYOID, TEXTARRAYOID];
    let values = [c0, c1, c2];
    let nulls = [n0, n1, n2];
    let rec = backend_utils_fmgr_funcapi_seams::record_from_values::call(
        m.mcx(),
        &coltypes,
        &values,
        &nulls,
    )?;
    Ok(ret_record(fcinfo, rec))
}

/// `pg_get_acl(classid oid, objid oid, objsubid int4) -> aclitem[]`
/// (objectaddress.c). Returns the object's ACL array, or NULL.
fn fc_pg_get_acl(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let classid = arg_oid(fcinfo, 0);
    let objid = arg_oid(fcinfo, 1);
    let objsubid = arg_i32(fcinfo, 2);

    let m = scratch_mcx();
    // Materialize the array image bytes out of the `m` borrow before re-borrowing
    // `fcinfo` for the result write.
    let bytes: Option<Vec<u8>> =
        match crate::fmgr_sql::pg_get_acl(m.mcx(), classid, objid, objsubid)? {
            None => None,
            // aclitem[] is a by-reference array image; carry it verbatim.
            Some(DatumV::ByRef(b)) => Some(b.as_slice().to_vec()),
            Some(other) => {
                panic!("pg_get_acl: expected ByRef aclitem[] Datum, got {other:?}")
            }
        };
    match bytes {
        None => Ok(ret_null(fcinfo)),
        Some(b) => {
            fcinfo.set_ref_result(RefPayload::Varlena(b));
            Ok(Datum::from_usize(0))
        }
    }
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    strict: bool,
    retset: bool,
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            strict,
            retset,
            func: None,
        },
        native,
    )
}

/// Register every SQL-callable `objectaddress.c` builtin whose boundary types
/// are expressible (C: their `fmgr_builtins[]` rows). Called from this crate's
/// `init_seams()`. OIDs/nargs/strict/retset transcribed exactly from
/// `pg_proc.dat`.
pub fn register_objectaddress_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        // pg_describe_object: oid '3537', proargtypes 'oid oid int4',
        // prorettype 'text'; no proisstrict (=> strict false), no proretset.
        builtin(
            3537,
            "pg_describe_object",
            3,
            true,
            false,
            fc_pg_describe_object,
        ),
        // pg_get_object_address: oid '3954', proargtypes 'text _text _text',
        // prorettype 'record'; provolatile 's', proisstrict default 't', not
        // retset (returns one record, not a set).
        builtin(
            3954,
            "pg_get_object_address",
            3,
            true,
            false,
            fc_pg_get_object_address,
        ),
        // pg_identify_object: oid '3839', proargtypes 'oid oid int4',
        // prorettype 'record'; provolatile 's', proisstrict default 't', not retset.
        builtin(
            3839,
            "pg_identify_object",
            3,
            true,
            false,
            fc_pg_identify_object,
        ),
        // pg_identify_object_as_address: oid '3382', proargtypes 'oid oid int4',
        // prorettype 'record'; provolatile 's', proisstrict default 't', not retset.
        builtin(
            3382,
            "pg_identify_object_as_address",
            3,
            true,
            false,
            fc_pg_identify_object_as_address,
        ),
        // pg_get_acl: oid '6385', proargtypes 'oid oid int4',
        // prorettype '_aclitem'; provolatile 's', proisstrict default 't', not retset.
        builtin(6385, "pg_get_acl", 3, true, false, fc_pg_get_acl),
    ]);
}
