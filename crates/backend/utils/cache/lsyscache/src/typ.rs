use crate::cache_lookup_error;
use crate::IOFuncSelector;
use datum::Datum;
use syscache_seams::PgTypeIoShape;
use types_core::{InvalidOid, Oid, RegProcedure, RECORDOID};
use types_error::{PgError, PgResult, ERRCODE_UNDEFINED_FUNCTION, ERRCODE_UNDEFINED_OBJECT};
use types_tuple::PgTypeShape;
use types_tuple::TYPSTORAGE_PLAIN;

// pg_type.h
pub const TYPTYPE_BASE: i8 = b'b' as i8;
pub const TYPTYPE_COMPOSITE: i8 = b'c' as i8;
pub const TYPTYPE_DOMAIN: i8 = b'd' as i8;
pub const TYPTYPE_ENUM: i8 = b'e' as i8;
pub const TYPTYPE_MULTIRANGE: i8 = b'm' as i8;
pub const TYPTYPE_PSEUDO: i8 = b'p' as i8;
pub const TYPTYPE_RANGE: i8 = b'r' as i8;
// fmgroids.h
pub const F_ARRAY_SUBSCRIPT_HANDLER: Oid = 6179;
// pg_type.dat
pub const BPCHAROID: Oid = 1042;

// TYPALIGN_INT ('i'), get_typalign's C fallback.
const TYPALIGN_INT: i8 = b'i' as i8;

#[cold]
pub(crate) fn type_lookup_failed(typid: Oid) -> Box<PgError> {
    cache_lookup_error(format!("cache lookup failed for type {typid}"))
}

fn type_shape(typid: Oid) -> PgResult<Option<PgTypeShape>> {
    syscache_seams::lookup_pg_type_shape::call(typid)
}

pub fn get_typisdefined(typid: Oid) -> PgResult<bool> {
    Ok(syscache_seams::pg_type_isdefined::call(typid)?.unwrap_or(false))
}

pub fn get_typlen(typid: Oid) -> PgResult<i16> {
    Ok(match type_shape(typid)? {
        Some(typtup) => typtup.typlen,
        None => 0,
    })
}

pub fn get_typbyval(typid: Oid) -> PgResult<bool> {
    Ok(match type_shape(typid)? {
        Some(typtup) => typtup.typbyval,
        None => false,
    })
}

pub fn get_typlenbyval(typid: Oid) -> PgResult<(i16, bool)> {
    match type_shape(typid)? {
        Some(typtup) => Ok((typtup.typlen, typtup.typbyval)),
        None => Err(type_lookup_failed(typid)),
    }
}

pub fn get_typlenbyvalalign(typid: Oid) -> PgResult<(i16, bool, i8)> {
    match type_shape(typid)? {
        Some(typtup) => Ok((typtup.typlen, typtup.typbyval, typtup.typalign)),
        None => Err(type_lookup_failed(typid)),
    }
}

// C takes the pg_type HeapTuple; here the projected IO shape carries the row.
pub fn getTypeIOParam(type_shape: &PgTypeIoShape) -> Oid {
    if type_shape.typelem != InvalidOid {
        type_shape.typelem
    } else {
        type_shape.oid
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TypeIoData {
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: i8,
    pub typdelim: i8,
    pub typioparam: Oid,
    pub func: Oid,
}

pub fn get_type_io_data(typid: Oid, which_func: IOFuncSelector) -> PgResult<TypeIoData> {
    if miscinit_seams::is_bootstrap_processing_mode::call() {
        panic!("get_type_io_data({typid}) in bootstrap mode: boot_get_type_io_data unported (bootstrap.c)");
    }
    let ts = syscache_seams::pg_type_io_shape::call(typid)?.ok_or_else(|| type_lookup_failed(typid))?;
    Ok(TypeIoData {
        typlen: ts.typlen,
        typbyval: ts.typbyval,
        typalign: ts.typalign,
        typdelim: ts.typdelim,
        typioparam: getTypeIOParam(&ts),
        func: match which_func {
            IOFuncSelector::IOFunc_input => ts.typinput,
            IOFuncSelector::IOFunc_output => ts.typoutput,
            IOFuncSelector::IOFunc_receive => ts.typreceive,
            IOFuncSelector::IOFunc_send => ts.typsend,
        },
    })
}

// #ifdef NOT_USED in C.
pub fn get_typalign(typid: Oid) -> PgResult<i8> {
    Ok(match type_shape(typid)? {
        Some(typtup) => typtup.typalign,
        None => TYPALIGN_INT,
    })
}

pub fn get_typstorage(typid: Oid) -> PgResult<i8> {
    Ok(match type_shape(typid)? {
        Some(typtup) => typtup.typstorage,
        None => TYPSTORAGE_PLAIN,
    })
}

pub fn get_typdefault<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    typid: Oid,
) -> PgResult<Option<types_nodes::Node<'mcx>>> {
    let defaults = syscache_seams::pg_type_default_strings::call(mcx, typid)?
        .ok_or_else(|| type_lookup_failed(typid))?;
    if let Some(bin) = defaults.typdefaultbin {
        return Ok(Some(readfuncs::stringToNode(mcx, bin.as_str())?));
    }
    let Some(str_default) = defaults.typdefault else {
        return Ok(None);
    };
    let io = syscache_seams::pg_type_io_shape::call(typid)?
        .ok_or_else(|| type_lookup_failed(typid))?;
    let mut cstr: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, str_default.len() + 1)?;
    mcx::vec_append_bytes(&mut cstr, str_default.as_bytes())?;
    mcx::vec_append_bytes(&mut cstr, &[0u8])?;
    let mut flinfo = fmgr_seams::fmgr_info::call(io.typinput)?;
    let mut fcinfo = types_fmgr::LocalFcinfo::<3>::fresh(InvalidOid);
    // SAFETY: mcx outlives the returned Const, which owns the datum.
    unsafe { fcinfo.set_result_mcx(mcx) };
    fcinfo.set_arg(0, datum::Datum::from_usize(cstr.as_ptr() as usize));
    fcinfo.set_arg(1, datum::Datum::from_oid(getTypeIOParam(&io)));
    fcinfo.set_arg(2, datum::Datum::from_i32(-1));
    let d = flinfo.invoke(&mut fcinfo)?;
    // Pre-convention input wrappers return FmgrInfo-scratch; copy into mcx
    // so the Const outlives the frame (C's makeConst datum already does).
    let d = adt_scalar::datum_copy(mcx, d, io.typbyval, io.typlen)?;
    let coll = get_typcollation(typid)?;
    Ok(Some(types_nodes::Node::mk_const(
        mcx,
        typid,
        -1,
        coll,
        io.typlen as i32,
        d,
        false,
        io.typbyval,
    )?))
}

pub fn getBaseType(typid: Oid) -> PgResult<Oid> {
    let mut typmod = -1;
    getBaseTypeAndTypmod(typid, &mut typmod)
}

pub fn getBaseTypeAndTypmod(mut typid: Oid, typmod: &mut i32) -> PgResult<Oid> {
    loop {
        let typtup = syscache_seams::pg_type_base_shape::call(typid)?
            .ok_or_else(|| type_lookup_failed(typid))?;
        if typtup.typtype != TYPTYPE_DOMAIN {
            return Ok(typid);
        }
        debug_assert!(*typmod == -1);
        typid = typtup.typbasetype;
        *typmod = typtup.typtypmod;
    }
}

pub fn get_typavgwidth(typid: Oid, typmod: i32) -> PgResult<i32> {
    let typlen = get_typlen(typid)?;
    if typlen > 0 {
        return Ok(typlen as i32);
    }
    let maxwidth = format_type::type_maximum_size(typid, typmod);
    if maxwidth > 0 {
        const BPCHAROID: Oid = 1042;
        if typid == BPCHAROID {
            return Ok(maxwidth);
        }
        if maxwidth <= 32 {
            return Ok(maxwidth);
        }
        if maxwidth < 1000 {
            return Ok(32 + (maxwidth - 32) / 2);
        }
        return Ok(32 + (1000 - 32) / 2);
    }
    Ok(32)
}

pub fn get_typtype(typid: Oid) -> PgResult<i8> {
    Ok(syscache_seams::pg_type_typtype::call(typid)?.unwrap_or(0))
}

pub fn type_is_rowtype(typid: Oid) -> PgResult<bool> {
    if typid == RECORDOID {
        return Ok(true);
    }
    match get_typtype(typid)? {
        t if t == TYPTYPE_COMPOSITE => Ok(true),
        t if t == TYPTYPE_DOMAIN => Ok(get_typtype(getBaseType(typid)?)? == TYPTYPE_COMPOSITE),
        _ => Ok(false),
    }
}

pub fn type_is_enum(typid: Oid) -> PgResult<bool> {
    Ok(get_typtype(typid)? == TYPTYPE_ENUM)
}

pub fn type_is_range(typid: Oid) -> PgResult<bool> {
    Ok(get_typtype(typid)? == TYPTYPE_RANGE)
}

pub fn type_is_multirange(typid: Oid) -> PgResult<bool> {
    Ok(get_typtype(typid)? == TYPTYPE_MULTIRANGE)
}

pub fn get_type_category_preferred(typid: Oid) -> PgResult<(i8, bool)> {
    syscache_seams::pg_type_category::call(typid)?.ok_or_else(|| type_lookup_failed(typid))
}

pub fn get_typ_typrelid(typid: Oid) -> PgResult<Oid> {
    Ok(syscache_seams::pg_type_typrelid::call(typid)?.unwrap_or(InvalidOid))
}

// IsTrueArrayType (pg_type.h).
pub fn is_true_array_type(typelem: Oid, typsubscript: Oid) -> bool {
    typelem != InvalidOid && typsubscript == F_ARRAY_SUBSCRIPT_HANDLER
}

pub fn get_element_type(typid: Oid) -> PgResult<Oid> {
    Ok(match syscache_seams::pg_type_element_shape::call(typid)? {
        Some(typtup) if is_true_array_type(typtup.typelem, typtup.typsubscript) => typtup.typelem,
        _ => InvalidOid,
    })
}

pub fn get_array_type(typid: Oid) -> PgResult<Oid> {
    Ok(syscache_seams::pg_type_typarray::call(typid)?.unwrap_or(InvalidOid))
}

pub fn get_promoted_array_type(typid: Oid) -> PgResult<Oid> {
    let array_type = get_array_type(typid)?;
    if array_type != InvalidOid {
        return Ok(array_type);
    }
    if get_element_type(typid)? != InvalidOid {
        return Ok(typid);
    }
    Ok(InvalidOid)
}

pub fn get_base_element_type(mut typid: Oid) -> PgResult<Oid> {
    loop {
        let Some(typtup) = syscache_seams::pg_type_base_shape::call(typid)? else {
            return Ok(InvalidOid);
        };
        if typtup.typtype != TYPTYPE_DOMAIN {
            return Ok(if is_true_array_type(typtup.typelem, typtup.typsubscript) {
                typtup.typelem
            } else {
                InvalidOid
            });
        }
        typid = typtup.typbasetype;
    }
}

#[track_caller]
#[cold]
fn shell_type_error(typid: Oid) -> Box<PgError> {
    // C renders the type name via format_type_be; the format_type crate deps
    // lsyscache, so this error keeps the raw oid (cycle).
    Box::new(
        PgError::error(format!("type {typid} is only a shell"))
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
    )
}

#[track_caller]
#[cold]
fn no_io_function_error(kind: &str, typid: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("no {kind} function available for type {typid}"))
            .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION),
    )
}

pub fn getTypeInputInfo(typid: Oid) -> PgResult<(Oid, Oid)> {
    let pt = syscache_seams::pg_type_io_shape::call(typid)?.ok_or_else(|| type_lookup_failed(typid))?;
    if !pt.typisdefined {
        return Err(shell_type_error(typid));
    }
    if pt.typinput == InvalidOid {
        return Err(no_io_function_error("input", typid));
    }
    Ok((pt.typinput, getTypeIOParam(&pt)))
}

pub fn getTypeOutputInfo(typid: Oid) -> PgResult<(Oid, bool)> {
    let pt = syscache_seams::pg_type_io_shape::call(typid)?.ok_or_else(|| type_lookup_failed(typid))?;
    if !pt.typisdefined {
        return Err(shell_type_error(typid));
    }
    if pt.typoutput == InvalidOid {
        return Err(no_io_function_error("output", typid));
    }
    Ok((pt.typoutput, !pt.typbyval && pt.typlen == -1))
}

pub fn getTypeBinaryInputInfo(typid: Oid) -> PgResult<(Oid, Oid)> {
    let pt = syscache_seams::pg_type_io_shape::call(typid)?.ok_or_else(|| type_lookup_failed(typid))?;
    if !pt.typisdefined {
        return Err(shell_type_error(typid));
    }
    if pt.typreceive == InvalidOid {
        return Err(no_io_function_error("binary input", typid));
    }
    Ok((pt.typreceive, getTypeIOParam(&pt)))
}

pub fn getTypeBinaryOutputInfo(typid: Oid) -> PgResult<(Oid, bool)> {
    let pt = syscache_seams::pg_type_io_shape::call(typid)?.ok_or_else(|| type_lookup_failed(typid))?;
    if !pt.typisdefined {
        return Err(shell_type_error(typid));
    }
    if pt.typsend == InvalidOid {
        return Err(no_io_function_error("binary output", typid));
    }
    Ok((pt.typsend, !pt.typbyval && pt.typlen == -1))
}

pub fn get_typmodin(typid: Oid) -> PgResult<Oid> {
    Ok(match syscache_seams::pg_type_io_shape::call(typid)? {
        Some(typtup) => typtup.typmodin,
        None => InvalidOid,
    })
}

// #ifdef NOT_USED in C.
pub fn get_typmodout(typid: Oid) -> PgResult<Oid> {
    Ok(match syscache_seams::pg_type_io_shape::call(typid)? {
        Some(typtup) => typtup.typmodout,
        None => InvalidOid,
    })
}

pub fn get_typcollation(typid: Oid) -> PgResult<Oid> {
    Ok(match type_shape(typid)? {
        Some(typtup) => typtup.typcollation,
        None => InvalidOid,
    })
}

pub fn type_is_collatable(typid: Oid) -> PgResult<bool> {
    Ok(get_typcollation(typid)? != InvalidOid)
}

pub fn get_typsubscript(typid: Oid) -> PgResult<(RegProcedure, Oid)> {
    Ok(match syscache_seams::pg_type_element_shape::call(typid)? {
        Some(typform) => (typform.typsubscript, typform.typelem),
        None => (InvalidOid, InvalidOid),
    })
}

// C returns the handler's SubscriptRoutines*; subscripting.h is unported, so
// the pointer crosses as the raw Datum from OidFunctionCall0.
pub fn getSubscriptingRoutines(typid: Oid) -> PgResult<Option<(Datum, Oid)>> {
    let (typsubscript, typelem) = get_typsubscript(typid)?;
    if typsubscript == InvalidOid {
        return Ok(None);
    }
    let mut flinfo = fmgr_seams::fmgr_info::call(typsubscript)?;
    let routines = types_fmgr::function_call0_coll(&mut flinfo, InvalidOid)?;
    Ok(Some((routines, typelem)))
}
