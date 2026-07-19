use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::{InvalidOid, Oid, FOREIGN_SERVER_RELATION_ID, USER_MAPPING_RELATION_ID};
use types_error::{
    PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_SYNTAX_ERROR,
    ERRCODE_UNDEFINED_OBJECT, ERROR,
};
use types_nodes::parsenodes::{DefElem, DefElemAction};

const TEXTOID: Oid = 25;

pub struct OptionPair<'mcx> {
    pub name: &'mcx str,
    // C untransformRelOptions builds a DefElem with a NULL arg for a text
    // element without '=' (reachable via a direct postgresql_fdw_validator
    // call on an arbitrary text[]); None mirrors that NULL.
    pub value: Option<&'mcx str>,
}

impl<'mcx> OptionPair<'mcx> {
    /// C defGetString (define.c) over the untransformed pair: a DefElem with
    /// a NULL arg raises `%s requires a parameter`.
    pub fn require_value(&self) -> PgResult<&'mcx str> {
        match self.value {
            Some(v) => Ok(v),
            None => Err(::elog::ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("{} requires a parameter", self.name))
                .into_error()
                .into()),
        }
    }
}

/// Borrow the flat varlena image behind a text[]/text datum. Catalog option
/// arrays never toast in this port; a toasted image is loud, not misread.
pub(crate) fn varlena_image<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: caller passes a datum into a held catalog tuple or an mcx image.
    unsafe {
        if !types_tuple::varatt::varatt_is_4b_u(p) && !types_tuple::varatt::varatt_is_1b(p) {
            panic!("unported: foreigncmds toasted/compressed options varlena");
        }
        core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p))
    }
}

pub(crate) fn text_body(image: &[u8]) -> &[u8] {
    let p = image.as_ptr();
    // SAFETY: image spans the whole varlena (varlena_image contract).
    unsafe {
        if types_tuple::varatt::varatt_is_1b(p) {
            &image[types_tuple::varatt::VARHDRSZ_SHORT..]
        } else {
            &image[types_tuple::varatt::VARHDRSZ..]
        }
    }
}

/// untransformRelOptions (reloptions.c) over a text[] datum.
pub fn untransform_options<'mcx>(
    mcx: Mcx<'mcx>,
    options: Option<Datum>,
) -> PgResult<PgVec<'mcx, OptionPair<'mcx>>> {
    let mut result: PgVec<'mcx, OptionPair<'mcx>> = PgVec::new_in(mcx);
    let Some(d) = options else {
        return Ok(result);
    };
    // pg_detoast_datum: tuple-stored arrays come back short-headered.
    let image = detoast::detoast_attr(mcx, varlena_image(d))?;
    let (elems, nulls) =
        arrayfuncs::construct::deconstruct_array_builtin(mcx, &image, TEXTOID, false)?;
    debug_assert!(!nulls.iter().any(|&n| n));
    for elem in elems.iter() {
        let body = text_body(varlena_image(*elem));
        let s = mcx::slice_borrow_in(mcx, body)?;
        // SAFETY: catalog text in server encoding; written by this module.
        let s = unsafe { core::str::from_utf8_unchecked(s) };
        // C (reloptions.c untransformRelOptions): text without '=' becomes a
        // DefElem with the whole string as name and a NULL value.
        match s.split_once('=') {
            Some((name, value)) => result.push(OptionPair { name, value: Some(value) }),
            None => result.push(OptionPair { name: s, value: None }),
        }
    }
    Ok(result)
}

// pg_options_to_table (foreign.c): expand a text[] of "name=value" options
// into (option_name, option_value) rows; a record without '=' yields a null
// value (untransformRelOptions' NULL-arg DefElem).
pub fn pg_options_to_table(
    flinfo: Option<&mut types_fmgr::FmgrInfo>,
    fcinfo: &mut types_fmgr::FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_options_to_table: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let array = fcinfo.arg(0);
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, funcapi::MAT_SRF_BLESS)?;

    let image = detoast::detoast_attr(mcx, varlena_image(array))?;
    let (elems, elem_nulls) =
        arrayfuncs::construct::deconstruct_array_builtin(mcx, &image, TEXTOID, false)?;
    debug_assert!(!elem_nulls.iter().any(|&n| n));
    for elem in elems.iter() {
        let body = text_body(varlena_image(*elem));
        let mut values = [Datum::null(); 2];
        let mut nulls = [false; 2];
        match body.iter().position(|&b| b == b'=') {
            Some(pos) => {
                values[0] = text_datum(mcx, &body[..pos])?;
                values[1] = text_datum(mcx, &body[pos + 1..])?;
            }
            None => {
                values[0] = text_datum(mcx, body)?;
                nulls[1] = true;
            }
        }
        srf.putvalues(&values, &nulls)?;
    }
    Ok(srf.finish(fcinfo))
}

fn text_datum(mcx: Mcx<'_>, body: &[u8]) -> PgResult<Datum> {
    let total = types_tuple::varatt::VARHDRSZ + body.len();
    let mut buf: PgVec<'_, u8> = mcx::vec_with_capacity_in(mcx, total)?;
    mcx::vec_append_bytes(&mut buf, &datum::varlena::set_varsize_4b(total))?;
    mcx::vec_append_bytes(&mut buf, body)?;
    // Leaked: the datum is materialized into the SRF tuplestore by putvalues.
    Ok(Datum::from_usize(buf.leak().as_ptr() as usize))
}

/// optionListToArray (foreigncmds.c): text[] image, or None for an empty list.
fn option_list_to_array<'mcx>(
    mcx: Mcx<'mcx>,
    options: &[OptionPair<'mcx>],
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    if options.is_empty() {
        return Ok(None);
    }
    let mut elems: PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(mcx, options.len())?;
    for opt in options {
        if opt.name.contains('=') {
            return Err(::elog::ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "invalid option name \"{}\": must not contain \"=\"",
                    opt.name
                ))
                .into_error()
                .into());
        }
        // C's optionListToArray reads the value via defGetString (define.c).
        let value = opt.require_value()?;
        let total = 4 + opt.name.len() + 1 + value.len();
        let mut buf: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, total)?;
        mcx::vec_append_bytes(&mut buf, &datum::varlena::set_varsize_4b(total))?;
        mcx::vec_append_bytes(&mut buf, opt.name.as_bytes())?;
        mcx::vec_append_bytes(&mut buf, b"=")?;
        mcx::vec_append_bytes(&mut buf, value.as_bytes())?;
        // Leaked: the element must outlive this loop for construct_array.
        elems.push(Datum::from_usize(buf.leak().as_ptr() as usize));
    }
    let image = arrayfuncs::construct::construct_array(mcx, &elems, TEXTOID, -1, false, b'i')?;
    Ok(Some(image))
}

/// transformGenericOptions (foreigncmds.c).
pub fn transformGenericOptions<'mcx>(
    mcx: Mcx<'mcx>,
    catalog_id: Oid,
    old_options: Option<Datum>,
    options: &types_nodes::list::NodeList<'mcx>,
    fdwvalidator: Oid,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let mut result_options = untransform_options(mcx, old_options)?;

    for optnode in options.iter() {
        let od = optnode.as_variant::<DefElem>().expect("DefElem");
        let odname = od.defname.unwrap_or("");
        let pos = result_options.iter().position(|p| p.name == odname);

        match od.defaction {
            DefElemAction::DEFELEM_DROP => {
                let Some(i) = pos else {
                    return Err(option_not_found(odname));
                };
                result_options.remove(i);
            }
            DefElemAction::DEFELEM_SET => {
                let Some(i) = pos else {
                    return Err(option_not_found(odname));
                };
                result_options[i].value = Some(commands_define::defGetString(mcx, od)?);
            }
            DefElemAction::DEFELEM_ADD | DefElemAction::DEFELEM_UNSPEC => {
                if pos.is_some() {
                    return Err(::elog::ereport(ERROR)
                        .errcode(ERRCODE_DUPLICATE_OBJECT)
                        .errmsg(format!("option \"{odname}\" provided more than once"))
                        .into_error()
                        .into());
                }
                let name = mcx::slice_borrow_in(mcx, odname.as_bytes())?;
                // SAFETY: bytes copied verbatim from a &str.
                let name = unsafe { core::str::from_utf8_unchecked(name) };
                result_options.push(OptionPair {
                    name,
                    value: Some(commands_define::defGetString(mcx, od)?),
                });
            }
        }
    }

    let result = option_list_to_array(mcx, &result_options)?;

    if fdwvalidator != InvalidOid {
        call_validator(mcx, fdwvalidator, result.as_deref(), catalog_id)?;
    }

    Ok(result)
}

#[cold]
fn option_not_found(name: &str) -> Box<types_error::PgError> {
    ::elog::ereport(ERROR)
        .errcode(ERRCODE_UNDEFINED_OBJECT)
        .errmsg(format!("option \"{name}\" not found"))
        .into_error()
        .into()
}

/// C passes a null options list as an empty array so validators need not be
/// non-strict.
fn call_validator<'mcx>(
    mcx: Mcx<'mcx>,
    fdwvalidator: Oid,
    options: Option<&[u8]>,
    catalog_id: Oid,
) -> PgResult<()> {
    let empty;
    let image = match options {
        Some(image) => image,
        None => {
            empty = arrayfuncs::construct::construct_empty_array(mcx, TEXTOID)?;
            &empty
        }
    };
    let mut flinfo = fmgr_core::fmgr_info(fdwvalidator)?;
    types_fmgr::function_call2_coll_in(
        &mut flinfo,
        InvalidOid,
        mcx,
        Datum::from_usize(image.as_ptr() as usize),
        Datum::from_oid(catalog_id),
    )?;
    Ok(())
}

// libpq_conninfo_options (foreign.c); the deprecated testing validator's set.
const LIBPQ_CONNINFO_OPTIONS: &[(&str, Oid)] = &[
    ("authtype", FOREIGN_SERVER_RELATION_ID),
    ("service", FOREIGN_SERVER_RELATION_ID),
    ("user", USER_MAPPING_RELATION_ID),
    ("password", USER_MAPPING_RELATION_ID),
    ("connect_timeout", FOREIGN_SERVER_RELATION_ID),
    ("dbname", FOREIGN_SERVER_RELATION_ID),
    ("host", FOREIGN_SERVER_RELATION_ID),
    ("hostaddr", FOREIGN_SERVER_RELATION_ID),
    ("port", FOREIGN_SERVER_RELATION_ID),
    ("tty", FOREIGN_SERVER_RELATION_ID),
    ("options", FOREIGN_SERVER_RELATION_ID),
    ("requiressl", FOREIGN_SERVER_RELATION_ID),
    ("sslmode", FOREIGN_SERVER_RELATION_ID),
    ("gsslib", FOREIGN_SERVER_RELATION_ID),
    ("gssdelegation", FOREIGN_SERVER_RELATION_ID),
];

const MAX_LEVENSHTEIN_STRLEN: usize = 255;

/// postgresql_fdw_validator (foreign.c).
pub fn postgresql_fdw_validator<'mcx>(
    mcx: Mcx<'mcx>,
    options_datum: Datum,
    catalog: Oid,
) -> PgResult<bool> {
    let options = untransform_options(mcx, Some(options_datum))?;
    for opt in options.iter() {
        if LIBPQ_CONNINFO_OPTIONS.iter().any(|&(n, ctx)| ctx == catalog && n == opt.name) {
            continue;
        }
        // ClosestMatchState with max_d = 4 (varlena.c).
        let mut min_d = -1i32;
        let mut closest: Option<&str> = None;
        let mut has_valid_options = false;
        for &(n, ctx) in LIBPQ_CONNINFO_OPTIONS {
            if ctx != catalog {
                continue;
            }
            has_valid_options = true;
            if opt.name.is_empty()
                || opt.name.len() > MAX_LEVENSHTEIN_STRLEN
                || n.len() > MAX_LEVENSHTEIN_STRLEN
            {
                continue;
            }
            let dist = varlena::levenshtein::varstr_levenshtein_less_equal(
                mcx,
                opt.name.as_bytes(),
                n.as_bytes(),
                1,
                1,
                1,
                4,
                true,
            )?;
            if dist <= 4 && dist as usize <= opt.name.len() / 2 && (min_d == -1 || dist < min_d) {
                min_d = dist;
                closest = Some(n);
            }
        }
        let mut err = ::elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!("invalid option \"{}\"", opt.name));
        err = if has_valid_options {
            match closest {
                Some(m) => err.errhint(format!("Perhaps you meant the option \"{m}\".")),
                None => err,
            }
        } else {
            err.errhint("There are no valid options in this context.".to_string())
        };
        return Err(err.into_error().into());
    }
    Ok(true)
}
