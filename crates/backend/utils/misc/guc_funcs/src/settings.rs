use arrayfuncs::construct::construct_array;
use datum::Datum;
use guc::enum_lookup::config_enum_lookup_by_value;
use guc::model::GUC_PENDING_RESTART;
use guc::registry::GucVariable;
use guc::units::{fmt_g, get_config_unit_name};
use guc_tables::{config_group_names, config_type_names, GucContext_Names, GucSource_Names};
use mcx::Mcx;
use types_core::TEXTOID;
use types_error::PgResult;
use types_fmgr::{
    varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};
use types_guc::{GUC_NO_SHOW_ALL, PGC_S_FILE};

use crate::{ConfigOptionIsVisible, ShowGUCOption, ROLE_PG_READ_ALL_SETTINGS};

const NUM_PG_SETTINGS_ATTS: usize = 17;

fn text_datum(mcx: Mcx<'_>, s: &str) -> PgResult<Datum> {
    Ok(varlena_result(varlena::cstring_to_text(mcx, s.as_bytes())?))
}

fn opt_text_datum(
    mcx: Mcx<'_>,
    s: Option<&str>,
    values: &mut [Datum],
    nulls: &mut [bool],
    i: usize,
) -> PgResult<()> {
    match s {
        Some(s) => values[i] = text_datum(mcx, s)?,
        None => nulls[i] = true,
    }
    Ok(())
}

pub fn fc_show_all_settings(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("show_all_settings: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    debug_assert_eq!(srf.tupdesc.natts as usize, NUM_PG_SETTINGS_ATTS);

    guc::store::with_store(|reg| -> PgResult<()> {
        // C's get_guc_variables array is kept sorted by guc_name_compare.
        let mut sorted: Vec<&GucVariable> = reg.iter().collect();
        sorted.sort_by(|a, b| guc::guc_name_compare(a.gen().name, b.gen().name));
        for conf in sorted {
            let gen = conf.gen();
            if gen.flags & GUC_NO_SHOW_ALL != 0 || !ConfigOptionIsVisible(conf)? {
                continue;
            }

            let mut values = [Datum::null(); NUM_PG_SETTINGS_ATTS];
            let mut nulls = [false; NUM_PG_SETTINGS_ATTS];

            values[0] = text_datum(mcx, gen.name)?;
            values[1] = text_datum(mcx, &ShowGUCOption(conf, false))?;
            opt_text_datum(mcx, get_config_unit_name(gen.flags), &mut values, &mut nulls, 2)?;
            values[3] = text_datum(mcx, config_group_names[gen.group as usize])?;
            opt_text_datum(mcx, gen.short_desc, &mut values, &mut nulls, 4)?;
            opt_text_datum(mcx, gen.long_desc, &mut values, &mut nulls, 5)?;
            values[6] = text_datum(mcx, GucContext_Names[gen.context as usize])?;
            values[7] = text_datum(mcx, config_type_names[gen.vartype as usize])?;
            values[8] = text_datum(mcx, GucSource_Names[gen.source as usize])?;

            let mut enum_arr = None;
            match conf {
                GucVariable::Bool(c) => {
                    nulls[9] = true;
                    nulls[10] = true;
                    nulls[11] = true;
                    values[12] = text_datum(mcx, if c.boot_val { "on" } else { "off" })?;
                    values[13] = text_datum(mcx, if c.reset_val { "on" } else { "off" })?;
                }
                GucVariable::Int(c) => {
                    values[9] = text_datum(mcx, &c.min.to_string())?;
                    values[10] = text_datum(mcx, &c.max.to_string())?;
                    nulls[11] = true;
                    values[12] = text_datum(mcx, &c.boot_val.to_string())?;
                    values[13] = text_datum(mcx, &c.reset_val.to_string())?;
                }
                GucVariable::Real(c) => {
                    values[9] = text_datum(mcx, &fmt_g(c.min))?;
                    values[10] = text_datum(mcx, &fmt_g(c.max))?;
                    nulls[11] = true;
                    values[12] = text_datum(mcx, &fmt_g(c.boot_val))?;
                    values[13] = text_datum(mcx, &fmt_g(c.reset_val))?;
                }
                GucVariable::String(c) => {
                    nulls[9] = true;
                    nulls[10] = true;
                    nulls[11] = true;
                    opt_text_datum(mcx, c.boot_val.as_deref(), &mut values, &mut nulls, 12)?;
                    opt_text_datum(mcx, c.reset_val.as_deref(), &mut values, &mut nulls, 13)?;
                }
                GucVariable::Enum(c) => {
                    nulls[9] = true;
                    nulls[10] = true;
                    let mut names: Vec<&str> =
                        c.entries().iter().filter(|e| !e.hidden).map(|e| e.name).collect();
                    // C's config_enum_get_options("{\"", "\"}", "\",\"") yields
                    // {""} (one empty element) when every entry is hidden.
                    if names.is_empty() {
                        names.push("");
                    }
                    let mut elems = Vec::with_capacity(names.len());
                    for n in &names {
                        elems.push(text_datum(mcx, n)?);
                    }
                    let arr = construct_array(mcx, &elems, TEXTOID, -1, false, b'i')?;
                    values[11] = Datum::from_usize(arr.as_ptr() as usize);
                    enum_arr = Some(arr);
                    values[12] = text_datum(
                        mcx,
                        config_enum_lookup_by_value(c, c.boot_val)
                            .expect("could not find enum option for boot_val"),
                    )?;
                    values[13] = text_datum(
                        mcx,
                        config_enum_lookup_by_value(c, c.reset_val)
                            .expect("could not find enum option for reset_val"),
                    )?;
                }
            }

            if gen.source == PGC_S_FILE
                && adt_acl::has_privs_of_role(miscinit::GetUserId(), ROLE_PG_READ_ALL_SETTINGS)?
            {
                opt_text_datum(mcx, gen.sourcefile.as_deref(), &mut values, &mut nulls, 14)?;
                values[15] = Datum::from_i32(gen.sourceline);
            } else {
                nulls[14] = true;
                nulls[15] = true;
            }

            values[16] = Datum::from_bool(gen.status & GUC_PENDING_RESTART != 0);

            srf.putvalues(&values, &nulls)?;
            drop(enum_arr);
        }
        Ok(())
    })
    .expect("GUC store not initialized")?;

    Ok(srf.finish(fcinfo))
}

// C: show_all_file_settings — re-scans the config files (apply_settings
// false) and reports every parsed item, ignored duplicates included.
pub fn fc_show_all_file_settings(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    const NUM_PG_FILE_SETTINGS_ATTS: usize = 7;

    let flinfo = flinfo.expect("show_all_file_settings: resolved FmgrInfo required");
    let (_, conf) = guc::process_config::process_config_file_internal_list(
        types_guc::PGC_SIGHUP,
        false,
        types_error::DEBUG3,
    )?;

    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    for (seqno, item) in conf.iter().enumerate() {
        let mut values = [Datum::null(); NUM_PG_FILE_SETTINGS_ATTS];
        let mut nulls = [false; NUM_PG_FILE_SETTINGS_ATTS];

        // sourceline is not meaningful without a sourcefile.
        match &item.filename {
            Some(f) => {
                values[0] = text_datum(mcx, &f.to_string_lossy())?;
                values[1] = Datum::from_i32(item.sourceline);
            }
            None => {
                nulls[0] = true;
                nulls[1] = true;
            }
        }
        values[2] = Datum::from_i32(seqno as i32 + 1);
        opt_text_datum(mcx, item.name.as_deref(), &mut values, &mut nulls, 3)?;
        opt_text_datum(mcx, item.value.as_deref(), &mut values, &mut nulls, 4)?;
        values[5] = Datum::from_bool(item.applied);
        opt_text_datum(mcx, item.errmsg.as_deref(), &mut values, &mut nulls, 6)?;

        srf.putvalues(&values, &nulls)?;
    }

    Ok(srf.finish(fcinfo))
}

fn config_by_name(fcinfo: &mut Fcinfo, missing_ok: bool) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 0 is a non-null text varlena.
    let name = unsafe { fcinfo.arg_varlena_packed(0)? };
    let name = String::from_utf8_lossy(name.data()).into_owned();
    let varval = guc::store::with_store(|reg| {
        guc::registry::get_config_option_by_name(reg, &name, missing_ok)
    })
    .expect("GUC store not initialized")?;
    let mcx = fcinfo.result_mcx();
    match varval {
        Some(v) => text_datum(mcx, &v),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_show_config_by_name(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    config_by_name(fcinfo, false)
}

pub fn fc_show_config_by_name_missing_ok(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let missing_ok = fcinfo.args_n::<2>()[1].value.as_bool();
    config_by_name(fcinfo, missing_ok)
}

pub fn fc_set_config_by_name(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    use guc::{GucAction, GUC_ACTION_LOCAL, GUC_ACTION_SET};
    let [a, b, c] = *fcinfo.args_n::<3>();
    if a.isnull {
        return Err(Box::new(
            types_error::PgError::error("SET requires parameter name")
                .with_sqlstate(types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED),
        ));
    }
    // SAFETY: nullness checked; non-null text args are live varlenas.
    let name = unsafe { fcinfo.arg_varlena_packed(0)? };
    let name = String::from_utf8_lossy(name.data()).into_owned();
    let value = if b.isnull {
        None
    } else {
        // SAFETY: as above.
        let v = unsafe { fcinfo.arg_varlena_packed(1)? };
        Some(String::from_utf8_lossy(v.data()).into_owned())
    };
    let is_local = !c.isnull && c.value.as_bool();
    let action: GucAction = if is_local { GUC_ACTION_LOCAL } else { GUC_ACTION_SET };
    guc::set_config_option(
        &name,
        value.as_deref(),
        crate::suset_or_userset()?,
        types_guc::PGC_S_SESSION,
        action,
        true,
        types_error::ErrorLevel(0),
        false,
    )?;
    let new_value = guc::store::with_store(|reg| {
        guc::registry::get_config_option_by_name(reg, &name, false)
    })
    .expect("GUC store not initialized")?
    .expect("missing_ok=false returned None");
    text_datum(fcinfo.result_mcx(), &new_value)
}

// C: pg_settings_get_flags — NULL for an unknown GUC, else the subset of
// MAX_GUC_FLAGS names whose bit is set on the GUC's `flags`.
pub fn fc_pg_settings_get_flags(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 0 is a non-null text varlena.
    let varname = unsafe { fcinfo.arg_varlena_packed(0)? };
    let varname = String::from_utf8_lossy(varname.data()).into_owned();
    let flags = guc::store::with_store(|reg| reg.find_option(&varname).map(|c| c.gen().flags))
        .expect("GUC store not initialized");
    let Some(flags) = flags else {
        return Ok(fcinfo.return_null());
    };

    const FLAG_NAMES: &[(i32, &str)] = &[
        (types_guc::GUC_EXPLAIN, "EXPLAIN"),
        (types_guc::GUC_NO_RESET, "NO_RESET"),
        (types_guc::GUC_NO_RESET_ALL, "NO_RESET_ALL"),
        (GUC_NO_SHOW_ALL, "NO_SHOW_ALL"),
        (types_guc::GUC_NOT_IN_SAMPLE, "NOT_IN_SAMPLE"),
        (types_guc::GUC_RUNTIME_COMPUTED, "RUNTIME_COMPUTED"),
    ];

    let mcx = fcinfo.result_mcx();
    let mut astate = None;
    let mut scratch: Vec<u8> = Vec::new();
    for (bit, name) in FLAG_NAMES {
        if flags & bit == 0 {
            continue;
        }
        scratch.clear();
        scratch.extend_from_slice(&datum::varlena::set_varsize_4b(4 + name.len()));
        scratch.extend_from_slice(name.as_bytes());
        let d = Datum::from_usize(scratch.as_ptr() as usize);
        astate = Some(::arrayfuncs::accum_array_result(mcx, astate.take(), d, false, TEXTOID)?);
    }
    let img = match &astate {
        None => ::arrayfuncs::construct_empty_array(mcx, TEXTOID)?,
        Some(st) => ::arrayfuncs::make_array_result(mcx, st)?,
    };
    types_fmgr::byref_result(mcx, &img)
}

const fn b(
    foid: types_core::Oid,
    name: &'static str,
    nargs: i16,
    strict: bool,
    retset: bool,
    func: PGFunction,
) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict, retset, func }
}

pub const GUC_FUNCS_BUILTINS: &[FmgrBuiltin] = &[
    b(2084, "show_all_settings", 0, true, true, fc_show_all_settings),
    b(3329, "show_all_file_settings", 0, true, true, fc_show_all_file_settings),
    b(2077, "show_config_by_name", 1, true, false, fc_show_config_by_name),
    b(3294, "show_config_by_name_missing_ok", 2, true, false, fc_show_config_by_name_missing_ok),
    b(2078, "set_config_by_name", 3, false, false, fc_set_config_by_name),
    b(6240, "pg_settings_get_flags", 1, true, false, fc_pg_settings_get_flags),
];
