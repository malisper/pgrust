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

pub const GUC_FUNCS_BUILTINS: &[FmgrBuiltin] =
    &[b(2084, "show_all_settings", 0, true, true, fc_show_all_settings)];
