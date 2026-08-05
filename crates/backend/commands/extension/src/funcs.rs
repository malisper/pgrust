// SQL-callable extension SRFs (extension.c:2334-2790). pg_extension_config_dump
// and pg_get_loaded_modules stay loud in fmgr's unported set.
use datum::Datum;
use mcx::Mcx;
use types_core::NAMEOID;
use types_error::PgResult;
use types_fmgr::{varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use types_tuple::NameData;

use crate::control::{for_each_primary_control_name, parse_control_in_dir};
use crate::graph::{find_install_path, get_ext_ver_list, EviData};
use crate::{check_valid_extension_name, read_extension_control_file, ExtensionControlFile};

fn text_datum(mcx: Mcx<'_>, s: &str) -> PgResult<Datum> {
    Ok(varlena_result(varlena::cstring_to_text(mcx, s.as_bytes())?))
}

fn name_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum> {
    let mut name = NameData::default();
    name.namestrcpy(s);
    let stable = mcx::slice_borrow_in(mcx, &name.data)?;
    Ok(Datum::from_usize(stable.as_ptr() as usize))
}

// convert_requires_to_datum (extension.c:2681).
fn convert_requires_to_datum(mcx: Mcx<'_>, requires: &[String]) -> PgResult<Datum> {
    let mut datums: Vec<Datum> = Vec::with_capacity(requires.len());
    for r in requires {
        datums.push(name_datum(mcx, r)?);
    }
    const NAMEDATALEN: i32 = 64;
    let arr = arrayfuncs::construct::construct_array(mcx, &datums, NAMEOID, NAMEDATALEN, false, b'c')?;
    let stable = arr.leak();
    Ok(Datum::from_usize(stable.as_ptr() as usize))
}

pub fn fc_pg_available_extensions(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_available_extensions: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    for_each_primary_control_name(|extname, location| {
        let control = parse_control_in_dir(extname, location)?;

        let mut values = [Datum::null(); 3];
        let mut nulls = [false; 3];
        values[0] = name_datum(mcx, &control.name)?;
        match &control.default_version {
            Some(v) => values[1] = text_datum(mcx, v)?,
            None => nulls[1] = true,
        }
        match &control.comment {
            Some(c) => values[2] = text_datum(mcx, c)?,
            None => nulls[2] = true,
        }
        srf.putvalues(&values, &nulls)?;
        Ok(true)
    })?;

    Ok(srf.finish(fcinfo))
}

pub fn fc_pg_available_extension_versions(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_available_extension_versions: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    for_each_primary_control_name(|extname, location| {
        let control = parse_control_in_dir(extname, location)?;
        get_available_versions_for_extension(mcx, &control, &mut srf)?;
        Ok(true)
    })?;

    Ok(srf.finish(fcinfo))
}

// get_available_versions_for_extension (extension.c:2508-2611).
fn get_available_versions_for_extension(
    mcx: Mcx<'_>,
    pcontrol: &ExtensionControlFile,
    srf: &mut funcapi::MaterializedSRF<'_>,
) -> PgResult<()> {
    let mut evi_list = get_ext_ver_list(pcontrol)?;

    for evi in 0..evi_list.len() {
        if !evi_list[evi].installable {
            continue;
        }

        let control = crate::control::read_extension_aux_control_file(pcontrol, &evi_list[evi].name)?;

        let mut values = [Datum::null(); 8];
        let mut nulls = [false; 8];
        values[0] = name_datum(mcx, &control.name)?;
        fill_version_row(mcx, &control, &evi_list[evi].name, &mut values, &mut nulls)?;
        match &control.schema {
            Some(s) => values[5] = name_datum(mcx, s)?,
            None => nulls[5] = true,
        }
        match &control.comment {
            Some(c) => values[7] = text_datum(mcx, c)?,
            None => nulls[7] = true,
        }
        srf.putvalues(&values, &nulls)?;

        // Non-directly-installable versions whose best install path starts
        // here inherit name/schema/comment.
        for evi2 in 0..evi_list.len() {
            if evi_list[evi2].installable {
                continue;
            }
            let mut best_path: Vec<String> = Vec::new();
            if find_install_path(&mut evi_list, evi2, &mut best_path) == Some(evi) {
                let control =
                    crate::control::read_extension_aux_control_file(pcontrol, &evi_list[evi2].name)?;
                fill_version_row(mcx, &control, &evi_list[evi2].name, &mut values, &mut nulls)?;
                srf.putvalues(&values, &nulls)?;
            }
        }
    }
    Ok(())
}

fn fill_version_row(
    mcx: Mcx<'_>,
    control: &ExtensionControlFile,
    version: &str,
    values: &mut [Datum; 8],
    nulls: &mut [bool; 8],
) -> PgResult<()> {
    values[1] = text_datum(mcx, version)?;
    values[2] = Datum::from_bool(control.superuser);
    values[3] = Datum::from_bool(control.trusted);
    values[4] = Datum::from_bool(control.relocatable);
    if control.requires.is_empty() {
        nulls[6] = true;
    } else {
        values[6] = convert_requires_to_datum(mcx, &control.requires)?;
        nulls[6] = false;
    }
    Ok(())
}

pub fn fc_pg_extension_update_paths(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_extension_update_paths: resolved FmgrInfo required");
    let extname = {
        let p = fcinfo.arg(0).as_usize() as *const u8;
        // SAFETY: name argument datum — 64 NUL-padded bytes.
        let bytes = unsafe { core::slice::from_raw_parts(p, 64) };
        let len = bytes.iter().position(|&b| b == 0).unwrap_or(64);
        core::str::from_utf8(&bytes[..len])
            .expect("name argument is server-encoding text")
            .to_string()
    };

    check_valid_extension_name(&extname)?;

    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    let control = read_extension_control_file(&extname)?;
    let mut evi_list: Vec<EviData> = get_ext_ver_list(&control)?;

    for evi1 in 0..evi_list.len() {
        for evi2 in 0..evi_list.len() {
            if evi1 == evi2 {
                continue;
            }
            let path = crate::graph::find_update_path(&mut evi_list, evi1, evi2, false, true);

            let mut values = [Datum::null(); 3];
            let mut nulls = [false; 3];
            values[0] = text_datum(mcx, &evi_list[evi1].name)?;
            values[1] = text_datum(mcx, &evi_list[evi2].name)?;
            if path.is_empty() {
                nulls[2] = true;
            } else {
                let mut pathbuf = evi_list[evi1].name.clone();
                for v in &path {
                    pathbuf.push_str("--");
                    pathbuf.push_str(v);
                }
                values[2] = text_datum(mcx, &pathbuf)?;
            }
            srf.putvalues(&values, &nulls)?;
        }
    }

    Ok(srf.finish(fcinfo))
}

