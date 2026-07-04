// Version-update-path graph (extension.c:1469-1773). C's pointer-linked
// ExtensionVersionInfo list becomes an index-handled Vec (one bulk-freed
// context in C).
use elog::ereport;
use types_error::{PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERROR};

use crate::control::{get_extension_script_directory, ExtensionControlFile};
use crate::is_extension_script_filename;

pub(crate) struct EviData {
    pub name: String,
    pub reachable: Vec<usize>,
    pub installable: bool,
    distance_known: bool,
    distance: i32,
    previous: Option<usize>,
}

pub(crate) fn get_ext_ver_info(versionname: &str, evi_list: &mut Vec<EviData>) -> usize {
    if let Some(i) = evi_list.iter().position(|e| e.name == versionname) {
        return i;
    }
    evi_list.push(EviData {
        name: versionname.to_string(),
        reachable: Vec::new(),
        installable: false,
        distance_known: false,
        distance: i32::MAX,
        previous: None,
    });
    evi_list.len() - 1
}

fn get_nearest_unprocessed_vertex(evi_list: &[EviData]) -> Option<usize> {
    let mut evi: Option<usize> = None;
    for (i, evi2) in evi_list.iter().enumerate() {
        if evi2.distance_known {
            continue;
        }
        if evi.is_none_or(|e| evi_list[e].distance > evi2.distance) {
            evi = Some(i);
        }
    }
    evi
}

pub(crate) fn get_ext_ver_list(control: &ExtensionControlFile) -> PgResult<Vec<EviData>> {
    let mut evi_list: Vec<EviData> = Vec::new();
    let extname = control.name.as_str();
    let location = get_extension_script_directory(control);

    let entries = std::fs::read_dir(&location).map_err(|e| {
        let mut b = ereport(ERROR);
        if let Some(errno) = e.raw_os_error() {
            b = b.with_saved_errno(errno).errcode_for_file_access();
        }
        b.errmsg(format!("could not open directory \"{location}\": %m")).into_error()
    })?;
    for de in entries {
        let de = de.map_err(|e| {
            let mut b = ereport(ERROR);
            if let Some(errno) = e.raw_os_error() {
                b = b.with_saved_errno(errno).errcode_for_file_access();
            }
            b.errmsg(format!("could not read directory \"{location}\": %m")).into_error()
        })?;
        let fname = de.file_name();
        let Some(fname) = fname.to_str() else { continue };
        if !is_extension_script_filename(fname) {
            continue;
        }
        if !fname.starts_with(extname) || !fname[extname.len()..].starts_with("--") {
            continue;
        }

        let vername = &fname[extname.len() + 2..fname.rfind('.').expect("suffix-checked")];
        match vername.find("--") {
            None => {
                let evi = get_ext_ver_info(vername, &mut evi_list);
                evi_list[evi].installable = true;
            }
            Some(sep) => {
                let (v1, v2) = (&vername[..sep], &vername[sep + 2..]);
                if v2.contains("--") {
                    continue;
                }
                let evi = get_ext_ver_info(v1, &mut evi_list);
                let evi2 = get_ext_ver_info(v2, &mut evi_list);
                evi_list[evi].reachable.push(evi2);
            }
        }
    }

    Ok(evi_list)
}

pub(crate) fn identify_update_path(
    control: &ExtensionControlFile,
    old_version: &str,
    new_version: &str,
) -> PgResult<Vec<String>> {
    let mut evi_list = get_ext_ver_list(control)?;
    let evi_start = get_ext_ver_info(old_version, &mut evi_list);
    let evi_target = get_ext_ver_info(new_version, &mut evi_list);

    let result = find_update_path(&mut evi_list, evi_start, evi_target, false, false);
    if result.is_empty() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "extension \"{}\" has no update path from version \"{old_version}\" to version \"{new_version}\"",
                control.name
            ))
            .into_error()
            .into());
    }
    Ok(result)
}

pub(crate) fn find_update_path(
    evi_list: &mut Vec<EviData>,
    evi_start: usize,
    evi_target: usize,
    reject_indirect: bool,
    reinitialize: bool,
) -> Vec<String> {
    debug_assert!(evi_start != evi_target);
    debug_assert!(!(reject_indirect && evi_list[evi_target].installable));

    if reinitialize {
        for evi in evi_list.iter_mut() {
            evi.distance_known = false;
            evi.distance = i32::MAX;
            evi.previous = None;
        }
    }

    evi_list[evi_start].distance = 0;

    while let Some(evi) = get_nearest_unprocessed_vertex(evi_list) {
        if evi_list[evi].distance == i32::MAX {
            break;
        }
        evi_list[evi].distance_known = true;
        if evi == evi_target {
            break;
        }
        for k in 0..evi_list[evi].reachable.len() {
            let evi2 = evi_list[evi].reachable[k];
            if reject_indirect && evi_list[evi2].installable {
                continue;
            }
            let newdist = evi_list[evi].distance + 1;
            if newdist < evi_list[evi2].distance {
                evi_list[evi2].distance = newdist;
                evi_list[evi2].previous = Some(evi);
            } else if newdist == evi_list[evi2].distance
                && evi_list[evi2]
                    .previous
                    .is_some_and(|prev| evi_list[evi].name < evi_list[prev].name)
            {
                // Deterministic tie-break on strcmp of version names.
                evi_list[evi2].previous = Some(evi);
            }
        }
    }

    if !evi_list[evi_target].distance_known {
        return Vec::new();
    }

    let mut result = Vec::new();
    let mut evi = evi_target;
    while evi != evi_start {
        result.push(evi_list[evi].name.clone());
        evi = evi_list[evi].previous.expect("path reconstruction from a known distance");
    }
    result.reverse();
    result
}

pub(crate) fn find_install_path(
    evi_list: &mut Vec<EviData>,
    evi_target: usize,
    best_path: &mut Vec<String>,
) -> Option<usize> {
    best_path.clear();

    if evi_list[evi_target].installable {
        return Some(evi_target);
    }

    let mut evi_start: Option<usize> = None;
    for evi1 in 0..evi_list.len() {
        if !evi_list[evi1].installable {
            continue;
        }
        let path = find_update_path(evi_list, evi1, evi_target, true, true);
        if path.is_empty() {
            continue;
        }
        let better = match evi_start {
            None => true,
            Some(start) => {
                path.len() < best_path.len()
                    || (path.len() == best_path.len()
                        && evi_list[start].name < evi_list[evi1].name)
            }
        };
        if better {
            evi_start = Some(evi1);
            *best_path = path;
        }
    }
    evi_start
}
