//! help_config.c (REL_18_6): `postgres --describe-config`. GucInfoMain
//! prints one tab-separated line per displayable GUC and exits 0; main.c:220
//! dispatches DISPATCH_DESCRIBE_CONFIG here.

use guc_tables::{all_settings, config_group_names, GucContext_Names};
use types_guc::{GUC_DISALLOW_IN_FILE, GUC_NOT_IN_SAMPLE, GUC_NO_SHOW_ALL};

use crate::enum_lookup::config_enum_lookup_by_value;
use crate::name::guc_name_compare;
use crate::registry::GucVariable;
use crate::units::fmt_g;

// displayStruct (help_config.c:70-76).
fn display_struct(var: &GucVariable) -> bool {
    var.gen().flags & (GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE) == 0
}

// printMixedStruct (help_config.c:84-131).
fn print_mixed_struct(out: &mut String, var: &GucVariable) {
    let gen = var.gen();
    out.push_str(gen.name);
    out.push('\t');
    out.push_str(GucContext_Names[gen.context as usize]);
    out.push('\t');
    out.push_str(config_group_names[gen.group as usize]);
    out.push('\t');
    match var {
        // C prints reset_val for bool/int/real (help_config.c:97, :103,
        // :110). GucInfoMain runs build_guc_variables ONLY — never
        // InitializeOneGUCOption, which is what copies boot_val into
        // reset_val — so the static tables' zero reset_val is what C
        // prints: FALSE / 0 / 0 for every one of them (postgres 18.6
        // --describe-config: `enable_seqscan ... BOOLEAN FALSE`,
        // `shared_buffers ... INTEGER 0 16 1073741823`). Strings and enums
        // print boot_val (:117, :122).
        GucVariable::Bool(_) => out.push_str("BOOLEAN\tFALSE\t\t\t"),
        GucVariable::Int(c) => {
            out.push_str(&format!("INTEGER\t0\t{}\t{}\t", c.min, c.max));
        }
        GucVariable::Real(c) => {
            out.push_str(&format!("REAL\t0\t{}\t{}\t", fmt_g(c.min), fmt_g(c.max)));
        }
        GucVariable::String(c) => {
            out.push_str(&format!("STRING\t{}\t\t\t", c.boot_val.as_deref().unwrap_or("")));
        }
        GucVariable::Enum(c) => {
            out.push_str(&format!(
                "ENUM\t{}\t\t\t",
                config_enum_lookup_by_value(c, c.boot_val).unwrap_or("")
            ));
        }
    }
    out.push_str(gen.short_desc.unwrap_or(""));
    out.push('\t');
    out.push_str(gen.long_desc.unwrap_or(""));
    out.push('\n');
}

/// GucInfoMain's stdout (help_config.c:47-65): the build_guc_variables
/// table — the static settings only, no InitializeGUCOptions hooks and no
/// environment — in get_guc_variables order (guc.c:890 guc_var_compare),
/// one printMixedStruct line per displayable variable.
pub fn guc_info_text() -> String {
    let mut vars: Vec<GucVariable> =
        all_settings().filter_map(crate::store::build_variable).collect();
    vars.sort_by(|a, b| guc_name_compare(a.gen().name, b.gen().name));
    let mut out = String::with_capacity(64 * 1024);
    for var in &vars {
        if display_struct(var) {
            print_mixed_struct(&mut out, var);
        }
    }
    out
}

/// GucInfoMain (help_config.c:47): print the table and exit(0). C's printf
/// output reaches the descriptor at exit; a write failure is ignored there
/// too.
pub fn GucInfoMain() -> ! {
    use std::io::Write;
    let text = guc_info_text();
    let mut stdout = std::io::stdout().lock();
    let _ = stdout.write_all(text.as_bytes());
    let _ = stdout.flush();
    std::process::exit(0)
}
