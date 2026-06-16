//! Out-of-F0-scope executor / SPI / catalog command bodies of `extension.c`.
//!
//! F0 ports `extension.c`'s parse/analysis half (control-file analysis, the
//! version-update-path graph, the catalog-read cores, `RemoveExtensionById`, the
//! `CreateExtension` parse-side, and the backend-globals). The CREATE / ALTER /
//! DROP orchestration and the supporting SRFs / catalog edits drive the
//! parser/analyzer/planner/executor/utility pipeline, SPI, catalog DML, the
//! fmgr/`Datum` value layer, and filesystem write/IO — subsystems not yet wired.
//!
//! Each entry point is a loud `panic!` (mirror-pg-and-panic) naming the C
//! function it stands in for. When those subsystems land, the bodies will be
//! ported against the same node tree the parse half already uses, reusing the
//! in-crate analysis functions directly.

macro_rules! deferred {
    ($cfn:literal, $what:literal) => {
        panic!(concat!(
            "backend-commands-extension: ",
            $cfn,
            " is out of F0 scope (F0 ports extension.c's parse/analysis half). ",
            "It drives ",
            $what,
            ", which is not yet wired.",
        ))
    };
}

/// `ExecAlterExtensionStmt` (C 3408-3544) + `ApplyExtensionUpdates` (C 3555-3702):
/// ALTER EXTENSION ... UPDATE.
pub fn ExecAlterExtensionStmt() -> ! {
    deferred!(
        "ExecAlterExtensionStmt/ApplyExtensionUpdates",
        "the syscache version lookup, the per-version pg_extension tuple edit, the \
         dependency delete/recreate, and the extension-script execution pipeline"
    )
}

/// `ApplyExtensionUpdates` (C 3555-3702): apply the chain of update scripts after
/// an install. Calls `execute_extension_script` (C 3690), so it is gated on the
/// same parser/analyzer/planner/executor/utility script-execution pipeline.
pub fn ApplyExtensionUpdates() -> ! {
    deferred!(
        "ApplyExtensionUpdates",
        "the per-version pg_extension tuple edit, the dependency delete/recreate, \
         and the extension-script execution pipeline (execute_extension_script)"
    )
}

/// `ExecAlterExtensionContentsStmt` (C 3713-3789) + `…Recurse` (C 3799-3929):
/// ALTER EXTENSION ... ADD/DROP member object.
///
/// Blocked: the recursion (`ExecAlterExtensionContentsRecurse`) calls
/// `recordExtObjInitPriv` (ADD, C 3848) and `removeExtObjInitPriv` (DROP,
/// C 3885) — aclchk.c's extension-membership initial-ACL helpers — which have
/// no port and no declared seam anywhere in the workspace (only the unrelated
/// `RemoveRoleFromInitPriv`/`ReplaceRoleInInitPriv` DROP/REASSIGN-OWNED helpers
/// exist). Per mirror-pg-and-panic, the whole command stays deferred rather than
/// silently dropping the initial-ACL recording; the DROP path additionally
/// reaches the also-deferred `extension_config_remove`.
pub fn ExecAlterExtensionContentsStmt() -> ! {
    deferred!(
        "ExecAlterExtensionContentsStmt/…Recurse",
        "recordExtObjInitPriv/removeExtObjInitPriv (aclchk.c extension-membership \
         initial-ACL helpers), which have no port and no seam"
    )
}

/// `execute_extension_script` (C 1196-1458) + `execute_sql_string` (C 1046-1165):
/// run an extension install/update script.
pub fn execute_extension_script() -> ! {
    deferred!(
        "execute_extension_script/execute_sql_string",
        "the superuser/GUC security-context switch and the whole \
         parser/analyzer/planner/executor/utility script-execution loop"
    )
}

/// `read_extension_script_file` (C 871-898): read a script file and convert it to
/// the database encoding.
pub fn read_extension_script_file() -> ! {
    deferred!(
        "read_extension_script_file",
        "read_whole_file filesystem IO and the pg_verify_mbstr/pg_any_to_server \
         encoding conversion"
    )
}

/// `get_function_sibling_type` (C 272-331) + `ext_sibling_callback` (C 343-354):
/// the function-sibling type cache.
pub fn get_function_sibling_type() -> ! {
    deferred!(
        "get_function_sibling_type",
        "the pg_depend extension lookup, the pg_type syscache resolution, and the \
         syscache-invalidation callback registration"
    )
}

/// `pg_available_extensions` (C 2334-2420): SRF over the available extensions.
pub fn pg_available_extensions() -> ! {
    deferred!(
        "pg_available_extensions",
        "the SRF tuplestore / fmgr Datum layer"
    )
}

/// `pg_available_extension_versions` (C 2432-2501) +
/// `get_available_versions_for_extension` (C 2508-2611).
pub fn pg_available_extension_versions() -> ! {
    deferred!(
        "pg_available_extension_versions/get_available_versions_for_extension",
        "the SRF tuplestore / fmgr Datum layer"
    )
}

/// `pg_extension_update_paths` (C 2707-2782): SRF over the update paths.
pub fn pg_extension_update_paths() -> ! {
    deferred!(
        "pg_extension_update_paths",
        "the SRF tuplestore / fmgr Datum layer"
    )
}

/// `pg_get_loaded_modules` (C 2975-3018): SRF over the loaded modules.
pub fn pg_get_loaded_modules() -> ! {
    deferred!(
        "pg_get_loaded_modules",
        "the loaded-module walk and the SRF tuplestore / fmgr Datum layer"
    )
}

/// `pg_extension_config_dump` (C 2792-2965): record a table/sequence in the
/// extension's extconfig/extcondition arrays.
pub fn pg_extension_config_dump() -> ! {
    deferred!(
        "pg_extension_config_dump",
        "the fmgr/Datum array machinery and the pg_extension catalog update"
    )
}

/// `extension_config_remove` (C 3028-3187): remove a table from the extension's
/// extconfig/extcondition arrays.
///
/// Blocked: the extcondition leg (C 3160-3176) deconstructs a `text[]` array,
/// squeezes out one element, and reconstructs it. The workspace's
/// `deconstruct_array_builtin` returns the legacy word-typed `types_datum::Datum`
/// (a bare `usize`), which cannot carry a by-reference `text` payload — there is
/// no value-typed `deconstruct_array` returning real `Datum::ByRef` elements to
/// feed back to `construct_array_values`. Reconstructing the text array would
/// therefore lose the condition strings. (The extconfig OID leg is by-value and
/// would be fine; only the text leg is blocked.) Reachable only from the also-
/// deferred `ExecAlterExtensionContentsStmt` DROP path.
pub fn extension_config_remove() -> ! {
    deferred!(
        "extension_config_remove",
        "a value-typed deconstruct_array for the by-reference text[] extcondition \
         squeeze (the legacy word-Datum deconstruct cannot carry text payloads)"
    )
}

/// `script_error_callback` (C 904-1030): the error-context callback for
/// script-execution failures.
pub fn script_error_callback() -> ! {
    deferred!(
        "script_error_callback",
        "the errcontext/errposition error-context primitives bound to the live executor"
    )
}
