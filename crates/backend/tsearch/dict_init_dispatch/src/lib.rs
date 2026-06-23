//! Dispatcher for `verify_dictoptions`' init-method call
//! (`commands/tsearchcmds.c`).
//!
//! C `verify_dictoptions` does
//! `OidFunctionCall1(initmethod, PointerGetDatum(deserialize_deflist(...)))`:
//! it invokes the text-search template's `init` method purely so the method can
//! validate the supplied options and `ereport(ERROR)` on a bad one. The return
//! value (the built dictionary object) is discarded — only `Ok`/`Err` matters.
//!
//! The owned model has no live fmgr `internal`-pointer ABI here, so this crate
//! installs the [`call_dict_init`] seam and dispatches on the init method's
//! `pg_proc` OID to the corresponding ported `*_init` function, threading the
//! `(defname, arg)` option list — each `arg` keeping its original `DefElem`
//! node kind (`T_Integer`/`T_Float`/`T_Boolean`/`T_String`/...), so the init
//! methods' `defGetBoolean`/`defGetInt32`/... see the same node tag C does.

#![no_std]

extern crate alloc;

use alloc::string::String;

use ::define_seams::DefElemArg;
use tsearchcmds_seams as ts_seams;
use mcx::{Mcx, MemoryContext};
use ::types_core::Oid;
use types_error::{PgError, PgResult};

/// Builtin text-search template `init` method OIDs (`pg_proc.dat` /
/// `catalog/pg_proc_d.h`). These are the only templates present in the
/// bootstrap `pg_ts_template.dat`; the `snowball` templates are created later
/// by `snowball.sql` with dynamically-assigned OIDs and are not reachable here.
const F_DSIMPLE_INIT: Oid = 3725;
const F_DSYNONYM_INIT: Oid = 3728;
const F_DISPELL_INIT: Oid = 3731;
const F_THESAURUS_INIT: Oid = 3740;

/// `OidFunctionCall1(initmethod, dictoptions)` — call the template init method
/// to validate the options. The built dictionary is discarded.
fn call_dict_init(
    initmethod: Oid,
    pairs: &[(String, Option<DefElemArg>)],
) -> PgResult<()> {
    // Private context for the throw-away dictionary the init method builds; the
    // C code "doesn't worry about leaking memory; our command will soon be over
    // anyway", but here we just drop the scratch arena when done.
    let ctx = MemoryContext::new("call_dict_init");
    let mcx: Mcx<'_> = ctx.mcx();

    match initmethod {
        F_DSIMPLE_INIT => {
            dict::dict_simple::dsimple_init(mcx, pairs)?;
        }
        F_DSYNONYM_INIT => {
            dict::dict_synonym::dsynonym_init(mcx, pairs)?;
        }
        F_DISPELL_INIT => {
            ispell_regis::dispell_init(mcx, pairs)?;
        }
        F_THESAURUS_INIT => {
            dict::dict_thesaurus::thesaurus_init(mcx, pairs)?;
        }
        other => {
            // Mirrors the C fmgr "function N not found" failure for an
            // init method whose OID this dispatcher does not know.
            return Err(PgError::error(alloc::format!(
                "text search dictionary init function {other} not found"
            )));
        }
    }

    Ok(())
}

/// Install the [`ts_seams::call_dict_init`] seam.
pub fn init_seams() {
    ts_seams::call_dict_init::set(call_dict_init);
}
