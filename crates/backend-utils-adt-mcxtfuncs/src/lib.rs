//! Port of PostgreSQL `src/backend/utils/adt/mcxtfuncs.c` — functions to show
//! backend memory context.
//!
//! Every function in `mcxtfuncs.c` is ported here with its full logic:
//!
//! * `int_list_to_array`                (static helper)
//! * `PutMemoryContextsStatsTupleStore` (static helper)
//! * `pg_get_backend_memory_contexts`   (SQL SRF)
//! * `pg_log_backend_memory_contexts`   (SQL function)
//!
//! The algorithmic core — the breadth-first walk over the live `MemoryContext`
//! tree, the transient `context_id` assignment, the ancestor `path`
//! construction, the dynahash relabeling, the identifier clipping, and the
//! per-column value layout — lives in this crate. The genuinely-external
//! operations cross the seams in [`backend_utils_adt_mcxtfuncs_seams`]: the live
//! `MemoryContext` tree navigation and per-context `methods->stats`
//! (`utils/mmgr/mcxt.c`), the SRF row sink (`funcapi.c` / `tuplestore.c`), and
//! the backend/auxiliary PID lookup (`procarray.c` / `proc.c`).
//!
//! # Project-wide fmgr/Datum-layer deferral
//!
//! The `PG_FUNCTION_ARGS` SQL entry points (`pg_get_backend_memory_contexts`,
//! `pg_log_backend_memory_contexts`) are part of the project-wide fmgr/Datum
//! deferral: argument extraction (`PG_GETARG_INT32`), result construction
//! (`PG_RETURN_BOOL`, the `(Datum) 0` SRF return), and the `InitMaterializedSRF`
//! / `fcinfo->resultinfo` set-returning-function protocol are not yet unified.
//! Those entry points are loud `panic!`s here; the **cores** they would call
//! ([`pg_get_backend_memory_contexts_core`],
//! [`pg_log_backend_memory_contexts`]) are fully implemented.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::format;
use alloc::vec;
use alloc::vec::Vec;

use backend_utils_error::ereport;
use types_error::error::{ERROR, WARNING};
use types_error::{ErrorLocation, PgResult};

use backend_storage_ipc_procsignal_seams::send_proc_signal;
use backend_utils_adt_mcxtfuncs_seams as seam;
use backend_utils_adt_mcxtfuncs_seams::{McxtRow, MemoryContextRef, MemoryContextType};
use backend_utils_mb_mbutils_seams::pg_mbcliplen;
use types_storage::ProcSignalReason;

pub mod fmgr_builtins;

/// `MEMORY_CONTEXT_IDENT_DISPLAY_SIZE` — the max bytes for showing identifiers of
/// `MemoryContext`.
const MEMORY_CONTEXT_IDENT_DISPLAY_SIZE: i32 = 1024;

/// `PG_GET_BACKEND_MEMORY_CONTEXTS_COLS` — number of output columns of
/// `pg_get_backend_memory_contexts`.
pub const PG_GET_BACKEND_MEMORY_CONTEXTS_COLS: usize = 10;

fn errloc(lineno: i32, funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("src/backend/utils/adt/mcxtfuncs.c", lineno, funcname)
}

/// `int_list_to_array`
///   Convert an `IntList` to an array of `INT4OID`s.
///
/// In C this `palloc`s a `Datum` array, fills it with `Int32GetDatum(i)`, then
/// `construct_array_builtin(.., INT4OID)`. The in-crate logic produces the `i32`
/// values (the `path`) in list order; the `int4[]` array Datum assembly itself
/// (`construct_array_builtin`) is folded into the `tuplestore_putvalues` seam,
/// which receives the `path` as `&[i32]` and builds the column on the provider
/// side.
fn int_list_to_array(list: &[i32]) -> Vec<i32> {
    // length = list_length(list); foreach_int(i, list) datum_array[idx] =
    // Int32GetDatum(i); construct_array_builtin(datum_array, length, INT4OID).
    list.to_vec()
}

/// `PutMemoryContextsStatsTupleStore`
///   Add details for the given `MemoryContext` to 'tupstore'.
///
/// `context_id_lookup` resolves each already-visited `MemoryContext` to its
/// transient `context_id`; it stands in for the C dynahash `HTAB`.
fn PutMemoryContextsStatsTupleStore(
    context: MemoryContextRef,
    context_id_lookup: &dyn Fn(MemoryContextRef) -> Option<i32>,
) -> PgResult<()> {
    // List *path = NIL;
    let mut path: Vec<i32> = Vec::new();

    let node = seam::context_node::call(context)?;

    // Assert(MemoryContextIsValid(context));

    // Figure out the transient context_id of this context and each of its
    // ancestors.
    //
    //   for (cur = context; cur != NULL; cur = cur->parent)
    //   {
    //       entry = hash_search(context_id_lookup, &cur, HASH_FIND, &found);
    //       if (!found) elog(ERROR, "hash table corrupted");
    //       path = lcons_int(entry->context_id, path);
    //   }
    let mut cur = Some(context);
    while let Some(cur_ref) = cur {
        let context_id = match context_id_lookup(cur_ref) {
            Some(id) => id,
            None => {
                // elog(ERROR, "hash table corrupted");
                ereport(ERROR)
                    .errmsg_internal("hash table corrupted")
                    .finish(errloc(98, "PutMemoryContextsStatsTupleStore"))?;
                unreachable!("ereport(ERROR) returned Ok");
            }
        };
        // path = lcons_int(entry->context_id, path);
        path.insert(0, context_id);

        let cur_node = seam::context_node::call(cur_ref)?;
        cur = cur_node.parent;
    }

    // Examine the context itself
    //   memset(&stat, 0, sizeof(stat));
    //   (*context->methods->stats)(context, NULL, NULL, &stat, true);
    let stat = seam::context_stats::call(context)?;

    // name = context->name; ident = context->ident;
    let mut name = node.name.clone();
    let mut ident = node.ident.clone();

    // To be consistent with logging output, we label dynahash contexts with just
    // the hash table name as with MemoryContextStatsPrint().
    //
    //   if (ident && strcmp(name, "dynahash") == 0) { name = ident; ident = NULL; }
    //
    // C dereferences `name` (via strcmp) unconditionally once `ident != NULL`; a
    // real MemoryContext always has a non-NULL `name`, so `name == Some(b"dynahash")`
    // is the literal equivalent for valid inputs.
    if ident.is_some() && name.as_deref() == Some(b"dynahash".as_slice()) {
        name = ident.take();
    }

    // if (name) values[0] = CStringGetTextDatum(name); else nulls[0] = true;
    // (folded into the McxtRow; raw server-encoding bytes carried so
    // CStringGetTextDatum is byte-for-byte faithful, no lossy UTF-8 conversion.)
    let name_col = name;

    // if (ident) { clip; values[1] = CStringGetTextDatum(clipped); } else nulls[1] = true;
    let ident_col = match ident {
        Some(ident) => {
            // int idlen = strlen(ident);
            let mut idlen = ident.len() as i32;

            // Some identifiers such as SQL query string can be very long, truncate
            // oversize identifiers.
            //
            //   if (idlen >= MEMORY_CONTEXT_IDENT_DISPLAY_SIZE)
            //       idlen = pg_mbcliplen(ident, idlen,
            //                            MEMORY_CONTEXT_IDENT_DISPLAY_SIZE - 1);
            if idlen >= MEMORY_CONTEXT_IDENT_DISPLAY_SIZE {
                idlen = pg_mbcliplen::call(&ident, idlen, MEMORY_CONTEXT_IDENT_DISPLAY_SIZE - 1);
            }

            // memcpy(clipped_ident, ident, idlen); clipped_ident[idlen] = '\0';
            // values[1] = CStringGetTextDatum(clipped_ident);  (raw bytes — no
            // lossy UTF-8 conversion; pg_mbcliplen clips on server-encoding
            // multibyte boundaries which need not be UTF-8.)
            Some(ident[..idlen as usize].to_vec())
        }
        None => None,
    };

    // switch (context->type) { case T_AllocSetContext: ...; default: "???" }
    let type_col: &[u8] = match node.context_type {
        MemoryContextType::AllocSet => b"AllocSet",
        MemoryContextType::Generation => b"Generation",
        MemoryContextType::Slab => b"Slab",
        MemoryContextType::Bump => b"Bump",
        MemoryContextType::Unknown => b"???",
    };

    // values[3] = Int32GetDatum(list_length(path));  /* level */
    let level = path.len() as i32;
    // values[4] = int_list_to_array(path);
    let path_array = int_list_to_array(&path);

    let row = McxtRow {
        // values[0] / nulls[0]
        name: name_col,
        // values[1] / nulls[1]
        ident: ident_col,
        // values[2]
        context_type: type_col.to_vec(),
        // values[3]
        level,
        // values[4]
        path: path_array,
        // values[5] = Int64GetDatum(stat.totalspace)
        total_bytes: stat.totalspace as i64,
        // values[6] = Int64GetDatum(stat.nblocks)
        n_blocks: stat.nblocks as i64,
        // values[7] = Int64GetDatum(stat.freespace)
        free_bytes: stat.freespace as i64,
        // values[8] = Int64GetDatum(stat.freechunks)
        free_chunks: stat.freechunks as i64,
        // values[9] = Int64GetDatum(stat.totalspace - stat.freespace)
        used_bytes: (stat.totalspace as i64).wrapping_sub(stat.freespace as i64),
    };

    // tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    seam::tuplestore_putvalues::call(row)?;

    // list_free(path);   (Vec dropped here)
    Ok(())
}

/// `pg_get_backend_memory_contexts` (core)
///   SQL SRF showing backend memory context.
///
/// The breadth-first walk and per-context row emission of the C SRF, minus the
/// fmgr `InitMaterializedSRF` set-up (deferred; see crate docs). Returns `()` on
/// success (C `return (Datum) 0`).
pub fn pg_get_backend_memory_contexts_core() -> PgResult<()> {
    // ctl.keysize = sizeof(MemoryContext); ctl.entrysize = sizeof(MemoryContextId);
    // context_id_lookup = hash_create("pg_get_backend_memory_contexts", 256, ...);
    //
    // The dynahash table maps a MemoryContext to its assigned context_id; modeled
    // as an insertion-ordered list keyed by handle identity.
    let mut context_id_lookup: Vec<(MemoryContextRef, i32)> = Vec::with_capacity(256);

    // InitMaterializedSRF(fcinfo, 0);  -- performed by the fmgr entry point.

    // Here we use a non-recursive algorithm to visit all MemoryContexts starting
    // with TopMemoryContext, so we can assign the context_id breadth-first.
    //
    //   contexts = list_make1(TopMemoryContext);
    let top = seam::top_memory_context::call()?;
    let mut contexts: Vec<MemoryContextRef> = vec![top];

    // TopMemoryContext will always have a context_id of 1
    let mut context_id: i32 = 1;

    // foreach_ptr(MemoryContextData, cur, contexts)
    //
    // The list grows while we iterate (children are appended), exactly like the C
    // foreach over a List that is being lappend()ed to.
    let mut idx = 0;
    while idx < contexts.len() {
        let cur = contexts[idx];

        // Record the context_id assigned to each MemoryContext.
        //   entry = hash_search(context_id_lookup, &cur, HASH_ENTER, &found);
        //   entry->context_id = context_id++;
        //   Assert(!found);
        debug_assert!(context_id_lookup.iter().all(|&(c, _)| c != cur));
        context_id_lookup.push((cur, context_id));
        context_id += 1;

        // PutMemoryContextsStatsTupleStore(rsinfo->setResult, rsinfo->setDesc,
        //                                  cur, context_id_lookup);
        let lookup = &context_id_lookup;
        PutMemoryContextsStatsTupleStore(cur, &|ctx| {
            lookup
                .iter()
                .find_map(|&(c, id)| if c == ctx { Some(id) } else { None })
        })?;

        // Append all children onto the contexts list so they're processed by
        // subsequent iterations.
        //   for (c = cur->firstchild; c != NULL; c = c->nextchild)
        //       contexts = lappend(contexts, c);
        let cur_node = seam::context_node::call(cur)?;
        let mut c = cur_node.firstchild;
        while let Some(child) = c {
            contexts.push(child);
            let c_node = seam::context_node::call(child)?;
            c = c_node.nextchild;
        }

        idx += 1;
    }

    // hash_destroy(context_id_lookup);   (Vec dropped)
    // return (Datum) 0;
    Ok(())
}

/// `pg_get_backend_memory_contexts`
///   SQL SRF showing backend memory context (fmgr entry point).
///
/// The `ReturnSetInfo`/`InitMaterializedSRF` set-returning-function protocol and
/// the `(Datum) 0` return belong to the project-wide fmgr/Datum-layer deferral;
/// the algorithm is [`pg_get_backend_memory_contexts_core`]. Loud panic until
/// the fmgr boundary is unified.
pub fn pg_get_backend_memory_contexts() -> ! {
    panic!("fmgr/Datum-layer deferral: pg_get_backend_memory_contexts (mcxtfuncs.c)")
}

/// `pg_log_backend_memory_contexts`
///   Signal a backend or an auxiliary process to log its memory contexts.
///
/// By default, only superusers are allowed to signal to log the memory contexts;
/// additional roles can be permitted with GRANT (enforced by the catalog ACL, not
/// in this function).
///
/// `pid` is `PG_GETARG_INT32(0)`. Returns the boolean SQL result.
pub fn pg_log_backend_memory_contexts(pid: i32) -> PgResult<bool> {
    // int pid = PG_GETARG_INT32(0); PGPROC *proc;
    // ProcNumber procNumber = INVALID_PROC_NUMBER;  (overwritten before use)

    // See if the process with given pid is a backend or an auxiliary process.
    //   proc = BackendPidGetProc(pid);
    //   if (proc == NULL) proc = AuxiliaryPidGetProc(pid);
    let proc = seam::pid_get_proc::call(pid)?;

    // BackendPidGetProc()/AuxiliaryPidGetProc() return NULL if the pid isn't
    // valid; but by the time we reach kill(), a process for which we got a valid
    // proc here might have terminated on its own.
    let Some(proc) = proc else {
        // This is just a warning so a loop-through-resultset will not abort if one
        // backend terminated on its own during the run.
        //   ereport(WARNING, (errmsg("PID %d is not a PostgreSQL server process", pid)));
        //   PG_RETURN_BOOL(false);
        ereport(WARNING)
            .errmsg(format!("PID {pid} is not a PostgreSQL server process"))
            .finish(errloc(293, "pg_log_backend_memory_contexts"))?;
        return Ok(false);
    };

    // procNumber = GetNumberFromPGProc(proc);
    let procNumber = proc.proc_number;

    // if (SendProcSignal(pid, PROCSIG_LOG_MEMORY_CONTEXT, procNumber) < 0)
    if send_proc_signal::call(pid, ProcSignalReason::PROCSIG_LOG_MEMORY_CONTEXT, procNumber) < 0 {
        // Again, just a warning to allow loops
        //   ereport(WARNING, (errmsg("could not send signal to process %d: %m", pid)));
        //   PG_RETURN_BOOL(false);
        //
        // `%m` expands against the current errno (the failing kill()/SendProcSignal)
        // in the error subsystem's message formatter.
        ereport(WARNING)
            .errmsg(format!("could not send signal to process {pid}: %m"))
            .finish(errloc(302, "pg_log_backend_memory_contexts"))?;
        return Ok(false);
    }

    // PG_RETURN_BOOL(true);
    Ok(true)
}

/// This crate owns no inward seams: every seam it touches is **outward** — the
/// live-context tree / stats / SRF sink (`backend-utils-adt-mcxtfuncs-seams`,
/// installed by the unported `mcxt.c` / `funcapi.c` / `procarray.c` owners) and
/// `send_proc_signal` (installed by procsignal). So `init_seams()` installs no
/// inward seams; it only registers this crate's fmgr builtins into the
/// fmgr-core builtin table.
pub fn init_seams() {
    fmgr_builtins::register_mcxtfuncs_builtins();
}

#[cfg(test)]
mod tests;
