#![allow(non_snake_case)]
#![forbid(unsafe_code)]

//! `backend/commands/explain_format.c` — format routines for explaining query
//! execution plans (PostgreSQL 18.3).
//!
//! The format emitters serialize EXPLAIN output into one of the four formats
//! (`EXPLAIN_FORMAT_TEXT` / `_XML` / `_JSON` / `_YAML`) by appending into the
//! [`ExplainState`]'s output buffer (`es.str`). Every function takes an
//! already-stringified property/label and an `&mut ExplainState`; nothing here
//! inspects a plan node.
//!
//! `es.str` is the context-allocated [`PgString`]; `es.grouping_stack` is the
//! context-allocated [`PgVec`] of `i32` (C's integer `List`). The C
//! `appendStringInfo*` family is `try_push_str`/`try_push` (fallible, so every
//! public function returns [`PgResult`] where the C returns `void`).
//!
//! The cross-subsystem string escapers come from `utils/adt/json.c` and
//! `utils/adt/xml.c` through their seam crates; `escape_yaml`'s C body is
//! literally `escape_json(buf, str)`, so it routes through the `escape_json`
//! seam rather than getting its own slot.

use ::mcx::{PgString, PgVec};
use ::types_error::PgResult;
use ::types_explain::{ExplainFormat, ExplainState};

use ::json_seams::escape_json;
use ::xml_seams::escape_xml;

// OR-able flags for ExplainXMLTag()
const X_OPENING: i32 = 0;
const X_CLOSING: i32 = 1;
const X_CLOSE_IMMEDIATE: i32 = 2;
const X_NOWHITESPACE: i32 = 4;

// ============================================================================
// StringInfo append helpers — the C `appendStringInfo*` family, fallible.
// ============================================================================

/// `appendStringInfoSpaces(buf, count)` — append `count` space characters.
fn append_spaces(buf: &mut PgString<'_>, count: i32) -> PgResult<()> {
    for _ in 0..count {
        buf.try_push(' ')?;
    }
    Ok(())
}

// ============================================================================
// grouping_stack helpers — the C integer `List` ops, on a PgVec<i32>.
// ============================================================================

/// `lcons_int(datum, list)` — prepend `datum` to the front of the integer list.
fn lcons_int(stack: &mut PgVec<'_, i32>, datum: i32) -> PgResult<()> {
    let mcx = *stack.allocator();
    stack
        .try_reserve(1)
        .map_err(|_| mcx.oom(core::mem::size_of::<i32>()))?;
    stack.insert(0, datum);
    Ok(())
}

/// `list_delete_first(list)` — drop the head cell. A pop on an empty stack would
/// be an `Assert`-level internal error in the C (balanced open/close), so we
/// only remove when present.
fn list_delete_first(stack: &mut PgVec<'_, i32>) {
    if !stack.is_empty() {
        stack.remove(0);
    }
}

/// `linitial_int(list)` — read the head cell's int value.
#[inline]
fn linitial_int(stack: &PgVec<'_, i32>) -> i32 {
    debug_assert!(!stack.is_empty(), "linitial_int on empty grouping_stack");
    stack[0]
}

// ============================================================================
// explain_format.c — ported functions
// ============================================================================

/// `ExplainPropertyList(qlabel, data, es)` — explain a property, such as sort
/// keys or targets, that takes the form of a list of unlabeled items.
pub fn ExplainPropertyList(
    qlabel: &str,
    data: &[&str],
    es: &mut ExplainState<'_>,
) -> PgResult<()> {
    let mut first = true;

    match es.format {
        ExplainFormat::EXPLAIN_FORMAT_TEXT => {
            ExplainIndentText(es)?;
            es.str.try_push_str(qlabel)?;
            es.str.try_push_str(": ")?;
            for &item in data {
                if !first {
                    es.str.try_push_str(", ")?;
                }
                es.str.try_push_str(item)?;
                first = false;
            }
            es.str.try_push('\n')?;
        }

        ExplainFormat::EXPLAIN_FORMAT_XML => {
            ExplainXMLTag(qlabel, X_OPENING, es)?;
            for &item in data {
                let indent = es.indent;
                append_spaces(&mut es.str, indent * 2 + 2)?;
                es.str.try_push_str("<Item>")?;
                let mcx = es.str.allocator();
                let s = escape_xml::call(mcx, item)?;
                es.str.try_push_str(s.as_str())?;
                es.str.try_push_str("</Item>\n")?;
            }
            ExplainXMLTag(qlabel, X_CLOSING, es)?;
        }

        ExplainFormat::EXPLAIN_FORMAT_JSON => {
            ExplainJSONLineEnding(es)?;
            let indent = es.indent;
            append_spaces(&mut es.str, indent * 2)?;
            escape_json::call(&mut es.str, qlabel)?;
            es.str.try_push_str(": [")?;
            for &item in data {
                if !first {
                    es.str.try_push_str(", ")?;
                }
                escape_json::call(&mut es.str, item)?;
                first = false;
            }
            es.str.try_push(']')?;
        }

        ExplainFormat::EXPLAIN_FORMAT_YAML => {
            ExplainYAMLLineStarting(es)?;
            es.str.try_push_str(qlabel)?;
            es.str.try_push_str(": ")?;
            for &item in data {
                let indent = es.indent;
                es.str.try_push('\n')?;
                append_spaces(&mut es.str, indent * 2 + 2)?;
                es.str.try_push_str("- ")?;
                escape_yaml(&mut es.str, item)?;
            }
        }
    }

    Ok(())
}

/// `ExplainPropertyListNested(qlabel, data, es)` — explain a property that takes
/// the form of a list of unlabeled items within another list.
pub fn ExplainPropertyListNested(
    qlabel: &str,
    data: &[&str],
    es: &mut ExplainState<'_>,
) -> PgResult<()> {
    let mut first = true;

    match es.format {
        ExplainFormat::EXPLAIN_FORMAT_TEXT | ExplainFormat::EXPLAIN_FORMAT_XML => {
            return ExplainPropertyList(qlabel, data, es);
        }

        ExplainFormat::EXPLAIN_FORMAT_JSON => {
            ExplainJSONLineEnding(es)?;
            let indent = es.indent;
            append_spaces(&mut es.str, indent * 2)?;
            es.str.try_push('[')?;
            for &item in data {
                if !first {
                    es.str.try_push_str(", ")?;
                }
                escape_json::call(&mut es.str, item)?;
                first = false;
            }
            es.str.try_push(']')?;
        }

        ExplainFormat::EXPLAIN_FORMAT_YAML => {
            ExplainYAMLLineStarting(es)?;
            es.str.try_push_str("- [")?;
            for &item in data {
                if !first {
                    es.str.try_push_str(", ")?;
                }
                escape_yaml(&mut es.str, item)?;
                first = false;
            }
            es.str.try_push(']')?;
        }
    }

    Ok(())
}

/// `ExplainProperty(qlabel, unit, value, numeric, es)` — explain a simple
/// property.
///
/// If `numeric` is true, the value is a number (or other value that doesn't need
/// quoting in JSON). If `unit` is non-NULL the text format will display it after
/// the value.
fn ExplainProperty(
    qlabel: &str,
    unit: Option<&str>,
    value: &str,
    numeric: bool,
    es: &mut ExplainState<'_>,
) -> PgResult<()> {
    match es.format {
        ExplainFormat::EXPLAIN_FORMAT_TEXT => {
            ExplainIndentText(es)?;
            if let Some(unit) = unit {
                // appendStringInfo(es->str, "%s: %s %s\n", qlabel, value, unit);
                es.str.try_push_str(qlabel)?;
                es.str.try_push_str(": ")?;
                es.str.try_push_str(value)?;
                es.str.try_push(' ')?;
                es.str.try_push_str(unit)?;
                es.str.try_push('\n')?;
            } else {
                // appendStringInfo(es->str, "%s: %s\n", qlabel, value);
                es.str.try_push_str(qlabel)?;
                es.str.try_push_str(": ")?;
                es.str.try_push_str(value)?;
                es.str.try_push('\n')?;
            }
        }

        ExplainFormat::EXPLAIN_FORMAT_XML => {
            let indent = es.indent;
            append_spaces(&mut es.str, indent * 2)?;
            ExplainXMLTag(qlabel, X_OPENING | X_NOWHITESPACE, es)?;
            let mcx = es.str.allocator();
            let s = escape_xml::call(mcx, value)?;
            es.str.try_push_str(s.as_str())?;
            ExplainXMLTag(qlabel, X_CLOSING | X_NOWHITESPACE, es)?;
            es.str.try_push('\n')?;
        }

        ExplainFormat::EXPLAIN_FORMAT_JSON => {
            ExplainJSONLineEnding(es)?;
            let indent = es.indent;
            append_spaces(&mut es.str, indent * 2)?;
            escape_json::call(&mut es.str, qlabel)?;
            es.str.try_push_str(": ")?;
            if numeric {
                es.str.try_push_str(value)?;
            } else {
                escape_json::call(&mut es.str, value)?;
            }
        }

        ExplainFormat::EXPLAIN_FORMAT_YAML => {
            ExplainYAMLLineStarting(es)?;
            es.str.try_push_str(qlabel)?;
            es.str.try_push_str(": ")?;
            if numeric {
                es.str.try_push_str(value)?;
            } else {
                escape_yaml(&mut es.str, value)?;
            }
        }
    }

    Ok(())
}

/// `ExplainPropertyText(qlabel, value, es)` — explain a string-valued property.
pub fn ExplainPropertyText(
    qlabel: &str,
    value: &str,
    es: &mut ExplainState<'_>,
) -> PgResult<()> {
    ExplainProperty(qlabel, None, value, false, es)
}

/// `ExplainPropertyInteger(qlabel, unit, value, es)` — explain an integer-valued
/// property.
pub fn ExplainPropertyInteger(
    qlabel: &str,
    unit: Option<&str>,
    value: i64,
    es: &mut ExplainState<'_>,
) -> PgResult<()> {
    // char buf[32]; snprintf(buf, sizeof(buf), INT64_FORMAT, value);
    // Fixed, small, non-data-derived buffer (the alloc-safety exemption).
    let mut buf = [0u8; 32];
    let s = i64_to_str(value, &mut buf);
    ExplainProperty(qlabel, unit, s, true, es)
}

/// `ExplainPropertyUInteger(qlabel, unit, value, es)` — explain an unsigned
/// integer-valued property.
pub fn ExplainPropertyUInteger(
    qlabel: &str,
    unit: Option<&str>,
    value: u64,
    es: &mut ExplainState<'_>,
) -> PgResult<()> {
    // char buf[32]; snprintf(buf, sizeof(buf), UINT64_FORMAT, value);
    let mut buf = [0u8; 32];
    let s = u64_to_str(value, &mut buf);
    ExplainProperty(qlabel, unit, s, true, es)
}

/// `ExplainPropertyFloat(qlabel, unit, value, ndigits, es)` — explain a
/// float-valued property, using the specified number of fractional digits.
pub fn ExplainPropertyFloat(
    qlabel: &str,
    unit: Option<&str>,
    value: f64,
    ndigits: i32,
    es: &mut ExplainState<'_>,
) -> PgResult<()> {
    // buf = psprintf("%.*f", ndigits, value);
    let prec = if ndigits < 0 { 0usize } else { ndigits as usize };
    // A psprintf into a heap buffer; the size is bounded by the formatted
    // double (non-data-derived), so a fixed stack buffer formatted via the
    // double's own Display with the requested precision suffices.
    use core::fmt::Write;
    let mut buf = FixedBuf::new();
    let _ = write!(buf, "{:.*}", prec, value);
    ExplainProperty(qlabel, unit, buf.as_str(), true, es)
}

/// `ExplainPropertyBool(qlabel, value, es)` — explain a bool-valued property.
pub fn ExplainPropertyBool(
    qlabel: &str,
    value: bool,
    es: &mut ExplainState<'_>,
) -> PgResult<()> {
    ExplainProperty(qlabel, None, if value { "true" } else { "false" }, true, es)
}

/// `ExplainOpenGroup(objtype, labelname, labeled, es)` — open a group of related
/// objects.
pub fn ExplainOpenGroup(
    objtype: &str,
    labelname: Option<&str>,
    labeled: bool,
    es: &mut ExplainState<'_>,
) -> PgResult<()> {
    match es.format {
        ExplainFormat::EXPLAIN_FORMAT_TEXT => { /* nothing to do */ }

        ExplainFormat::EXPLAIN_FORMAT_XML => {
            ExplainXMLTag(objtype, X_OPENING, es)?;
            es.indent += 1;
        }

        ExplainFormat::EXPLAIN_FORMAT_JSON => {
            ExplainJSONLineEnding(es)?;
            let indent = es.indent;
            append_spaces(&mut es.str, 2 * indent)?;
            if let Some(labelname) = labelname {
                escape_json::call(&mut es.str, labelname)?;
                es.str.try_push_str(": ")?;
            }
            es.str.try_push(if labeled { '{' } else { '[' })?;

            // In JSON format, the grouping_stack is an integer list.  0 means
            // we've emitted nothing at this grouping level, 1 means we've
            // emitted something (and so the next item needs a comma). See
            // ExplainJSONLineEnding().
            lcons_int(&mut es.grouping_stack, 0)?;
            es.indent += 1;
        }

        ExplainFormat::EXPLAIN_FORMAT_YAML => {
            // In YAML format, the grouping stack is an integer list.  0 means
            // we've emitted nothing at this grouping level AND this grouping
            // level is unlabeled and must be marked with "- ".  See
            // ExplainYAMLLineStarting().
            ExplainYAMLLineStarting(es)?;
            if let Some(labelname) = labelname {
                es.str.try_push_str(labelname)?;
                es.str.try_push_str(": ")?;
                lcons_int(&mut es.grouping_stack, 1)?;
            } else {
                es.str.try_push_str("- ")?;
                lcons_int(&mut es.grouping_stack, 0)?;
            }
            es.indent += 1;
        }
    }

    Ok(())
}

/// `ExplainCloseGroup(objtype, labelname, labeled, es)` — close a group of related
/// objects. Parameters must match the corresponding [`ExplainOpenGroup`] call.
pub fn ExplainCloseGroup(
    objtype: &str,
    _labelname: Option<&str>,
    labeled: bool,
    es: &mut ExplainState<'_>,
) -> PgResult<()> {
    match es.format {
        ExplainFormat::EXPLAIN_FORMAT_TEXT => { /* nothing to do */ }

        ExplainFormat::EXPLAIN_FORMAT_XML => {
            es.indent -= 1;
            ExplainXMLTag(objtype, X_CLOSING, es)?;
        }

        ExplainFormat::EXPLAIN_FORMAT_JSON => {
            es.indent -= 1;
            let indent = es.indent;
            es.str.try_push('\n')?;
            append_spaces(&mut es.str, 2 * indent)?;
            es.str.try_push(if labeled { '}' } else { ']' })?;
            list_delete_first(&mut es.grouping_stack);
        }

        ExplainFormat::EXPLAIN_FORMAT_YAML => {
            es.indent -= 1;
            list_delete_first(&mut es.grouping_stack);
        }
    }

    Ok(())
}

/// `ExplainOpenSetAsideGroup(objtype, labelname, labeled, depth, es)` — open a
/// group of related objects, without emitting actual data.
pub fn ExplainOpenSetAsideGroup(
    _objtype: &str,
    labelname: Option<&str>,
    _labeled: bool,
    depth: i32,
    es: &mut ExplainState<'_>,
) -> PgResult<()> {
    match es.format {
        ExplainFormat::EXPLAIN_FORMAT_TEXT => { /* nothing to do */ }

        ExplainFormat::EXPLAIN_FORMAT_XML => {
            es.indent += depth;
        }

        ExplainFormat::EXPLAIN_FORMAT_JSON => {
            lcons_int(&mut es.grouping_stack, 0)?;
            es.indent += depth;
        }

        ExplainFormat::EXPLAIN_FORMAT_YAML => {
            if labelname.is_some() {
                lcons_int(&mut es.grouping_stack, 1)?;
            } else {
                lcons_int(&mut es.grouping_stack, 0)?;
            }
            es.indent += depth;
        }
    }

    Ok(())
}

/// `ExplainSaveGroup(es, depth, state_save)` — pop one level of grouping state,
/// allowing for a re-push later. The C writes the saved integer through
/// `*state_save`; idiomatically it is returned (0 for TEXT/XML, matching the
/// un-touched C out-parameter for those formats).
pub fn ExplainSaveGroup(es: &mut ExplainState<'_>, depth: i32) -> i32 {
    let mut state_save = 0;
    match es.format {
        ExplainFormat::EXPLAIN_FORMAT_TEXT => { /* nothing to do */ }

        ExplainFormat::EXPLAIN_FORMAT_XML => {
            es.indent -= depth;
        }

        ExplainFormat::EXPLAIN_FORMAT_JSON => {
            es.indent -= depth;
            state_save = linitial_int(&es.grouping_stack);
            list_delete_first(&mut es.grouping_stack);
        }

        ExplainFormat::EXPLAIN_FORMAT_YAML => {
            es.indent -= depth;
            state_save = linitial_int(&es.grouping_stack);
            list_delete_first(&mut es.grouping_stack);
        }
    }
    state_save
}

/// `ExplainRestoreGroup(es, depth, state_save)` — re-push one level of grouping
/// state, undoing the effects of [`ExplainSaveGroup`].
pub fn ExplainRestoreGroup(
    es: &mut ExplainState<'_>,
    depth: i32,
    state_save: i32,
) -> PgResult<()> {
    match es.format {
        ExplainFormat::EXPLAIN_FORMAT_TEXT => { /* nothing to do */ }

        ExplainFormat::EXPLAIN_FORMAT_XML => {
            es.indent += depth;
        }

        ExplainFormat::EXPLAIN_FORMAT_JSON => {
            lcons_int(&mut es.grouping_stack, state_save)?;
            es.indent += depth;
        }

        ExplainFormat::EXPLAIN_FORMAT_YAML => {
            lcons_int(&mut es.grouping_stack, state_save)?;
            es.indent += depth;
        }
    }

    Ok(())
}

/// `ExplainCreateWorkersState(num_workers)` — allocate the per-worker output
/// redirection state for an ANALYZE'd parallel plan node.
pub fn ExplainCreateWorkersState<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    num_workers: i32,
) -> PgResult<::types_explain::ExplainWorkersState<'mcx>> {
    let n = num_workers.max(0) as usize;
    let mut worker_inited: PgVec<'mcx, bool> = PgVec::new_in(mcx);
    let mut worker_str: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);
    let mut worker_state_save: PgVec<'mcx, i32> = PgVec::new_in(mcx);
    worker_inited.try_reserve(n).map_err(|_| mcx.oom(n))?;
    worker_str.try_reserve(n).map_err(|_| mcx.oom(n))?;
    worker_state_save.try_reserve(n).map_err(|_| mcx.oom(n))?;
    for _ in 0..n {
        worker_inited.push(false);
        // palloc0'd StringInfoData: an empty (uninitialized) buffer; the real
        // `initStringInfo` happens lazily in ExplainOpenWorker on first use.
        worker_str.push(PgString::new_in(mcx));
        worker_state_save.push(0);
    }
    Ok(::types_explain::ExplainWorkersState {
        num_workers,
        worker_inited,
        worker_str,
        worker_state_save,
        prev_str: None,
    })
}

/// `ExplainOpenWorker(n, es)` — begin or resume output into the set-aside group
/// for worker `n`. Swaps `es.str` with the per-worker buffer (the owned-model
/// form of C's `es->str = &wstate->worker_str[n]` pointer swing), saving the
/// prior buffer in `prev_str`.
pub fn ExplainOpenWorker(n: i32, es: &mut ExplainState<'_>) -> PgResult<()> {
    let wstate = es
        .workers_state
        .as_ref()
        .expect("ExplainOpenWorker: workers_state present");
    debug_assert!(n >= 0 && n < wstate.num_workers);
    let n = n as usize;
    let inited = wstate.worker_inited[n];

    // Save prior output buffer (C: wstate->prev_str = es->str). In the owned
    // model `es.str` is the buffer itself, so swap it out into the worker slot
    // and stash what was there in `prev_str`.
    let mut taken = core::mem::replace(
        &mut es.workers_state.as_mut().unwrap().worker_str[n],
        PgString::new_in(es.str.allocator()),
    );
    core::mem::swap(&mut es.str, &mut taken);
    es.workers_state.as_mut().unwrap().prev_str = Some(taken);

    if !inited {
        // First time through: the buffer (now `es.str`) is already fresh
        // (`initStringInfo`).
        ExplainOpenSetAsideGroup("Worker", None, true, 2, es)?;

        if es.format != ExplainFormat::EXPLAIN_FORMAT_TEXT {
            ExplainPropertyInteger("Worker Number", None, n as i64, es)?;
        }
        es.workers_state.as_mut().unwrap().worker_inited[n] = true;
    } else {
        // Restore formatting state saved by the last ExplainCloseWorker().
        let saved = es.workers_state.as_ref().unwrap().worker_state_save[n];
        ExplainRestoreGroup(es, 2, saved)?;
    }

    if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
        if es.str.len() == 0 {
            ExplainIndentText(es)?;
            es.str.try_push_str(&std::format!("Worker {n}:  "))?;
        }
        es.indent += 1;
    }

    Ok(())
}

/// `ExplainCloseWorker(n, es)` — end output for worker `n`; must pair with a
/// previous [`ExplainOpenWorker`] call. Swings `es.str` back to the prior buffer.
pub fn ExplainCloseWorker(n: i32, es: &mut ExplainState<'_>) -> PgResult<()> {
    debug_assert!(es.workers_state.is_some());
    let n = n as usize;

    // Save formatting state, then pop the formatting stack.
    let saved = ExplainSaveGroup(es, 2);
    es.workers_state.as_mut().unwrap().worker_state_save[n] = saved;

    if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
        // If we produced no actual output line(s), truncate off the partial
        // "Worker N:" line emitted by ExplainOpenWorker.
        while es.str.len() > 0 && es.str.as_bytes().last().copied() != Some(b'\n') {
            let new_len = es.str.len() - 1;
            es.str.truncate(new_len);
        }
        es.indent -= 1;
    }

    // Restore prior output buffer (C: es->str = wstate->prev_str). Swing the
    // worker buffer back into its slot and move the saved buffer back into
    // `es.str`.
    let mut prev = es
        .workers_state
        .as_mut()
        .unwrap()
        .prev_str
        .take()
        .expect("ExplainCloseWorker: prev_str saved by ExplainOpenWorker");
    core::mem::swap(&mut es.str, &mut prev);
    es.workers_state.as_mut().unwrap().worker_str[n] = prev;

    Ok(())
}

/// `ExplainFlushWorkersState(es)` — print per-worker info for the current node,
/// then drop the [`::types_explain::ExplainWorkersState`].
pub fn ExplainFlushWorkersState(es: &mut ExplainState<'_>) -> PgResult<()> {
    // Take the workers_state out so we can read its buffers while appending to
    // `es.str` (the C reads `wstate->worker_str[i]` while writing `es->str`).
    let wstate = es
        .workers_state
        .take()
        .expect("ExplainFlushWorkersState: workers_state present");

    ExplainOpenGroup("Workers", Some("Workers"), false, es)?;
    for i in 0..wstate.num_workers as usize {
        if wstate.worker_inited[i] {
            // This must match the previous ExplainOpenSetAsideGroup call.
            ExplainOpenGroup("Worker", None, true, es)?;
            es.str.try_push_str(wstate.worker_str[i].as_str())?;
            ExplainCloseGroup("Worker", None, true, es)?;
        }
    }
    ExplainCloseGroup("Workers", Some("Workers"), false, es)?;

    // C `pfree`s the worker buffers + the wstate; here `wstate` is dropped.
    drop(wstate);
    Ok(())
}

/// `ExplainDummyGroup(objtype, labelname, es)` — emit a "dummy" group that never
/// has any members.
pub fn ExplainDummyGroup(
    objtype: &str,
    labelname: Option<&str>,
    es: &mut ExplainState<'_>,
) -> PgResult<()> {
    match es.format {
        ExplainFormat::EXPLAIN_FORMAT_TEXT => { /* nothing to do */ }

        ExplainFormat::EXPLAIN_FORMAT_XML => {
            ExplainXMLTag(objtype, X_CLOSE_IMMEDIATE, es)?;
        }

        ExplainFormat::EXPLAIN_FORMAT_JSON => {
            ExplainJSONLineEnding(es)?;
            let indent = es.indent;
            append_spaces(&mut es.str, 2 * indent)?;
            if let Some(labelname) = labelname {
                escape_json::call(&mut es.str, labelname)?;
                es.str.try_push_str(": ")?;
            }
            escape_json::call(&mut es.str, objtype)?;
        }

        ExplainFormat::EXPLAIN_FORMAT_YAML => {
            ExplainYAMLLineStarting(es)?;
            if let Some(labelname) = labelname {
                escape_yaml(&mut es.str, labelname)?;
                es.str.try_push_str(": ")?;
            } else {
                es.str.try_push_str("- ")?;
            }
            escape_yaml(&mut es.str, objtype)?;
        }
    }

    Ok(())
}

/// `ExplainBeginOutput(es)` — emit the start-of-output boilerplate.
pub fn ExplainBeginOutput(es: &mut ExplainState<'_>) -> PgResult<()> {
    match es.format {
        ExplainFormat::EXPLAIN_FORMAT_TEXT => { /* nothing to do */ }

        ExplainFormat::EXPLAIN_FORMAT_XML => {
            es.str
                .try_push_str("<explain xmlns=\"http://www.postgresql.org/2009/explain\">\n")?;
            es.indent += 1;
        }

        ExplainFormat::EXPLAIN_FORMAT_JSON => {
            // top-level structure is an array of plans
            es.str.try_push('[')?;
            lcons_int(&mut es.grouping_stack, 0)?;
            es.indent += 1;
        }

        ExplainFormat::EXPLAIN_FORMAT_YAML => {
            lcons_int(&mut es.grouping_stack, 0)?;
        }
    }

    Ok(())
}

/// `ExplainEndOutput(es)` — emit the end-of-output boilerplate.
pub fn ExplainEndOutput(es: &mut ExplainState<'_>) -> PgResult<()> {
    match es.format {
        ExplainFormat::EXPLAIN_FORMAT_TEXT => { /* nothing to do */ }

        ExplainFormat::EXPLAIN_FORMAT_XML => {
            es.indent -= 1;
            es.str.try_push_str("</explain>")?;
        }

        ExplainFormat::EXPLAIN_FORMAT_JSON => {
            es.indent -= 1;
            es.str.try_push_str("\n]")?;
            list_delete_first(&mut es.grouping_stack);
        }

        ExplainFormat::EXPLAIN_FORMAT_YAML => {
            list_delete_first(&mut es.grouping_stack);
        }
    }

    Ok(())
}

/// `ExplainSeparatePlans(es)` — put an appropriate separator between multiple
/// plans.
pub fn ExplainSeparatePlans(es: &mut ExplainState<'_>) -> PgResult<()> {
    match es.format {
        ExplainFormat::EXPLAIN_FORMAT_TEXT => {
            // add a blank line
            es.str.try_push('\n')?;
        }

        ExplainFormat::EXPLAIN_FORMAT_XML
        | ExplainFormat::EXPLAIN_FORMAT_JSON
        | ExplainFormat::EXPLAIN_FORMAT_YAML => {
            /* nothing to do */
        }
    }

    Ok(())
}

/// `ExplainXMLTag(tagname, flags, es)` — emit opening or closing XML tag.
///
/// XML restricts tag names more than our other output formats, eg they can't
/// contain white space or slashes. Replace invalid characters with dashes, so
/// that for example "I/O Read Time" becomes "I-O-Read-Time".
fn ExplainXMLTag(tagname: &str, flags: i32, es: &mut ExplainState<'_>) -> PgResult<()> {
    const VALID: &[u8] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.";

    let indent = es.indent;

    if (flags & X_NOWHITESPACE) == 0 {
        append_spaces(&mut es.str, 2 * indent)?;
    }
    es.str.try_push('<')?;
    if (flags & X_CLOSING) != 0 {
        es.str.try_push('/')?;
    }
    // The C iterates the NUL-terminated string byte by byte; each byte either
    // passes through (when in `valid`) or becomes '-'.
    for &b in tagname.as_bytes() {
        let ch = if VALID.contains(&b) { b as char } else { '-' };
        es.str.try_push(ch)?;
    }
    if (flags & X_CLOSE_IMMEDIATE) != 0 {
        es.str.try_push_str(" /")?;
    }
    es.str.try_push('>')?;
    if (flags & X_NOWHITESPACE) == 0 {
        es.str.try_push('\n')?;
    }

    Ok(())
}

/// `ExplainIndentText(es)` — indent a text-format line.
///
/// We indent by two spaces per indentation level. However, when emitting data
/// for a parallel worker there might already be data on the current line (cf.
/// `ExplainOpenWorker`); in that case, don't indent any more.
pub fn ExplainIndentText(es: &mut ExplainState<'_>) -> PgResult<()> {
    debug_assert_eq!(es.format, ExplainFormat::EXPLAIN_FORMAT_TEXT); // Assert(es->format == EXPLAIN_FORMAT_TEXT);
    let indent = es.indent;
    // if (es->str->len == 0 || es->str->data[es->str->len - 1] == '\n')
    let at_line_start =
        es.str.is_empty() || es.str.as_bytes().last().copied() == Some(b'\n');
    if at_line_start {
        append_spaces(&mut es.str, indent * 2)?;
    }
    Ok(())
}

/// `ExplainJSONLineEnding(es)` — emit a JSON line ending.
///
/// JSON requires a comma after each property but the last. To facilitate this,
/// in JSON format, the text emitted for each property begins just prior to the
/// preceding line-break (and comma, if applicable).
fn ExplainJSONLineEnding(es: &mut ExplainState<'_>) -> PgResult<()> {
    debug_assert_eq!(es.format, ExplainFormat::EXPLAIN_FORMAT_JSON); // Assert(es->format == EXPLAIN_FORMAT_JSON);
    if linitial_int(&es.grouping_stack) != 0 {
        es.str.try_push(',')?;
    } else {
        es.grouping_stack[0] = 1;
    }
    es.str.try_push('\n')?;
    Ok(())
}

/// `ExplainYAMLLineStarting(es)` — indent a YAML line.
fn ExplainYAMLLineStarting(es: &mut ExplainState<'_>) -> PgResult<()> {
    debug_assert_eq!(es.format, ExplainFormat::EXPLAIN_FORMAT_YAML); // Assert(es->format == EXPLAIN_FORMAT_YAML);
    if linitial_int(&es.grouping_stack) == 0 {
        es.grouping_stack[0] = 1;
    } else {
        let indent = es.indent;
        es.str.try_push('\n')?;
        append_spaces(&mut es.str, indent * 2)?;
    }
    Ok(())
}

/// `escape_yaml(buf, str)` — YAML is a superset of JSON; the YAML quoting rules
/// are ridiculously complicated, so we chose to just quote everything. The C
/// body is literally `escape_json(buf, str)`, so this routes through the
/// `escape_json` seam.
fn escape_yaml(buf: &mut PgString<'_>, str: &str) -> PgResult<()> {
    escape_json::call(buf, str)
}

// ============================================================================
// Small fixed-buffer integer/float formatting (the `char buf[32]` exemption).
// ============================================================================

/// Render an `i64` decimally into a fixed buffer (C `INT64_FORMAT`).
fn i64_to_str(value: i64, buf: &mut [u8; 32]) -> &str {
    use core::fmt::Write;
    let len = {
        let mut fb = FixedBufRef::new(buf);
        let _ = write!(fb, "{}", value);
        fb.len
    };
    core::str::from_utf8(&buf[..len]).unwrap_or("")
}

/// Render a `u64` decimally into a fixed buffer (C `UINT64_FORMAT`).
fn u64_to_str(value: u64, buf: &mut [u8; 32]) -> &str {
    use core::fmt::Write;
    let len = {
        let mut fb = FixedBufRef::new(buf);
        let _ = write!(fb, "{}", value);
        fb.len
    };
    core::str::from_utf8(&buf[..len]).unwrap_or("")
}

/// A small fixed-capacity formatting target (the `char buf[N]` exemption: a
/// non-data-derived, bounded scratch buffer that never allocates).
struct FixedBuf {
    bytes: [u8; 64],
    len: usize,
}

impl FixedBuf {
    fn new() -> Self {
        FixedBuf { bytes: [0u8; 64], len: 0 }
    }
    fn as_str(&self) -> &str {
        // Only whole `&str` writes ever land here.
        core::str::from_utf8(&self.bytes[..self.len]).unwrap_or("")
    }
}

impl core::fmt::Write for FixedBuf {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        let b = s.as_bytes();
        let end = self.len + b.len();
        if end > self.bytes.len() {
            return Err(core::fmt::Error);
        }
        self.bytes[self.len..end].copy_from_slice(b);
        self.len = end;
        Ok(())
    }
}

/// Like [`FixedBuf`] but over a borrowed buffer (for the `char buf[32]` sites).
struct FixedBufRef<'a> {
    bytes: &'a mut [u8; 32],
    len: usize,
}

impl<'a> FixedBufRef<'a> {
    fn new(bytes: &'a mut [u8; 32]) -> Self {
        FixedBufRef { bytes, len: 0 }
    }
}

impl core::fmt::Write for FixedBufRef<'_> {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        let b = s.as_bytes();
        let end = self.len + b.len();
        if end > self.bytes.len() {
            return Err(core::fmt::Error);
        }
        self.bytes[self.len..end].copy_from_slice(b);
        self.len = end;
        Ok(())
    }
}

/// This crate owns no cross-cycle (`backend-commands-explain-format-seams`)
/// declarations — its callers (`explain.c` and the extension explainers) will
/// depend on it directly when they land — so there is nothing to install.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
