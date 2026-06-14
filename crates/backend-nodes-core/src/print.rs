//! Family: **print** — `nodes/print.c`, debug/EXPLAIN node dumping.
//!
//! `print` / `pprint` / `elog_node_display` / `format_node_dump` /
//! `pretty_format_node_dump` / `print_rt` / `print_expr` / `print_pathkeys` /
//! `print_tl` / `print_slot`. These render a node tree (via the `outfuncs`
//! serializer + ad-hoc expr printers) to stdout / a `StringInfo` / the server
//! log.
//!
//! ## Faithful model
//!
//! * The two **formatters** (`format_node_dump`, `pretty_format_node_dump`) are
//!   pure `char *`-to-`char *` reflow routines: ported field-for-field over the
//!   byte stream, allocating the result in `mcx` (C: palloc'd `StringInfo`), so
//!   they take `Mcx` and return [`PgResult`].
//! * The three **dispatchers** (`print`, `pprint`, `elog_node_display`) call the
//!   whole-tree serializer `nodeToStringWithLocations` (owned by the unported
//!   `outfuncs` unit, reached through the `node_to_string_with_locations` seam),
//!   reflow it, and emit it (`printf` → `stdout`; `ereport` → the server log).
//! * The five **ad-hoc printers** (`print_rt`, `print_expr`, `print_pathkeys`,
//!   `print_tl`, `print_slot`) reach into carrier fields that are either
//!   deliberately trimmed out of the foreign-owned carrier structs
//!   (`parsenodes::RangeTblEntry.{eref,inh,inFromCl}`,
//!   `primnodes::TargetEntry.{resno,ressortgroupref}`), an entirely-unmodeled
//!   foreign planner type (`pathnodes` `EquivalenceMember`/`ec_members`), or the
//!   not-yet-ported `TupleTableSlot` execution runtime (`debugtup`, owned by
//!   `printtup` over the execTuples slot model). `print_expr` additionally needs
//!   `get_rte_attribute_name` / `rt_fetch` (parser `parsetree`). Faithfully
//!   building these would require expanding foreign carrier structs from this
//!   family file — which the decomp scope forbids — so each routes to its
//!   genuine (unported) owner through a seam and panics until that owner lands
//!   (`mirror-pg-and-panic`).

#![allow(unused)]

use backend_utils_error::{ereport, ErrorLevel};
use mcx::{Mcx, PgString, PgVec};
use types_error::{ErrorLocation, PgResult};
use types_nodes::nodes::Node;

use backend_nodes_core_seams as seams;

/// `print(obj)` — print the contents of a `Node` to stdout.
///
/// C: `s = nodeToStringWithLocations(obj); f = format_node_dump(s);
/// printf("%s\n", f); fflush(stdout);`.
pub fn print(mcx: Mcx<'_>, obj: &Node<'_>) -> PgResult<()> {
    let s = seams::node_to_string_with_locations::call(mcx, obj)?;
    let f = format_node_dump(mcx, s.as_str())?;
    println!("{}", f.as_str());
    use std::io::Write;
    let _ = std::io::stdout().flush();
    Ok(())
}

/// `pprint(obj)` — pretty-print the contents of a `Node` to stdout.
pub fn pprint(mcx: Mcx<'_>, obj: &Node<'_>) -> PgResult<()> {
    let s = seams::node_to_string_with_locations::call(mcx, obj)?;
    let f = pretty_format_node_dump(mcx, s.as_str())?;
    println!("{}", f.as_str());
    use std::io::Write;
    let _ = std::io::stdout().flush();
    Ok(())
}

/// `elog_node_display(lev, title, obj, pretty)` — send the pretty-printed
/// contents of a `Node` to the server log.
///
/// C: `ereport(lev, (errmsg_internal("%s:", title), errdetail_internal("%s", f)))`.
pub fn elog_node_display(
    mcx: Mcx<'_>,
    lev: ErrorLevel,
    title: &str,
    obj: &Node<'_>,
    pretty: bool,
) -> PgResult<()> {
    let s = seams::node_to_string_with_locations::call(mcx, obj)?;
    let f = if pretty {
        pretty_format_node_dump(mcx, s.as_str())?
    } else {
        format_node_dump(mcx, s.as_str())?
    };
    ereport(lev)
        .errmsg_internal(format!("{title}:"))
        .errdetail_internal(f.as_str().to_string())
        .finish(ErrorLocation {
            filename: None,
            lineno: 0,
            funcname: None,
        })
}

/// `format_node_dump(dump)` — format a `nodeToString` output for display on a
/// terminal. This version just tries to break at whitespace. The result is a
/// fresh `mcx`-allocated string.
pub fn format_node_dump<'mcx>(mcx: Mcx<'mcx>, dump: &str) -> PgResult<PgString<'mcx>> {
    const LINELEN: usize = 78;
    // C: char line[LINELEN + 1]; we mirror the fixed buffer over the byte stream.
    let mut line = [0u8; LINELEN + 1];
    let dump = dump.as_bytes();
    let mut out: PgVec<'mcx, u8> = PgVec::new_in(mcx);

    let mut i: usize = 0;
    let mut j: usize;
    loop {
        j = 0;
        while j < LINELEN && i < dump.len() && dump[i] != b'\0' {
            line[j] = dump[i];
            i += 1;
            j += 1;
        }
        // dump[i] == '\0'  (in Rust: end of the &str, no embedded NUL)
        if i >= dump.len() || dump[i] == b'\0' {
            break;
        }
        if dump[i] == b' ' {
            // ok to break at adjacent space
            i += 1;
        } else {
            // for (k = j - 1; k > 0; k--) if (line[k] == ' ') break;
            let mut k = j as isize - 1;
            while k > 0 {
                if line[k as usize] == b' ' {
                    break;
                }
                k -= 1;
            }
            if k > 0 {
                let k = k as usize;
                // back up; will reprint all after space
                i -= j - k - 1;
                j = k;
            }
        }
        // line[j] = '\0'; appendStringInfo(&str, "%s\n", line);
        append_line(mcx, &mut out, &line[..j])?;
    }
    if j > 0 {
        append_line(mcx, &mut out, &line[..j])?;
    }
    finish(out)
}

/// `pretty_format_node_dump(dump)` — format a `nodeToString` output for display
/// on a terminal, indenting intelligently. The result is a fresh `mcx`-allocated
/// string.
pub fn pretty_format_node_dump<'mcx>(mcx: Mcx<'mcx>, dump: &str) -> PgResult<PgString<'mcx>> {
    const INDENTSTOP: usize = 3;
    const MAXINDENT: usize = 60;
    const LINELEN: usize = 78;
    let mut line = [0u8; LINELEN + 1];
    let dump = dump.as_bytes();
    let mut out: PgVec<'mcx, u8> = PgVec::new_in(mcx);

    let mut indent_lev: usize = 0; // logical indent level
    let mut indent_dist: usize = 0; // physical indent distance
    let mut i: usize = 0;
    let mut j: usize = 0;
    // `dump[i]` past the end stands in for the C NUL terminator.
    let at = |i: usize| -> u8 {
        if i < dump.len() {
            dump[i]
        } else {
            b'\0'
        }
    };
    loop {
        j = 0;
        while j < indent_dist {
            line[j] = b' ';
            j += 1;
        }
        while j < LINELEN && at(i) != b'\0' {
            line[j] = dump[i];
            match line[j] {
                b'}' => {
                    if j != indent_dist {
                        // print data before the }
                        append_line(mcx, &mut out, &line[..j])?;
                    }
                    // print the } at indentDist
                    line[indent_dist] = b'}';
                    append_line(mcx, &mut out, &line[..indent_dist + 1])?;
                    // outdent
                    if indent_lev > 0 {
                        indent_lev -= 1;
                        indent_dist = core::cmp::min(indent_lev * INDENTSTOP, MAXINDENT);
                    }
                    // j will equal indentDist on next loop iteration
                    j = indent_dist; // C: j = indentDist - 1; then i++ + j++ below
                    // suppress whitespace just after }
                    while at(i + 1) == b' ' {
                        i += 1;
                    }
                    // Compensate for the C `j = indentDist - 1` + the loop's `j++`.
                    i += 1;
                    continue;
                }
                b')' => {
                    // force line break after ), unless another ) follows
                    if at(i + 1) != b')' {
                        append_line(mcx, &mut out, &line[..j + 1])?;
                        j = indent_dist; // C: j = indentDist - 1; then j++ below
                        while at(i + 1) == b' ' {
                            i += 1;
                        }
                        i += 1;
                        continue;
                    }
                }
                b'{' => {
                    // force line break before {
                    if j != indent_dist {
                        append_line(mcx, &mut out, &line[..j])?;
                    }
                    // indent
                    indent_lev += 1;
                    indent_dist = core::cmp::min(indent_lev * INDENTSTOP, MAXINDENT);
                    j = 0;
                    while j < indent_dist {
                        line[j] = b' ';
                        j += 1;
                    }
                    line[j] = dump[i];
                }
                b':' => {
                    // force line break before :
                    if j != indent_dist {
                        append_line(mcx, &mut out, &line[..j])?;
                    }
                    j = indent_dist;
                    line[j] = dump[i];
                }
                _ => {}
            }
            i += 1;
            j += 1;
        }
        if at(i) == b'\0' {
            break;
        }
        append_line(mcx, &mut out, &line[..j])?;
    }
    if j > 0 {
        append_line(mcx, &mut out, &line[..j])?;
    }
    finish(out)
}

/// `appendStringInfo(&str, "%s\n", line)` — append the line bytes plus a
/// newline, charged to `mcx`.
fn append_line<'mcx>(mcx: Mcx<'mcx>, out: &mut PgVec<'mcx, u8>, line: &[u8]) -> PgResult<()> {
    out.try_reserve(line.len() + 1).map_err(|_| mcx.oom(line.len() + 1))?;
    out.extend_from_slice(line);
    out.push(b'\n');
    Ok(())
}

/// Wrap the accumulated dump bytes as the palloc'd `char *` result. The
/// reflowed `nodeToString` text is valid UTF-8.
fn finish(out: PgVec<'_, u8>) -> PgResult<PgString<'_>> {
    Ok(PgString::from_utf8(out).expect("node dump is valid UTF-8"))
}

/// `print_rt(rtable)` — print the contents of a range table.
///
/// Reads `rte->eref->aliasname`, `rte->inh`, and `rte->inFromCl`, none of which
/// are modeled on `types_nodes::parsenodes::RangeTblEntry` (the carrier is
/// trimmed by its owning crate, and `eref`/`Alias` is not modeled at all). The
/// faithful printer is owned by the (unported) `outfuncs`/parsetree surface;
/// seam-and-panic until that owner lands.
pub fn print_rt(rtable: &[types_nodes::parsenodes::RangeTblEntry<'_>]) -> PgResult<()> {
    seams::print_rt::call(rtable)
}

/// `print_expr(expr, rtable)` — print an expression.
///
/// The Var/Const/OpExpr/FuncExpr field reads are all modeled, but the default
/// `Var` arm needs `rt_fetch(varno, rtable)->eref->aliasname` and
/// `get_rte_attribute_name(rte, varattno)` (parser `parsetree`), and the trimmed
/// `RangeTblEntry` carrier has no `eref`. Owned by the (unported)
/// parsetree/lsyscache surface; seam-and-panic until it lands.
pub fn print_expr(
    expr: Option<&Node<'_>>,
    rtable: &[types_nodes::parsenodes::RangeTblEntry<'_>],
) -> PgResult<()> {
    seams::print_expr::call(expr, rtable)
}

/// `print_pathkeys(pathkeys, rtable)` — print a list of `PathKey`s.
///
/// Walks `pathkey->pk_eclass->ec_members`, chasing `ec_merged`. The
/// `types_pathnodes::EquivalenceClass` carrier models only `ec_merged` (as an
/// `EcId` handle); `EquivalenceMember`/`ec_members`/`em_expr` are not modeled at
/// all, and resolving the `EcId` handle needs the planner's `eq_classes`
/// side-table. Owned by the (unported) planner pathkeys surface; seam-and-panic
/// until it lands.
pub fn print_pathkeys(
    pathkeys: &[types_pathnodes::PathKey],
    rtable: &[types_nodes::parsenodes::RangeTblEntry<'_>],
) -> PgResult<()> {
    seams::print_pathkeys::call(pathkeys, rtable)
}

/// `print_tl(tlist, rtable)` — print a targetlist in a more legible way.
///
/// Reads `tle->resno` and `tle->ressortgroupref`, neither modeled on
/// `types_nodes::primnodes::TargetEntry` (trimmed by its owning crate), and
/// delegates each entry to [`print_expr`]. Owned by the (unported)
/// outfuncs/parsetree surface; seam-and-panic until it lands.
pub fn print_tl(
    tlist: &[types_nodes::primnodes::TargetEntry<'_>],
    rtable: &[types_nodes::parsenodes::RangeTblEntry<'_>],
) -> PgResult<()> {
    seams::print_tl::call(tlist, rtable)
}

/// `print_slot(slot)` — print out the tuple in the given `TupleTableSlot`.
///
/// C: `debugtup(slot, NULL)`. `debugtup` is ported in the `printtup` unit but it
/// runs over the `TupleTableSlot` execution runtime (`PrinttupRuntime` /
/// `slot_getattr`), which the execTuples slot model does not yet expose here.
/// Seam-and-panic until that runtime lands.
pub fn print_slot(slot: &types_nodes::tuptable::SlotBase<'_>) -> PgResult<()> {
    seams::print_slot::call(slot)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_node_dump_breaks_at_whitespace() {
        let ctx = mcx::MemoryContext::new("t");
        let mcx = ctx.mcx();
        // Short input: one line, trailing newline appended.
        let f = format_node_dump(mcx, "{QUERY :foo 1}").unwrap();
        assert_eq!(f.as_str(), "{QUERY :foo 1}\n");

        // Wrap at a space once past LINELEN (78). Build a >78-char run with a
        // space at position 50 so the back-up-to-space path fires.
        let mut s = String::new();
        s.push_str(&"a".repeat(50));
        s.push(' ');
        s.push_str(&"b".repeat(40)); // total 91 chars, no NUL
        let f = format_node_dump(mcx, &s).unwrap();
        // First line is the 50 a's (broken at the space), second the b's.
        let lines: Vec<&str> = f.as_str().lines().collect();
        assert_eq!(lines[0], "a".repeat(50));
        assert_eq!(lines[1], "b".repeat(40));
    }

    #[test]
    fn pretty_format_indents_braces() {
        let ctx = mcx::MemoryContext::new("t");
        let mcx = ctx.mcx();
        // A nested node string indents children by INDENTSTOP (3) per level and
        // breaks before `{`/`:` and after `)`.
        let f = pretty_format_node_dump(mcx, "{OUTER :inner {INNER :x 1}}").unwrap();
        let out = f.as_str();
        // The outermost `{` indents one level (INDENTSTOP = 3); the nested
        // `{INNER` opens at two levels (6 spaces); `:` forces a break at its
        // level's indent; closing `}`s print at their owning level's column.
        assert_eq!(
            out,
            "   {OUTER \n   :inner \n      {INNER \n      :x 1\n      }\n   }\n",
            "got:\n{out}"
        );
    }

    #[test]
    fn pretty_format_empty() {
        let ctx = mcx::MemoryContext::new("t");
        let mcx = ctx.mcx();
        let f = pretty_format_node_dump(mcx, "").unwrap();
        assert_eq!(f.as_str(), "");
    }
}
