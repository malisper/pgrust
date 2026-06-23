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

use utils_error::{ereport, ErrorLevel};
use mcx::{Mcx, PgString, PgVec};
use types_error::{ErrorLocation, PgResult};
use nodes::nodes::Node;

use nodes_core_seams as seams;
use ruleutils_seams as seams_ruleutils;
use lsyscache_seams as seams_lsyscache;
use fmgr_seams as seams_fmgr;

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

// The special `Var.varno` magic values (primnodes.h): references into the
// executor's INNER/OUTER/INDEX tuple slots rather than a real range-table entry.
const INNER_VAR: i32 = -1;
const OUTER_VAR: i32 = -2;
const INDEX_VAR: i32 = -3;

/// `print_rt(rtable)` — print the contents of a range table to stdout
/// (`nodes/print.c`). Faithful field-for-field port: one row per RTE with its
/// `rtekind`-specific second column, then the `inh`/`inFromCl` flags.
pub fn print_rt(mcx: Mcx<'_>, rtable: &[nodes::parsenodes::RangeTblEntry<'_>]) -> PgResult<()> {
    use nodes::parsenodes::RTEKind;

    let _ = mcx; // C uses no allocation here; kept for the seam signature.
    print!("resno\trefname  \trelid\tinFromCl\n");
    print!("-----\t---------\t-----\t--------\n");
    for (idx, rte) in rtable.iter().enumerate() {
        let i = idx + 1;
        // C: rte->eref->aliasname — the eref Alias is always present on a built RTE.
        let aliasname = rte
            .eref
            .as_ref()
            .and_then(|a| a.aliasname.as_deref())
            .unwrap_or("");
        match rte.rtekind {
            RTEKind::RTE_RELATION => {
                // relkind is a `char` in C, printed with %c.
                print!("{}\t{}\t{}\t{}", i, aliasname, rte.relid, rte.relkind as u8 as char);
            }
            RTEKind::RTE_SUBQUERY => print!("{}\t{}\t[subquery]", i, aliasname),
            RTEKind::RTE_JOIN => print!("{}\t{}\t[join]", i, aliasname),
            RTEKind::RTE_FUNCTION => print!("{}\t{}\t[rangefunction]", i, aliasname),
            RTEKind::RTE_TABLEFUNC => print!("{}\t{}\t[table function]", i, aliasname),
            RTEKind::RTE_VALUES => print!("{}\t{}\t[values list]", i, aliasname),
            RTEKind::RTE_CTE => print!("{}\t{}\t[cte]", i, aliasname),
            RTEKind::RTE_NAMEDTUPLESTORE => print!("{}\t{}\t[tuplestore]", i, aliasname),
            RTEKind::RTE_RESULT => print!("{}\t{}\t[result]", i, aliasname),
            RTEKind::RTE_GROUP => print!("{}\t{}\t[group]", i, aliasname),
        }
        print!(
            "\t{}\t{}\n",
            if rte.inh { "inh" } else { "" },
            if rte.inFromCl { "inFromCl" } else { "" }
        );
    }
    use std::io::Write;
    let _ = std::io::stdout().flush();
    Ok(())
}

/// `print_expr(expr, rtable)` — print an expression to stdout (`nodes/print.c`).
/// Handles the `<>`/Var/Const/OpExpr/FuncExpr cases, falling back to
/// `"unknown expr"`. The `Var` default arm resolves the relation/attribute names
/// through the parser/lsyscache owner seams; Const value text through the type's
/// output function; operator/function names through lsyscache.
pub fn print_expr<'a>(
    mcx: Mcx<'a>,
    expr: Option<&Node<'_>>,
    rtable: &[nodes::parsenodes::RangeTblEntry<'a>],
) -> PgResult<()> {
    // C: `if (expr == NULL) { printf("<>"); return; }`.
    let node = match expr {
        None => {
            print!("<>");
            use std::io::Write;
            let _ = std::io::stdout().flush();
            return Ok(());
        }
        Some(n) => n,
    };
    // print_expr only inspects Expr-derived nodes (IsA(expr, Var/Const/...));
    // anything else falls through to "unknown expr".
    match node.as_expr() {
        Some(e) => print_expr_inner(mcx, Some(e), rtable)?,
        None => print!("unknown expr"),
    }
    use std::io::Write;
    let _ = std::io::stdout().flush();
    Ok(())
}

/// The `Expr`-level recursion behind [`print_expr`]. C passes `Expr *` children
/// directly back into `print_expr((Node *) child, ...)`; this repo carries those
/// children as borrowed [`Expr`] values, so the recursion stays at the `&Expr`
/// level (no `Node` allocation/clone). `None` mirrors a NULL child (`"<>"`).
fn print_expr_inner<'a>(
    mcx: Mcx<'a>,
    expr: Option<&nodes::primnodes::Expr>,
    rtable: &[nodes::parsenodes::RangeTblEntry<'a>],
) -> PgResult<()> {
    use nodes::primnodes::Expr;

    let e = match expr {
        None => {
            print!("<>");
            return Ok(());
        }
        Some(e) => e,
    };

    match e {
        Expr::Var(var) => {
            let (relname, attname): (String, String) = match var.varno {
                INNER_VAR => ("INNER".to_string(), "?".to_string()),
                OUTER_VAR => ("OUTER".to_string(), "?".to_string()),
                INDEX_VAR => ("INDEX".to_string(), "?".to_string()),
                _ => {
                    // C: Assert(varno > 0 && varno <= list_length(rtable));
                    //    rte = rt_fetch(varno, rtable);
                    let rte = &rtable[(var.varno - 1) as usize];
                    let relname = rte
                        .eref
                        .as_ref()
                        .and_then(|a| a.aliasname.as_deref())
                        .unwrap_or("")
                        .to_string();
                    // get_rte_attribute_name(rte, var->varattno) — parser parsetree.
                    let attname =
                        seams_ruleutils::get_rte_attribute_name::call(mcx, rte, var.varattno)?
                            .as_str()
                            .to_string();
                    (relname, attname)
                }
            };
            print!("{}.{}", relname, attname);
        }
        Expr::Const(c) => {
            if c.constisnull {
                print!("NULL");
                return Ok(());
            }
            // getTypeOutputInfo(consttype, &typoutput, &typIsVarlena);
            let (typoutput, _typ_is_varlena) =
                seams_lsyscache::get_type_output_info::call(c.consttype)?;
            // OidOutputFunctionCall(typoutput, constvalue) — varlena-aware output.
            let outputstr =
                seams_fmgr::oid_output_function_call::call(mcx, typoutput, &c.constvalue)?;
            print!("{}", String::from_utf8_lossy(&outputstr));
        }
        // Only `IsA(expr, OpExpr)` is special-cased in C; `DistinctExpr` and
        // `NullIfExpr` have distinct node tags and fall through to "unknown expr".
        Expr::OpExpr(opexpr) => {
            // get_opname(e->opno) — NULL when the operator is gone.
            let opname = seams_lsyscache::get_opname::call(mcx, opexpr.opno)?;
            let opname_str: &str = opname
                .as_ref()
                .map(|s| s.as_str())
                .unwrap_or("(invalid operator)");
            if opexpr.args.len() > 1 {
                // print_expr(get_leftop); printf(" op "); print_expr(get_rightop).
                print_expr_inner(mcx, opexpr.args.first(), rtable)?;
                print!(" {} ", opname_str);
                print_expr_inner(mcx, opexpr.args.get(1), rtable)?;
            } else {
                print!("{} ", opname_str);
                print_expr_inner(mcx, opexpr.args.first(), rtable)?;
            }
        }
        Expr::FuncExpr(funcexpr) => {
            // get_func_name(e->funcid) — NULL when the function is gone.
            let funcname = seams_lsyscache::get_func_name::call(mcx, funcexpr.funcid)?;
            let funcname_str: &str = funcname
                .as_ref()
                .map(|s| s.as_str())
                .unwrap_or("(invalid function)");
            print!("{}(", funcname_str);
            for (k, arg) in funcexpr.args.iter().enumerate() {
                print_expr_inner(mcx, Some(arg), rtable)?;
                if k + 1 < funcexpr.args.len() {
                    print!(",");
                }
            }
            print!(")");
        }
        _ => print!("unknown expr"),
    }
    Ok(())
}

/// `print_pathkeys(pathkeys, rtable)` — print a list of `PathKey`s.
///
/// Walks `pathkey->pk_eclass->ec_members`, chasing `ec_merged`. The
/// `pathnodes::EquivalenceClass` carrier models only `ec_merged` (as an
/// `EcId` handle); `EquivalenceMember`/`ec_members`/`em_expr` are not modeled at
/// all, and resolving the `EcId` handle needs the planner's `eq_classes`
/// side-table. Owned by the (unported) planner pathkeys surface; seam-and-panic
/// until it lands.
pub fn print_pathkeys(
    pathkeys: &[pathnodes::PathKey],
    rtable: &[nodes::parsenodes::RangeTblEntry<'_>],
) -> PgResult<()> {
    seams::print_pathkeys::call(pathkeys, rtable)
}

/// `print_tl(tlist, rtable)` — print a targetlist in a more legible way
/// (`nodes/print.c`). Each entry: `resno`, `resname` (or `<null>`), the
/// `ressortgroupref` (when nonzero), then the entry's expression via
/// [`print_expr`].
pub fn print_tl<'a>(
    mcx: Mcx<'a>,
    tlist: &[nodes::primnodes::TargetEntry<'_>],
    rtable: &[nodes::parsenodes::RangeTblEntry<'a>],
) -> PgResult<()> {
    print!("(\n");
    for tle in tlist {
        print!(
            "\t{} {}\t",
            tle.resno,
            tle.resname.as_deref().unwrap_or("<null>")
        );
        if tle.ressortgroupref != 0 {
            print!("({}):\t", tle.ressortgroupref);
        } else {
            print!("    :\t");
        }
        // print_expr((Node *) tle->expr, rtable).
        print_expr_inner(mcx, tle.expr.as_deref(), rtable)?;
        print!("\n");
    }
    print!(")\n");
    use std::io::Write;
    let _ = std::io::stdout().flush();
    Ok(())
}

/// `print_slot(slot)` — print out the tuple in the given `TupleTableSlot`.
///
/// C: `debugtup(slot, NULL)`. `debugtup` is ported in the `printtup` unit but it
/// runs over the `TupleTableSlot` execution runtime (`PrinttupRuntime` /
/// `slot_getattr`), which the execTuples slot model does not yet expose here.
/// Seam-and-panic until that runtime lands.
pub fn print_slot(slot: &nodes::tuptable::SlotBase<'_>) -> PgResult<()> {
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
