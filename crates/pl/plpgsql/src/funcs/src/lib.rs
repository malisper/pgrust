//! `backend-pl-plpgsql-funcs` — misc support functions for PL/pgSQL.
//!
//! A faithful, owned-tree port of `src/pl/plpgsql/src/pl_funcs.c`
//! (PostgreSQL 18.3). Covers:
//!   * the compiler's namespace stack (`plpgsql_ns_*`),
//!   * the statement / GET DIAGNOSTICS type-name helpers
//!     (`plpgsql_stmt_typename`, `plpgsql_getdiag_kindname`),
//!   * the statement-tree walker (`plpgsql_statement_tree_walker`),
//!   * marking of local assignment targets
//!     (`plpgsql_mark_local_assignment_targets`),
//!   * function-memory release (`plpgsql_free_function_memory`,
//!     `plpgsql_delete_callback`),
//!   * the debug dumper (`plpgsql_dumptree`).
//!
//! The namespace stack is a per-backend `static PLpgSQL_nsitem *ns_top` in C
//! (PL/pgSQL compilation is single-threaded); it is modeled here with a
//! thread-local owning the head of the chain (each item owns its `prev` via
//! `Box`), matching the `pl_scanner` thread-local precedent.
//!
//! External calls: `bms_*` go directly to `backend-nodes-core`; `SPI_freeplan`,
//! `MemoryContextDelete`, and `cfunc->use_count` go through their owners' seam
//! crates (the SPI / mcxt owners are present; `funccache.c` is not yet ported,
//! so `cfunc_use_count` panics until it lands — mirror-PG-and-panic).

#![allow(non_camel_case_types, non_snake_case)]

use core::cell::RefCell;

use spi_seams::spi_freeplan;
use nodes_core::bitmapset::{bms_add_member, bms_copy, bms_free, bms_is_member};
use funccache_seams::cfunc_use_count;
use mcxt_seams::MemoryContextDelete;
use mcx::{Mcx, PgBox};
use types_error::PgResult;
use types_logical::MemoryContextHandle;
use nodes::bitmapset::Bitmapset;
use plpgsql::*;
use types_ri_triggers::SpiPlanPtr;

/// A locally-built `Bitmapset` of datum numbers (`local_dnos`), owned in the
/// compile-time `Mcx` arena. C's NULL set maps to `None`.
type LocalDnos<'mcx> = Option<PgBox<'mcx, Bitmapset<'mcx>>>;

// ---------------------------------------------------------------------------
// Local variables for namespace handling
//
// The namespace structure actually forms a tree, of which only one linear
// list or "chain" (from the youngest item to the root) is accessible from any
// one plpgsql statement. During initial parsing of a function, ns_top points
// to the youngest item accessible from the block currently being parsed. We
// store the entire tree, however, since at runtime we will need to access the
// chain that's relevant to any one statement.
//
// Block boundaries in the namespace chain are marked by PLPGSQL_NSTYPE_LABEL
// items.
// ---------------------------------------------------------------------------
thread_local! {
    static NS_TOP: RefCell<Option<Box<PLpgSQL_nsitem>>> = const { RefCell::new(None) };
}

/// `plpgsql_ns_init` — Initialize namespace processing for a new function.
pub fn plpgsql_ns_init() {
    NS_TOP.with(|t| {
        *t.borrow_mut() = None;
    });
}

/// `plpgsql_ns_push` — Create a new namespace level.
pub fn plpgsql_ns_push(label: Option<&str>, label_type: PLpgSQL_label_type) {
    let label = label.unwrap_or("");
    plpgsql_ns_additem(
        PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_LABEL,
        label_type as i32,
        label,
    );
}

/// `plpgsql_ns_pop` — Pop entries back to (and including) the last label.
pub fn plpgsql_ns_pop() {
    NS_TOP.with(|t| {
        let mut top = t.borrow_mut();
        assert!(top.is_some(), "ns_top != NULL");
        // while (ns_top->itemtype != PLPGSQL_NSTYPE_LABEL) ns_top = ns_top->prev;
        loop {
            let is_label = matches!(
                top.as_ref().map(|n| n.itemtype),
                Some(PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_LABEL)
            );
            if is_label {
                break;
            }
            let prev = top.as_mut().unwrap().prev.take();
            *top = prev;
        }
        // ns_top = ns_top->prev;  (drop the label itself)
        let prev = top.as_mut().unwrap().prev.take();
        *top = prev;
    });
}

/// `plpgsql_ns_top` — Fetch the current namespace chain end.
///
/// In C this returns the live `ns_top` pointer. Because our chain is owned by
/// the thread-local, we hand the caller a deep clone of the current chain (a
/// borrowed-tree snapshot); the compiler typically attaches this to an expr's
/// `ns` field, which is itself an owned `Option<Box<PLpgSQL_nsitem>>`.
pub fn plpgsql_ns_top() -> Option<Box<PLpgSQL_nsitem>> {
    NS_TOP.with(|t| t.borrow().clone())
}

/// `plpgsql_ns_additem` — Add an item to the current namespace chain.
pub fn plpgsql_ns_additem(itemtype: PLpgSQL_nsitem_type, itemno: i32, name: &str) {
    // Assert(name != NULL);  (name: &str is always non-null)
    NS_TOP.with(|t| {
        let mut top = t.borrow_mut();
        // first item added must be a label
        assert!(
            top.is_some() || itemtype == PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_LABEL,
            "ns_top != NULL || itemtype == PLPGSQL_NSTYPE_LABEL"
        );

        let nse = Box::new(PLpgSQL_nsitem {
            itemtype,
            itemno,
            prev: top.take(),
            name: name.to_string(),
        });
        *top = Some(nse);
    });
}

/// `plpgsql_ns_lookup` — Lookup an identifier in the given namespace chain.
///
/// Note that this only searches for variables, not labels.
///
/// If `localmode` is true, only the topmost block level is searched.
///
/// `name1` must be non-NULL. Pass `None` for `name2` and/or `name3` if parsing
/// a name with fewer than three components.
///
/// If `names_used` isn't `None`, `*names_used` receives the number of names
/// matched: 0 if no match, 1 if `name1` matched an unqualified variable name,
/// 2 if `name1` and `name2` matched a block label + variable name.
///
/// Note that `name3` is never directly matched to anything. However, if it
/// isn't `None`, we will disregard qualified matches to scalar variables.
/// Similarly, if `name2` isn't `None`, we disregard unqualified matches to
/// scalar variables.
pub fn plpgsql_ns_lookup<'a>(
    ns_cur: &'a PLpgSQL_nsitem,
    localmode: bool,
    name1: &str,
    name2: Option<&str>,
    name3: Option<&str>,
    names_used: Option<&mut i32>,
) -> Option<&'a PLpgSQL_nsitem> {
    let mut names_used = names_used;
    // Outer loop iterates once per block level in the namespace chain
    let mut ns_cur: Option<&'a PLpgSQL_nsitem> = Some(ns_cur);
    while let Some(cur) = ns_cur {
        // Check this level for unqualified match to variable name.
        //
        // for (nsitem = ns_cur;
        //      nsitem->itemtype != PLPGSQL_NSTYPE_LABEL;
        //      nsitem = nsitem->prev)
        let mut nsitem: &'a PLpgSQL_nsitem = cur;
        while nsitem.itemtype != PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_LABEL {
            if nsitem.name == name1
                && (name2.is_none() || nsitem.itemtype != PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_VAR)
            {
                if let Some(nu) = names_used.as_deref_mut() {
                    *nu = 1;
                }
                return Some(nsitem);
            }
            nsitem = nsitem.prev.as_deref().expect(
                "namespace chain must terminate at a LABEL before NULL during unqualified scan",
            );
        }

        // At this point `nsitem` is the block-label item terminating this
        // level. Check this level for qualified match to variable name.
        if name2.is_some() && nsitem.name == name1 {
            // for (nsitem = ns_cur; ...; nsitem = nsitem->prev)
            let mut nsitem2: &'a PLpgSQL_nsitem = cur;
            while nsitem2.itemtype != PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_LABEL {
                if nsitem2.name == name2.unwrap()
                    && (name3.is_none()
                        || nsitem2.itemtype != PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_VAR)
                {
                    if let Some(nu) = names_used.as_deref_mut() {
                        *nu = 2;
                    }
                    return Some(nsitem2);
                }
                nsitem2 = nsitem2.prev.as_deref().expect(
                    "namespace chain must terminate at a LABEL before NULL during qualified scan",
                );
            }
        }

        if localmode {
            break; // do not look into upper levels
        }

        // ns_cur = nsitem->prev;  (nsitem is the LABEL terminating this level)
        ns_cur = nsitem.prev.as_deref();
    }

    // This is just to suppress possibly-uninitialized-variable warnings
    if let Some(nu) = names_used.as_deref_mut() {
        *nu = 0;
    }
    None // No match found
}

/// `plpgsql_ns_lookup_label` — Lookup a label in the given namespace chain.
pub fn plpgsql_ns_lookup_label<'a>(
    ns_cur: &'a PLpgSQL_nsitem,
    name: &str,
) -> Option<&'a PLpgSQL_nsitem> {
    let mut ns_cur: Option<&'a PLpgSQL_nsitem> = Some(ns_cur);
    while let Some(cur) = ns_cur {
        if cur.itemtype == PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_LABEL && cur.name == name {
            return Some(cur);
        }
        ns_cur = cur.prev.as_deref();
    }

    None // label not found
}

/// `plpgsql_ns_find_nearest_loop` — Find innermost loop label in namespace chain.
pub fn plpgsql_ns_find_nearest_loop(ns_cur: &PLpgSQL_nsitem) -> Option<&PLpgSQL_nsitem> {
    let mut ns_cur: Option<&PLpgSQL_nsitem> = Some(ns_cur);
    while let Some(cur) = ns_cur {
        if cur.itemtype == PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_LABEL
            && cur.itemno == PLpgSQL_label_type::PLPGSQL_LABEL_LOOP as i32
        {
            return Some(cur);
        }
        ns_cur = cur.prev.as_deref();
    }

    None // no loop found
}

// ---------------------------------------------------------------------------
// Owned-snapshot convenience wrappers over the current namespace top.
//
// The grammar / compiler invoke each lookup as `plpgsql_ns_lookup*(
// plpgsql_ns_top(), ...)`. The borrowing API above returns references into the
// live chain, so these wrappers run the lookup against `plpgsql_ns_top()` and
// return owned values (the grammar only needs the boolean / itemno / a
// snapshot, not a live reference).
// ---------------------------------------------------------------------------

/// `plpgsql_ns_lookup(plpgsql_ns_top(), true, name, NULL, NULL, NULL) != NULL`
/// — is `name` already declared at the current (local) block level?
pub fn plpgsql_ns_lookup_local(name: &str) -> bool {
    match plpgsql_ns_top() {
        Some(top) => plpgsql_ns_lookup(&top, true, name, None, None, None).is_some(),
        None => false,
    }
}

/// `plpgsql_ns_lookup(plpgsql_ns_top(), false, name1[, name2[, name3]], NULL)`
/// for a `decl_aliasitem`, returning an owned snapshot of the matched nsitem
/// (without its `prev` chain — only the matched item's fields are needed).
pub fn plpgsql_ns_lookup_alias_snapshot(names: &[String]) -> Option<PLpgSQL_nsitem> {
    let top = plpgsql_ns_top()?;
    let name1 = names.first().map(String::as_str)?;
    let name2 = names.get(1).map(String::as_str);
    let name3 = names.get(2).map(String::as_str);
    plpgsql_ns_lookup(&top, false, name1, name2, name3, None).map(|item| PLpgSQL_nsitem {
        itemtype: item.itemtype,
        itemno: item.itemno,
        prev: None,
        name: item.name.clone(),
    })
}

/// `plpgsql_ns_lookup_label(plpgsql_ns_top(), name)` — return the label's
/// `itemno` (a [`PLpgSQL_label_type`] value) if found.
pub fn plpgsql_ns_lookup_label_itemno(name: &str) -> Option<i32> {
    let top = plpgsql_ns_top()?;
    plpgsql_ns_lookup_label(&top, name).map(|item| item.itemno)
}

/// `plpgsql_ns_find_nearest_loop(plpgsql_ns_top()) != NULL`.
pub fn plpgsql_ns_has_nearest_loop() -> bool {
    match plpgsql_ns_top() {
        Some(top) => plpgsql_ns_find_nearest_loop(&top).is_some(),
        None => false,
    }
}

/// Statement type as a string, for use in error messages etc.
/// (`plpgsql_stmt_typename`)
pub fn plpgsql_stmt_typename(stmt: &PLpgSQL_stmt) -> &'static str {
    match stmt {
        PLpgSQL_stmt::Block(_) => "statement block",
        PLpgSQL_stmt::Assign(_) => "assignment",
        PLpgSQL_stmt::If(_) => "IF",
        PLpgSQL_stmt::Case(_) => "CASE",
        PLpgSQL_stmt::Loop(_) => "LOOP",
        PLpgSQL_stmt::While(_) => "WHILE",
        PLpgSQL_stmt::Fori(_) => "FOR with integer loop variable",
        PLpgSQL_stmt::Fors(_) => "FOR over SELECT rows",
        PLpgSQL_stmt::Forc(_) => "FOR over cursor",
        PLpgSQL_stmt::ForeachA(_) => "FOREACH over array",
        PLpgSQL_stmt::Exit(s) => {
            if s.is_exit {
                "EXIT"
            } else {
                "CONTINUE"
            }
        }
        PLpgSQL_stmt::Return(_) => "RETURN",
        PLpgSQL_stmt::ReturnNext(_) => "RETURN NEXT",
        PLpgSQL_stmt::ReturnQuery(_) => "RETURN QUERY",
        PLpgSQL_stmt::Raise(_) => "RAISE",
        PLpgSQL_stmt::Assert(_) => "ASSERT",
        PLpgSQL_stmt::Execsql(_) => "SQL statement",
        PLpgSQL_stmt::Dynexecute(_) => "EXECUTE",
        PLpgSQL_stmt::Dynfors(_) => "FOR over EXECUTE statement",
        PLpgSQL_stmt::Getdiag(s) => {
            if s.is_stacked {
                "GET STACKED DIAGNOSTICS"
            } else {
                "GET DIAGNOSTICS"
            }
        }
        PLpgSQL_stmt::Open(_) => "OPEN",
        PLpgSQL_stmt::Fetch(s) => {
            if s.is_move {
                "MOVE"
            } else {
                "FETCH"
            }
        }
        PLpgSQL_stmt::Close(_) => "CLOSE",
        PLpgSQL_stmt::Perform(_) => "PERFORM",
        PLpgSQL_stmt::Call(s) => {
            if s.is_call {
                "CALL"
            } else {
                "DO"
            }
        }
        PLpgSQL_stmt::Commit(_) => "COMMIT",
        PLpgSQL_stmt::Rollback(_) => "ROLLBACK",
    }
}

/// GET DIAGNOSTICS item name as a string, for use in error messages etc.
/// (`plpgsql_getdiag_kindname`)
pub fn plpgsql_getdiag_kindname(kind: PLpgSQL_getdiag_kind) -> &'static str {
    use PLpgSQL_getdiag_kind::*;
    match kind {
        PLPGSQL_GETDIAG_ROW_COUNT => "ROW_COUNT",
        PLPGSQL_GETDIAG_ROUTINE_OID => "PG_ROUTINE_OID",
        PLPGSQL_GETDIAG_CONTEXT => "PG_CONTEXT",
        PLPGSQL_GETDIAG_ERROR_CONTEXT => "PG_EXCEPTION_CONTEXT",
        PLPGSQL_GETDIAG_ERROR_DETAIL => "PG_EXCEPTION_DETAIL",
        PLPGSQL_GETDIAG_ERROR_HINT => "PG_EXCEPTION_HINT",
        PLPGSQL_GETDIAG_RETURNED_SQLSTATE => "RETURNED_SQLSTATE",
        PLPGSQL_GETDIAG_COLUMN_NAME => "COLUMN_NAME",
        PLPGSQL_GETDIAG_CONSTRAINT_NAME => "CONSTRAINT_NAME",
        PLPGSQL_GETDIAG_DATATYPE_NAME => "PG_DATATYPE_NAME",
        PLPGSQL_GETDIAG_MESSAGE_TEXT => "MESSAGE_TEXT",
        PLPGSQL_GETDIAG_TABLE_NAME => "TABLE_NAME",
        PLPGSQL_GETDIAG_SCHEMA_NAME => "SCHEMA_NAME",
    }
}

// ---------------------------------------------------------------------------
// Support for recursing through a PL/pgSQL statement tree
//
// The point of this code is to encapsulate knowledge of where the
// sub-statements and expressions are in a statement tree, avoiding duplication
// of code. The caller supplies two callbacks, one to be invoked on statements
// and one to be invoked on expressions. (The recursion should be started by
// invoking the statement callback on function->action.) The statement callback
// should do any statement-type-specific action it needs, then recurse by
// calling `plpgsql_statement_tree_walker`. The expression callback can be a
// no-op if no per-expression behavior is needed.
//
// Faithful port note: the C callbacks take a `void *context`. Here both
// callbacks borrow shared mutable state captured in the closures; to allow each
// callback to re-enter the walker (which also needs the callbacks), the
// callbacks are passed as `&mut dyn FnMut` and the per-statement recursion is
// performed by having the *statement* callback call the walker itself, exactly
// as the C code does.
// ---------------------------------------------------------------------------

/// `plpgsql_statement_tree_walker` (`..._impl`) — visit the immediate
/// sub-statements and sub-expressions of `stmt`.
///
/// `stmt_callback` is invoked on each immediate child statement and
/// `expr_callback` on each immediate child expression. Mutable access is
/// granted to every visited node (matching the C pointers), so callers such as
/// `mark_expr`/`free_expr` can update the tree in place.
pub fn plpgsql_statement_tree_walker(
    stmt: &mut PLpgSQL_stmt,
    stmt_callback: &mut dyn FnMut(&mut PLpgSQL_stmt),
    expr_callback: &mut dyn FnMut(&mut PLpgSQL_expr),
) {
    // E_WALK on an Option<Box<PLpgSQL_expr>>: the C code happily passes NULL
    // exprs to the callback (mark_expr/free_expr both guard against NULL), so we
    // only invoke the callback when the optional is present, which is the
    // observable equivalent for those NULL-tolerant callbacks.
    macro_rules! e_walk_opt {
        ($opt:expr) => {
            if let Some(ex) = $opt.as_deref_mut() {
                expr_callback(ex);
            }
        };
    }
    macro_rules! s_list_walk {
        ($lst:expr) => {
            for st in $lst.iter_mut() {
                stmt_callback(st);
            }
        };
    }
    macro_rules! e_list_walk {
        ($lst:expr) => {
            for ex in $lst.iter_mut() {
                expr_callback(ex);
            }
        };
    }

    match stmt {
        PLpgSQL_stmt::Block(bstmt) => {
            s_list_walk!(bstmt.body);
            if let Some(exceptions) = bstmt.exceptions.as_mut() {
                for exc in exceptions.exc_list.iter_mut() {
                    // conditions list has no interesting sub-structure
                    s_list_walk!(exc.action);
                }
            }
        }
        PLpgSQL_stmt::Assign(astmt) => {
            e_walk_opt!(astmt.expr);
        }
        PLpgSQL_stmt::If(ifstmt) => {
            e_walk_opt!(ifstmt.cond);
            s_list_walk!(ifstmt.then_body);
            for elif in ifstmt.elsif_list.iter_mut() {
                e_walk_opt!(elif.cond);
                s_list_walk!(elif.stmts);
            }
            s_list_walk!(ifstmt.else_body);
        }
        PLpgSQL_stmt::Case(cstmt) => {
            e_walk_opt!(cstmt.t_expr);
            for cwt in cstmt.case_when_list.iter_mut() {
                e_walk_opt!(cwt.expr);
                s_list_walk!(cwt.stmts);
            }
            s_list_walk!(cstmt.else_stmts);
        }
        PLpgSQL_stmt::Loop(lstmt) => {
            s_list_walk!(lstmt.body);
        }
        PLpgSQL_stmt::While(wstmt) => {
            e_walk_opt!(wstmt.cond);
            s_list_walk!(wstmt.body);
        }
        PLpgSQL_stmt::Fori(fori) => {
            e_walk_opt!(fori.lower);
            e_walk_opt!(fori.upper);
            e_walk_opt!(fori.step);
            s_list_walk!(fori.body);
        }
        PLpgSQL_stmt::Fors(fors) => {
            s_list_walk!(fors.body);
            e_walk_opt!(fors.query);
        }
        PLpgSQL_stmt::Forc(forc) => {
            s_list_walk!(forc.body);
            e_walk_opt!(forc.argquery);
        }
        PLpgSQL_stmt::ForeachA(fstmt) => {
            e_walk_opt!(fstmt.expr);
            s_list_walk!(fstmt.body);
        }
        PLpgSQL_stmt::Exit(estmt) => {
            e_walk_opt!(estmt.cond);
        }
        PLpgSQL_stmt::Return(rstmt) => {
            e_walk_opt!(rstmt.expr);
        }
        PLpgSQL_stmt::ReturnNext(rstmt) => {
            e_walk_opt!(rstmt.expr);
        }
        PLpgSQL_stmt::ReturnQuery(rstmt) => {
            e_walk_opt!(rstmt.query);
            e_walk_opt!(rstmt.dynquery);
            e_list_walk!(rstmt.params);
        }
        PLpgSQL_stmt::Raise(rstmt) => {
            e_list_walk!(rstmt.params);
            for opt in rstmt.options.iter_mut() {
                e_walk_opt!(opt.expr);
            }
        }
        PLpgSQL_stmt::Assert(astmt) => {
            e_walk_opt!(astmt.cond);
            e_walk_opt!(astmt.message);
        }
        PLpgSQL_stmt::Execsql(xstmt) => {
            e_walk_opt!(xstmt.sqlstmt);
        }
        PLpgSQL_stmt::Dynexecute(dstmt) => {
            e_walk_opt!(dstmt.query);
            e_list_walk!(dstmt.params);
        }
        PLpgSQL_stmt::Dynfors(dstmt) => {
            s_list_walk!(dstmt.body);
            e_walk_opt!(dstmt.query);
            e_list_walk!(dstmt.params);
        }
        PLpgSQL_stmt::Getdiag(_) => {
            // no interesting sub-structure
        }
        PLpgSQL_stmt::Open(ostmt) => {
            e_walk_opt!(ostmt.argquery);
            e_walk_opt!(ostmt.query);
            e_walk_opt!(ostmt.dynquery);
            e_list_walk!(ostmt.params);
        }
        PLpgSQL_stmt::Fetch(fstmt) => {
            e_walk_opt!(fstmt.expr);
        }
        PLpgSQL_stmt::Close(_) => {
            // no interesting sub-structure
        }
        PLpgSQL_stmt::Perform(pstmt) => {
            e_walk_opt!(pstmt.expr);
        }
        PLpgSQL_stmt::Call(cstmt) => {
            e_walk_opt!(cstmt.expr);
        }
        PLpgSQL_stmt::Commit(_) | PLpgSQL_stmt::Rollback(_) => {
            // no interesting sub-structure
        }
    }
}

// ---------------------------------------------------------------------------
// Mark assignment source expressions that have local target variables, that is,
// the target variable is declared within the exception block most closely
// containing the assignment itself. (Such target variables need not be
// preserved if the assignment's source expression raises an error, since the
// variable will no longer be accessible afterwards. Detecting this allows
// better optimization.)
//
// This code need not be called if the plpgsql function contains no exception
// blocks, because mark_expr_as_assignment_source will have set all the flags to
// true already. Also, we need not reconsider default-value expressions for
// variables, because variable declarations are necessarily within the nearest
// exception block. (In DECLARE ... BEGIN ... EXCEPTION ... END, the variable
// initializations are done before entering the exception scope.)
//
// Within the recursion, local_dnos is a Bitmapset of dnos of variables known to
// be declared within the current exception level.
// ---------------------------------------------------------------------------

fn mark_stmt<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &mut PLpgSQL_stmt,
    local_dnos: &LocalDnos<'mcx>,
) -> PgResult<()> {
    // (The C `if (stmt == NULL) return;` guard is satisfied by the call sites
    // always passing a present statement; lists never contain null elements.)
    if let PLpgSQL_stmt::Block(block) = stmt {
        if block.exceptions.is_some() {
            // The block creates a new exception scope, so variables declared at
            // outer levels are nonlocal. For that matter, so are any variables
            // declared in the block's DECLARE section. Hence, we must pass down
            // empty local_dnos.
            let empty: LocalDnos<'mcx> = None;
            let mut err: PgResult<()> = Ok(());
            plpgsql_statement_tree_walker(
                stmt,
                &mut |s| {
                    if err.is_ok() {
                        err = mark_stmt(mcx, s, &empty);
                    }
                },
                &mut |e| mark_expr(e, &empty),
            );
            err
        } else {
            // Otherwise, the block does not create a new exception scope, and any
            // variables it declares can also be considered local within it. Note
            // that only initializable datum types (VAR, REC) are included in
            // initvarnos; but that's sufficient for our purposes.
            let mut local_dnos = bms_copy(mcx, local_dnos.as_deref())?;
            for i in 0..(block.n_initvars as usize) {
                local_dnos = Some(bms_add_member(mcx, local_dnos, block.initvarnos[i])?);
            }
            let mut err: PgResult<()> = Ok(());
            plpgsql_statement_tree_walker(
                stmt,
                &mut |s| {
                    if err.is_ok() {
                        err = mark_stmt(mcx, s, &local_dnos);
                    }
                },
                &mut |e| mark_expr(e, &local_dnos),
            );
            bms_free(local_dnos);
            err
        }
    } else {
        let mut err: PgResult<()> = Ok(());
        plpgsql_statement_tree_walker(
            stmt,
            &mut |s| {
                if err.is_ok() {
                    err = mark_stmt(mcx, s, local_dnos);
                }
            },
            &mut |e| mark_expr(e, local_dnos),
        );
        err
    }
}

fn mark_expr(expr: &mut PLpgSQL_expr, local_dnos: &LocalDnos) {
    // If this expression has an assignment target, check whether the target is
    // local, and mark the expression accordingly.
    if expr.target_param >= 0 {
        expr.target_is_local = bms_is_member(expr.target_param, local_dnos.as_deref());
    }
}

/// `plpgsql_mark_local_assignment_targets`
///
/// `mcx` is the function's compile-time allocation arena (C: `palloc` in
/// `CurrentMemoryContext`); the transient `local_dnos` bitmapsets live there.
pub fn plpgsql_mark_local_assignment_targets(
    mcx: Mcx,
    func: &mut PLpgSQL_function,
) -> PgResult<()> {
    // Function parameters can be treated as local targets at outer level
    let mut local_dnos: LocalDnos = None;
    for i in 0..(func.fn_nargs as usize) {
        local_dnos = Some(bms_add_member(mcx, local_dnos, func.fn_argvarnos[i])?);
    }
    // mark_stmt((PLpgSQL_stmt *) func->action, local_dnos);
    let result = if let Some(action) = func.action.as_mut() {
        let mut stmt = PLpgSQL_stmt::Block(core::mem::replace(action, dummy_block()));
        let r = mark_stmt(mcx, &mut stmt, &local_dnos);
        let PLpgSQL_stmt::Block(walked) = stmt else {
            unreachable!()
        };
        *action = walked;
        r
    } else {
        Ok(())
    };
    bms_free(local_dnos);
    result
}

// ---------------------------------------------------------------------------
// Release memory when a PL/pgSQL function is no longer needed
//
// This code only needs to deal with cleaning up PLpgSQL_expr nodes, which may
// contain references to saved SPI Plans that must be freed. The function tree
// itself, along with subsidiary data, is freed in one swoop by freeing the
// function's permanent memory context.
// ---------------------------------------------------------------------------

fn free_stmt(stmt: &mut PLpgSQL_stmt) -> PgResult<()> {
    // (C guards `if (stmt == NULL) return;`; non-null here by construction.)
    // Both the statement and expression callbacks can produce a SPI_freeplan
    // error; share the first one through a cell (both closures need write
    // access, so a plain captured `&mut` won't satisfy the borrow checker).
    let err: RefCell<PgResult<()>> = RefCell::new(Ok(()));
    plpgsql_statement_tree_walker(
        stmt,
        &mut |s| {
            if err.borrow().is_ok() {
                *err.borrow_mut() = free_stmt(s);
            }
        },
        &mut |e| {
            if err.borrow().is_ok() {
                *err.borrow_mut() = free_expr(e);
            }
        },
    );
    err.into_inner()
}

fn free_expr(expr: &mut PLpgSQL_expr) -> PgResult<()> {
    if let Some(plan) = expr.plan.take() {
        spi_freeplan::call(SpiPlanPtr(plan.0))?;
        // expr->plan = NULL;  (already cleared by take())
    }
    Ok(())
}

/// `plpgsql_free_function_memory`
pub fn plpgsql_free_function_memory(func: &mut PLpgSQL_function) -> PgResult<()> {
    // Better not call this on an in-use function
    assert_eq!(
        cfunc_use_count::call(func.cfunc),
        0,
        "func->cfunc.use_count == 0"
    );

    // Release plans associated with variable declarations
    for i in 0..(func.ndatums as usize) {
        let d = &mut func.datums[i];
        match d {
            // PLPGSQL_DTYPE_VAR / PLPGSQL_DTYPE_PROMISE
            PLpgSQL_datum::Var(var) => {
                if let Some(dv) = var.default_val.as_deref_mut() {
                    free_expr(dv)?;
                }
                if let Some(ce) = var.cursor_explicit_expr.as_deref_mut() {
                    free_expr(ce)?;
                }
            }
            // PLPGSQL_DTYPE_ROW
            PLpgSQL_datum::Row(_) => {}
            // PLPGSQL_DTYPE_REC
            PLpgSQL_datum::Rec(rec) => {
                if let Some(dv) = rec.default_val.as_deref_mut() {
                    free_expr(dv)?;
                }
            }
            // PLPGSQL_DTYPE_RECFIELD
            PLpgSQL_datum::Recfield(_) => {}
        }
    }
    func.ndatums = 0;

    // Release plans in statement tree
    if let Some(action) = func.action.take() {
        let mut stmt = PLpgSQL_stmt::Block(action);
        free_stmt(&mut stmt)?;
        // func->action = NULL;  (cleared by take())
    }

    // And finally, release all memory except the PLpgSQL_function struct itself
    // (which has to be kept around because there may be multiple fn_extra
    // pointers to it).
    if let Some(cxt) = func.fn_cxt.take() {
        MemoryContextDelete::call(MemoryContextHandle(cxt.0 as usize));
    }
    // func->fn_cxt = NULL;  (cleared by take())
    Ok(())
}

/// Deletion callback used by funccache.c (`plpgsql_delete_callback`).
///
/// In C this casts `CachedFunction *cfunc` back to its enclosing
/// `PLpgSQL_function *`. The funccache header is embedded as the first field of
/// `PLpgSQL_function`, so the recovered pointer is the function itself; we model
/// that by taking the function directly.
pub fn plpgsql_delete_callback(func: &mut PLpgSQL_function) -> PgResult<()> {
    plpgsql_free_function_memory(func)
}

// ---------------------------------------------------------------------------
// Debug functions for analyzing the compiled code
//
// Sadly, there doesn't seem to be any way to let plpgsql_statement_tree_walker
// bear some of the burden for this.
// ---------------------------------------------------------------------------

thread_local! {
    static DUMP_INDENT: RefCell<i32> = const { RefCell::new(0) };
}

fn dump_indent_get() -> i32 {
    DUMP_INDENT.with(|d| *d.borrow())
}

fn dump_indent_add(delta: i32) {
    DUMP_INDENT.with(|d| *d.borrow_mut() += delta);
}

fn dump_indent_set(v: i32) {
    DUMP_INDENT.with(|d| *d.borrow_mut() = v);
}

fn dump_ind() {
    for _ in 0..dump_indent_get() {
        print!(" ");
    }
}

fn dump_stmt(stmt: &PLpgSQL_stmt) {
    let lineno = stmt_lineno(stmt);
    print!("{:3}:", lineno);
    match stmt {
        PLpgSQL_stmt::Block(s) => dump_block(s),
        PLpgSQL_stmt::Assign(s) => dump_assign(s),
        PLpgSQL_stmt::If(s) => dump_if(s),
        PLpgSQL_stmt::Case(s) => dump_case(s),
        PLpgSQL_stmt::Loop(s) => dump_loop(s),
        PLpgSQL_stmt::While(s) => dump_while(s),
        PLpgSQL_stmt::Fori(s) => dump_fori(s),
        PLpgSQL_stmt::Fors(s) => dump_fors(s),
        PLpgSQL_stmt::Forc(s) => dump_forc(s),
        PLpgSQL_stmt::ForeachA(s) => dump_foreach_a(s),
        PLpgSQL_stmt::Exit(s) => dump_exit(s),
        PLpgSQL_stmt::Return(s) => dump_return(s),
        PLpgSQL_stmt::ReturnNext(s) => dump_return_next(s),
        PLpgSQL_stmt::ReturnQuery(s) => dump_return_query(s),
        PLpgSQL_stmt::Raise(s) => dump_raise(s),
        PLpgSQL_stmt::Assert(s) => dump_assert(s),
        PLpgSQL_stmt::Execsql(s) => dump_execsql(s),
        PLpgSQL_stmt::Dynexecute(s) => dump_dynexecute(s),
        PLpgSQL_stmt::Dynfors(s) => dump_dynfors(s),
        PLpgSQL_stmt::Getdiag(s) => dump_getdiag(s),
        PLpgSQL_stmt::Open(s) => dump_open(s),
        PLpgSQL_stmt::Fetch(s) => dump_fetch(s),
        PLpgSQL_stmt::Close(s) => dump_close(s),
        PLpgSQL_stmt::Perform(s) => dump_perform(s),
        PLpgSQL_stmt::Call(s) => dump_call(s),
        PLpgSQL_stmt::Commit(s) => dump_commit(s),
        PLpgSQL_stmt::Rollback(s) => dump_rollback(s),
    }
}

fn dump_stmts(stmts: &[PLpgSQL_stmt]) {
    dump_indent_add(2);
    for s in stmts {
        dump_stmt(s);
    }
    dump_indent_add(-2);
}

fn dump_block(block: &PLpgSQL_stmt_block) {
    let name: &str = match block.label.as_deref() {
        None => "*unnamed*",
        Some(l) => l,
    };

    dump_ind();
    println!("BLOCK <<{}>>", name);

    dump_stmts(&block.body);

    if let Some(exceptions) = block.exceptions.as_ref() {
        for exc in exceptions.exc_list.iter() {
            dump_ind();
            print!("    EXCEPTION WHEN ");
            let mut cond = exc.conditions.as_deref();
            let mut first = true;
            while let Some(c) = cond {
                if !first {
                    print!(" OR ");
                }
                first = false;
                print!("{}", c.condname);
                cond = c.next.as_deref();
            }
            println!(" THEN");
            dump_stmts(&exc.action);
        }
    }

    dump_ind();
    println!("    END -- {}", name);
}

fn dump_assign(stmt: &PLpgSQL_stmt_assign) {
    dump_ind();
    print!("ASSIGN var {} := ", stmt.varno);
    dump_expr(stmt.expr.as_deref().expect("ASSIGN expr"));
    println!();
}

fn dump_if(stmt: &PLpgSQL_stmt_if) {
    dump_ind();
    print!("IF ");
    dump_expr(stmt.cond.as_deref().expect("IF cond"));
    println!(" THEN");
    dump_stmts(&stmt.then_body);
    for elif in stmt.elsif_list.iter() {
        dump_ind();
        print!("    ELSIF ");
        dump_expr(elif.cond.as_deref().expect("ELSIF cond"));
        println!(" THEN");
        dump_stmts(&elif.stmts);
    }
    if !stmt.else_body.is_empty() {
        dump_ind();
        println!("    ELSE");
        dump_stmts(&stmt.else_body);
    }
    dump_ind();
    println!("    ENDIF");
}

fn dump_case(stmt: &PLpgSQL_stmt_case) {
    dump_ind();
    print!("CASE {} ", stmt.t_varno);
    if let Some(t_expr) = stmt.t_expr.as_deref() {
        dump_expr(t_expr);
    }
    println!();
    dump_indent_add(6);
    for cwt in stmt.case_when_list.iter() {
        dump_ind();
        print!("WHEN ");
        dump_expr(cwt.expr.as_deref().expect("CASE WHEN expr"));
        println!();
        dump_ind();
        println!("THEN");
        dump_indent_add(2);
        dump_stmts(&cwt.stmts);
        dump_indent_add(-2);
    }
    if stmt.have_else {
        dump_ind();
        println!("ELSE");
        dump_indent_add(2);
        dump_stmts(&stmt.else_stmts);
        dump_indent_add(-2);
    }
    dump_indent_add(-6);
    dump_ind();
    println!("    ENDCASE");
}

fn dump_loop(stmt: &PLpgSQL_stmt_loop) {
    dump_ind();
    println!("LOOP");

    dump_stmts(&stmt.body);

    dump_ind();
    println!("    ENDLOOP");
}

fn dump_while(stmt: &PLpgSQL_stmt_while) {
    dump_ind();
    print!("WHILE ");
    dump_expr(stmt.cond.as_deref().expect("WHILE cond"));
    println!();

    dump_stmts(&stmt.body);

    dump_ind();
    println!("    ENDWHILE");
}

fn dump_fori(stmt: &PLpgSQL_stmt_fori) {
    dump_ind();
    println!(
        "FORI {} {}",
        stmt.var.as_ref().expect("FORI var").refname,
        if stmt.reverse != 0 {
            "REVERSE"
        } else {
            "NORMAL"
        }
    );

    dump_indent_add(2);
    dump_ind();
    print!("    lower = ");
    dump_expr(stmt.lower.as_deref().expect("FORI lower"));
    println!();
    dump_ind();
    print!("    upper = ");
    dump_expr(stmt.upper.as_deref().expect("FORI upper"));
    println!();
    if let Some(step) = stmt.step.as_deref() {
        dump_ind();
        print!("    step = ");
        dump_expr(step);
        println!();
    }
    dump_indent_add(-2);

    dump_stmts(&stmt.body);

    dump_ind();
    println!("    ENDFORI");
}

fn dump_fors(stmt: &PLpgSQL_stmt_fors) {
    dump_ind();
    print!(
        "FORS {} ",
        variable_refname(stmt.var.as_deref().expect("FORS var"))
    );
    dump_expr(stmt.query.as_deref().expect("FORS query"));
    println!();

    dump_stmts(&stmt.body);

    dump_ind();
    println!("    ENDFORS");
}

fn dump_forc(stmt: &PLpgSQL_stmt_forc) {
    dump_ind();
    print!(
        "FORC {} ",
        variable_refname(stmt.var.as_deref().expect("FORC var"))
    );
    println!("curvar={}", stmt.curvar);

    dump_indent_add(2);
    if let Some(argquery) = stmt.argquery.as_deref() {
        dump_ind();
        print!("  arguments = ");
        dump_expr(argquery);
        println!();
    }
    dump_indent_add(-2);

    dump_stmts(&stmt.body);

    dump_ind();
    println!("    ENDFORC");
}

fn dump_foreach_a(stmt: &PLpgSQL_stmt_foreach_a) {
    dump_ind();
    print!("FOREACHA var {} ", stmt.varno);
    if stmt.slice != 0 {
        print!("SLICE {} ", stmt.slice);
    }
    print!("IN ");
    dump_expr(stmt.expr.as_deref().expect("FOREACH_A expr"));
    println!();

    dump_stmts(&stmt.body);

    dump_ind();
    print!("    ENDFOREACHA");
}

fn dump_open(stmt: &PLpgSQL_stmt_open) {
    dump_ind();
    println!("OPEN curvar={}", stmt.curvar);

    dump_indent_add(2);
    if let Some(argquery) = stmt.argquery.as_deref() {
        dump_ind();
        print!("  arguments = '");
        dump_expr(argquery);
        println!("'");
    }
    if let Some(query) = stmt.query.as_deref() {
        dump_ind();
        print!("  query = '");
        dump_expr(query);
        println!("'");
    }
    if let Some(dynquery) = stmt.dynquery.as_deref() {
        dump_ind();
        print!("  execute = '");
        dump_expr(dynquery);
        println!("'");

        if !stmt.params.is_empty() {
            dump_indent_add(2);
            dump_ind();
            println!("    USING");
            dump_indent_add(2);
            let mut i = 1;
            for p in stmt.params.iter() {
                dump_ind();
                print!("    parameter ${}: ", i);
                i += 1;
                dump_expr(p);
                println!();
            }
            dump_indent_add(-4);
        }
    }
    dump_indent_add(-2);
}

fn dump_fetch(stmt: &PLpgSQL_stmt_fetch) {
    dump_ind();

    if !stmt.is_move {
        println!("FETCH curvar={}", stmt.curvar);
        dump_cursor_direction(stmt);

        dump_indent_add(2);
        if let Some(target) = stmt.target.as_deref() {
            dump_ind();
            println!("    target = {} {}", target.dno, target.refname);
        }
        dump_indent_add(-2);
    } else {
        println!("MOVE curvar={}", stmt.curvar);
        dump_cursor_direction(stmt);
    }
}

fn dump_cursor_direction(stmt: &PLpgSQL_stmt_fetch) {
    dump_indent_add(2);
    dump_ind();
    match stmt.direction {
        FetchDirection::FETCH_FORWARD => print!("    FORWARD "),
        FetchDirection::FETCH_BACKWARD => print!("    BACKWARD "),
        FetchDirection::FETCH_ABSOLUTE => print!("    ABSOLUTE "),
        FetchDirection::FETCH_RELATIVE => print!("    RELATIVE "),
    }

    if let Some(expr) = stmt.expr.as_deref() {
        dump_expr(expr);
        println!();
    } else {
        println!("{}", stmt.how_many);
    }

    dump_indent_add(-2);
}

fn dump_close(stmt: &PLpgSQL_stmt_close) {
    dump_ind();
    println!("CLOSE curvar={}", stmt.curvar);
}

fn dump_perform(stmt: &PLpgSQL_stmt_perform) {
    dump_ind();
    print!("PERFORM expr = ");
    dump_expr(stmt.expr.as_deref().expect("PERFORM expr"));
    println!();
}

fn dump_call(stmt: &PLpgSQL_stmt_call) {
    dump_ind();
    print!("{} expr = ", if stmt.is_call { "CALL" } else { "DO" });
    dump_expr(stmt.expr.as_deref().expect("CALL expr"));
    println!();
}

fn dump_commit(stmt: &PLpgSQL_stmt_commit) {
    dump_ind();
    if stmt.chain {
        println!("COMMIT AND CHAIN");
    } else {
        println!("COMMIT");
    }
}

fn dump_rollback(stmt: &PLpgSQL_stmt_rollback) {
    dump_ind();
    if stmt.chain {
        println!("ROLLBACK AND CHAIN");
    } else {
        println!("ROLLBACK");
    }
}

fn dump_exit(stmt: &PLpgSQL_stmt_exit) {
    dump_ind();
    print!("{}", if stmt.is_exit { "EXIT" } else { "CONTINUE" });
    if let Some(label) = stmt.label.as_deref() {
        print!(" label='{}'", label);
    }
    if let Some(cond) = stmt.cond.as_deref() {
        print!(" WHEN ");
        dump_expr(cond);
    }
    println!();
}

fn dump_return(stmt: &PLpgSQL_stmt_return) {
    dump_ind();
    print!("RETURN ");
    if stmt.retvarno >= 0 {
        print!("variable {}", stmt.retvarno);
    } else if let Some(expr) = stmt.expr.as_deref() {
        dump_expr(expr);
    } else {
        print!("NULL");
    }
    println!();
}

fn dump_return_next(stmt: &PLpgSQL_stmt_return_next) {
    dump_ind();
    print!("RETURN NEXT ");
    if stmt.retvarno >= 0 {
        print!("variable {}", stmt.retvarno);
    } else if let Some(expr) = stmt.expr.as_deref() {
        dump_expr(expr);
    } else {
        print!("NULL");
    }
    println!();
}

fn dump_return_query(stmt: &PLpgSQL_stmt_return_query) {
    dump_ind();
    if let Some(query) = stmt.query.as_deref() {
        print!("RETURN QUERY ");
        dump_expr(query);
        println!();
    } else {
        print!("RETURN QUERY EXECUTE ");
        dump_expr(stmt.dynquery.as_deref().expect("RETURN QUERY dynquery"));
        println!();
        if !stmt.params.is_empty() {
            dump_indent_add(2);
            dump_ind();
            println!("    USING");
            dump_indent_add(2);
            let mut i = 1;
            for p in stmt.params.iter() {
                dump_ind();
                print!("    parameter ${}: ", i);
                i += 1;
                dump_expr(p);
                println!();
            }
            dump_indent_add(-4);
        }
    }
}

fn dump_raise(stmt: &PLpgSQL_stmt_raise) {
    let mut i = 0;

    dump_ind();
    print!("RAISE level={}", stmt.elog_level);
    if let Some(condname) = stmt.condname.as_deref() {
        print!(" condname='{}'", condname);
    }
    if let Some(message) = stmt.message.as_deref() {
        print!(" message='{}'", message);
    }
    println!();
    dump_indent_add(2);
    for p in stmt.params.iter() {
        dump_ind();
        print!("    parameter {}: ", i);
        i += 1;
        dump_expr(p);
        println!();
    }
    if !stmt.options.is_empty() {
        dump_ind();
        println!("    USING");
        dump_indent_add(2);
        for opt in stmt.options.iter() {
            dump_ind();
            use PLpgSQL_raise_option_type::*;
            match opt.opt_type {
                PLPGSQL_RAISEOPTION_ERRCODE => print!("    ERRCODE = "),
                PLPGSQL_RAISEOPTION_MESSAGE => print!("    MESSAGE = "),
                PLPGSQL_RAISEOPTION_DETAIL => print!("    DETAIL = "),
                PLPGSQL_RAISEOPTION_HINT => print!("    HINT = "),
                PLPGSQL_RAISEOPTION_COLUMN => print!("    COLUMN = "),
                PLPGSQL_RAISEOPTION_CONSTRAINT => print!("    CONSTRAINT = "),
                PLPGSQL_RAISEOPTION_DATATYPE => print!("    DATATYPE = "),
                PLPGSQL_RAISEOPTION_TABLE => print!("    TABLE = "),
                PLPGSQL_RAISEOPTION_SCHEMA => print!("    SCHEMA = "),
            }
            dump_expr(opt.expr.as_deref().expect("RAISE option expr"));
            println!();
        }
        dump_indent_add(-2);
    }
    dump_indent_add(-2);
}

fn dump_assert(stmt: &PLpgSQL_stmt_assert) {
    dump_ind();
    print!("ASSERT ");
    dump_expr(stmt.cond.as_deref().expect("ASSERT cond"));
    println!();

    dump_indent_add(2);
    if let Some(message) = stmt.message.as_deref() {
        dump_ind();
        print!("    MESSAGE = ");
        dump_expr(message);
        println!();
    }
    dump_indent_add(-2);
}

fn dump_execsql(stmt: &PLpgSQL_stmt_execsql) {
    dump_ind();
    print!("EXECSQL ");
    dump_expr(stmt.sqlstmt.as_deref().expect("EXECSQL sqlstmt"));
    println!();

    dump_indent_add(2);
    if let Some(target) = stmt.target.as_deref() {
        dump_ind();
        println!(
            "    INTO{} target = {} {}",
            if stmt.strict { " STRICT" } else { "" },
            target.dno,
            target.refname
        );
    }
    dump_indent_add(-2);
}

fn dump_dynexecute(stmt: &PLpgSQL_stmt_dynexecute) {
    dump_ind();
    print!("EXECUTE ");
    dump_expr(stmt.query.as_deref().expect("DYNEXECUTE query"));
    println!();

    dump_indent_add(2);
    if let Some(target) = stmt.target.as_deref() {
        dump_ind();
        println!(
            "    INTO{} target = {} {}",
            if stmt.strict { " STRICT" } else { "" },
            target.dno,
            target.refname
        );
    }
    if !stmt.params.is_empty() {
        dump_ind();
        println!("    USING");
        dump_indent_add(2);
        let mut i = 1;
        for p in stmt.params.iter() {
            dump_ind();
            print!("    parameter {}: ", i);
            i += 1;
            dump_expr(p);
            println!();
        }
        dump_indent_add(-2);
    }
    dump_indent_add(-2);
}

fn dump_dynfors(stmt: &PLpgSQL_stmt_dynfors) {
    dump_ind();
    print!(
        "FORS {} EXECUTE ",
        variable_refname(stmt.var.as_deref().expect("DYNFORS var"))
    );
    dump_expr(stmt.query.as_deref().expect("DYNFORS query"));
    println!();
    if !stmt.params.is_empty() {
        dump_indent_add(2);
        dump_ind();
        println!("    USING");
        dump_indent_add(2);
        let mut i = 1;
        for p in stmt.params.iter() {
            dump_ind();
            print!("    parameter ${}: ", i);
            i += 1;
            dump_expr(p);
            println!();
        }
        dump_indent_add(-4);
    }
    dump_stmts(&stmt.body);
    dump_ind();
    println!("    ENDFORS");
}

fn dump_getdiag(stmt: &PLpgSQL_stmt_getdiag) {
    dump_ind();
    print!(
        "GET {} DIAGNOSTICS ",
        if stmt.is_stacked { "STACKED" } else { "CURRENT" }
    );
    for (idx, diag_item) in stmt.diag_items.iter().enumerate() {
        if idx != 0 {
            print!(", ");
        }
        print!(
            "{{var {}}} = {}",
            diag_item.target,
            plpgsql_getdiag_kindname(diag_item.kind)
        );
    }
    println!();
}

fn dump_expr(expr: &PLpgSQL_expr) {
    print!("'{}'", expr.query);
    if expr.target_param >= 0 {
        print!(
            " target {}{}",
            expr.target_param,
            if expr.target_is_local { " (local)" } else { "" }
        );
    }
}

/// `plpgsql_dumptree`
pub fn plpgsql_dumptree(func: &PLpgSQL_function) {
    println!(
        "\nExecution tree of successfully compiled PL/pgSQL function {}:",
        func.fn_signature
    );

    println!("\nFunction's data area:");
    for i in 0..(func.ndatums as usize) {
        let d = &func.datums[i];

        print!("    entry {}: ", i);
        match d {
            // PLPGSQL_DTYPE_VAR / PLPGSQL_DTYPE_PROMISE
            PLpgSQL_datum::Var(var) => {
                let datatype = var.datatype.as_deref().expect("VAR datatype");
                println!(
                    "VAR {:<16} type {} (typoid {}) atttypmod {}",
                    var.refname, datatype.typname, datatype.typoid, datatype.atttypmod
                );
                if var.isconst {
                    println!("                                  CONSTANT");
                }
                if var.notnull {
                    println!("                                  NOT NULL");
                }
                if let Some(default_val) = var.default_val.as_deref() {
                    print!("                                  DEFAULT ");
                    dump_expr(default_val);
                    println!();
                }
                if let Some(cursor_explicit_expr) = var.cursor_explicit_expr.as_deref() {
                    if var.cursor_explicit_argrow >= 0 {
                        println!(
                            "                                  CURSOR argument row {}",
                            var.cursor_explicit_argrow
                        );
                    }

                    print!("                                  CURSOR IS ");
                    dump_expr(cursor_explicit_expr);
                    println!();
                }
                if var.promise != PLpgSQL_promise_type::PLPGSQL_PROMISE_NONE {
                    println!(
                        "                                  PROMISE {}",
                        var.promise as i32
                    );
                }
            }
            // PLPGSQL_DTYPE_ROW
            PLpgSQL_datum::Row(row) => {
                print!("ROW {:<16} fields", row.refname);
                for j in 0..(row.nfields as usize) {
                    print!(" {}=var {}", row.fieldnames[j], row.varnos[j]);
                }
                println!();
            }
            // PLPGSQL_DTYPE_REC
            PLpgSQL_datum::Rec(rec) => {
                println!("REC {:<16} typoid {}", rec.refname, rec.rectypeid);
                if rec.isconst {
                    println!("                                  CONSTANT");
                }
                if rec.notnull {
                    println!("                                  NOT NULL");
                }
                if let Some(default_val) = rec.default_val.as_deref() {
                    print!("                                  DEFAULT ");
                    dump_expr(default_val);
                    println!();
                }
            }
            // PLPGSQL_DTYPE_RECFIELD
            PLpgSQL_datum::Recfield(recfield) => {
                println!(
                    "RECFIELD {:<16} of REC {}",
                    recfield.fieldname, recfield.recparentno
                );
            }
        }
    }
    println!("\nFunction's statements:");

    dump_indent_set(0);
    let action = func.action.as_deref().expect("func->action");
    print!("{:3}:", action.lineno);
    dump_block(action);
    println!("\nEnd of execution tree of function {}\n", func.fn_signature);
    // fflush(stdout): Rust's stdout is line-buffered/auto-flushed; the trailing
    // newlines above already flush each line. An explicit flush keeps parity.
    use std::io::Write;
    let _ = std::io::stdout().flush();
}

// ---------------------------------------------------------------------------
// Small helpers bridging the owned-enum tree to the C struct-punning idioms.
// ---------------------------------------------------------------------------

/// Read the `lineno` header field of any statement variant (the C code reaches
/// through the `PLpgSQL_stmt *` supertype).
fn stmt_lineno(stmt: &PLpgSQL_stmt) -> i32 {
    match stmt {
        PLpgSQL_stmt::Block(s) => s.lineno,
        PLpgSQL_stmt::Assign(s) => s.lineno,
        PLpgSQL_stmt::If(s) => s.lineno,
        PLpgSQL_stmt::Case(s) => s.lineno,
        PLpgSQL_stmt::Loop(s) => s.lineno,
        PLpgSQL_stmt::While(s) => s.lineno,
        PLpgSQL_stmt::Fori(s) => s.lineno,
        PLpgSQL_stmt::Fors(s) => s.lineno,
        PLpgSQL_stmt::Forc(s) => s.lineno,
        PLpgSQL_stmt::ForeachA(s) => s.lineno,
        PLpgSQL_stmt::Exit(s) => s.lineno,
        PLpgSQL_stmt::Return(s) => s.lineno,
        PLpgSQL_stmt::ReturnNext(s) => s.lineno,
        PLpgSQL_stmt::ReturnQuery(s) => s.lineno,
        PLpgSQL_stmt::Raise(s) => s.lineno,
        PLpgSQL_stmt::Assert(s) => s.lineno,
        PLpgSQL_stmt::Execsql(s) => s.lineno,
        PLpgSQL_stmt::Dynexecute(s) => s.lineno,
        PLpgSQL_stmt::Dynfors(s) => s.lineno,
        PLpgSQL_stmt::Getdiag(s) => s.lineno,
        PLpgSQL_stmt::Open(s) => s.lineno,
        PLpgSQL_stmt::Fetch(s) => s.lineno,
        PLpgSQL_stmt::Close(s) => s.lineno,
        PLpgSQL_stmt::Perform(s) => s.lineno,
        PLpgSQL_stmt::Call(s) => s.lineno,
        PLpgSQL_stmt::Commit(s) => s.lineno,
        PLpgSQL_stmt::Rollback(s) => s.lineno,
    }
}

/// Read the shared `PLpgSQL_variable` header `refname` field (the C code reaches
/// through `((PLpgSQL_var *) var)->refname`, valid because var/row/rec share the
/// `PLpgSQL_variable` prefix).
fn variable_refname(var: &PLpgSQL_variable) -> &str {
    &var.refname
}

/// Placeholder block used to temporarily move out of `func.action` while
/// rewriting it in place during `plpgsql_mark_local_assignment_targets`.
fn dummy_block() -> Box<PLpgSQL_stmt_block> {
    Box::new(PLpgSQL_stmt_block {
        cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_BLOCK,
        lineno: 0,
        stmtid: 0,
        label: None,
        body: Vec::new(),
        n_initvars: 0,
        initvarnos: Vec::new(),
        exceptions: None,
    })
}
