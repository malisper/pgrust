//! `bootstrap/bootparse.y` + the `boot_yylex_init` driver from
//! `bootstrap/bootscanner.l` — the BKI bootstrap-language front end.
//!
//! This is a faithful port of the bison grammar (`bootparse.y`).  Because the
//! bootstrap backend is single-threaded, the C `yyscan_t` reentrant scanner is
//! modelled as a process-local (`thread_local`) [`BootScanner`].  `boot_yylex_init`
//! slurps the BKI input stream (`yyin`, default `stdin`) into the scanner's owned
//! buffer; `boot_yyparse` runs the recursive-descent equivalent of the LALR
//! parser, driving the catalog-loader callbacks in `backend-bootstrap-bootstrap`.
//!
//! The grammar's per-line working allocations (the C `per_line_ctx`, created
//! under `CurTransactionContext`) are made in the process/transaction memory
//! context threaded in as `mcx: Mcx<'static>`.  The owned model has no separate
//! resettable per-line context, so `do_start` / `do_end` reduce to the
//! `CHECK_FOR_INTERRUPTS()` + interactive-prompt bookkeeping; the working
//! values are dropped naturally at the end of each statement.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::RefCell;
use std::io::Read;

use backend_utils_error::ereport;
use mcx::{Mcx, PgString, PgVec};
use types_error::{ErrorLocation, PgResult};
use types_error::{DEBUG4, ERROR, FATAL};

use backend_bootstrap_bootscanner::{self as scanner, BootScanner, BootToken, BootTokenKind};

const FILE: &str = "bootparse.y";

fn loc(lineno: i32, funcname: &str) -> ErrorLocation {
    ErrorLocation::new(FILE, lineno, funcname)
}

/* ----------------------------------------------------------------
 *		constants from headers (referenced by the grammar actions)
 * ---------------------------------------------------------------- */

/// `HEAP_TABLE_AM_OID` (`catalog/pg_am_d.h`) — the heap table AM's OID.
const HEAP_TABLE_AM_OID: types_core::Oid = 2;

/* =========================================================================
 * Process-local scanner state (the C reentrant `yyscan_t`).
 *
 * The bootstrap backend is single-threaded; the scanner created by
 * `boot_yylex_init` lives for the duration of one `boot_yyparse` call.
 * ========================================================================= */

thread_local! {
    static SCANNER: RefCell<Option<BootScanner>> = const { RefCell::new(None) };
}

// `num_columns_read` (bootparse.y file-static) — the count of values read for
// the in-progress `INSERT_TUPLE`.
thread_local! {
    static NUM_COLUMNS_READ: RefCell<i32> = const { RefCell::new(0) };
}

fn num_columns_read() -> i32 {
    NUM_COLUMNS_READ.with(|c| *c.borrow())
}
fn set_num_columns_read(v: i32) {
    NUM_COLUMNS_READ.with(|c| *c.borrow_mut() = v);
}

/* =========================================================================
 * do_start / do_end — the grammar's per-line context bookkeeping.
 * ========================================================================= */

/// `do_start()` (bootparse.y): switch to the per-line working context.  In the
/// owned model there is no separate resettable arena, so this is a no-op that
/// preserves the call-site structure.
fn do_start() {
    /* per_line_ctx switch: owned-model no-op (values dropped at statement end). */
}

/// `do_end()` (bootparse.y): reclaim per-line memory, check for interrupts, and
/// emit the interactive `bootstrap>` prompt when reading from a tty.
fn do_end() -> PgResult<()> {
    /* MemoryContextReset(per_line_ctx): owned-model no-op. */
    /* CHECK_FOR_INTERRUPTS() — allow SIGINT to kill bootstrap run. */
    // (No cross-unit interrupt seam is reached on this branch; the bootstrap
    // backend's signal handling is installed in bootstrap.c's bootstrap_signals.)
    /*
     * The interactive `bootstrap>` prompt (printf when isatty(0)) is suppressed:
     * initdb always pipes the BKI input, so stdin is never a tty in practice.
     */
    Ok(())
}

/* =========================================================================
 * Scanner driver: boot_yylex_init / yylex helpers.
 * ========================================================================= */

/// `boot_yylex_init` (bootscanner.l driver, called from `BootstrapModeMain`):
/// initialize the reentrant scanner over the BKI input stream.  Returns the C
/// nonzero error code on failure (bootstrap.c `elog(ERROR)`s when nonzero);
/// `0` on success.
///
/// The C scanner reads from `yyin` (default `stdin`).  The owned scanner buffers
/// the whole input up front; a read error yields a nonzero return so the caller
/// `elog(ERROR)`s, matching `yylex_init`'s failure surface.
fn boot_yylex_init() -> i32 {
    let mut input = String::new();
    if std::io::stdin().read_to_string(&mut input).is_err() {
        /* yylex_init() failed (nonzero) — caller elog(ERROR)s. */
        return 1;
    }
    SCANNER.with(|s| *s.borrow_mut() = Some(scanner::boot_scanner_init(input)));
    0
}

/// `boot_yylex(yyscanner)` — fetch the next token from the process-local
/// scanner.  Mirrors the bison `yylex` call.
fn yylex() -> PgResult<BootToken> {
    SCANNER.with(|s| {
        let mut borrow = s.borrow_mut();
        let sc = borrow
            .as_mut()
            .expect("boot_yylex_init must run before boot_yyparse");
        scanner::boot_yylex(sc)
    })
}

/// `boot_yyerror(yyscanner, message)` — `elog(ERROR, "%s at line %d", ...)`.
fn yyerror(message: &str) -> PgResult<()> {
    SCANNER.with(|s| {
        let borrow = s.borrow();
        let sc = borrow
            .as_ref()
            .expect("boot_yylex_init must run before boot_yyparse");
        scanner::boot_yyerror(sc, message)
    })
}

/// `atooid(x)` (`postgres_ext.h`: `((Oid) strtoul((x), NULL, 10))`).
fn atooid(s: &str) -> types_core::Oid {
    // strtoul stops at the first non-digit and never errors here; mirror it.
    let mut acc: u64 = 0;
    for c in s.bytes() {
        if c.is_ascii_digit() {
            acc = acc.wrapping_mul(10).wrapping_add((c - b'0') as u64);
        } else {
            break;
        }
    }
    acc as u32
}

/* =========================================================================
 * Recursive-descent parser: a faithful expansion of the LALR grammar.
 *
 * Every statement is uniquely distinguished by its leading token, so the LR
 * automaton reduces cleanly to recursive descent with one-token lookahead.
 * ========================================================================= */

/// The token-stream cursor: holds the current lookahead token, mirroring
/// bison's single-token lookahead (`yychar`).
struct Parser {
    /// The current lookahead token (bison `yychar`); `None` once consumed.
    tok: Option<BootToken>,
}

impl Parser {
    fn new() -> PgResult<Self> {
        let tok = yylex()?;
        Ok(Parser { tok: Some(tok) })
    }

    /// Peek at the current lookahead token's kind.
    fn peek(&self) -> BootTokenKind {
        self.tok
            .as_ref()
            .map(|t| t.kind)
            .unwrap_or(BootTokenKind::Eof)
    }

    /// Consume and return the current token, advancing the lookahead.
    fn advance(&mut self) -> PgResult<BootToken> {
        let cur = self.tok.take().expect("advance past EOF");
        if cur.kind != BootTokenKind::Eof {
            self.tok = Some(yylex()?);
        } else {
            self.tok = Some(cur.clone());
        }
        Ok(cur)
    }

    /// Require a token of exactly `kind`, consuming it; bison `yyerror` on
    /// mismatch.
    fn expect(&mut self, kind: BootTokenKind) -> PgResult<BootToken> {
        if self.peek() == kind {
            self.advance()
        } else {
            yyerror("syntax error")?;
            unreachable!("yyerror returns Err");
        }
    }

    /* ---- boot_ident: ID or any unreserved keyword (returns its text) ---- */

    /// `boot_ident` — an `ID`, or any unreserved keyword used as an identifier
    /// (`OPEN`, `XCLOSE`, ... `XNULL`).  Mirrors the grammar's `boot_ident`
    /// alternatives (`ID { $$ = $1 }` / keyword `{ $$ = pstrdup($1) }`).
    fn boot_ident(&mut self) -> PgResult<String> {
        match self.peek() {
            BootTokenKind::Id
            | BootTokenKind::Open
            | BootTokenKind::Close
            | BootTokenKind::Create
            | BootTokenKind::InsertTuple
            | BootTokenKind::Declare
            | BootTokenKind::Index
            | BootTokenKind::On
            | BootTokenKind::Using
            | BootTokenKind::Build
            | BootTokenKind::Indices
            | BootTokenKind::Unique
            | BootTokenKind::Toast
            | BootTokenKind::ObjId
            | BootTokenKind::Bootstrap
            | BootTokenKind::SharedRelation
            | BootTokenKind::RowtypeOid
            | BootTokenKind::Force
            | BootTokenKind::Not
            | BootTokenKind::Null => {
                let t = self.advance()?;
                // ID carries its `str`; keywords carry their constant `kw`
                // string. Either way `boot_ident` yields the text (pstrdup'd in
                // C; an owned String here).
                Ok(t.semantic_text().unwrap_or("").to_string())
            }
            _ => {
                yyerror("syntax error")?;
                unreachable!("yyerror returns Err");
            }
        }
    }

    /// `oidspec: boot_ident { $$ = atooid($1); }`.
    fn oidspec(&mut self) -> PgResult<types_core::Oid> {
        let id = self.boot_ident()?;
        Ok(atooid(&id))
    }
}

/* =========================================================================
 * boot_yyparse — the grammar entry point.
 * ========================================================================= */

/// `boot_yyparse(yyscanner)` (bootparse.y): parse the BKI input stream, driving
/// the catalog-loader callbacks.
fn boot_yyparse(mcx: Mcx<'static>) -> PgResult<()> {
    let mut p = Parser::new()?;

    /* TopLevel: Boot_Queries | (empty) */
    /* Boot_Queries: Boot_Query | Boot_Queries Boot_Query */
    loop {
        match p.peek() {
            BootTokenKind::Eof => break,
            BootTokenKind::Open => boot_open_stmt(mcx, &mut p)?,
            BootTokenKind::Close => boot_close_stmt(mcx, &mut p)?,
            BootTokenKind::Create => boot_create_stmt(mcx, &mut p)?,
            BootTokenKind::InsertTuple => boot_insert_stmt(mcx, &mut p)?,
            BootTokenKind::Declare => boot_declare_stmt(mcx, &mut p)?,
            BootTokenKind::Build => boot_build_inds_stmt(mcx, &mut p)?,
            _ => {
                yyerror("syntax error")?;
                unreachable!("yyerror returns Err");
            }
        }
    }

    Ok(())
}

/* ---- Boot_OpenStmt: OPEN boot_ident ---- */
fn boot_open_stmt(mcx: Mcx<'static>, p: &mut Parser) -> PgResult<()> {
    p.expect(BootTokenKind::Open)?;
    let name = p.boot_ident()?;

    do_start();
    backend_bootstrap_bootstrap::boot_openrel(mcx, &name)?;
    do_end()?;
    Ok(())
}

/* ---- Boot_CloseStmt: XCLOSE boot_ident ---- */
fn boot_close_stmt(mcx: Mcx<'static>, p: &mut Parser) -> PgResult<()> {
    p.expect(BootTokenKind::Close)?;
    let name = p.boot_ident()?;

    do_start();
    backend_bootstrap_bootstrap::closerel(mcx, Some(&name))?;
    do_end()?;
    Ok(())
}

/* ----------------------------------------------------------------
 * Boot_CreateStmt:
 *   XCREATE boot_ident oidspec optbootstrap optsharedrelation optrowtypeoid
 *   LPAREN boot_column_list RPAREN
 * ---------------------------------------------------------------- */
fn boot_create_stmt(mcx: Mcx<'static>, p: &mut Parser) -> PgResult<()> {
    p.expect(BootTokenKind::Create)?;
    let relname = p.boot_ident()?; // $2
    let relid = p.oidspec()?; // $3
    let optbootstrap = opt_bootstrap(p)?; // $4
    let optsharedrelation = opt_shared_relation(p)?; // $5
    let optrowtypeoid = opt_rowtype_oid(p)?; // $6

    p.expect(BootTokenKind::LeftParen)?;

    /* mid-rule action after LPAREN */
    do_start();
    backend_bootstrap_bootstrap::set_numattr(0);
    ereport(DEBUG4)
        .errmsg_internal(format!(
            "creating{}{} relation {} {}",
            if optbootstrap != 0 { " bootstrap" } else { "" },
            if optsharedrelation != 0 { " shared" } else { "" },
            relname,
            relid
        ))
        .finish(loc(162, "Boot_CreateStmt"))?;

    boot_column_list(mcx, p)?;

    /* action before RPAREN */
    do_end()?;

    p.expect(BootTokenKind::RightParen)?;

    /* final action */
    do_start();

    let numattr = backend_bootstrap_bootstrap::numattr();
    let mut attrs: Vec<types_tuple::heaptuple::FormData_pg_attribute> =
        Vec::with_capacity(numattr as usize);
    for i in 0..numattr as usize {
        attrs.push(
            backend_bootstrap_bootstrap::attrtypes(i)
                .expect("attrtypes[i] set by DefineAttr for i < numattr"),
        );
    }
    let tupdesc = backend_access_common_tupdesc::CreateTupleDesc(mcx, &attrs)?;

    let shared_relation = optsharedrelation != 0;

    /*
     * The catalogs that use the relation mapper are the bootstrap catalogs plus
     * the shared catalogs.
     */
    let mapped_relation = optbootstrap != 0 || shared_relation;

    if optbootstrap != 0 {
        if backend_bootstrap_bootstrap::boot_reldesc_is_open() {
            ereport(DEBUG4)
                .errmsg_internal(
                    "create bootstrap: warning, open relation exists, closing first",
                )
                .finish(loc(202, "Boot_CreateStmt"))?;
            backend_bootstrap_bootstrap::closerel(mcx, None)?;
        }

        let res = backend_catalog_heap::heap_create(
            mcx,
            &relname,
            types_core::catalog::PG_CATALOG_NAMESPACE,
            if shared_relation {
                types_catalog::catalog::GLOBALTABLESPACE_OID
            } else {
                0
            },
            relid,
            types_core::primitive::InvalidOid, // relfilenumber (InvalidOid)
            HEAP_TABLE_AM_OID,
            &tupdesc,
            types_tuple::access::RELKIND_RELATION,
            types_core::catalog::RELPERSISTENCE_PERMANENT,
            shared_relation,
            mapped_relation,
            true, // allow_system_table_mods
            true, // create_storage
        )?;

        /*
         * C: `boot_reldesc = heap_create(...)` — store the just-created open
         * relation.  In this repo heap_create returns the new relcache entry's
         * OID (the entry persists, registry-owned); re-open it with NoLock to
         * obtain the owned Relation value to stash in boot_reldesc, mirroring
         * heap_create's own internal relation_open(NoLock) refcount bump.
         */
        let rel = backend_access_common_relation::relation_open(
            mcx,
            res.rel,
            types_storage::lock::NoLock,
        )?;
        backend_bootstrap_bootstrap::set_boot_reldesc(Some(rel));

        ereport(DEBUG4)
            .errmsg_internal("bootstrap relation created")
            .finish(loc(221, "Boot_CreateStmt"))?;
    } else {
        let id = backend_catalog_heap_seams::heap_create_with_catalog::call(
            backend_catalog_heap_seams::HeapCreateWithCatalogArgs {
                relname: relname.clone(),
                relnamespace: types_core::catalog::PG_CATALOG_NAMESPACE,
                reltablespace: if shared_relation {
                    types_catalog::catalog::GLOBALTABLESPACE_OID
                } else {
                    0
                },
                relid,
                reltypeid: optrowtypeoid,
                reloftypeid: types_core::primitive::InvalidOid,
                ownerid: types_core::catalog::BOOTSTRAP_SUPERUSERID,
                accessmtd: HEAP_TABLE_AM_OID,
                tupdesc,
                relkind: types_tuple::access::RELKIND_RELATION,
                relpersistence: types_core::catalog::RELPERSISTENCE_PERMANENT,
                shared_relation,
                mapped_relation,
                oncommit: types_nodes::primnodes::OnCommitAction::ONCOMMIT_NOOP,
                reloptions: types_cluster::RelOptionsToken {
                    // (Datum) 0 — NULL reloptions.
                    is_null: true,
                    bytes: Vec::new(),
                },
                use_user_acl: false,
                allow_system_table_mods: true,
                is_internal: false,
                relrewrite: types_core::primitive::InvalidOid,
            },
        )?;

        ereport(DEBUG4)
            .errmsg_internal(format!("relation created with OID {}", id))
            .finish(loc(248, "Boot_CreateStmt"))?;
    }
    do_end()?;
    Ok(())
}

/* ---- optbootstrap: XBOOTSTRAP {1} | {0} ---- */
fn opt_bootstrap(p: &mut Parser) -> PgResult<i32> {
    if p.peek() == BootTokenKind::Bootstrap {
        p.advance()?;
        Ok(1)
    } else {
        Ok(0)
    }
}

/* ---- optsharedrelation: XSHARED_RELATION {1} | {0} ---- */
fn opt_shared_relation(p: &mut Parser) -> PgResult<i32> {
    if p.peek() == BootTokenKind::SharedRelation {
        p.advance()?;
        Ok(1)
    } else {
        Ok(0)
    }
}

/* ---- optrowtypeoid: XROWTYPE_OID oidspec {$2} | {InvalidOid} ---- */
fn opt_rowtype_oid(p: &mut Parser) -> PgResult<types_core::Oid> {
    if p.peek() == BootTokenKind::RowtypeOid {
        p.advance()?;
        p.oidspec()
    } else {
        Ok(types_core::primitive::InvalidOid)
    }
}

/* ----------------------------------------------------------------
 * boot_column_list: boot_column_def (COMMA boot_column_def)*
 * boot_column_def:  boot_ident EQUALS boot_ident boot_column_nullness
 * ---------------------------------------------------------------- */
fn boot_column_list(mcx: Mcx<'static>, p: &mut Parser) -> PgResult<()> {
    boot_column_def(mcx, p)?;
    while p.peek() == BootTokenKind::Comma {
        p.advance()?;
        boot_column_def(mcx, p)?;
    }
    Ok(())
}

fn boot_column_def(mcx: Mcx<'static>, p: &mut Parser) -> PgResult<()> {
    let name = p.boot_ident()?; // $1
    p.expect(BootTokenKind::Equals)?;
    let type_ = p.boot_ident()?; // $3
    let nullness = boot_column_nullness(p)?; // $4

    /* if (++numattr > MAXATTR) elog(FATAL, "too many columns"); */
    let numattr = backend_bootstrap_bootstrap::numattr() + 1;
    backend_bootstrap_bootstrap::set_numattr(numattr);
    if numattr as usize > backend_bootstrap_bootstrap::MAXATTR {
        return ereport(FATAL)
            .errmsg_internal("too many columns")
            .finish(loc(446, "boot_column_def"));
    }
    /* DefineAttr($1, $3, numattr-1, $4); */
    backend_bootstrap_bootstrap::DefineAttr(mcx, &name, &type_, numattr - 1, nullness)?;
    Ok(())
}

/* ----------------------------------------------------------------
 * boot_column_nullness:
 *   XFORCE XNOT XNULL  { BOOTCOL_NULL_FORCE_NOT_NULL }
 *   | XFORCE XNULL     { BOOTCOL_NULL_FORCE_NULL }
 *   |                  { BOOTCOL_NULL_AUTO }
 * ---------------------------------------------------------------- */
fn boot_column_nullness(p: &mut Parser) -> PgResult<i32> {
    if p.peek() == BootTokenKind::Force {
        p.advance()?; // XFORCE
        if p.peek() == BootTokenKind::Not {
            p.advance()?; // XNOT
            p.expect(BootTokenKind::Null)?; // XNULL
            Ok(backend_bootstrap_bootstrap::BOOTCOL_NULL_FORCE_NOT_NULL)
        } else {
            p.expect(BootTokenKind::Null)?; // XNULL
            Ok(backend_bootstrap_bootstrap::BOOTCOL_NULL_FORCE_NULL)
        }
    } else {
        Ok(backend_bootstrap_bootstrap::BOOTCOL_NULL_AUTO)
    }
}

/* ----------------------------------------------------------------
 * Boot_InsertStmt:
 *   INSERT_TUPLE LPAREN boot_column_val_list RPAREN
 * ---------------------------------------------------------------- */
fn boot_insert_stmt(mcx: Mcx<'static>, p: &mut Parser) -> PgResult<()> {
    p.expect(BootTokenKind::InsertTuple)?;

    /* mid-rule action */
    do_start();
    ereport(DEBUG4)
        .errmsg_internal("inserting row")
        .finish(loc(258, "Boot_InsertStmt"))?;
    set_num_columns_read(0);

    p.expect(BootTokenKind::LeftParen)?;
    boot_column_val_list(mcx, p)?;
    p.expect(BootTokenKind::RightParen)?;

    /* final action */
    let numattr = backend_bootstrap_bootstrap::numattr();
    if num_columns_read() != numattr {
        return ereport(ERROR)
            .errmsg_internal(format!(
                "incorrect number of columns in row (expected {}, got {})",
                numattr,
                num_columns_read()
            ))
            .finish(loc(264, "Boot_InsertStmt"));
    }
    if !backend_bootstrap_bootstrap::boot_reldesc_is_open() {
        return ereport(FATAL)
            .errmsg_internal("relation not open")
            .finish(loc(267, "Boot_InsertStmt"));
    }
    backend_bootstrap_bootstrap::InsertOneTuple(mcx)?;
    do_end()?;
    Ok(())
}

/* ----------------------------------------------------------------
 * boot_column_val_list:
 *   boot_column_val ((COMMA)? boot_column_val)*
 *
 * (The grammar allows values separated by optional commas or just
 * whitespace.)
 * ---------------------------------------------------------------- */
fn boot_column_val_list(mcx: Mcx<'static>, p: &mut Parser) -> PgResult<()> {
    boot_column_val(mcx, p)?;
    loop {
        match p.peek() {
            BootTokenKind::Comma => {
                p.advance()?;
                boot_column_val(mcx, p)?;
            }
            // A bare value (no comma separator) continues the list.
            BootTokenKind::Id
            | BootTokenKind::NullVal
            | BootTokenKind::Open
            | BootTokenKind::Close
            | BootTokenKind::Create
            | BootTokenKind::InsertTuple
            | BootTokenKind::Declare
            | BootTokenKind::Index
            | BootTokenKind::On
            | BootTokenKind::Using
            | BootTokenKind::Build
            | BootTokenKind::Indices
            | BootTokenKind::Unique
            | BootTokenKind::Toast
            | BootTokenKind::ObjId
            | BootTokenKind::Bootstrap
            | BootTokenKind::SharedRelation
            | BootTokenKind::RowtypeOid
            | BootTokenKind::Force
            | BootTokenKind::Not
            | BootTokenKind::Null => {
                boot_column_val(mcx, p)?;
            }
            _ => break,
        }
    }
    Ok(())
}

/* ----------------------------------------------------------------
 * boot_column_val:
 *   boot_ident { InsertOneValue($1, num_columns_read++); }
 *   | NULLVAL  { InsertOneNull(num_columns_read++); }
 * ---------------------------------------------------------------- */
fn boot_column_val(mcx: Mcx<'static>, p: &mut Parser) -> PgResult<()> {
    if p.peek() == BootTokenKind::NullVal {
        p.advance()?;
        let n = num_columns_read();
        set_num_columns_read(n + 1);
        backend_bootstrap_bootstrap::InsertOneNull(n)?;
    } else {
        let value = p.boot_ident()?;
        let n = num_columns_read();
        set_num_columns_read(n + 1);
        backend_bootstrap_bootstrap::InsertOneValue(mcx, &value, n)?;
    }
    Ok(())
}

/* ----------------------------------------------------------------
 * Boot_DeclareIndexStmt / Boot_DeclareUniqueIndexStmt / Boot_DeclareToastStmt
 * all begin with XDECLARE.
 * ---------------------------------------------------------------- */
fn boot_declare_stmt(mcx: Mcx<'static>, p: &mut Parser) -> PgResult<()> {
    p.expect(BootTokenKind::Declare)?;
    match p.peek() {
        BootTokenKind::Unique => boot_declare_unique_index_stmt(mcx, p),
        BootTokenKind::Index => boot_declare_index_stmt(mcx, p, false),
        BootTokenKind::Toast => boot_declare_toast_stmt(mcx, p),
        _ => {
            yyerror("syntax error")?;
            unreachable!("yyerror returns Err");
        }
    }
}

/* ----------------------------------------------------------------
 * Boot_DeclareIndexStmt:
 *   XDECLARE INDEX boot_ident oidspec ON boot_ident USING boot_ident
 *   LPAREN boot_index_params RPAREN
 *
 * Boot_DeclareUniqueIndexStmt:
 *   XDECLARE UNIQUE INDEX boot_ident oidspec ON boot_ident USING boot_ident
 *   LPAREN boot_index_params RPAREN
 *
 * (XDECLARE already consumed by boot_declare_stmt; for the unique variant the
 * UNIQUE token is consumed here.)
 * ---------------------------------------------------------------- */
fn boot_declare_unique_index_stmt(mcx: Mcx<'static>, p: &mut Parser) -> PgResult<()> {
    p.expect(BootTokenKind::Unique)?;
    boot_declare_index_stmt(mcx, p, true)
}

fn boot_declare_index_stmt(mcx: Mcx<'static>, p: &mut Parser, unique: bool) -> PgResult<()> {
    p.expect(BootTokenKind::Index)?;
    let idxname = p.boot_ident()?; // index name
    let index_oid = p.oidspec()?; // preassigned index OID
    p.expect(BootTokenKind::On)?;
    let tablename = p.boot_ident()?; // relation to index
    p.expect(BootTokenKind::Using)?;
    let access_method = p.boot_ident()?; // AM name
    p.expect(BootTokenKind::LeftParen)?;
    let index_params = boot_index_params(mcx, p)?;
    p.expect(BootTokenKind::RightParen)?;

    ereport(DEBUG4)
        .errmsg_internal(format!("creating index \"{}\"", idxname))
        .finish(loc(279, "Boot_DeclareIndexStmt"))?;

    do_start();

    /* stmt->relation = makeRangeVar(NULL, tablename, -1); */
    let range_var = make_range_var(mcx, &tablename)?;
    let relation_node = mcx::alloc_in(mcx, types_nodes::nodes::Node::RangeVar(range_var))?;

    let stmt = types_nodes::ddlnodes::IndexStmt {
        idxname: Some(PgString::from_str_in(&idxname, mcx)?),
        relation: Some(relation_node),
        accessMethod: Some(PgString::from_str_in(&access_method, mcx)?),
        tableSpace: None,
        indexParams: index_params,
        indexIncludingParams: PgVec::new_in(mcx),
        options: PgVec::new_in(mcx),
        whereClause: None,
        excludeOpNames: PgVec::new_in(mcx),
        idxcomment: None,
        indexOid: types_core::primitive::InvalidOid,
        oldNumber: types_core::primitive::InvalidRelFileNumber,
        oldCreateSubid: types_core::xact::InvalidSubTransactionId,
        oldFirstRelfilelocatorSubid: types_core::xact::InvalidSubTransactionId,
        unique,
        nulls_not_distinct: false,
        primary: false,
        isconstraint: false,
        iswithoutoverlaps: false,
        deferrable: false,
        initdeferred: false,
        transformed: false,
        concurrent: false,
        if_not_exists: false,
        reset_default_tblspc: false,
    };

    /*
     * locks and races need not concern us in bootstrap mode.
     *
     * relationId = RangeVarGetRelid(stmt->relation, NoLock, false);
     *
     * The namespace seam takes the (owned-String) `types_tuple::access::RangeVar`
     * shape; build it from the same table name as the node above (C uses the one
     * RangeVar object for both the IndexStmt field and the lookup).
     */
    let lookup_rv = make_tuple_range_var(&tablename);
    let relation_id = backend_catalog_namespace_seams::range_var_get_relid::call(
        mcx,
        &lookup_rv,
        types_storage::lock::NoLock,
        false,
    )?;

    backend_commands_indexcmds_seams::define_index::call(
        mcx,
        backend_commands_indexcmds_seams::DefineIndexArgs {
            table_id: relation_id,
            stmt,
            index_relation_id: index_oid,
            parent_index_id: types_core::primitive::InvalidOid,
            parent_constraint_id: types_core::primitive::InvalidOid,
            total_parts: -1,
            is_alter_table: false,
            check_rights: false,
            check_not_in_use: false,
            skip_build: true,
            quiet: false,
        },
    )?;
    do_end()?;
    Ok(())
}

/* ----------------------------------------------------------------
 * Boot_DeclareToastStmt:
 *   XDECLARE XTOAST oidspec oidspec ON boot_ident
 * (XDECLARE already consumed; XTOAST consumed here.)
 * ---------------------------------------------------------------- */
fn boot_declare_toast_stmt(mcx: Mcx<'static>, p: &mut Parser) -> PgResult<()> {
    p.expect(BootTokenKind::Toast)?;
    let toast_oid = p.oidspec()?; // $3
    let toast_index_oid = p.oidspec()?; // $4
    p.expect(BootTokenKind::On)?;
    let relname = p.boot_ident()?; // $6

    ereport(DEBUG4)
        .errmsg_internal(format!("creating toast table for table \"{}\"", relname))
        .finish(loc(382, "Boot_DeclareToastStmt"))?;

    do_start();
    backend_catalog_toasting::BootstrapToastTable(mcx, &relname, toast_oid, toast_index_oid)?;
    do_end()?;
    Ok(())
}

/* ----------------------------------------------------------------
 * Boot_BuildIndsStmt: XBUILD INDICES
 * ---------------------------------------------------------------- */
fn boot_build_inds_stmt(mcx: Mcx<'static>, p: &mut Parser) -> PgResult<()> {
    p.expect(BootTokenKind::Build)?;
    p.expect(BootTokenKind::Indices)?;

    do_start();
    backend_bootstrap_bootstrap::build_indices(mcx)?;
    do_end()?;
    Ok(())
}

/* ----------------------------------------------------------------
 * boot_index_params:
 *   boot_index_param (COMMA boot_index_param)*
 *   { list of IndexElem }
 * ---------------------------------------------------------------- */
fn boot_index_params<'mcx>(
    mcx: Mcx<'mcx>,
    p: &mut Parser,
) -> PgResult<PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>>> {
    let mut params: PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>> = PgVec::new_in(mcx);
    params.push(boot_index_param(mcx, p)?);
    while p.peek() == BootTokenKind::Comma {
        p.advance()?;
        params.push(boot_index_param(mcx, p)?);
    }
    Ok(params)
}

/* ----------------------------------------------------------------
 * boot_index_param: boot_ident boot_ident
 *   {
 *     IndexElem n; n->name = $1; n->opclass = list_make1(makeString($2)); ...
 *   }
 * ---------------------------------------------------------------- */
fn boot_index_param<'mcx>(
    mcx: Mcx<'mcx>,
    p: &mut Parser,
) -> PgResult<types_nodes::nodes::NodePtr<'mcx>> {
    let name = p.boot_ident()?; // $1: column name
    let opclass_name = p.boot_ident()?; // $2: opclass name

    /* n->opclass = list_make1(makeString($2)); */
    let mut opclass: PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>> = PgVec::new_in(mcx);
    let string_node = types_nodes::value::StringNode {
        sval: PgString::from_str_in(&opclass_name, mcx)?,
    };
    opclass.push(mcx::alloc_in(
        mcx,
        types_nodes::nodes::Node::String(string_node),
    )?);

    let elem = types_nodes::ddlnodes::IndexElem {
        name: Some(PgString::from_str_in(&name, mcx)?),
        expr: None,
        indexcolname: None,
        collation: PgVec::new_in(mcx), // NIL
        opclass,
        opclassopts: PgVec::new_in(mcx),
        ordering: types_nodes::rawnodes::SortByDir::SORTBY_DEFAULT,
        nulls_ordering: types_nodes::rawnodes::SortByNulls::SORTBY_NULLS_DEFAULT,
    };

    mcx::alloc_in(mcx, types_nodes::nodes::Node::IndexElem(elem))
}

/* =========================================================================
 * makeRangeVar(NULL, relname, -1) (makefuncs.c) — node constructor.
 * ========================================================================= */

/// `makeRangeVar(NULL, relname, -1)` building the `IndexStmt.relation` node
/// (`types_nodes::rawnodes::RangeVar`, the parse-tree shape).
fn make_range_var<'mcx>(
    mcx: Mcx<'mcx>,
    relname: &str,
) -> PgResult<types_nodes::rawnodes::RangeVar<'mcx>> {
    Ok(types_nodes::rawnodes::RangeVar {
        catalogname: None,
        schemaname: None,
        relname: Some(PgString::from_str_in(relname, mcx)?),
        inh: true, // makeRangeVar sets inh = true
        relpersistence: types_core::catalog::RELPERSISTENCE_PERMANENT as i8,
        alias: None,
        location: -1,
    })
}

/// The owned-String `RangeVar` shape the `range_var_get_relid` seam expects
/// (catalog/namespace lookup form).  Same identity as [`make_range_var`].
fn make_tuple_range_var(relname: &str) -> types_tuple::access::RangeVar {
    types_tuple::access::RangeVar {
        catalogname: None,
        schemaname: None,
        relname: relname.to_string(),
        inh: true,
        relpersistence: types_core::catalog::RELPERSISTENCE_PERMANENT,
        location: -1,
    }
}

/* =========================================================================
 * Seam installation.
 * ========================================================================= */

/// Install this unit's seams (`boot_yylex_init`, `boot_yyparse`).
pub fn init_seams() {
    backend_bootstrap_bootparse_seams::boot_yylex_init::set(boot_yylex_init);
    backend_bootstrap_bootparse_seams::boot_yyparse::set(boot_yyparse);
}
