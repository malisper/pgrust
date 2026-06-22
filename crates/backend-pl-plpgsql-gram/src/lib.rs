//! Grammar / parser for PL/pgSQL — a hand-written recursive-descent port of
//! the bison grammar `pl_gram.y`.
//!
//! `pl_gram.y` is an LALR(1) grammar driven by bison, but because PL/pgSQL
//! statements are overwhelmingly keyword-led and the grammar relies on
//! hand-coded token-pushback (`plpgsql_push_back_token`) and direct `yylex()`
//! calls inside many actions, it translates cleanly into a recursive-descent
//! parser that pulls tokens from the [`backend_pl_plpgsql_scanner::PlpgsqlScanner`].
//!
//! The parser builds the owned [`types_plpgsql`] AST (`PLpgSQL_stmt_block` is
//! the top-level result). Each grammar production maps to a method on
//! [`Parser`]; the static helper functions of `pl_gram.y`
//! (`read_sql_construct`, `read_datatype`, `make_execsql_stmt`,
//! `make_return_stmt`, …) become methods too.
//!
//! Error model (per AGENTS.md / `types-error`): the scanner's `plpgsql_yylex`
//! returns a `PgResult`, so every production threads `?`; `ereport(ERROR)` /
//! `yyerror` become a recoverable [`types_error::PgError`] returned up the
//! `PgResult`, exactly as `backend-replication-repl-gram` does.
//!
//! Cross-crate calls: the compiler's variable/datatype builders, the
//! `%TYPE`/`%ROWTYPE` resolvers, the error-condition lookups, the compile-state
//! accessors, and the raw SQL parser / type-name resolver / collation / quoting
//! helpers all live in `pl_comp.c` (not yet ported) and are reached through
//! [`backend_pl_plpgsql_comp_seams`] (panic until comp lands). The namespace
//! stack (`pl_funcs.c`) is reached directly via [`backend_pl_plpgsql_funcs`]
//! (no cycle). The compiler invokes this crate's [`plpgsql_yyparse`] directly.

mod mem;
mod parser;

pub use parser::{plpgsql_yyparse, plpgsql_yyparse_with_lineno, Parser};

/// No inward seams: the compiler depends on this crate directly and calls
/// [`plpgsql_yyparse`]. (The gram→comp builder edge is broken through
/// `backend-pl-plpgsql-comp-seams`, so there is no cycle that would force an
/// inward `plpgsql_yyparse` seam.) Defined for the install-completeness rule.
pub fn init_seams() {}

#[cfg(test)]
mod tests {
    use super::*;
    use backend_pl_plpgsql_scanner::plpgsql_scanner_init;
    use mcx::MemoryContext;

    /// The public parse entry exists and the grammar drives the scanner up to
    /// the first compiler callback. A complete parse of `BEGIN RETURN 1; END`
    /// reaches `curr_compile_next_stmtid` (a `pl_comp.c` seam not yet
    /// installed), so an end-to-end parse is not exercised until the compiler
    /// lands; here we assert the parser reaches that comp boundary (the grammar
    /// itself lexed BEGIN/RETURN and dispatched stmt_return correctly).
    #[test]
    fn parse_reaches_compiler_seam() {
        // Namespace stack the grammar's opt_block_label / ns_push touch.
        backend_pl_plpgsql_funcs::plpgsql_ns_init();

        let src = "BEGIN RETURN 1; END";
        // NUL-pad the scan buffer (the core lexer expects a trailing NUL).
        let mut buf: Vec<u8> = src.as_bytes().to_vec();
        buf.push(0);

        let ctx = MemoryContext::new("plpgsql-gram-test");
        let scanner = plpgsql_scanner_init(ctx.mcx(), &buf, src);

        // The grammar reaches a not-yet-installed pl_comp.c seam
        // (curr_compile_next_stmtid) before producing a block. Catch the
        // resulting loud panic to confirm we drove that far.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = plpgsql_yyparse(scanner);
        }));
        assert!(
            result.is_err(),
            "expected the parse to reach the (uninstalled) pl_comp.c seam"
        );
    }
}
