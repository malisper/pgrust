//! Unit tests for the pure, seam-free helpers. The format/escaping paths and
//! the drivers exercise cross-subsystem seams (out-functions, encoding, the
//! file/frontend sinks) that are not installed in a unit-test process, so they
//! are covered by the unit's smoke test, not here.

#![cfg(test)]

use super::*;

#[test]
fn highbit() {
    assert!(!is_highbit_set(0x7f));
    assert!(is_highbit_set(0x80));
    assert!(is_highbit_set(0xff));
}

#[test]
fn binary_signature_matches_c() {
    // "PGCOPY\n\377\r\n\0" (copyto.c:109).
    assert_eq!(
        &BINARY_SIGNATURE,
        b"PGCOPY\n\xff\r\n\0",
    );
}

#[test]
fn routine_selection() {
    // The C dispatch: csv_mode wins, then binary, else text.
    fn opts(csv: bool, bin: bool) -> CopyFormatOptions<'static> {
        // A throwaway options value; only the two flags matter here.
        let mcx = leak_ctx();
        CopyFormatOptions {
            file_encoding: -1,
            binary: bin,
            csv_mode: csv,
            header_line: CopyHeaderChoice::COPY_HEADER_FALSE,
            null_print: PgString::from_str_in("\\N", mcx).unwrap(),
            null_print_client: PgString::from_str_in("\\N", mcx).unwrap(),
            delim: b'\t',
            quote: b'"',
            escape: b'"',
            force_quote: None,
            force_quote_all: false,
            force_quote_flags: PgVec::new_in(mcx),
        }
    }
    assert_eq!(copy_to_get_routine(&opts(true, false)), CopyToRoutineKind::Csv);
    assert_eq!(copy_to_get_routine(&opts(true, true)), CopyToRoutineKind::Csv);
    assert_eq!(copy_to_get_routine(&opts(false, true)), CopyToRoutineKind::Binary);
    assert_eq!(copy_to_get_routine(&opts(false, false)), CopyToRoutineKind::Text);
}

/// A leaked static memory context for tests (never freed; fine for a test).
fn leak_ctx() -> Mcx<'static> {
    use ::mcx::MemoryContext;
    let ctx: &'static MemoryContext =
        alloc::boxed::Box::leak(alloc::boxed::Box::new(MemoryContext::new("copyto test")));
    ctx.mcx()
}
