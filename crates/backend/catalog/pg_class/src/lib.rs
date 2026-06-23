//! Idiomatic port of `backend/catalog/pg_class.c` — routines to support
//! manipulation of the `pg_class` relation.
//!
//! `pg_class.c` exports exactly one function,
//! [`errdetail_relkind_not_supported`], which maps a `pg_class.relkind` code to
//! a fixed errdetail message used when an operation does not apply to a
//! particular relation kind. In C the function returns the `int` produced by
//! `errdetail()` (always 0), registering the detail text on the in-progress
//! `ereport` as a side effect; in this value-based error model the caller folds
//! the returned detail string into its own ereport, so the port returns the
//! detail text by value. The `default` arm's
//! `elog(ERROR, "unrecognized relkind: '%c'", relkind)` becomes a recoverable
//! [`PgError`] carrying the exact message text and source location.

use types_error::{PgError, PgResult};
use types_tuple::{
    RELKIND_COMPOSITE_TYPE, RELKIND_FOREIGN_TABLE, RELKIND_INDEX, RELKIND_MATVIEW,
    RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_SEQUENCE,
    RELKIND_TOASTVALUE, RELKIND_VIEW,
};

/// `src/backend/catalog/pg_class.c`, for error-location metadata.
const PG_CLASS_C: &str = "src/backend/catalog/pg_class.c";
/// Line of the `elog(ERROR, ...)` in `errdetail_relkind_not_supported`.
const ERRDETAIL_RELKIND_NOT_SUPPORTED_LINE: i32 = 49;

/// Issue an errdetail() informing that the relkind is not supported for this
/// operation (pg_class.c:23-52).
///
/// `relkind` is the `relkind` character of a `pg_class` row (one of the
/// `RELKIND_*` constants). A recognized relkind yields its corresponding detail
/// string; an unrecognized relkind maps to the C `elog(ERROR, ...)` and is
/// surfaced here as an erroring [`PgResult`].
pub fn errdetail_relkind_not_supported(relkind: u8) -> PgResult<String> {
    let detail = match relkind {
        RELKIND_RELATION => "This operation is not supported for tables.",
        RELKIND_INDEX => "This operation is not supported for indexes.",
        RELKIND_SEQUENCE => "This operation is not supported for sequences.",
        RELKIND_TOASTVALUE => "This operation is not supported for TOAST tables.",
        RELKIND_VIEW => "This operation is not supported for views.",
        RELKIND_MATVIEW => "This operation is not supported for materialized views.",
        RELKIND_COMPOSITE_TYPE => "This operation is not supported for composite types.",
        RELKIND_FOREIGN_TABLE => "This operation is not supported for foreign tables.",
        RELKIND_PARTITIONED_TABLE => "This operation is not supported for partitioned tables.",
        RELKIND_PARTITIONED_INDEX => "This operation is not supported for partitioned indexes.",
        _ => {
            return Err(PgError::error(format!(
                "unrecognized relkind: '{}'",
                relkind as char
            ))
            .with_location(
                PG_CLASS_C,
                ERRDETAIL_RELKIND_NOT_SUPPORTED_LINE,
                "errdetail_relkind_not_supported",
            ));
        }
    };
    Ok(detail.to_string())
}

/// Install this crate's seams.
pub fn init_seams() {
    pg_class_seams::errdetail_relkind_not_supported::set(
        errdetail_relkind_not_supported,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use types_error::ERROR;

    #[test]
    fn relkind_constants_match_postgres_header() {
        assert_eq!(RELKIND_RELATION, b'r');
        assert_eq!(RELKIND_INDEX, b'i');
        assert_eq!(RELKIND_SEQUENCE, b'S');
        assert_eq!(RELKIND_TOASTVALUE, b't');
        assert_eq!(RELKIND_VIEW, b'v');
        assert_eq!(RELKIND_MATVIEW, b'm');
        assert_eq!(RELKIND_COMPOSITE_TYPE, b'c');
        assert_eq!(RELKIND_FOREIGN_TABLE, b'f');
        assert_eq!(RELKIND_PARTITIONED_TABLE, b'p');
        assert_eq!(RELKIND_PARTITIONED_INDEX, b'I');
    }

    #[test]
    fn errdetail_messages_match_postgres() {
        let cases: [(u8, &str); 10] = [
            (RELKIND_RELATION, "This operation is not supported for tables."),
            (RELKIND_INDEX, "This operation is not supported for indexes."),
            (RELKIND_SEQUENCE, "This operation is not supported for sequences."),
            (RELKIND_TOASTVALUE, "This operation is not supported for TOAST tables."),
            (RELKIND_VIEW, "This operation is not supported for views."),
            (RELKIND_MATVIEW, "This operation is not supported for materialized views."),
            (RELKIND_COMPOSITE_TYPE, "This operation is not supported for composite types."),
            (RELKIND_FOREIGN_TABLE, "This operation is not supported for foreign tables."),
            (RELKIND_PARTITIONED_TABLE, "This operation is not supported for partitioned tables."),
            (RELKIND_PARTITIONED_INDEX, "This operation is not supported for partitioned indexes."),
        ];
        for (relkind, detail) in cases {
            assert_eq!(errdetail_relkind_not_supported(relkind).unwrap(), detail);
        }
    }

    #[test]
    fn unknown_relkind_elog_errors() {
        let err = errdetail_relkind_not_supported(b'x').unwrap_err();
        assert_eq!(err.level(), ERROR);
        assert_eq!(err.message(), "unrecognized relkind: 'x'");
        let location = err.location().unwrap();
        assert_eq!(location.filename.as_deref(), Some(PG_CLASS_C));
        assert_eq!(location.lineno, ERRDETAIL_RELKIND_NOT_SUPPORTED_LINE);
        assert_eq!(
            location.funcname.as_deref(),
            Some("errdetail_relkind_not_supported")
        );
    }
}
