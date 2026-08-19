// pg_class.c.
use types_error::{PgError, PgResult};

pub fn errdetail_relkind_not_supported(relkind: u8) -> PgResult<String> {
    let noun = match relkind {
        b'r' => "tables",
        b'i' => "indexes",
        b'S' => "sequences",
        b't' => "TOAST tables",
        b'v' => "views",
        b'm' => "materialized views",
        b'c' => "composite types",
        b'f' => "foreign tables",
        b'p' => "partitioned tables",
        b'I' => "partitioned indexes",
        other => {
            return Err(Box::new(PgError::error(format!(
                "unrecognized relkind: '{}'",
                other as char
            ))))
        }
    };
    Ok(format!("This operation is not supported for {noun}."))
}

pub fn init_seams() {
    pg_class_seams::errdetail_relkind_not_supported::set(errdetail_relkind_not_supported);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detail_matches_c_wording() {
        for (relkind, noun) in [
            (b'r', "tables"),
            (b'i', "indexes"),
            (b'S', "sequences"),
            (b't', "TOAST tables"),
            (b'v', "views"),
            (b'm', "materialized views"),
            (b'c', "composite types"),
            (b'f', "foreign tables"),
            (b'p', "partitioned tables"),
            (b'I', "partitioned indexes"),
        ] {
            assert_eq!(
                errdetail_relkind_not_supported(relkind).unwrap(),
                format!("This operation is not supported for {noun}.")
            );
        }
    }

    #[test]
    fn unknown_relkind_is_elog_error() {
        let err = errdetail_relkind_not_supported(b'x').unwrap_err();
        assert_eq!(err.message, "unrecognized relkind: 'x'");
    }
}
