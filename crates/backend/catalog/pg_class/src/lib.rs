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
