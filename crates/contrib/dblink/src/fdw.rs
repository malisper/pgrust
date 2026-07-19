// dblink_fdw_validator + option-context rules (dblink.c
// is_valid_dblink_option / is_valid_dblink_fdw_option). The set of legal
// keywords is libpq's PQconndefaults table (pgclient::CONNINFO_OPTIONS); the
// context rules mirror C's dispchar-based classification.
use datum::Datum;
use types_core::{Oid, FOREIGN_DATA_WRAPPER_RELATION_ID, FOREIGN_SERVER_RELATION_ID,
    USER_MAPPING_RELATION_ID};
use types_error::{PgError, PgResult, ERRCODE_FDW_OPTION_NAME_NOT_FOUND};
use types_fmgr::FmgrInfo;
use types_fmgr::FunctionCallInfoBaseData as Fcinfo;

pub const FDW_CONTEXT: Oid = FOREIGN_DATA_WRAPPER_RELATION_ID;
pub const SERVER_CONTEXT: Oid = FOREIGN_SERVER_RELATION_ID;
pub const USER_MAPPING_CONTEXT: Oid = USER_MAPPING_RELATION_ID;

const MAX_LEVENSHTEIN_STRLEN: usize = 255;

// is_valid_dblink_option: a libpq keyword, minus debug ('D') options,
// client_encoding, and oauth_*; "user" and secure ('*') options belong only in
// USER MAPPING, everything else only in FOREIGN SERVER.
pub fn is_valid_dblink_option(option: &str, context: Oid) -> bool {
    let Some(opt) = pgclient::conninfo::lookup_option(option) else {
        return false;
    };
    if opt.dispchar.contains('D') {
        return false;
    }
    if option == "client_encoding" {
        return false;
    }
    if option.starts_with("oauth_") {
        return false;
    }
    if option == "user" || opt.dispchar.contains('*') {
        context == USER_MAPPING_RELATION_ID
    } else {
        context == FOREIGN_SERVER_RELATION_ID
    }
}

// is_valid_dblink_fdw_option: also permits use_scram_passthrough.
fn is_valid_dblink_fdw_option(option: &str, context: Oid) -> bool {
    option == "use_scram_passthrough" || is_valid_dblink_option(option, context)
}

#[cold]
fn invalid_option_error(mcx: mcx::Mcx<'_>, name: &str, context: Oid) -> PgResult<Box<PgError>> {
    let mut min_d = -1i32;
    let mut closest: Option<&str> = None;
    let mut has_valid_options = false;
    for opt in pgclient::CONNINFO_OPTIONS {
        if !is_valid_dblink_option(opt.keyword, context) {
            continue;
        }
        has_valid_options = true;
        if name.is_empty()
            || name.len() > MAX_LEVENSHTEIN_STRLEN
            || opt.keyword.len() > MAX_LEVENSHTEIN_STRLEN
        {
            continue;
        }
        let dist = varlena::levenshtein::varstr_levenshtein_less_equal(
            mcx,
            name.as_bytes(),
            opt.keyword.as_bytes(),
            1,
            1,
            1,
            4,
            true,
        )?;
        if dist <= 4 && dist as usize <= name.len() / 2 && (min_d == -1 || dist < min_d) {
            min_d = dist;
            closest = Some(opt.keyword);
        }
    }
    let mut e = PgError::error(format!("invalid option \"{name}\""))
        .with_sqlstate(ERRCODE_FDW_OPTION_NAME_NOT_FOUND);
    e = if has_valid_options {
        match closest {
            Some(m) => e.with_hint(format!("Perhaps you meant the option \"{m}\".")),
            None => e,
        }
    } else {
        e.with_hint("There are no valid options in this context.")
    };
    Ok(Box::new(e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn option_context_rules() {
        // secure ('*') + "user" belong only in USER MAPPING.
        assert!(is_valid_dblink_option("password", USER_MAPPING_CONTEXT));
        assert!(!is_valid_dblink_option("password", SERVER_CONTEXT));
        assert!(is_valid_dblink_option("user", USER_MAPPING_CONTEXT));
        assert!(!is_valid_dblink_option("user", SERVER_CONTEXT));
        // ordinary options belong only in FOREIGN SERVER.
        assert!(is_valid_dblink_option("dbname", SERVER_CONTEXT));
        assert!(!is_valid_dblink_option("dbname", USER_MAPPING_CONTEXT));
        assert!(is_valid_dblink_option("host", SERVER_CONTEXT));
        // banned everywhere.
        assert!(!is_valid_dblink_option("client_encoding", SERVER_CONTEXT));
        assert!(!is_valid_dblink_option("client_encoding", USER_MAPPING_CONTEXT));
        assert!(!is_valid_dblink_option("replication", SERVER_CONTEXT)); // debug 'D'
        assert!(!is_valid_dblink_option("oauth_issuer", SERVER_CONTEXT));
        assert!(!is_valid_dblink_option("oauth_client_id", USER_MAPPING_CONTEXT));
        assert!(!is_valid_dblink_option("bogus", SERVER_CONTEXT));
    }

    #[test]
    fn fdw_specific_option() {
        assert!(is_valid_dblink_fdw_option("use_scram_passthrough", SERVER_CONTEXT));
        assert!(!is_valid_dblink_option("use_scram_passthrough", SERVER_CONTEXT));
    }
}

pub fn fc_dblink_fdw_validator(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let options = foreigncmds::options::untransform_options(mcx, Some(fcinfo.arg(0)))?;
    let context = fcinfo.arg(1).as_oid();

    for opt in options.iter() {
        if !is_valid_dblink_fdw_option(opt.name, context) {
            return Err(invalid_option_error(mcx, opt.name, context)?);
        }
    }
    Ok(Datum::null())
}
