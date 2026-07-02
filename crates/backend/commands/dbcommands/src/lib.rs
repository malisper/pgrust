#![allow(non_snake_case)]

use cache_syscache::cacheinfo::DATABASEOID;
use cache_syscache::{ReleaseSysCache, SearchSysCache1, SysCacheGetAttrNotNull, SysCacheKey};
use datum::Datum;
use types_core::Oid;
use types_error::PgResult;

const ANUM_PG_DATABASE_DATNAME: i32 = 2;
const NAMEDATALEN: usize = 64;

pub fn get_database_name(dbid: Oid) -> PgResult<Option<String>> {
    let Some(tuple) = SearchSysCache1(DATABASEOID, SysCacheKey::Value(Datum::from_oid(dbid)))?
    else {
        return Ok(None);
    };
    let d = SysCacheGetAttrNotNull(DATABASEOID, &tuple, ANUM_PG_DATABASE_DATNAME)?;
    // SAFETY: datname is a NameData column; the datum points at its
    // NUL-terminated 64-byte buffer inside the pinned tuple image.
    let name = unsafe {
        let p = d.as_usize() as *const u8;
        let mut len = 0usize;
        while len < NAMEDATALEN && *p.add(len) != 0 {
            len += 1;
        }
        core::str::from_utf8_unchecked(core::slice::from_raw_parts(p, len)).to_owned()
    };
    ReleaseSysCache(tuple);
    Ok(Some(name))
}

pub fn init_seams() {
    dbcommands_seams::get_database_name::set(get_database_name);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn install_seams() {
        init_seams();
        assert!(dbcommands_seams::get_database_name::is_installed());
        // Unbooted catcache: loud stop, never a fabricated name.
        assert!(std::panic::catch_unwind(|| dbcommands_seams::get_database_name::call(1)).is_err());
    }
}
