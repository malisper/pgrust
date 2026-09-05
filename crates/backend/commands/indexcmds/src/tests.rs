use types_core::catalog::{INT4OID, TEXTOID};

const VARCHAROID: types_core::Oid = 1043;

fn install() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        syscache_seams::pg_type_category::set(|typid| {
            Ok(match typid {
                TEXTOID => Some((b'S' as i8, true)),
                VARCHAROID => Some((b'S' as i8, false)),
                INT4OID => Some((b'N' as i8, false)),
                _ => None,
            })
        });
    });
}

#[test]
fn is_preferred_type_matches_c() {
    install();
    assert!(crate::IsPreferredType(b'S' as i8, TEXTOID).unwrap());
    assert!(!crate::IsPreferredType(b'S' as i8, VARCHAROID).unwrap());
    assert!(!crate::IsPreferredType(b'N' as i8, TEXTOID).unwrap());
    assert!(crate::IsPreferredType(crate::TYPCATEGORY_INVALID, TEXTOID).unwrap());
}

// get_am_name (amcmds.c:192-206) returns NULL for an AM oid with no pg_am
// row, and errdetail's %s renders that NULL as "(null)"
// (src/port/snprintf.c:691) — never a made-up placeholder.
#[test]
fn missing_am_name_renders_as_c_null_in_error_details() {
    use cache_syscache::cacheinfo::AMOID;
    use cache_syscache::SysCacheKey;
    use catcache::CCFastKind;
    use datum::Datum;

    catcache::testing::init_cache_bare(AMOID, 1, [CCFastKind::Int4; 4], 8, None);
    catcache::testing::insert_negative(
        AMOID,
        &[
            SysCacheKey::Value(Datum::from_oid(4242)),
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
        ],
    );
    assert_eq!(crate::define::get_am_name(4242).unwrap(), "(null)");
}
