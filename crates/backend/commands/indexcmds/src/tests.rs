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
