//! Unit tests for the pure, seam-free logic of `parse_oper.c`: the
//! polymorphic-type predicate, the name-list rendering, the cache-key
//! `strlcpy`, and the cache flush. The catalog/coercion-dependent functions
//! drive seams whose owners are not installed in a unit-test process, so they
//! are exercised by the integration gate, not here.

use super::*;

#[test]
fn polymorphic_predicate_matches_the_any_family() {
    assert!(is_polymorphic_type(ANYELEMENTOID));
    assert!(is_polymorphic_type(ANYARRAYOID));
    assert!(is_polymorphic_type(ANYCOMPATIBLEMULTIRANGEOID));
    assert!(!is_polymorphic_type(BOOLOID));
    assert!(!is_polymorphic_type(InvalidOid));
}

#[test]
fn name_list_renders_dotted() {
    assert_eq!(name_list_to_string(&["+".to_string()]), "+");
    assert_eq!(
        name_list_to_string(&["pg_catalog".to_string(), "+".to_string()]),
        "pg_catalog.+"
    );
}

#[test]
fn strlcpy_namedata_truncates_and_zero_pads() {
    let buf = strlcpy_namedata("=");
    assert_eq!(buf[0], b'=');
    assert_eq!(buf[1], 0);

    let long = "x".repeat(100);
    let buf = strlcpy_namedata(&long);
    // strlcpy copies at most NAMEDATALEN-1 bytes and always NUL-terminates.
    assert_eq!(buf[NAMEDATALEN_USZ - 1], 0);
    assert_eq!(&buf[..NAMEDATALEN_USZ - 1], &[b'x'; NAMEDATALEN_USZ - 1]);
}

#[test]
fn cache_key_default_is_zero_filled() {
    let key = OprCacheKey::default();
    assert_eq!(key.left_arg, InvalidOid);
    assert_eq!(key.right_arg, InvalidOid);
    assert!(key.oprname.iter().all(|&b| b == 0));
    assert!(key.search_path.iter().all(|&o| o == InvalidOid));
}

#[test]
fn invalidate_oper_cache_clears_entries() {
    // Seed the cache directly (find_oper_cache_entry's init path needs no seam).
    {
        let mut cache = OPR_CACHE_HASH.lock().expect("oper cache");
        let mut m = BTreeMap::new();
        m.insert(OprCacheKey::default(), 42);
        *cache = Some(m);
    }
    invalidate_oper_cache();
    let cache = OPR_CACHE_HASH.lock().expect("oper cache");
    assert!(cache.as_ref().expect("cache present").is_empty());
}
