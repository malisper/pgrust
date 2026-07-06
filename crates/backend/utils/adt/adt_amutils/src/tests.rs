use super::*;

#[test]
fn prop_names_case_insensitive() {
    assert!(matches!(lookup_prop_name(b"ASC"), Prop::Asc));
    assert!(matches!(lookup_prop_name(b"distance_orderable"), Prop::DistanceOrderable));
    assert!(matches!(lookup_prop_name(b"Can_Include"), Prop::CanInclude));
    assert!(matches!(lookup_prop_name(b"bogus"), Prop::Unknown));
    assert!(matches!(lookup_prop_name(b"asc2"), Prop::Unknown));
}

// canonical_index_am consults the pg_am.amhandler syscache seam for
// non-builtin oids (RelationInitTableAccessMethod's lookup); unit rigs have
// no syscache below them. The stub answers with pg_am.dat's REAL builtin
// handler oids (330-335) and None for unknown oids (C's !HeapTupleIsValid),
// so the flag-vs-C assertions and the unknown-AM probe both exercise the
// genuine paths — not a vacuous pass. Same projection-rig class as
// type_is_visible / pg_constraint_primary_key_attnos / expandExpressionListStar.
fn install_amhandler_stub() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        syscache_seams::pg_am_amhandler::set(|amoid| {
            Ok(match amoid {
                BTREE_AM_OID => Some(330),
                HASH_AM_OID => Some(331),
                GIST_AM_OID => Some(332),
                GIN_AM_OID => Some(333),
                SPGIST_AM_OID => Some(334),
                BRIN_AM_OID => Some(335),
                _ => None,
            })
        });
    });
}

#[test]
fn am_flag_rows_match_c_handlers() {
    install_amhandler_stub();
    let bt = am_flags(BTREE_AM_OID).unwrap();
    assert!(bt.amcanorder && bt.amcanunique && bt.amsearcharray && bt.has_ambuildphasename);
    let hash = am_flags(HASH_AM_OID).unwrap();
    assert!(hash.amcanbackward && !hash.amcanorder && !hash.amcaninclude);
    let gin = am_flags(GIN_AM_OID).unwrap();
    assert!(!gin.has_amgettuple && gin.has_ambuildphasename && gin.amcanmulticol);
    let brin = am_flags(BRIN_AM_OID).unwrap();
    assert!(!brin.has_amgettuple && brin.amsearchnulls && !brin.amclusterable);
    assert!(am_flags(42).is_none());
}

#[test]
fn phasenames_match_c() {
    assert_eq!(bt_phasename(1), Some("initializing"));
    assert_eq!(bt_phasename(5), Some("loading tuples in tree"));
    assert_eq!(bt_phasename(6), None);
    assert_eq!(gin_phasename(3), Some("sorting tuples (workers)"));
    assert_eq!(gin_phasename(6), Some("merging tuples"));
    assert_eq!(gin_phasename(7), None);
}

#[test]
fn phasenum_truncates_like_pg_getarg_int32() {
    let phasenum = 0x1_0000_0001i64 as i32 as i64;
    assert_eq!(phasenum, 1);
}
