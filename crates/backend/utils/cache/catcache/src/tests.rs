use datum::Datum;

use crate::compute::*;
use crate::search::*;
use crate::testing;
use crate::{with_state, NONE};

use std::sync::atomic::{AtomicI32, Ordering};

static NEXT_ID: AtomicI32 = AtomicI32::new(50);

fn fresh_id() -> i32 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

fn oid_key(v: u32) -> CatCKey<'static> {
    CatCKey::Value(Datum::from_oid(v))
}

const KINDS1: [CCFastKind; 4] = [CCFastKind::Int4; 4];

#[test]
fn hash_combine_matches_c_shape() {
    let ks = [oid_key(7), oid_key(11), oid_key(13), oid_key(17)];
    let h1 = int4_hash(Datum::from_oid(7));
    let h2 = int4_hash(Datum::from_oid(11));
    let h3 = int4_hash(Datum::from_oid(13));
    let h4 = int4_hash(Datum::from_oid(17));
    let expect = h4.rotate_left(24) ^ h3.rotate_left(16) ^ h2.rotate_left(8) ^ h1;
    assert_eq!(compute_hash_value(&KINDS1, 4, &ks), expect);
    assert_eq!(compute_hash_value(&KINDS1, 1, &ks), h1);
    assert_eq!(int4_hash(Datum::from_oid(1259)), hashfn::murmurhash32(1259));
}

#[test]
fn name_semantics_match_strncmp() {
    assert!(name_eq(b"pg_class", b"pg_class"));
    assert!(!name_eq(b"pg_class", b"pg_klass"));
    assert!(name_eq(b"abc\0zzzz", b"abc"));
    let long_a = [b'a'; 80];
    let long_b = [b'a'; 70];
    assert!(name_eq(&long_a, &long_b));
    assert_eq!(name_hash(b"abc\0zzz"), hashfn::hash_bytes(b"abc"));
}

#[test]
fn get_cc_hash_eq_funcs_table() {
    assert_eq!(get_cc_hash_eq_funcs(16), (CCFastKind::Char, F_BOOLEQ));
    assert_eq!(get_cc_hash_eq_funcs(19), (CCFastKind::Name, F_NAMEEQ));
    assert_eq!(get_cc_hash_eq_funcs(21), (CCFastKind::Int2, F_INT2EQ));
    assert_eq!(get_cc_hash_eq_funcs(23), (CCFastKind::Int4, F_INT4EQ));
    assert_eq!(get_cc_hash_eq_funcs(25), (CCFastKind::Text, F_TEXTEQ));
    assert_eq!(get_cc_hash_eq_funcs(26), (CCFastKind::Int4, F_OIDEQ));
    assert_eq!(get_cc_hash_eq_funcs(2206), (CCFastKind::Int4, F_OIDEQ));
    assert_eq!(get_cc_hash_eq_funcs(30), (CCFastKind::OidVector, F_OIDVECTOREQ));
}

#[test]
#[should_panic(expected = "not supported as catcache key")]
fn get_cc_hash_eq_funcs_rejects_unknown() {
    let _ = get_cc_hash_eq_funcs(700);
}

fn tiny_image() -> Vec<u8> {
    /* 23-byte header + pad + 8B data, t_hoff 24; the hit path never decodes it */
    let mut img = vec![0u8; 32];
    img[22] = 24; /* t_hoff */
    img
}

#[test]
fn oid_hit_negative_and_miss_shape() {
    let id = fresh_id();
    testing::init_cache_bare(id, 1, KINDS1, 4, None);
    let img = tiny_image();
    testing::insert_positive(id, &[oid_key(1259), CatCKey::UNUSED, CatCKey::UNUSED, CatCKey::UNUSED], &img);
    testing::insert_negative(id, &[oid_key(4444), CatCKey::UNUSED, CatCKey::UNUSED, CatCKey::UNUSED]);

    let t = SearchCatCache1(id, oid_key(1259)).unwrap().expect("hit");
    assert_eq!(t.tuple().t_len, img.len() as u32);
    assert_eq!(t.tuple().t_data().t_hoff, 24);
    ReleaseCatCache(t);

    assert!(SearchCatCache1(id, oid_key(4444)).unwrap().is_none());

    let a = SearchCatCache1(id, oid_key(1259)).unwrap().unwrap();
    let b = SearchCatCache1(id, oid_key(1259)).unwrap().unwrap();
    ReleaseCatCache(a);
    ReleaseCatCache(b);
    assert_eq!(testing::cache_ntup(id), 2);
}

#[test]
fn two_key_hit_general_lane() {
    let id = fresh_id();
    let kinds = [CCFastKind::Int4, CCFastKind::Int2, CCFastKind::Int4, CCFastKind::Int4];
    testing::init_cache_bare(id, 2, kinds, 4, None);
    let img = tiny_image();
    let k = [oid_key(1259), CatCKey::Value(Datum::from_i16(3)), CatCKey::UNUSED, CatCKey::UNUSED];
    testing::insert_positive(id, &k, &img);

    let t = SearchCatCache2(id, oid_key(1259), CatCKey::Value(Datum::from_i16(3)))
        .unwrap()
        .expect("2-key hit");
    ReleaseCatCache(t);
    /* different second key: compare-miss falls through to the uninstalled scan seam */
    assert!(std::panic::catch_unwind(|| {
        SearchCatCache2(id, oid_key(1259), CatCKey::Value(Datum::from_i16(4)))
    })
    .is_err());
}

#[test]
fn name_key_hit() {
    let id = fresh_id();
    let kinds = [CCFastKind::Name, CCFastKind::Int4, CCFastKind::Int4, CCFastKind::Int4];
    testing::init_cache_bare(id, 1, kinds, 4, None);
    let img = tiny_image();
    testing::insert_positive(
        id,
        &[CatCKey::Str("pg_class"), CatCKey::UNUSED, CatCKey::UNUSED, CatCKey::UNUSED],
        &img,
    );
    let t = SearchCatCache1(id, CatCKey::Str("pg_class")).unwrap().expect("name hit");
    ReleaseCatCache(t);
    assert!(std::panic::catch_unwind(|| SearchCatCache1(id, CatCKey::Str("pg_clasz"))).is_err());
}

#[test]
fn move_to_front_on_hit() {
    let id = fresh_id();
    testing::init_cache_bare(id, 1, KINDS1, 1, None); /* one bucket */
    let img = tiny_image();
    for oid in [10u32, 11, 12] {
        testing::insert_positive(id, &[oid_key(oid), CatCKey::UNUSED, CatCKey::UNUSED, CatCKey::UNUSED], &img);
    }
    /* head is last inserted (12); hitting 10 moves it to the head */
    let t = SearchCatCache1(id, oid_key(10)).unwrap().unwrap();
    ReleaseCatCache(t);
    with_state(|st| {
        let c = st.cache(id);
        let head = c.cc_bucket[0];
        assert_eq!(c.tuples[head as usize].keys[0].as_u32(), 10);
        let mut n = 0;
        let mut cur = head;
        let mut prev = NONE;
        while cur != NONE {
            assert_eq!(c.tuples[cur as usize].prev, prev);
            prev = cur;
            cur = c.tuples[cur as usize].next;
            n += 1;
        }
        assert_eq!(n, 3);
    });
}

#[test]
fn invalidate_and_reset() {
    let id = fresh_id();
    testing::init_cache_bare(id, 1, KINDS1, 4, None);
    let img = tiny_image();
    testing::insert_positive(id, &[oid_key(77), CatCKey::UNUSED, CatCKey::UNUSED, CatCKey::UNUSED], &img);
    testing::insert_negative(id, &[oid_key(78), CatCKey::UNUSED, CatCKey::UNUSED, CatCKey::UNUSED]);
    assert_eq!(testing::cache_ntup(id), 2);

    /* unreferenced entry: removed outright */
    let hv = with_state(|st| {
        let c = st.cache(id);
        compute_hash_value(&c.cc_kind, 1, &[oid_key(77), CatCKey::UNUSED, CatCKey::UNUSED, CatCKey::UNUSED])
    });
    crate::CatCacheInvalidate(id, hv);
    assert_eq!(testing::cache_ntup(id), 1);

    /* referenced entry: marked dead, freed on release */
    testing::insert_positive(id, &[oid_key(77), CatCKey::UNUSED, CatCKey::UNUSED, CatCKey::UNUSED], &img);
    let t = SearchCatCache1(id, oid_key(77)).unwrap().unwrap();
    crate::CatCacheInvalidate(id, hv);
    assert_eq!(testing::cache_ntup(id), 2); /* still counted: pinned */
    with_state(|st| assert!(st.cache(id).tuples[t.slot as usize].dead));
    ReleaseCatCache(t);
    assert_eq!(testing::cache_ntup(id), 1);

    crate::ResetCatalogCachesExt(false).unwrap();
    assert_eq!(testing::cache_ntup(id), 0);
}

#[test]
fn rehash_preserves_entries() {
    let id = fresh_id();
    testing::init_cache_bare(id, 1, KINDS1, 2, None);
    let img = tiny_image();
    for oid in 0..40u32 {
        testing::insert_negative(id, &[oid_key(oid), CatCKey::UNUSED, CatCKey::UNUSED, CatCKey::UNUSED]);
        let _ = img;
    }
    with_state(|st| {
        crate::graph::maybe_rehash(st, id);
        assert!(st.cache(id).cc_nbuckets > 2);
    });
    for oid in 0..40u32 {
        assert!(SearchCatCache1(id, oid_key(oid)).unwrap().is_none());
    }
}

#[test]
fn packed_byref_key_roundtrip() {
    let buf = [0u8, 1, 2, 3, 4, 5, 6, 7];
    let k = crate::pack_ref(2, 4);
    // SAFETY: off+len within buf.
    let s = unsafe { crate::stored_bytes(buf.as_ptr(), k) };
    assert_eq!(s, &[2, 3, 4, 5]);
}

// The UnsafeCell state-access kernel (the Miri target).
mod state_kernel {
    use super::*;

    #[test]
    fn sequential_borrows_roundtrip() {
        let id = fresh_id();
        testing::init_cache_bare(id, 1, KINDS1, 4, None);
        with_state(|st| st.cache_mut(id).cc_ntup += 5);
        assert_eq!(with_state(|st| st.cache(id).cc_ntup), 5);
        with_state(|st| st.cache_mut(id).cc_ntup = 0);
        assert_eq!(with_state(|st| st.cache(id).cc_ntup), 0);
    }

    #[test]
    fn pinned_image_survives_state_mutation() {
        let id = fresh_id();
        testing::init_cache_bare(id, 1, KINDS1, 4, None);
        let img = super::tiny_image();
        testing::insert_positive(id, &[oid_key(9), CatCKey::UNUSED, CatCKey::UNUSED, CatCKey::UNUSED], &img);
        let t = SearchCatCache1(id, oid_key(9)).unwrap().unwrap();
        /* slot-vec growth while the pin is live: the image is a separate stable allocation */
        for oid in 100..164u32 {
            testing::insert_negative(id, &[oid_key(oid), CatCKey::UNUSED, CatCKey::UNUSED, CatCKey::UNUSED]);
        }
        assert_eq!(t.tuple().t_len, img.len() as u32);
        assert_eq!(t.tuple().t_data().t_hoff, 24);
        ReleaseCatCache(t);
    }

    #[test]
    #[cfg(debug_assertions)]
    fn reentrant_access_panics_in_debug() {
        let nested = std::panic::catch_unwind(|| with_state(|_outer| with_state(|_inner| 0u8)));
        assert!(nested.is_err(), "re-entrancy guard failed to fire");
        let _ = with_state(|st| st.caches.len());
    }
}
