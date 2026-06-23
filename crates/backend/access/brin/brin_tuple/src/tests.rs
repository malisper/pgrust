//! Unit tests for the parts of the BRIN tuple codec that need no unported
//! seam: the on-disk header layout, placeholder formation, equality, and the
//! null-bit / data-offset accessors.

extern crate std;

use ::mcx::MemoryContext;
use brin::{
    BrinDesc, BrinOpcInfo, BRIN_EMPTY_RANGE_MASK, BRIN_NULLS_MASK, BRIN_PLACEHOLDER_MASK,
    SIZE_OF_BRIN_TUPLE,
};
use ::types_core::NAMEDATALEN;
use ::types_tuple::heaptuple::{CompactAttribute, FormData_pg_attribute, NameData, TupleDescData};
use types_typcache::{TypeCacheEntry, TYPSTORAGE_PLAIN};

use crate::internal::{bitmaplen, maxalign, BrinTupleImage};
use crate::tuple::{brin_form_placeholder_tuple, brin_tuples_equal};

fn int4_typcache() -> TypeCacheEntry {
    // pg_type for int4: typlen 4, byval, align 'i', storage plain.
    TypeCacheEntry {
        type_id: 23,
        typlen: 4,
        typbyval: true,
        typalign: b'i' as i8,
        typstorage: TYPSTORAGE_PLAIN,
        typtype: b'b' as i8,
    }
}

fn index_tupdesc<'mcx>(mcx: ::mcx::Mcx<'mcx>, natts: usize) -> ::mcx::PgBox<'mcx, TupleDescData<'mcx>> {
    let mut compact_attrs = ::mcx::vec_with_capacity_in(mcx, natts).unwrap();
    let mut attrs = ::mcx::vec_with_capacity_in(mcx, natts).unwrap();
    for i in 0..natts {
        compact_attrs.push(CompactAttribute {
            attcacheoff: -1,
            attlen: 4,
            attbyval: true,
            attispackable: false,
            atthasmissing: false,
            attisdropped: false,
            attgenerated: false,
            attnullability: 0,
            attalignby: 4,
        });
        attrs.push(FormData_pg_attribute {
            attrelid: 0,
            attname: NameData {
                data: [0u8; NAMEDATALEN as usize],
            },
            atttypid: 23,
            attlen: 4,
            attnum: (i + 1) as i16,
            atttypmod: -1,
            attndims: 0,
            attbyval: true,
            attalign: b'i' as i8,
            attstorage: TYPSTORAGE_PLAIN,
            attcompression: 0,
            attnotnull: false,
            atthasdef: false,
            atthasmissing: false,
            attidentity: 0,
            attgenerated: 0,
            attisdropped: false,
            attislocal: true,
            attinhcount: 0,
            attcollation: 0,
        });
    }
    ::mcx::alloc_in(
        mcx,
        TupleDescData {
            natts: natts as i32,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: -1,
            constr: None,
            compact_attrs,
            attrs,
        },
    )
    .unwrap()
}

fn make_index_rel<'mcx>(mcx: ::mcx::Mcx<'mcx>, natts: usize) -> ::rel::Relation<'mcx> {
    use rel::{FormData_pg_class, RelationData};
    use ::types_storage::RelFileLocator;
    let rd = RelationData {
        rd_id: 1,
        rd_locator: RelFileLocator {
            spcOid: 0,
            dbOid: 0,
            relNumber: 0,
        },
        rd_backend: ::types_core::INVALID_PROC_NUMBER,
        rd_rel: FormData_pg_class {
            relname: ::mcx::PgString::from_str_in("brinidx", mcx).unwrap(),
            relnamespace: 0,
            relowner: 0,
            relrowsecurity: false,
            relpages: 0,
            reltuples: 0.0,
            relallvisible: 0,
            reltoastrelid: 0,
            reltablespace: 0,
            relfilenode: 0,
            relisshared: false,
            relhasindex: false,
            relhassubclass: false,
            relpersistence: b'p',
            relkind: b'i',
            reltype: 0,
            relam: 0,
            relispopulated: true,
            relreplident: b'n',
            relispartition: false,
            relfrozenxid: 0,
            relminmxid: 0,
        },
        rd_att: index_tupdesc(mcx, natts),
        rd_options: None,
        rd_index: None,
        rd_opcintype: ::mcx::PgVec::new_in(mcx),
        rd_opfamily: ::mcx::PgVec::new_in(mcx),
        rd_indoption: ::mcx::PgVec::new_in(mcx),
        rd_indcollation: ::mcx::PgVec::new_in(mcx),
        rd_trigdesc: None,
        pgstat_enabled: false,
    };
    ::rel::Relation::open(rd, None)
}

fn brin_desc<'mcx>(mcx: ::mcx::Mcx<'mcx>, natts: usize) -> BrinDesc<'mcx> {
    let mut bd_info = ::mcx::vec_with_capacity_in(mcx, natts).unwrap();
    for _ in 0..natts {
        let mut typcache = ::mcx::vec_with_capacity_in(mcx, 1).unwrap();
        typcache.push(int4_typcache());
        bd_info.push(
            ::mcx::alloc_in(
                mcx,
                BrinOpcInfo {
                    oi_nstored: 1,
                    oi_regular_nulls: true,
                    oi_opaque: None,
                    oi_typcache: typcache,
                },
            )
            .unwrap(),
        );
    }
    BrinDesc {
        bd_index: make_index_rel(mcx, natts),
        bd_tupdesc: index_tupdesc(mcx, natts),
        bd_totalstored: natts as i32,
        bd_info,
    }
}

#[test]
fn maxalign_bitmaplen_match_c() {
    assert_eq!(maxalign(5), 8);
    assert_eq!(maxalign(8), 8);
    assert_eq!(maxalign(9), 16);
    assert_eq!(bitmaplen(1), 1);
    assert_eq!(bitmaplen(8), 1);
    assert_eq!(bitmaplen(9), 2);
    assert_eq!(bitmaplen(16), 2);
}

#[test]
fn placeholder_tuple_layout() {
    let root = MemoryContext::new("test");
    let mcx = root.mcx();
    let brdesc = brin_desc(mcx, 3);

    let (img, size) = brin_form_placeholder_tuple(mcx, &brdesc, 42).unwrap();

    // len = MAXALIGN(SizeOfBrinTuple + BITMAPLEN(natts*2)).
    let expected_len = maxalign(SIZE_OF_BRIN_TUPLE + bitmaplen(3 * 2));
    assert_eq!(size, expected_len);
    assert_eq!(img.len(), expected_len);

    // bt_blkno round-trips.
    assert_eq!(img.bt_blkno(), 42);

    // bt_info: hoff in the offset bits, plus NULLS | PLACEHOLDER | EMPTY_RANGE.
    let info = img.bt_info();
    assert_ne!(info & BRIN_NULLS_MASK, 0);
    assert_ne!(info & BRIN_PLACEHOLDER_MASK, 0);
    assert_ne!(info & BRIN_EMPTY_RANGE_MASK, 0);

    // allnulls bits: the first `natts` bits of the bitmap are all set
    // (reversed sense: 1 == null). hasnulls (next natts bits) untouched (0).
    let bits = &img.bytes[SIZE_OF_BRIN_TUPLE..];
    for attnum in 0..3usize {
        assert_ne!(bits[attnum >> 3] & (1 << (attnum & 7)), 0, "allnull bit {attnum}");
    }
    for attnum in 3..6usize {
        assert_eq!(bits[attnum >> 3] & (1 << (attnum & 7)), 0, "hasnull bit {attnum}");
    }
}

#[test]
fn tuples_equal_compares_length_and_bytes() {
    let a = [1u8, 2, 3, 4];
    let b = [1u8, 2, 3, 4];
    let c = [1u8, 2, 3, 5];
    assert!(brin_tuples_equal(&a, 4, &b, 4));
    assert!(!brin_tuples_equal(&a, 4, &c, 4));
    assert!(!brin_tuples_equal(&a, 4, &b, 3));
}

#[test]
fn zeroed_image_round_trips_header() {
    let root = MemoryContext::new("test");
    let mcx = root.mcx();
    let mut img = BrinTupleImage::zeroed(mcx, 16).unwrap();
    img.set_bt_blkno(0xDEAD_BEEF);
    img.set_bt_info(0x12);
    img.or_bt_info(0x80);
    assert_eq!(img.bt_blkno(), 0xDEAD_BEEF);
    assert_eq!(img.bt_info(), 0x12 | 0x80);
}
