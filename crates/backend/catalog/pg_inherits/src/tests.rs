// audit-18.6 b089 witnesses: the pg_inherits.c / partition.c syscache-miss
// arms are elog(ERROR, "cache lookup failed for ...") in C (catchable,
// SQLSTATE XX000), never a silent InvalidOid/false fallthrough.
use super::*;
use std::sync::Once;
use syscache_seams::{PgClassLsShape, PgTypeBaseShape};

const KNOWN_REL: Oid = 5001;
const KNOWN_IDX: Oid = 5002;
const MISSING_REL: Oid = 5999;
const SUB_TYPE: Oid = 90004;
const MISSING_TYPE: Oid = 90999;
const TYPTYPE_COMPOSITE: i8 = b'c' as i8;

static SEAMS: Once = Once::new();

fn install() {
    SEAMS.call_once(|| {
        use syscache_seams as s;
        s::lookup_pg_class_ls_shape::set(|relid| {
            Ok((relid == KNOWN_REL || relid == KNOWN_IDX).then_some(PgClassLsShape {
                relnamespace: 2200,
                reltype: SUB_TYPE,
                relam: 0,
                reltablespace: 0,
                relnatts: 1,
                relkind: b'r' as i8,
                relpersistence: b'p' as i8,
                relispartition: false,
                relhassubclass: false,
            }))
        });
        s::pg_type_base_shape::set(|typid| {
            Ok((typid == SUB_TYPE).then_some(PgTypeBaseShape {
                typtype: TYPTYPE_COMPOSITE,
                typbasetype: InvalidOid,
                typtypmod: -1,
                typelem: InvalidOid,
                typsubscript: InvalidOid,
            }))
        });
        s::pg_type_typrelid::set(|typid| Ok((typid == SUB_TYPE).then_some(KNOWN_REL)));
        // The partition's index list names an index whose pg_class row is gone
        // (dropped concurrently): partition.c:190 index_get_partition.
        relcache_seams::relation_get_index_list::set(|mcx, _relid| {
            let mut v = PgVec::new_in(mcx);
            v.push(MISSING_REL);
            Ok(v)
        });
    });
}

fn assert_cache_lookup_failed<T: std::fmt::Debug>(r: PgResult<T>, want: &str) {
    match r {
        Err(e) => {
            assert_eq!(e.message(), want);
            assert_eq!(e.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
            assert_eq!(e.level(), ERROR);
        }
        Ok(v) => panic!("expected error {want:?}, got Ok({v:?})"),
    }
}

// pg_inherits.c:362 has_subclass: SearchSysCache1(RELOID) miss.
#[test]
fn has_subclass_missing_relation_is_cache_lookup_error() {
    install();
    assert!(!has_subclass(KNOWN_REL).unwrap());
    assert_cache_lookup_failed(
        has_subclass(MISSING_REL),
        "cache lookup failed for relation 5999",
    );
}

// pg_inherits.c:420 typeInheritsFrom: typeidTypeRelid(superclassTypeId) on a
// missing type (parse_type.c typeidTypeRelid elog).
#[test]
fn type_inherits_from_missing_superclass_type_is_cache_lookup_error() {
    install();
    assert_cache_lookup_failed(
        typeInheritsFrom(SUB_TYPE, MISSING_TYPE),
        "cache lookup failed for type 90999",
    );
}

// partition.c:190 index_get_partition: SearchSysCache1(RELOID, partIdx) miss.
#[test]
fn index_get_partition_missing_index_is_cache_lookup_error() {
    install();
    let cx = mcx::MemoryContext::new_bump("b089");
    assert_cache_lookup_failed(
        index_get_partition(cx.mcx(), KNOWN_REL, KNOWN_IDX),
        "cache lookup failed for relation 5999",
    );
}
