use super::*;
use datum::Datum;
use mcx::MemoryContext;
use std::sync::Once;
use types_nodes::{Node, NodeList};

const REL_OID: Oid = 5001;
const COUNTED_REL_OID: Oid = 5002;

static SEAMS: Once = Once::new();
static COUNTED_LOOKUPS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn install() {
    SEAMS.call_once(|| {
        syscache_seams::pg_attribute_attoptions::set(|mcx, relid, attnum| {
            if relid == COUNTED_REL_OID {
                COUNTED_LOOKUPS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return Ok(Some(None));
            }
            if relid != REL_OID {
                return Ok(None);
            }
            Ok(match attnum {
                1 => {
                    let arg = Node::mk(mcx, ::types_nodes::String { sval: "100" })?;
                    let def = Node::mk(
                        mcx,
                        ::types_nodes::parsenodes::DefElem {
                            defnamespace: None,
                            defname: Some("n_distinct"),
                            arg: Some(arg),
                            defaction:
                                ::types_nodes::parsenodes::DefElemAction::DEFELEM_UNSPEC,
                            location: -1,
                        },
                    )?;
                    let list = NodeList::make1(mcx, def)?;
                    let img = reloptions::transformRelOptions(
                        mcx, None, &list, None, &[], false, false,
                    )?
                    .expect("options image");
                    Some(Some(Datum::from_usize(img.leak().as_ptr() as usize)))
                }
                2 => Some(None),
                _ => None,
            })
        });
    });
}

#[test]
fn parses_present_null_and_missing_attoptions() {
    install();
    let cx = MemoryContext::new("t");
    let m = cx.mcx();
    let opts = get_attribute_options(m, REL_OID, 1).unwrap().unwrap();
    assert_eq!(opts.n_distinct, 100.0);
    assert_eq!(opts.n_distinct_inherited, 0.0);
    assert!(get_attribute_options(m, REL_OID, 2).unwrap().is_none());
    assert!(get_attribute_options(m, REL_OID, 9).unwrap().is_none());
    assert!(get_attribute_options(m, 1, 1).unwrap().is_none());
}

// attoptcache.c:53-124: parsed options are cached per (attrelid, attnum) and
// the ATTNUM syscache callback flushes the whole hash; a second lookup must
// not consult the syscache again until an invalidation arrives.
#[test]
fn caches_until_the_attnum_callback_flushes() {
    install();
    let cx = MemoryContext::new("t");
    let m = cx.mcx();
    let before = COUNTED_LOOKUPS.load(std::sync::atomic::Ordering::SeqCst);
    assert!(get_attribute_options(m, COUNTED_REL_OID, 1).unwrap().is_none());
    assert!(get_attribute_options(m, COUNTED_REL_OID, 1).unwrap().is_none());
    assert!(get_attribute_options(m, COUNTED_REL_OID, 1).unwrap().is_none());
    assert_eq!(COUNTED_LOOKUPS.load(std::sync::atomic::Ordering::SeqCst) - before, 1);
    // A different attnum is a different entry.
    assert!(get_attribute_options(m, COUNTED_REL_OID, 2).unwrap().is_none());
    assert_eq!(COUNTED_LOOKUPS.load(std::sync::atomic::Ordering::SeqCst) - before, 2);
    // ATTNUM invalidation: everything is re-read.
    InvalidateAttoptCacheCallback(Datum::null(), 7, 0);
    assert!(get_attribute_options(m, COUNTED_REL_OID, 1).unwrap().is_none());
    assert_eq!(COUNTED_LOOKUPS.load(std::sync::atomic::Ordering::SeqCst) - before, 3);
}
