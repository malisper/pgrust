//! tid.c currtid family: currtid_byrelname (1294) + its currtid_internal
//! (tid.c:296) and currtid_for_view (tid.c:338) helpers.

use mcx::Mcx;
use rel_vocab::RangeVar;
use types_core::{Oid, RELPERSISTENCE_PERMANENT};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_SYNTAX_ERROR};
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::ACL_SELECT;
use types_rel::{Relation, RELKIND_VIEW};
use types_tuple::htup::SelfItemPointerAttributeNumber;
use types_tuple::ItemPointerData;

const ACLCHECK_OK: i32 = 0;
const TIDOID: Oid = 27;

// SplitIdentifierString (varlena.c), ASCII-identifier arm: no lsyscache/
// varlena dependency is reachable from adt_scalar (cycle), so the dotted-name
// split is reimplemented here rather than shared.
fn split_qualified_name(s: &str) -> Option<Vec<String>> {
    let b = s.as_bytes();
    let mut names = Vec::new();
    let mut p = 0usize;
    let isspace = |c: u8| matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0B | 0x0C);
    while p < b.len() && isspace(b[p]) {
        p += 1;
    }
    if p == b.len() {
        return Some(names);
    }
    loop {
        let mut cur = Vec::new();
        if b[p] == b'"' {
            let mut q = p + 1;
            loop {
                let rel = b[q..].iter().position(|&c| c == b'"')?;
                let endp = q + rel;
                cur.extend_from_slice(&b[q..endp]);
                if b.get(endp + 1) == Some(&b'"') {
                    cur.push(b'"');
                    q = endp + 2;
                } else {
                    p = endp + 1;
                    break;
                }
            }
        } else {
            let start = p;
            while p < b.len() && b[p] != b'.' && !isspace(b[p]) {
                p += 1;
            }
            if p == start {
                return None;
            }
            cur = b[start..p].iter().map(|c| c.to_ascii_lowercase()).collect();
        }
        while p < b.len() && isspace(b[p]) {
            p += 1;
        }
        let done = if p < b.len() && b[p] == b'.' {
            p += 1;
            while p < b.len() && isspace(b[p]) {
                p += 1;
            }
            false
        } else if p == b.len() {
            true
        } else {
            return None;
        };
        cur.truncate((types_core::fmgr::NAMEDATALEN - 1) as usize);
        names.push(String::from_utf8_lossy(&cur).into_owned());
        if done {
            return Some(names);
        }
    }
}

#[track_caller]
#[cold]
fn invalid_name_syntax() -> Box<PgError> {
    Box::new(
        PgError::error("invalid name syntax").with_sqlstate(types_error::ERRCODE_INVALID_NAME),
    )
}

fn make_range_var(names: &[String]) -> PgResult<RangeVar<'_>> {
    match names {
        [r] => Ok(RangeVar {
            catalogname: None,
            schemaname: None,
            relname: r,
            inh: true,
            relpersistence: RELPERSISTENCE_PERMANENT,
            location: -1,
        }),
        [s, r] => Ok(RangeVar {
            catalogname: None,
            schemaname: Some(s),
            relname: r,
            inh: true,
            relpersistence: RELPERSISTENCE_PERMANENT,
            location: -1,
        }),
        [c, s, r] => Ok(RangeVar {
            catalogname: Some(c),
            schemaname: Some(s),
            relname: r,
            inh: true,
            relpersistence: RELPERSISTENCE_PERMANENT,
            location: -1,
        }),
        _ => Err(too_many_dotted_names(names)),
    }
}

#[track_caller]
#[cold]
fn too_many_dotted_names(names: &[String]) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "improper relation name (too many dotted names): {}",
            names.join(".")
        ))
        .with_sqlstate(ERRCODE_SYNTAX_ERROR),
    )
}

fn get_relkind_objtype(relkind: u8) -> i32 {
    use types_nodes::parsenodes::ObjectType::*;
    (match relkind {
        types_rel::RELKIND_RELATION | types_rel::RELKIND_PARTITIONED_TABLE => OBJECT_TABLE,
        types_rel::RELKIND_INDEX | types_rel::RELKIND_PARTITIONED_INDEX => OBJECT_INDEX,
        types_rel::RELKIND_SEQUENCE => OBJECT_SEQUENCE,
        RELKIND_VIEW => OBJECT_VIEW,
        types_rel::RELKIND_MATVIEW => OBJECT_MATVIEW,
        types_rel::RELKIND_FOREIGN_TABLE => OBJECT_FOREIGN_TABLE,
        _ => OBJECT_TABLE,
    }) as i32
}

fn namespace_name(nspid: Oid) -> PgResult<String> {
    Ok(syscache_seams::pg_namespace_nspname::call(nspid)?
        .map(|n| String::from_utf8_lossy(n.name_str()).into_owned())
        .unwrap_or_default())
}

// currtid_internal (tid.c:296).
pub fn currtid_internal<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tid: ItemPointerData,
) -> PgResult<ItemPointerData> {
    let relid = rel.data_rc().rd_id;
    let roleid = miscinit_seams::get_user_id::call();
    let (aclresult, _missing) =
        aclchk_seams::pg_class_aclcheck_ext::call(relid, roleid, ACL_SELECT)?;
    if aclresult != ACLCHECK_OK {
        aclchk_seams::aclcheck_error::call(
            aclresult,
            get_relkind_objtype(rel.data_rc().rd_rel.relkind),
            rel.data_rc().name(),
        )?;
    }

    if rel.data_rc().rd_rel.relkind == RELKIND_VIEW {
        return currtid_for_view(mcx, rel, tid);
    }

    if !types_rel::RELKIND_HAS_STORAGE(rel.data_rc().rd_rel.relkind) {
        let nsp = namespace_name(rel.data_rc().namespace())?;
        return Err(no_storage_for_currtid(&nsp, rel.data_rc().name()));
    }

    let snapshot = snapmgr_seams::get_latest_snapshot::call()?;
    let snapshot = snapmgr_seams::register_snapshot::call(snapshot)?;
    let result = tableam_seams::table_tid_get_latest::call(mcx, rel.alias(), snapshot.clone(), tid);
    snapmgr_seams::unregister_snapshot::call(snapshot);
    result
}

#[track_caller]
#[cold]
fn no_storage_for_currtid(nsp: &str, relname: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "cannot look at latest visible tid for relation \"{nsp}.{relname}\""
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

#[track_caller]
#[cold]
fn view_unsupported(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED))
}

// currtid_for_view (tid.c:338).
fn currtid_for_view<'mcx>(
    mcx: Mcx<'mcx>,
    viewrel: &Relation<'mcx>,
    tid: ItemPointerData,
) -> PgResult<ItemPointerData> {
    let att = viewrel.data_rc().descr();
    let mut tididx: Option<usize> = None;
    for i in 0..att.natts as usize {
        let a = &att.attrs[i];
        if a.attname.name_str() == b"ctid" {
            if a.atttypid != TIDOID {
                return Err(view_unsupported("ctid isn't of type TID"));
            }
            tididx = Some(i);
            break;
        }
    }
    let Some(tididx) = tididx else {
        return Err(view_unsupported("currtid cannot handle views with no CTID"));
    };

    let rules = relcache_seams::relation_get_rules::call(viewrel.data_rc().rd_id)?;
    if rules.is_empty() {
        return Err(view_unsupported("the view has no rules"));
    }
    for rule in &rules {
        if rule.event != CmdType::CMD_SELECT as i32 {
            continue;
        }
        let actions_node = readfuncs::stringToNode(mcx, &rule.action_src)?;
        let actions = actions_node.as_list().expect("ev_action is a List");
        if actions.len() != 1 {
            return Err(view_unsupported("only one select rule is allowed in views"));
        }
        let query = actions.iter().next().unwrap().as_query().expect("rule action is a Query");
        let tle = query.targetList.iter().find_map(|n| {
            let te = n.as_target_entry()?;
            (te.resno as usize == tididx + 1).then_some(te)
        });
        if let Some(tle) = tle {
            if let Some(var) = tle.expr.as_var() {
                if var.varno >= 0 && var.varattno as i32 == SelfItemPointerAttributeNumber {
                    let rte = query
                        .rtable
                        .iter()
                        .nth((var.varno - 1) as usize)
                        .and_then(|n| n.as_range_tbl_entry());
                    if let Some(rte) = rte {
                        let base = table::table_open(mcx, rte.relid, types_rel::AccessShareLock)?;
                        let result = currtid_internal(mcx, &base, tid);
                        base.close(types_rel::AccessShareLock)?;
                        return result;
                    }
                }
            }
        }
        break;
    }
    Err(view_unsupported("currtid cannot handle this view"))
}

// currtid_byrelname (tid.c:418).
pub fn currtid_byrelname<'mcx>(
    mcx: Mcx<'mcx>,
    relname: &str,
    tid: ItemPointerData,
) -> PgResult<ItemPointerData> {
    let names = split_qualified_name(relname).ok_or_else(invalid_name_syntax)?;
    if names.is_empty() {
        return Err(invalid_name_syntax());
    }
    let rv = make_range_var(&names)?;
    let rel = table::table_openrv(mcx, &rv, types_rel::AccessShareLock)?;
    let result = currtid_internal(mcx, &rel, tid);
    rel.close(types_rel::AccessShareLock)?;
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_qualified_name_dotted() {
        assert_eq!(split_qualified_name("t1").unwrap(), vec!["t1"]);
        assert_eq!(split_qualified_name("s1.t1").unwrap(), vec!["s1", "t1"]);
        assert_eq!(split_qualified_name("DB.S1.T1").unwrap(), vec!["db", "s1", "t1"]);
        assert_eq!(split_qualified_name("\"Mixed Case\"").unwrap(), vec!["Mixed Case"]);
        assert_eq!(split_qualified_name("  t1  ").unwrap(), vec!["t1"]);
        assert!(split_qualified_name("\"unterminated").is_none());
        assert!(split_qualified_name("a..b").is_none());
    }

    #[test]
    fn make_range_var_shapes() {
        let names = vec!["t1".to_string()];
        let rv = make_range_var(&names).unwrap();
        assert_eq!(rv.relname, "t1");
        assert!(rv.schemaname.is_none());

        let names = vec!["s1".to_string(), "t1".to_string()];
        let rv = make_range_var(&names).unwrap();
        assert_eq!(rv.schemaname, Some("s1"));
        assert_eq!(rv.relname, "t1");

        let names =
            vec!["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string()];
        assert!(make_range_var(&names).is_err());
    }

    #[test]
    fn relkind_objtype_mapping() {
        use types_nodes::parsenodes::ObjectType;
        assert_eq!(get_relkind_objtype(types_rel::RELKIND_RELATION), ObjectType::OBJECT_TABLE as i32);
        assert_eq!(get_relkind_objtype(types_rel::RELKIND_VIEW), ObjectType::OBJECT_VIEW as i32);
        assert_eq!(get_relkind_objtype(types_rel::RELKIND_SEQUENCE), ObjectType::OBJECT_SEQUENCE as i32);
        assert_eq!(get_relkind_objtype(types_rel::RELKIND_INDEX), ObjectType::OBJECT_INDEX as i32);
        assert_eq!(get_relkind_objtype(b'?'), ObjectType::OBJECT_TABLE as i32);
    }
}
