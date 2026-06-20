//! `expandTableLikeClause` (`parser/parse_utilcmd.c`) — the delayed
//! `CREATE TABLE ... (LIKE ...)` processing.
//!
//! `transformTableLikeClause` (see [`crate::like`]) handles the column-copy and
//! NOT NULL legs at parse time, but defers DEFAULTS / GENERATED / CONSTRAINTS /
//! INDEXES / STATISTICS to here, because those need the final column numbers the
//! new child table received from `DefineRelation`. `utility.c`
//! (`ProcessUtilitySlow`) calls this once the child exists, through the
//! `expand_table_like_clause` out-seam this crate installs in `init_seams`.
//!
//! It returns a `List` of utility statements to run: an `AlterTableStmt` that
//! installs cooked defaults and CHECK constraints, followed by `IndexStmt`s for
//! INCLUDING INDEXES and any constraint `CommentStmt`s.
//!
//! DEFERRED LEG — INCLUDING STATISTICS: the C calls
//! `RelationGetStatExtList(relation)` + `generateClonedExtStatsStmt(...)` to
//! clone extended-statistics objects. `generateClonedExtStatsStmt`
//! (`statscmds.c`) is not yet ported, so the STATISTICS leg is not produced
//! here. `CREATE TABLE ... INCLUDING STATISTICS` (or INCLUDING ALL) silently
//! omits cloned extended statistics; all other legs are faithful.

use alloc::format;
use alloc::string::ToString;

use mcx::{Mcx, PgString, PgVec};

use types_core::primitive::InvalidOid;
use types_core::Oid;
use types_error::{PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
use types_nodes::ddlnodes::{
    AlterTableCmd, AlterTableStmt, AlterTableType, CommentStmt, ConstrType,
};
use types_nodes::parsenodes::DROP_RESTRICT;
use types_nodes::nodes::Node;
use types_nodes::parsenodes::{OBJECT_TABCONSTRAINT, OBJECT_TABLE};
use types_storage::lock::{AccessShareLock, NoLock};

use backend_access_common_next::attmap::build_attrmap_by_name;
use backend_access_common_relation::relation_open;
use backend_access_common_tupdesc::TupleDescGetDefault;
use backend_access_index_indexam::{index_close, index_open};
use backend_access_table_table::table_close;
use backend_commands_comment::GetComment;
use backend_commands_vacuum_seams as vacuum_seams;
use backend_nodes_outfuncs::nodeToString;
use backend_nodes_read_seams as read_seams;
use backend_rewrite_core::replace::map_variable_attnos;
use backend_utils_error::ereport;

use crate::cloned_index::generateClonedIndexStmt;
use crate::column::default_constraint;
use crate::core::{make_string, NodePtr};

/// `RelationRelationId` (`pg_class.h`, OID 1259).
const RELATION_RELATION_ID: Oid = 1259;
/// `ConstraintRelationId` (`pg_constraint.h`, OID 2606).
const CONSTRAINT_RELATION_ID: Oid = 2606;
/// `RELKIND_FOREIGN_TABLE` (`pg_class.h`).
const RELKIND_FOREIGN_TABLE: u8 = b'f';

// TableLikeOption bits (`nodes/parsenodes.h`).
const CREATE_TABLE_LIKE_COMMENTS: u32 = 1 << 0;
const CREATE_TABLE_LIKE_CONSTRAINTS: u32 = 1 << 2;
const CREATE_TABLE_LIKE_DEFAULTS: u32 = 1 << 3;
const CREATE_TABLE_LIKE_GENERATED: u32 = 1 << 4;
const CREATE_TABLE_LIKE_INDEXES: u32 = 1 << 6;

/// `expandTableLikeClause(heapRel, table_like_clause)` (parse_utilcmd.c).
///
/// `heap_rv` is the newly-created child table's `RangeVar`; `like_clause` is the
/// (already-transformed, `relationOid`-carrying) `TableLikeClause`. Returns the
/// list of utility statements to run after the child exists.
pub fn expandTableLikeClause<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rv: NodePtr<'mcx>,
    like_clause: NodePtr<'mcx>,
) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    let mut result: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    let mut atsubcmds: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);

    let Some(tlc) = like_clause.as_ref().as_tablelikeclause() else {
        unreachable!(
            "expandTableLikeClause: not a TableLikeClause node: {}",
            like_clause.node_tag()
        );
    };
    let options = tlc.options;
    let relation_oid = tlc.relationOid;

    if relation_oid == InvalidOid {
        return Err(types_error::PgError::error(
            "expandTableLikeClause called on untransformed LIKE clause",
        ));
    }

    let Some(heap_rangevar) = heap_rv.as_ref().as_rangevar() else {
        unreachable!(
            "expandTableLikeClause: heapRel not a RangeVar node: {}",
            heap_rv.node_tag()
        );
    };
    // Snapshot the child schema/relname for the constraint comment statements.
    let heap_schemaname = heap_rangevar
        .schemaname
        .as_ref()
        .map_or_else(alloc::string::String::new, |s| s.as_str().to_string());
    let heap_relname = heap_rangevar
        .relname
        .as_ref()
        .map_or_else(alloc::string::String::new, |s| s.as_str().to_string());

    // Open the parent relation (we still hold the lock taken by
    // transformTableLikeClause — open by OID to be sure we get the same table).
    let relation = relation_open(mcx, relation_oid, NoLock)?;
    let relname = relation.name().to_string();

    // Open the newly-created child relation (we hold lock on it too) so we can
    // build the attno map. relation_openrv(heapRel, NoLock) opens by name.
    let child_access_rv = crate::like::access_range_var(heap_rangevar);
    let childrel =
        backend_access_common_relation::relation_openrv(mcx, &child_access_rv, NoLock)?;
    let childrel_relkind = childrel.rd_rel.relkind;

    // build_attrmap_by_name(childrel, parent, false) — map parent attnos to the
    // child's. (re-checks type match; can't fail since both are locked.)
    let attmap = build_attrmap_by_name(mcx, &childrel.rd_att, &relation.rd_att, false)?;

    let parent_natts = relation.rd_att.natts as usize;
    let has_constr = relation.rd_att.constr.is_some();

    // Process defaults, if required.
    if options & (CREATE_TABLE_LIKE_DEFAULTS | CREATE_TABLE_LIKE_GENERATED) != 0 && has_constr {
        for parent_attno in 0..parent_natts {
            let attribute = relation.rd_att.attr(parent_attno);
            if attribute.attisdropped {
                continue;
            }

            // Copy default, if present and it should be copied. Separate options
            // for plain defaults vs GENERATED defaults.
            let want = if attribute.attgenerated != 0 {
                options & CREATE_TABLE_LIKE_GENERATED != 0
            } else {
                options & CREATE_TABLE_LIKE_DEFAULTS != 0
            };
            if attribute.atthasdef && want {
                let mut this_default =
                    TupleDescGetDefault(mcx, &relation.rd_att, (parent_attno + 1) as i16)?
                        .ok_or_else(|| {
                            types_error::PgError::error(format!(
                                "default expression not found for attribute {} of relation \"{}\"",
                                parent_attno + 1,
                                relname
                            ))
                        })?;

                let mut found_whole_row = false;
                map_variable_attnos(
                    &mut this_default,
                    1,
                    0,
                    &attmap.attnums,
                    InvalidOid,
                    &mut found_whole_row,
                    mcx,
                )?;

                if found_whole_row {
                    let attname =
                        core::str::from_utf8(attribute.attname.name_str()).unwrap_or("");
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("cannot convert whole-row table reference")
                        .errdetail(format!(
                            "Generation expression for column \"{attname}\" contains a whole-row reference to table \"{relname}\"."
                        ))
                        .into_error());
                }

                let atsubcmd = AlterTableCmd {
                    subtype: AlterTableType::AT_CookedColumnDefault,
                    name: None,
                    num: attmap.attnums[parent_attno],
                    newowner: None,
                    def: Some(this_default),
                    behavior: DROP_RESTRICT,
                    missing_ok: false,
                    recurse: false,
                };
                atsubcmds
                    .push(mcx::alloc_in(mcx, Node::mk_alter_table_cmd(mcx, atsubcmd)?)?);
            }
        }
    }

    // Copy CHECK constraints if requested, adjusting attribute numbers.
    if options & CREATE_TABLE_LIKE_CONSTRAINTS != 0 && has_constr {
        // Collect the check entries first as owned strings (the borrow of
        // `relation.rd_att` ends before we mutate `atsubcmds` / call seams).
        type CheckEntry = (
            Option<alloc::string::String>,
            alloc::string::String,
            bool,
            bool,
        );
        let checks: alloc::vec::Vec<CheckEntry> = {
            let constr = relation.rd_att.constr.as_ref().unwrap();
            (0..constr.num_check as usize)
                .map(|i| {
                    let c = &constr.check[i];
                    (
                        c.ccname.as_ref().map(|s| s.as_str().to_string()),
                        c.ccbin.as_ref().map_or_else(alloc::string::String::new, |s| {
                            s.as_str().to_string()
                        }),
                        c.ccenforced,
                        c.ccnoinherit,
                    )
                })
                .collect()
        };

        for (ccname, ccbin, ccenforced, ccnoinherit) in checks {
            let mut ccbin_node = read_seams::string_to_node::call(mcx, &ccbin)?;

            let mut found_whole_row = false;
            map_variable_attnos(
                &mut ccbin_node,
                1,
                0,
                &attmap.attnums,
                InvalidOid,
                &mut found_whole_row,
                mcx,
            )?;

            // Reject whole-row variables (LIKE divergence guarantee).
            if found_whole_row {
                let cn = ccname.as_deref().unwrap_or("");
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("cannot convert whole-row table reference")
                    .errdetail(format!(
                        "Constraint \"{cn}\" contains a whole-row reference to table \"{relname}\"."
                    ))
                    .into_error());
            }

            let cooked = nodeToString(mcx, ccbin_node.as_ref())?;

            let mut n = default_constraint(mcx);
            n.contype = ConstrType::CONSTR_CHECK;
            n.conname = match &ccname {
                Some(s) => Some(PgString::from_str_in(s, mcx)?),
                None => None,
            };
            n.location = -1;
            n.is_enforced = ccenforced;
            n.initially_valid = ccenforced; // sic
            n.is_no_inherit = ccnoinherit;
            n.raw_expr = None;
            n.cooked_expr = Some(cooked);
            // Skip validation; the new table should be empty.
            n.skip_validation = true;
            let conname_owned = ccname.clone();

            let atsubcmd = AlterTableCmd {
                subtype: AlterTableType::AT_AddConstraint,
                name: None,
                num: 0,
                newowner: None,
                def: Some(mcx::alloc_in(mcx, Node::mk_constraint(mcx, n)?)?),
                behavior: DROP_RESTRICT,
                missing_ok: false,
                recurse: false,
            };
            atsubcmds
                .push(mcx::alloc_in(mcx, Node::mk_alter_table_cmd(mcx, atsubcmd)?)?);

            // Copy comment on constraint.
            if options & CREATE_TABLE_LIKE_COMMENTS != 0 {
                if let Some(ref cn) = conname_owned {
                    let con_oid = backend_catalog_pg_constraint::get_relation_constraint_oid(
                        mcx,
                        relation_oid,
                        cn,
                        false,
                    )?;
                    if let Some(comment) =
                        GetComment(mcx, con_oid, CONSTRAINT_RELATION_ID, 0)?
                    {
                        let mut object: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
                        object.push(make_string(mcx, &heap_schemaname)?);
                        object.push(make_string(mcx, &heap_relname)?);
                        object.push(make_string(mcx, cn)?);
                        let stmt = CommentStmt {
                            objtype: OBJECT_TABCONSTRAINT,
                            object: Some(mcx::alloc_in(mcx, Node::mk_list(mcx, object)?)?),
                            comment: Some(PgString::from_str_in(&comment, mcx)?),
                        };
                        result.push(mcx::alloc_in(mcx, Node::mk_comment_stmt(mcx, stmt)?)?);
                    }
                }
            }
        }
    }

    // If we generated any ALTER TABLE actions, wrap them in a single ALTER TABLE
    // and put it at the FRONT of result (so it runs before the CommentStmts).
    if !atsubcmds.is_empty() {
        let atcmd = AlterTableStmt {
            relation: Some(mcx::alloc_in(mcx, heap_rv.clone_in(mcx)?)?),
            cmds: atsubcmds,
            objtype: OBJECT_TABLE,
            missing_ok: false,
        };
        let atcmd_node = mcx::alloc_in(mcx, Node::mk_alter_table_stmt(mcx, atcmd)?)?;
        // lcons(atcmd, result).
        let mut new_result: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
        new_result.push(atcmd_node);
        for s in result.drain(..) {
            new_result.push(s);
        }
        result = new_result;
    }

    // Process indexes if required.
    if options & CREATE_TABLE_LIKE_INDEXES != 0
        && relation.rd_rel.relhasindex
        && childrel_relkind != RELKIND_FOREIGN_TABLE
    {
        let parent_indexes = vacuum_seams::relation_get_index_list::call(relation_oid)?;

        for parent_index_oid in parent_indexes {
            let parent_index = index_open(mcx, parent_index_oid, AccessShareLock)?;

            // Build CREATE INDEX statement to recreate the parent_index.
            let (mut index_stmt, _con_oid) =
                generateClonedIndexStmt(mcx, Some(heap_rangevar), &parent_index, &attmap)?;

            // Copy comment on index, if requested (rides on idxcomment so we
            // don't need to know the new index's name yet).
            if options & CREATE_TABLE_LIKE_COMMENTS != 0 {
                if let Some(comment) =
                    GetComment(mcx, parent_index_oid, RELATION_RELATION_ID, 0)?
                {
                    index_stmt.idxcomment = Some(PgString::from_str_in(&comment, mcx)?);
                }
            }

            result.push(mcx::alloc_in(mcx, Node::mk_index_stmt(mcx, index_stmt)?)?);

            index_close(parent_index, AccessShareLock)?;
        }
    }

    // INCLUDING STATISTICS: DEFERRED — generateClonedExtStatsStmt unported.
    // (See module docs.)

    // Done with child rel.
    table_close(childrel, NoLock)?;

    // Close the parent rel, keeping the AccessShareLock until xact commit.
    table_close(relation, NoLock)?;

    Ok(result)
}
