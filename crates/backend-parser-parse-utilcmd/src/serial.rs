//! `generateSerialExtraStmts` (`parse_utilcmd.c`) — generate the implicit
//! `CREATE SEQUENCE` + `ALTER SEQUENCE ... OWNED BY` statements that back a
//! SERIAL / `GENERATED ... AS IDENTITY` column, choose the sequence name, and
//! set the column's `identitySequence`.
//!
//! The function appends the generated statements to the context's before/after
//! lists in place (C: `cxt->blist = lappend(...)`, `cxt->alist = lappend(...)`)
//! and returns the chosen `(snamespace, sname)` so the caller can build the
//! `nextval('seqname'::regclass)` default.
//!
//! For CREATE TABLE / CREATE FOREIGN TABLE, `cxt.rel_oid` is `InvalidOid` and
//! the namespace/persistence come from `cxt.relation` (the `RangeVar`). For the
//! ALTER TABLE path (`cxt.rel_oid` valid) the existing relation is opened
//! through the relcache to read its namespace, persistence and owner — exactly
//! the `cxt->rel` reads of the C.

use mcx::{Mcx, PgString, PgVec};

use backend_utils_error::ereport;
use types_core::primitive::InvalidOid;
use types_core::{Oid, OidIsValid};
use types_error::pg_error::PgError;
use types_error::{PgResult, ERRCODE_INVALID_TABLE_DEFINITION, ERRCODE_SYNTAX_ERROR, ERROR};

use types_nodes::ddlnodes::{AlterSeqStmt, CreateSeqStmt, DefElem, DEFELEM_UNSPEC};
use types_nodes::nodes::Node;
use types_nodes::rawnodes::{ColumnDef, RangeVar, TypeName};
use types_nodes::value::StringNode;

use backend_access_common_relation::relation_open;
use backend_catalog_namespace::{
    makeRangeVarFromNameList, RangeVarAdjustRelationPersistence, RangeVarGetCreationNamespace,
};
use backend_commands_indexcmds::ChooseRelationName;
use backend_utils_cache_lsyscache::namespace_range_index_pubsub::get_namespace_name;
use types_storage::lock::NoLock;

use crate::core::{CreateStmtContext, NodePtr};
use crate::errpos::parser_errposition;

use alloc::string::{String, ToString};
use alloc::vec::Vec;

/// `RELPERSISTENCE_*` (`catalog/pg_class.h`).
const RELPERSISTENCE_PERMANENT: i8 = b'p' as i8;
const RELPERSISTENCE_UNLOGGED: i8 = b'u' as i8;
const RELPERSISTENCE_TEMP: i8 = b't' as i8;

/// `makeRangeVar(schemaname, relname, location)` (`nodes/makefuncs.c`) — a
/// `RangeVar` with `inh = true` and `relpersistence = RELPERSISTENCE_PERMANENT`.
fn make_range_var<'mcx>(
    mcx: Mcx<'mcx>,
    schemaname: Option<&str>,
    relname: &str,
    location: i32,
) -> PgResult<RangeVar<'mcx>> {
    Ok(RangeVar {
        catalogname: None,
        schemaname: match schemaname {
            Some(s) => Some(PgString::from_str_in(s, mcx)?),
            None => None,
        },
        relname: Some(PgString::from_str_in(relname, mcx)?),
        inh: true,
        relpersistence: RELPERSISTENCE_PERMANENT,
        alias: None,
        location,
    })
}

/// `makeString(str)` as a boxed `Node`.
fn make_string_node<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<NodePtr<'mcx>> {
    mcx::alloc_in(
        mcx,
        Node::mk_string(
            mcx,
            StringNode {
                sval: PgString::from_str_in(s, mcx)?,
            },
        ),
    )
}

/// `makeTypeNameFromOid(typeOid, typmod)` (`nodes/makefuncs.c`).
fn make_type_name_node<'mcx>(mcx: Mcx<'mcx>, type_oid: Oid, typmod: i32) -> PgResult<NodePtr<'mcx>> {
    mcx::alloc_in(
        mcx,
        Node::mk_type_name(
            mcx,
            TypeName {
                names: PgVec::new_in(mcx),
                typeOid: type_oid,
                setof: false,
                pct_type: false,
                typmods: PgVec::new_in(mcx),
                typemod: typmod,
                arrayBounds: PgVec::new_in(mcx),
                location: -1,
            },
        ),
    )
}

/// `makeDefElem(name, arg, location)` (`nodes/makefuncs.c`).
fn make_def_elem<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    arg: Option<NodePtr<'mcx>>,
    location: i32,
) -> PgResult<DefElem<'mcx>> {
    Ok(DefElem {
        defnamespace: None,
        defname: Some(PgString::from_str_in(name, mcx)?),
        arg,
        defaction: DEFELEM_UNSPEC,
        location,
    })
}

/// `generateSerialExtraStmts(cxt, column, seqtypid, seqoptions, for_identity,
/// col_exists, &snamespace_p, &sname_p)`.
///
/// Appends the generated `CreateSeqStmt` to `cxt.blist`, the `AlterSeqStmt`
/// (OWNED BY) to `cxt.blist` when `col_exists` else `cxt.alist`, sets
/// `column.identitySequence`, and returns `(snamespace, sname)`.
pub fn generateSerialExtraStmts<'mcx>(
    cxt: &mut CreateStmtContext<'mcx>,
    column: &mut ColumnDef<'mcx>,
    seqtypid: Oid,
    seqoptions: PgVec<'mcx, NodePtr<'mcx>>,
    for_identity: bool,
    col_exists: bool,
) -> PgResult<(PgString<'mcx>, PgString<'mcx>)> {
    let mcx = cxt.mcx;

    // Check for non-SQL-standard options (not supported within CREATE SEQUENCE,
    // because they'd be redundant), and remove them from the list if found. We
    // build a fresh `kept_options` (C makes a list_copy first, then deletes the
    // matched cells via foreach_delete_current).
    let mut name_el: Option<NodePtr<'mcx>> = None;
    let mut logged_el: Option<NodePtr<'mcx>> = None;
    let mut kept_options: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    for opt in seqoptions.into_iter() {
        let defname = opt
            .as_defelem()
            .and_then(|d| d.defname.as_ref())
            .map(PgString::as_str)
            .unwrap_or("")
            .to_string();
        if defname == "sequence_name" {
            if name_el.is_some() {
                let loc = opt.as_defelem().map_or(-1, |d| d.location);
                return Err(conflicting_def_elem(cxt, loc));
            }
            name_el = Some(opt);
        } else if defname == "logged" || defname == "unlogged" {
            if logged_el.is_some() {
                let loc = opt.as_defelem().map_or(-1, |d| d.location);
                return Err(conflicting_def_elem(cxt, loc));
            }
            logged_el = Some(opt);
        } else {
            kept_options.push(opt);
        }
    }

    let colname = column
        .colname
        .as_ref()
        .map(PgString::as_str)
        .unwrap_or("")
        .to_string();
    let relname = cxt.relname().to_string();

    // Read the persistence / owner from cxt->rel (ALTER) or cxt->relation (CREATE).
    let (rel_persistence, owner_id): (i8, Oid) = if OidIsValid(cxt.rel_oid) {
        let rel = relation_open(mcx, cxt.rel_oid, NoLock)?;
        (rel.rd_rel.relpersistence as i8, rel.rd_rel.relowner)
    } else {
        let persistence = cxt
            .relation
            .as_deref()
            .and_then(|n| n.as_rangevar())
            .map_or(RELPERSISTENCE_PERMANENT, |rv| rv.relpersistence);
        (persistence, InvalidOid)
    };

    // Determine namespace and name to use for the sequence.
    let (snamespace, sname): (PgString<'mcx>, PgString<'mcx>) = if let Some(name_el) =
        name_el.as_ref()
    {
        // Use specified name: makeRangeVarFromNameList(castNode(List, nameEl->arg)).
        let names = name_el
            .as_defelem()
            .and_then(|d| d.arg.as_deref())
            .and_then(|n| n.as_list());
        let rv = make_range_var_from_name_list_node(mcx, names)?;
        let snamespace = match rv.schemaname.as_ref() {
            Some(schema) => PgString::from_str_in(schema.as_str(), mcx)?,
            None => {
                // Given unqualified SEQUENCE NAME, select namespace.
                let snamespaceid = sequence_namespace_id(cxt, mcx, false)?;
                namespace_name_str(mcx, snamespaceid)?
            }
        };
        let sname = match rv.relname.as_ref() {
            Some(r) => PgString::from_str_in(r.as_str(), mcx)?,
            None => PgString::from_str_in("", mcx)?,
        };
        (snamespace, sname)
    } else {
        // Generate a name.
        let snamespaceid = sequence_namespace_id(cxt, mcx, true)?;
        let snamespace = namespace_name_str(mcx, snamespaceid)?;
        let chosen = ChooseRelationName(mcx, &relname, Some(&colname), "seq", snamespaceid, false)?;
        (snamespace, PgString::from_str_in(&chosen, mcx)?)
    };

    // Determine the persistence of the sequence (copy the table's; LOGGED /
    // UNLOGGED overrides unless TEMP).
    let mut seqpersistence = rel_persistence;
    if let Some(logged_el) = logged_el.as_ref() {
        let logged_defname = logged_el
            .as_defelem()
            .and_then(|d| d.defname.as_ref())
            .map(PgString::as_str)
            .unwrap_or("")
            .to_string();
        let logged_loc = logged_el.as_defelem().map_or(-1, |d| d.location);
        if seqpersistence == RELPERSISTENCE_TEMP {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                .errmsg("cannot set logged status of a temporary sequence")
                .errposition(parser_errposition(&cxt.pstate, logged_loc))
                .into_error());
        } else if logged_defname == "logged" {
            seqpersistence = RELPERSISTENCE_PERMANENT;
        } else {
            seqpersistence = RELPERSISTENCE_UNLOGGED;
        }
    }

    // Build the CREATE SEQUENCE command.
    let mut seq_rangevar = make_range_var(mcx, Some(snamespace.as_str()), sname.as_str(), -1)?;
    seq_rangevar.relpersistence = seqpersistence;

    // If a sequence data type was specified, prepend it (lcons) to the options.
    let mut create_options: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    if OidIsValid(seqtypid) {
        let as_arg = make_type_name_node(mcx, seqtypid, -1)?;
        let as_def = make_def_elem(mcx, "as", Some(as_arg), -1)?;
        create_options.push(mcx::alloc_in(mcx, Node::mk_def_elem(mcx, as_def))?);
    }
    for o in kept_options.into_iter() {
        create_options.push(o);
    }

    let seqstmt = CreateSeqStmt {
        sequence: Some(mcx::alloc_in(mcx, Node::mk_range_var(mcx, seq_rangevar))?),
        options: create_options,
        ownerId: owner_id,
        for_identity,
        if_not_exists: false,
    };
    cxt.blist
        .push(mcx::alloc_in(mcx, Node::mk_create_seq_stmt(mcx, seqstmt))?);

    // Store the identity sequence name on the column.
    column.identitySequence = Some(mcx::alloc_in(
        mcx,
        make_range_var(mcx, Some(snamespace.as_str()), sname.as_str(), -1)?,
    )?);

    // Build the ALTER SEQUENCE ... OWNED BY command.
    let alt_rangevar = make_range_var(mcx, Some(snamespace.as_str()), sname.as_str(), -1)?;
    let mut attnamelist: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    attnamelist.push(make_string_node(mcx, snamespace.as_str())?);
    attnamelist.push(make_string_node(mcx, &relname)?);
    attnamelist.push(make_string_node(mcx, &colname)?);
    let attnamelist_node = mcx::alloc_in(mcx, Node::mk_list(mcx, attnamelist))?;
    let owned_by = make_def_elem(mcx, "owned_by", Some(attnamelist_node), -1)?;
    let mut alt_options: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    alt_options.push(mcx::alloc_in(mcx, Node::mk_def_elem(mcx, owned_by))?);

    let altseqstmt = AlterSeqStmt {
        sequence: Some(mcx::alloc_in(mcx, Node::mk_range_var(mcx, alt_rangevar))?),
        options: alt_options,
        for_identity,
        missing_ok: false,
    };
    let altseq_node = mcx::alloc_in(mcx, Node::mk_alter_seq_stmt(mcx, altseqstmt))?;

    if col_exists {
        cxt.blist.push(altseq_node);
    } else {
        cxt.alist.push(altseq_node);
    }

    Ok((snamespace, sname))
}

/// `errorConflictingDefElem(defel, cxt->pstate)`.
fn conflicting_def_elem(cxt: &CreateStmtContext<'_>, location: i32) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg("conflicting or redundant options")
        .errposition(parser_errposition(&cxt.pstate, location))
        .into_error()
}

/// The namespace OID for the sequence: `RelationGetNamespace(cxt->rel)` (ALTER)
/// or `RangeVarGetCreationNamespace(cxt->relation)` (CREATE). On the
/// generate-a-name CREATE path, C also calls
/// `RangeVarAdjustRelationPersistence(cxt->relation, snamespaceid)` — gated by
/// `adjust_persistence`.
fn sequence_namespace_id<'mcx>(
    cxt: &mut CreateStmtContext<'mcx>,
    mcx: Mcx<'mcx>,
    adjust_persistence: bool,
) -> PgResult<Oid> {
    if OidIsValid(cxt.rel_oid) {
        let rel = relation_open(mcx, cxt.rel_oid, NoLock)?;
        Ok(rel.rd_rel.relnamespace)
    } else {
        let rv_access = cxt
            .relation
            .as_deref()
            .and_then(|n| n.as_rangevar())
            .map(to_access_range_var)
            .ok_or_else(|| {
                ereport(ERROR)
                    .errmsg_internal("generateSerialExtraStmts: cxt->relation is not a RangeVar")
                    .into_error()
            })?;
        let nsid = RangeVarGetCreationNamespace(mcx, &rv_access)?;
        if adjust_persistence {
            // RangeVarAdjustRelationPersistence reads/writes relpersistence and
            // may raise; mutate a transient access copy then write the (possibly
            // adjusted) persistence back onto the node-tree RangeVar.
            let mut access_rv = rv_access;
            RangeVarAdjustRelationPersistence(mcx, &mut access_rv, nsid)?;
            if let Some(rv) = cxt.relation.as_deref_mut().and_then(|n| n.as_rangevar_mut()) {
                rv.relpersistence = access_rv.relpersistence as i8;
            }
        }
        Ok(nsid)
    }
}

/// `get_namespace_name(nspid)` returning a non-NULL string.
fn namespace_name_str<'mcx>(mcx: Mcx<'mcx>, nspid: Oid) -> PgResult<PgString<'mcx>> {
    match get_namespace_name(mcx, nspid)? {
        Some(s) => Ok(s),
        None => Err(ereport(ERROR)
            .errmsg_internal(alloc::format!("cache lookup failed for namespace {nspid}"))
            .into_error()),
    }
}

/// Convert the node-tree `RangeVar` into the catalog-namespace
/// `types_tuple::access::RangeVar`.
fn to_access_range_var(rv: &RangeVar<'_>) -> types_tuple::access::RangeVar {
    types_tuple::access::RangeVar {
        catalogname: rv.catalogname.as_ref().map(|s| s.as_str().to_string()),
        schemaname: rv.schemaname.as_ref().map(|s| s.as_str().to_string()),
        relname: rv
            .relname
            .as_ref()
            .map_or_else(String::new, |s| s.as_str().to_string()),
        inh: rv.inh,
        relpersistence: rv.relpersistence as u8,
        location: rv.location,
    }
}

/// `makeRangeVarFromNameList(names)` over the node-tree list, returning a
/// node-tree `RangeVar`.
fn make_range_var_from_name_list_node<'mcx>(
    mcx: Mcx<'mcx>,
    names: Option<&PgVec<'mcx, NodePtr<'mcx>>>,
) -> PgResult<RangeVar<'mcx>> {
    let mut namelist: Vec<Option<String>> = Vec::new();
    if let Some(names) = names {
        for n in names.iter() {
            let s = n.as_string().map(|sn| sn.sval.as_str().to_string());
            namelist.push(s);
        }
    }
    let rv = makeRangeVarFromNameList(&namelist)?;
    Ok(RangeVar {
        catalogname: match rv.catalogname {
            Some(s) => Some(PgString::from_str_in(&s, mcx)?),
            None => None,
        },
        schemaname: match rv.schemaname {
            Some(s) => Some(PgString::from_str_in(&s, mcx)?),
            None => None,
        },
        relname: Some(PgString::from_str_in(&rv.relname, mcx)?),
        inh: rv.inh,
        relpersistence: rv.relpersistence as i8,
        alias: None,
        location: rv.location,
    })
}
