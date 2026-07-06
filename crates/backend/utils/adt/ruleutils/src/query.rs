//! deparse_namespace + get_query_def spine for SELECT (ruleutils.c). Query
//! shapes outside the stored-view SELECT set are loud named panics.

use std::rc::Rc;

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::nodes_enums::{CmdType, LimitOption, LockClauseStrength, LockWaitPolicy};
use types_nodes::parsenodes::{CTEMaterialize, RangeTblFunction, SetOperation, WindowClause};
use types_nodes::primnodes::{CoercionForm, OverridingKind, SubLinkType};
use types_nodes::rawnodes::{
    FRAMEOPTION_BETWEEN, FRAMEOPTION_END_CURRENT_ROW, FRAMEOPTION_END_OFFSET,
    FRAMEOPTION_END_OFFSET_FOLLOWING, FRAMEOPTION_END_OFFSET_PRECEDING,
    FRAMEOPTION_END_UNBOUNDED_FOLLOWING, FRAMEOPTION_EXCLUDE_CURRENT_ROW,
    FRAMEOPTION_EXCLUDE_GROUP, FRAMEOPTION_EXCLUDE_TIES, FRAMEOPTION_GROUPS,
    FRAMEOPTION_NONDEFAULT, FRAMEOPTION_RANGE, FRAMEOPTION_ROWS,
    FRAMEOPTION_START_CURRENT_ROW, FRAMEOPTION_START_OFFSET,
    FRAMEOPTION_START_OFFSET_FOLLOWING, FRAMEOPTION_START_OFFSET_PRECEDING,
    FRAMEOPTION_START_UNBOUNDED_PRECEDING,
};
use types_nodes::primnodes::{Alias, FromExpr, JoinExpr};
use types_nodes::{JoinType, Node, NodeList, NodeTag, Query, RTEKind, RangeTblEntry};

use crate::deparse::{
    append_context_keyword, get_const_expr, get_rule_expr, get_variable, remove_trailing_spaces,
    DeparseContext, PRETTYINDENT_JOIN, PRETTYINDENT_STD, PRETTYINDENT_VAR,
};
use crate::{gap, generate_operator_name, generate_relation_name, quote_identifier};

const NAMEDATALEN: usize = 64;

#[derive(Default)]
pub(crate) struct DeparseColumns {
    pub colnames: Vec<Option<String>>,
    pub new_colnames: Vec<String>,
    pub is_new_col: Vec<bool>,
    pub printaliases: bool,
    pub leftrti: usize,
    pub rightrti: usize,
    pub leftattnos: Vec<i32>,
    pub rightattnos: Vec<i32>,
    pub using_names: Vec<String>,
    pub parent_using: Vec<String>,
}

pub(crate) struct DeparseNamespace<'mcx> {
    pub rtable: Vec<&'mcx RangeTblEntry<'mcx>>,
    pub rtable_names: Vec<Option<String>>,
    pub rtable_columns: Vec<DeparseColumns>,
    pub unique_using: bool,
    pub using_names: Vec<String>,
    pub ctes: Vec<&'mcx types_nodes::parsenodes::CommonTableExpr<'mcx>>,
    pub subplans: Option<&'mcx types_nodes::list::OptNodeList<'mcx>>,
    // SQL-function-body deparse (print_function_sqlbody).
    pub funcname: Option<String>,
    pub argnames: Option<Vec<String>>,
    // Indexed by child relid (0 unused), as C's palloc0'd array.
    pub appendrels: Option<Vec<Option<&'mcx types_nodes::plannodes::AppendRelInfo<'mcx>>>>,
    pub plan: core::cell::RefCell<crate::plan::DpnsPlan<'mcx>>,
}

impl<'mcx> DeparseNamespace<'mcx> {
    pub(crate) fn empty(rtable: Vec<&'mcx RangeTblEntry<'mcx>>) -> Self {
        DeparseNamespace {
            rtable,
            rtable_names: Vec::new(),
            rtable_columns: Vec::new(),
            unique_using: false,
            using_names: Vec::new(),
            ctes: Vec::new(),
            subplans: None,
            funcname: None,
            argnames: None,
            appendrels: None,
            plan: core::cell::RefCell::new(crate::plan::DpnsPlan::default()),
        }
    }
}

pub(crate) fn deparse_context_for<'mcx>(
    mcx: Mcx<'mcx>,
    aliasname: &str,
    relid: types_core::Oid,
) -> PgResult<DeparseNamespace<'mcx>> {
    let mut alias = Node::build::<Alias>(mcx)?;
    alias.aliasname = Some(crate::str_in(mcx, aliasname)?);
    let alias_ref = alias.seal_ref();
    let mut rte = Node::build::<RangeTblEntry>(mcx)?;
    rte.rtekind = RTEKind::RTE_RELATION;
    rte.relid = relid;
    rte.relkind = b'r';
    rte.rellockmode = 1;
    rte.alias = Some(alias_ref);
    rte.eref = Some(alias_ref);
    rte.inFromCl = true;
    let rte_ref = rte.seal_ref();

    let mut dpns = DeparseNamespace::empty(vec![rte_ref]);
    set_rtable_names(mcx, &mut dpns, &[], None)?;
    set_simple_column_names(mcx, &mut dpns)?;
    Ok(dpns)
}

pub(crate) fn set_rtable_names<'mcx>(
    mcx: Mcx<'mcx>,
    dpns: &mut DeparseNamespace<'mcx>,
    parents: &[Rc<DeparseNamespace<'mcx>>],
    rels_used: Option<&types_nodes::bitmapset::Bitmapset<'mcx>>,
) -> PgResult<()> {
    let mut entries: Vec<(String, u32)> = Vec::new();
    for p in parents {
        for name in p.rtable_names.iter().flatten() {
            if !entries.iter().any(|(n, _)| n == name) {
                entries.push((name.clone(), 0));
            }
        }
    }
    for (i, rte) in dpns.rtable.iter().enumerate() {
        let refname: Option<String> = if rels_used.is_some_and(|ru| !ru.is_member(i as i32 + 1)) {
            None
        } else if let Some(alias) = rte.alias {
            alias.aliasname.map(str::to_owned)
        } else if rte.rtekind == RTEKind::RTE_RELATION {
            lsyscache::get_rel_name(mcx, rte.relid)?.map(|s| s.as_str().to_owned())
        } else if rte.rtekind == RTEKind::RTE_JOIN {
            None
        } else {
            rte.eref.and_then(|e| e.aliasname).map(str::to_owned)
        };
        let refname = match refname {
            None => None,
            Some(name) => match entries.iter().position(|(n, _)| *n == name) {
                None => {
                    entries.push((name.clone(), 0));
                    Some(name)
                }
                Some(idx) => {
                    let mut base = name.clone();
                    loop {
                        entries[idx].1 += 1;
                        let counter = entries[idx].1;
                        let mut modname = format!("{base}_{counter}");
                        while modname.len() >= NAMEDATALEN {
                            let mut cut = base.len() - 1;
                            while !base.is_char_boundary(cut) {
                                cut -= 1;
                            }
                            base.truncate(cut);
                            modname = format!("{base}_{counter}");
                        }
                        if !entries.iter().any(|(n, _)| *n == modname) {
                            entries.push((modname.clone(), 0));
                            break Some(modname);
                        }
                    }
                }
            },
        };
        dpns.rtable_names.push(refname);
    }
    Ok(())
}

pub(crate) fn set_deparse_for_query<'mcx>(
    mcx: Mcx<'mcx>,
    query: &'mcx Query<'mcx>,
    parents: &[Rc<DeparseNamespace<'mcx>>],
) -> PgResult<DeparseNamespace<'mcx>> {
    let rtable: Vec<&RangeTblEntry<'_>> = query
        .rtable
        .iter()
        .map(|n| n.as_range_tbl_entry().expect("rtable entry"))
        .collect();
    let mut dpns = DeparseNamespace::empty(rtable);
    dpns.ctes = query
        .cteList
        .iter()
        .map(|n| n.as_common_table_expr().expect("cteList entry"))
        .collect();
    {
        let mut ps = dpns.plan.borrow_mut();
        ps.ret_old_alias = query.returningOldAlias;
        ps.ret_new_alias = query.returningNewAlias;
    }
    set_rtable_names(mcx, &mut dpns, parents, None)?;
    for _ in 0..dpns.rtable.len() {
        dpns.rtable_columns.push(DeparseColumns::default());
    }
    if let Some(jt) = query.jointree {
        dpns.unique_using = from_expr_children(jt).any(|n| has_dangerous_join_using(&dpns, n));
        let parent_using: Vec<String> = Vec::new();
        for child in from_expr_children(jt) {
            set_using_names(&mut dpns, child, &parent_using)?;
        }
    }
    for i in 0..dpns.rtable.len() {
        if dpns.rtable[i].rtekind == RTEKind::RTE_JOIN {
            set_join_column_names(&mut dpns, i)?;
        } else {
            set_relation_column_names(mcx, &mut dpns, i)?;
        }
    }
    Ok(dpns)
}

pub(crate) fn set_simple_column_names<'mcx>(
    mcx: Mcx<'mcx>,
    dpns: &mut DeparseNamespace<'mcx>,
) -> PgResult<()> {
    for _ in 0..dpns.rtable.len() {
        dpns.rtable_columns.push(DeparseColumns::default());
    }
    for i in 0..dpns.rtable.len() {
        if dpns.rtable[i].rtekind != RTEKind::RTE_JOIN {
            set_relation_column_names(mcx, dpns, i)?;
        }
    }
    Ok(())
}

fn from_expr_children<'a, 'mcx>(
    jt: &'a FromExpr<'mcx>,
) -> impl Iterator<Item = Node<'mcx>> + 'a {
    jt.fromlist.iter()
}

fn has_dangerous_join_using(dpns: &DeparseNamespace<'_>, jtnode: Node<'_>) -> bool {
    match jtnode.node_tag() {
        NodeTag::T_RangeTblRef => false,
        NodeTag::T_FromExpr => {
            from_expr_children(jtnode.as_from_expr().unwrap())
                .any(|n| has_dangerous_join_using(dpns, n))
        }
        NodeTag::T_JoinExpr => {
            let j = jtnode.as_join_expr().unwrap();
            if j.alias.is_none() && !j.usingClause.is_nil() {
                let jrte = dpns.rtable[j.rtindex as usize - 1];
                for i in 0..jrte.joinmergedcols as usize {
                    if jrte.joinaliasvars.nth(i).node_tag() != NodeTag::T_Var {
                        return true;
                    }
                }
            }
            has_dangerous_join_using(dpns, j.larg) || has_dangerous_join_using(dpns, j.rarg)
        }
        other => panic!("has_dangerous_join_using: unrecognized jointree node {other:?}"),
    }
}

fn jt_rtindex(node: Node<'_>) -> usize {
    match node.node_tag() {
        NodeTag::T_RangeTblRef => node.as_range_tbl_ref().unwrap().rtindex as usize,
        NodeTag::T_JoinExpr => node.as_join_expr().unwrap().rtindex as usize,
        other => panic!("identify_join_columns: unrecognized jointree node {other:?}"),
    }
}

fn identify_join_columns(
    j: &JoinExpr<'_>,
    jrte: &RangeTblEntry<'_>,
    colinfo: &mut DeparseColumns,
) {
    colinfo.leftrti = jt_rtindex(j.larg);
    colinfo.rightrti = jt_rtindex(j.rarg);
    let numjoincols = jrte.joinaliasvars.len();
    debug_assert_eq!(
        numjoincols,
        jrte.eref.map(|e| e.colnames.len()).unwrap_or(0),
        "identify_join_columns: broken join RTE"
    );
    colinfo.leftattnos = vec![0; numjoincols];
    colinfo.rightattnos = vec![0; numjoincols];
    let mut jcolno = 0usize;
    for leftattno in jrte.joinleftcols.iter() {
        colinfo.leftattnos[jcolno] = leftattno;
        jcolno += 1;
    }
    for (rcolno, rightattno) in jrte.joinrightcols.iter().enumerate() {
        if rcolno < jrte.joinmergedcols as usize {
            colinfo.rightattnos[rcolno] = rightattno;
        } else {
            colinfo.rightattnos[jcolno] = rightattno;
            jcolno += 1;
        }
    }
    debug_assert_eq!(jcolno, numjoincols);
}

fn expand_colnames_array_to(colinfo: &mut DeparseColumns, n: usize) {
    if n > colinfo.colnames.len() {
        colinfo.colnames.resize(n, None);
    }
}

fn colname_is_unique(
    colname: &str,
    dpns_using_names: &[String],
    colinfo: &DeparseColumns,
) -> bool {
    if colinfo.colnames.iter().flatten().any(|n| n == colname) {
        return false;
    }
    if colinfo.new_colnames.iter().any(|n| n == colname) {
        return false;
    }
    if colinfo.parent_using.iter().any(|n| n == colname) {
        return false;
    }
    if dpns_using_names.iter().any(|n| n == colname) {
        return false;
    }
    true
}

fn make_colname_unique(
    colname: &str,
    dpns_using_names: &[String],
    colinfo: &DeparseColumns,
) -> String {
    if colname_is_unique(colname, dpns_using_names, colinfo) {
        return colname.to_owned();
    }
    let mut base = colname.to_owned();
    let mut i = 0u32;
    loop {
        i += 1;
        let mut modname = format!("{base}_{i}");
        while modname.len() >= NAMEDATALEN {
            let mut cut = base.len() - 1;
            while !base.is_char_boundary(cut) {
                cut -= 1;
            }
            base.truncate(cut);
            modname = format!("{base}_{i}");
        }
        if colname_is_unique(&modname, dpns_using_names, colinfo) {
            return modname;
        }
    }
}

fn set_using_names(
    dpns: &mut DeparseNamespace<'_>,
    jtnode: Node<'_>,
    parent_using: &[String],
) -> PgResult<()> {
    match jtnode.node_tag() {
        NodeTag::T_RangeTblRef => Ok(()),
        NodeTag::T_FromExpr => {
            for child in from_expr_children(jtnode.as_from_expr().unwrap()) {
                set_using_names(dpns, child, parent_using)?;
            }
            Ok(())
        }
        NodeTag::T_JoinExpr => {
            let j = jtnode.as_join_expr().unwrap();
            let jidx = j.rtindex as usize - 1;
            let rte = dpns.rtable[jidx];
            let mut colinfo = std::mem::take(&mut dpns.rtable_columns[jidx]);
            identify_join_columns(j, rte, &mut colinfo);
            let leftidx = colinfo.leftrti - 1;
            let rightidx = colinfo.rightrti - 1;

            if rte.alias.is_none() {
                for i in 0..colinfo.colnames.len() {
                    let Some(colname) = colinfo.colnames[i].clone() else { continue };
                    if colinfo.leftattnos[i] > 0 {
                        let la = colinfo.leftattnos[i] as usize;
                        expand_colnames_array_to(&mut dpns.rtable_columns[leftidx], la);
                        dpns.rtable_columns[leftidx].colnames[la - 1] = Some(colname.clone());
                    }
                    if colinfo.rightattnos[i] > 0 {
                        let ra = colinfo.rightattnos[i] as usize;
                        expand_colnames_array_to(&mut dpns.rtable_columns[rightidx], ra);
                        dpns.rtable_columns[rightidx].colnames[ra - 1] = Some(colname);
                    }
                }
            }

            let mut child_using: Vec<String> = parent_using.to_vec();
            if !j.usingClause.is_nil() {
                expand_colnames_array_to(&mut colinfo, j.usingClause.len());
                for (i, uc) in j.usingClause.iter().enumerate() {
                    let pushed_down = colinfo.colnames[i].clone();
                    let mut colname = match pushed_down {
                        Some(pushed) => pushed,
                        None => {
                            let written = uc.as_string().expect("USING name").sval;
                            let preferred = match rte.alias {
                                Some(a) if i < a.colnames.len() => {
                                    a.colnames.nth(i).as_string().expect("alias colname").sval
                                }
                                _ => written,
                            };
                            let unique = make_colname_unique(
                                preferred,
                                &dpns.using_names,
                                &colinfo,
                            );
                            if dpns.unique_using {
                                dpns.using_names.push(unique.clone());
                            }
                            colinfo.colnames[i] = Some(unique.clone());
                            unique
                        }
                    };
                    colinfo.using_names.push(colname.clone());
                    child_using.push(colname.clone());

                    if colinfo.leftattnos[i] > 0 {
                        let la = colinfo.leftattnos[i] as usize;
                        expand_colnames_array_to(&mut dpns.rtable_columns[leftidx], la);
                        dpns.rtable_columns[leftidx].colnames[la - 1] = Some(colname.clone());
                    }
                    if colinfo.rightattnos[i] > 0 {
                        let ra = colinfo.rightattnos[i] as usize;
                        expand_colnames_array_to(&mut dpns.rtable_columns[rightidx], ra);
                        dpns.rtable_columns[rightidx].colnames[ra - 1] =
                            Some(std::mem::take(&mut colname));
                    }
                }
            }

            dpns.rtable_columns[leftidx].parent_using = child_using.clone();
            dpns.rtable_columns[rightidx].parent_using = child_using.clone();
            dpns.rtable_columns[jidx] = colinfo;

            set_using_names(dpns, j.larg, &child_using)?;
            set_using_names(dpns, j.rarg, &child_using)
        }
        other => panic!("set_using_names: unrecognized jointree node {other:?}"),
    }
}

fn relation_real_colnames(relid: types_core::Oid) -> PgResult<Vec<Option<String>>> {
    let natts = lsyscache::get_relnatts(relid)?;
    let mut out = Vec::with_capacity(natts.max(0) as usize);
    // Shape lookup returns None for dropped columns; attno <= relnatts always has a row.
    for attno in 1..=natts {
        out.push(
            syscache_seams::lookup_pg_attribute_shape::call(relid, attno as i16)?
                .map(|att| String::from_utf8_lossy(att.attname.name_str()).into_owned()),
        );
    }
    Ok(out)
}

fn set_relation_column_names<'mcx>(
    mcx: Mcx<'mcx>,
    dpns: &mut DeparseNamespace<'mcx>,
    idx: usize,
) -> PgResult<()> {
    let rte = dpns.rtable[idx];
    let mut colinfo = std::mem::take(&mut dpns.rtable_columns[idx]);

    let real_colnames: Vec<Option<String>> = match rte.rtekind {
        RTEKind::RTE_RELATION => relation_real_colnames(rte.relid)?,
        RTEKind::RTE_FUNCTION if !rte.functions.is_nil() => {
            let (colnames, _) = parse_relation::expandRTE(
                mcx,
                rte,
                1,
                0,
                types_nodes::primnodes::VarReturningType::VAR_RETURNING_DEFAULT,
                -1,
                true,
            )?;
            colnames
                .iter()
                .map(|c| {
                    let s = c.as_string().expect("expanded colname").sval;
                    if s.is_empty() { None } else { Some(s.to_owned()) }
                })
                .collect()
        }
        _ => rte
            .eref
            .map(|e| {
                e.colnames
                    .iter()
                    .map(|c| {
                        let s = c.as_string().expect("eref colname").sval;
                        if s.is_empty() { None } else { Some(s.to_owned()) }
                    })
                    .collect()
            })
            .unwrap_or_default(),
    };

    let ncolumns = real_colnames.len();
    expand_colnames_array_to(&mut colinfo, ncolumns);
    colinfo.new_colnames = Vec::with_capacity(ncolumns);
    colinfo.is_new_col = Vec::with_capacity(ncolumns);

    let noldcolumns = rte.eref.map(|e| e.colnames.len()).unwrap_or(0);
    let mut changed_any = false;
    for i in 0..ncolumns {
        let Some(real_colname) = &real_colnames[i] else {
            debug_assert!(colinfo.colnames[i].is_none());
            continue;
        };
        if colinfo.colnames[i].is_none() {
            let preferred: &str = match rte.alias {
                Some(a) if i < a.colnames.len() => {
                    a.colnames.nth(i).as_string().expect("alias colname").sval
                }
                _ => real_colname,
            };
            let unique = make_colname_unique(preferred, &dpns.using_names, &colinfo);
            colinfo.colnames[i] = Some(unique);
        }
        let colname = colinfo.colnames[i].clone().expect("assigned above");
        colinfo.new_colnames.push(colname.clone());
        colinfo.is_new_col.push(i >= noldcolumns);
        if !changed_any && colname != *real_colname {
            changed_any = true;
        }
    }

    colinfo.printaliases = match rte.rtekind {
        RTEKind::RTE_RELATION => changed_any,
        RTEKind::RTE_FUNCTION => true,
        RTEKind::RTE_TABLEFUNC => false,
        _ => {
            if rte.alias.is_some_and(|a| !a.colnames.is_nil()) {
                true
            } else {
                changed_any
            }
        }
    };
    dpns.rtable_columns[idx] = colinfo;
    Ok(())
}

fn set_join_column_names(dpns: &mut DeparseNamespace<'_>, idx: usize) -> PgResult<()> {
    let rte = dpns.rtable[idx];
    let mut colinfo = std::mem::take(&mut dpns.rtable_columns[idx]);
    let leftidx = colinfo.leftrti - 1;
    let rightidx = colinfo.rightrti - 1;

    let noldcolumns = rte.eref.map(|e| e.colnames.len()).unwrap_or(0);
    expand_colnames_array_to(&mut colinfo, noldcolumns);

    let mut changed_any = false;
    for i in colinfo.using_names.len()..noldcolumns {
        debug_assert!(colinfo.leftattnos[i] != 0 || colinfo.rightattnos[i] != 0);
        let real_colname: Option<String> = if colinfo.leftattnos[i] > 0 {
            dpns.rtable_columns[leftidx].colnames[colinfo.leftattnos[i] as usize - 1].clone()
        } else if colinfo.rightattnos[i] > 0 {
            dpns.rtable_columns[rightidx].colnames[colinfo.rightattnos[i] as usize - 1].clone()
        } else {
            rte.eref.map(|e| {
                e.colnames.nth(i).as_string().expect("eref colname").sval.to_owned()
            })
        };
        let Some(real_colname) = real_colname else {
            colinfo.colnames[i] = None;
            continue;
        };
        if rte.alias.is_none() {
            colinfo.colnames[i] = Some(real_colname);
            continue;
        }
        if colinfo.colnames[i].is_none() {
            let preferred: &str = match rte.alias {
                Some(a) if i < a.colnames.len() => {
                    a.colnames.nth(i).as_string().expect("alias colname").sval
                }
                _ => &real_colname,
            };
            let unique = make_colname_unique(preferred, &dpns.using_names, &colinfo);
            colinfo.colnames[i] = Some(unique);
        }
        if !changed_any && colinfo.colnames[i].as_deref() != Some(real_colname.as_str()) {
            changed_any = true;
        }
    }

    let left = &dpns.rtable_columns[leftidx];
    let right = &dpns.rtable_columns[rightidx];
    let nnewcolumns =
        left.new_colnames.len() + right.new_colnames.len() - colinfo.using_names.len();
    colinfo.new_colnames = Vec::with_capacity(nnewcolumns);
    colinfo.is_new_col = Vec::with_capacity(nnewcolumns);

    let mut leftmerged = vec![false; left.colnames.len() + 1];
    let mut rightmerged = vec![false; right.colnames.len() + 1];
    let mut i = 0usize;
    while i < noldcolumns && colinfo.leftattnos[i] != 0 && colinfo.rightattnos[i] != 0 {
        colinfo
            .new_colnames
            .push(colinfo.colnames[i].clone().expect("merged column name assigned"));
        colinfo.is_new_col.push(false);
        if colinfo.leftattnos[i] > 0 {
            leftmerged[colinfo.leftattnos[i] as usize] = true;
        }
        if colinfo.rightattnos[i] > 0 {
            rightmerged[colinfo.rightattnos[i] as usize] = true;
        }
        i += 1;
    }

    let leftattnos = colinfo.leftattnos.clone();
    let rightattnos = colinfo.rightattnos.clone();
    for (child, merged, attnos) in
        [(left, &leftmerged, &leftattnos), (right, &rightmerged, &rightattnos)]
    {
        let mut ic = 0usize;
        for jc in 0..child.new_colnames.len() {
            let child_colname = &child.new_colnames[jc];
            if !child.is_new_col[jc] {
                while ic < child.colnames.len() && child.colnames[ic].is_none() {
                    ic += 1;
                }
                debug_assert!(ic < child.colnames.len());
                ic += 1;
                if merged[ic] {
                    continue;
                }
                while i < colinfo.colnames.len() && colinfo.colnames[i].is_none() {
                    i += 1;
                }
                debug_assert!(i < colinfo.colnames.len());
                debug_assert_eq!(ic as i32, attnos[i]);
                colinfo
                    .new_colnames
                    .push(colinfo.colnames[i].clone().expect("existing join column"));
                colinfo.is_new_col.push(child.is_new_col[jc]);
                i += 1;
            } else {
                let assigned = if rte.alias.is_some() {
                    let unique =
                        make_colname_unique(child_colname, &dpns.using_names, &colinfo);
                    if !changed_any && unique != *child_colname {
                        changed_any = true;
                    }
                    unique
                } else {
                    child_colname.clone()
                };
                colinfo.new_colnames.push(assigned);
                colinfo.is_new_col.push(child.is_new_col[jc]);
            }
        }
    }
    debug_assert_eq!(colinfo.new_colnames.len(), nnewcolumns);

    colinfo.printaliases = if rte.alias.is_some() { changed_any } else { false };
    dpns.rtable_columns[idx] = colinfo;
    Ok(())
}

fn get_rtable_name(rtindex: usize, ctx: &DeparseContext<'_>) -> Option<String> {
    ctx.namespaces[0].rtable_names[rtindex - 1].clone()
}

pub(crate) fn get_query_def<'mcx>(
    query: &'mcx Query<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
    result_desc: Option<Rc<Vec<String>>>,
    col_names_visible: bool,
) -> PgResult<()> {
    // C scribbles the flattened targetList/havingQual back into the Query;
    // the owned tree is immutable, so the flattened lists thread as params.
    let (target_list, having_qual, rtable_size) = if query.hasGroupRTE {
        let tl = vars::flatten_group_exprs_list(ctx.mcx, query, &query.targetList)?
            .unwrap_or(&query.targetList);
        let hq = match query.havingQual {
            Some(h) => Some(vars::flatten_group_exprs(ctx.mcx, query, h)?),
            None => None,
        };
        (tl, hq, query.rtable.len() - 1)
    } else {
        (&query.targetList, query.havingQual, query.rtable.len())
    };
    // C AcquireRewriteLocks the rtable here; lock acquisition is another
    // lane, so names/columns read the live catalogs unlocked.
    let dpns = set_deparse_for_query(ctx.mcx, query, &ctx.namespaces)?;

    let save_varprefix = ctx.varprefix;
    let save_result_desc = ctx.result_desc.take();
    let save_target_list = ctx.target_list.take();
    let save_window_clause = ctx.window_clause.take();
    let save_colnames_visible = ctx.colnames_visible;
    let save_in_group_by = ctx.in_group_by;
    let save_var_in_order_by = ctx.var_in_order_by;
    let save_indent = ctx.indent_level;

    ctx.varprefix = !ctx.namespaces.is_empty() || rtable_size != 1;
    ctx.colnames_visible = col_names_visible;
    ctx.in_group_by = false;
    ctx.var_in_order_by = false;
    ctx.namespaces.insert(0, Rc::new(dpns));

    let r = match query.commandType {
        CmdType::CMD_SELECT => {
            ctx.result_desc = result_desc;
            get_select_query_def(query, target_list, having_qual, ctx)
        }
        CmdType::CMD_UPDATE => get_update_query_def(query, ctx),
        CmdType::CMD_INSERT => get_insert_query_def(query, ctx),
        CmdType::CMD_DELETE => get_delete_query_def(query, ctx),
        CmdType::CMD_MERGE => get_merge_query_def(query, ctx),
        CmdType::CMD_NOTHING => {
            ctx.buf.push_str("NOTHING");
            Ok(())
        }
        // get_utility_query_def: only NOTIFY can appear in rules.
        CmdType::CMD_UTILITY => {
            let stmt = query
                .utilityStmt
                .and_then(|n| n.as_notify_stmt())
                .unwrap_or_else(|| gap("get_utility_query_def", "non-NOTIFY utility statement"));
            append_context_keyword(ctx, "", 0, PRETTYINDENT_STD, 1);
            let name = stmt.conditionname.expect("NOTIFY has a condition name");
            ctx.buf.push_str(&format!("NOTIFY {}", quote_identifier(name)));
            if let Some(payload) = stmt.payload {
                ctx.buf.push_str(", ");
                crate::deparse::simple_quote_literal(&mut ctx.buf, payload);
            }
            Ok(())
        }
        other => gap("get_query_def", &format!("{other:?} deparse")),
    };

    ctx.namespaces.remove(0);
    ctx.varprefix = save_varprefix;
    ctx.result_desc = save_result_desc;
    ctx.target_list = save_target_list;
    ctx.window_clause = save_window_clause;
    ctx.colnames_visible = save_colnames_visible;
    ctx.in_group_by = save_in_group_by;
    ctx.var_in_order_by = save_var_in_order_by;
    ctx.indent_level = save_indent;
    r
}

fn get_with_clause<'mcx>(query: &'mcx Query<'mcx>, ctx: &mut DeparseContext<'mcx>) -> PgResult<()> {
    if query.cteList.is_nil() {
        return Ok(());
    }
    if ctx.pretty_indent() {
        ctx.indent_level += PRETTYINDENT_STD;
        ctx.buf.push(' ');
    }
    let mut sep = if query.hasRecursive { "WITH RECURSIVE " } else { "WITH " };
    for cte_node in query.cteList.iter() {
        let cte = cte_node.as_common_table_expr().expect("cteList entry");
        ctx.buf.push_str(sep);
        ctx.buf.push_str(&quote_identifier(cte.ctename.expect("CTE has a name")));
        if !cte.aliascolnames.is_nil() {
            ctx.buf.push('(');
            let mut first = true;
            for col in cte.aliascolnames.iter() {
                if !first {
                    ctx.buf.push_str(", ");
                }
                first = false;
                ctx.buf.push_str(&quote_identifier(col.as_string().expect("colname").sval));
            }
            ctx.buf.push(')');
        }
        ctx.buf.push_str(" AS ");
        match cte.ctematerialized {
            CTEMaterialize::CTEMaterializeDefault => {}
            CTEMaterialize::CTEMaterializeAlways => ctx.buf.push_str("MATERIALIZED "),
            CTEMaterialize::CTEMaterializeNever => ctx.buf.push_str("NOT MATERIALIZED "),
        }
        ctx.buf.push('(');
        if ctx.pretty_indent() {
            append_context_keyword(ctx, "", 0, 0, 0);
        }
        let ctequery = cte
            .ctequery
            .and_then(|n| n.as_query())
            .expect("transformed CTE holds a Query");
        get_query_def(ctequery, ctx, None, true)?;
        if ctx.pretty_indent() {
            append_context_keyword(ctx, "", 0, 0, 0);
        }
        ctx.buf.push(')');
        if let Some(sc) = cte.search_clause.and_then(|n| n.as_cte_search_clause()) {
            ctx.buf.push_str(&format!(
                " SEARCH {} FIRST BY ",
                if sc.search_breadth_first { "BREADTH" } else { "DEPTH" }
            ));
            let mut first = true;
            for col in sc.search_col_list.iter() {
                if !first {
                    ctx.buf.push_str(", ");
                }
                first = false;
                ctx.buf.push_str(&quote_identifier(col.as_string().expect("colname").sval));
            }
            ctx.buf.push_str(&format!(
                " SET {}",
                quote_identifier(sc.search_seq_column.expect("SEARCH SET column"))
            ));
        }
        if let Some(cc) = cte.cycle_clause.and_then(|n| n.as_cte_cycle_clause()) {
            ctx.buf.push_str(" CYCLE ");
            let mut first = true;
            for col in cc.cycle_col_list.iter() {
                if !first {
                    ctx.buf.push_str(", ");
                }
                first = false;
                ctx.buf.push_str(&quote_identifier(col.as_string().expect("colname").sval));
            }
            ctx.buf.push_str(&format!(
                " SET {}",
                quote_identifier(cc.cycle_mark_column.expect("CYCLE SET column"))
            ));
            let mark_value = cc.cycle_mark_value.expect("cycle_mark_value");
            let mark_default = cc.cycle_mark_default.expect("cycle_mark_default");
            let cmv = mark_value.as_const().expect("cycle_mark_value is a Const");
            let cmd = mark_default.as_const().expect("cycle_mark_default is a Const");
            let default_marks = cmv.consttype == types_core::BOOLOID
                && !cmv.constisnull
                && cmv.constvalue.as_bool()
                && cmd.consttype == types_core::BOOLOID
                && !cmd.constisnull
                && !cmd.constvalue.as_bool();
            if !default_marks {
                ctx.buf.push_str(" TO ");
                get_rule_expr(mark_value, ctx, false)?;
                ctx.buf.push_str(" DEFAULT ");
                get_rule_expr(mark_default, ctx, false)?;
            }
            ctx.buf.push_str(&format!(
                " USING {}",
                quote_identifier(cc.cycle_path_column.expect("CYCLE USING column"))
            ));
        }
        sep = ", ";
    }
    if ctx.pretty_indent() {
        ctx.indent_level -= PRETTYINDENT_STD;
        append_context_keyword(ctx, "", 0, 0, 0);
    } else {
        ctx.buf.push(' ');
    }
    Ok(())
}

fn get_values_def<'mcx>(
    values_lists: &'mcx NodeList<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    ctx.buf.push_str("VALUES ");
    let mut first_list = true;
    for sublist in values_lists.iter() {
        if !first_list {
            ctx.buf.push_str(", ");
        }
        first_list = false;
        ctx.buf.push('(');
        let mut first_col = true;
        for col in sublist.as_list().expect("VALUES sublist").iter() {
            if !first_col {
                ctx.buf.push(',');
            }
            first_col = false;
            get_rule_expr_toplevel(col, ctx, false)?;
        }
        ctx.buf.push(')');
    }
    Ok(())
}

fn get_rule_expr_toplevel<'mcx>(
    node: Node<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
    showimplicit: bool,
) -> PgResult<()> {
    match node.as_var() {
        Some(v) => get_variable(node, v, 0, true, ctx).map(|_| ()),
        None => get_rule_expr(node, ctx, showimplicit),
    }
}

fn get_select_query_def<'mcx>(
    query: &'mcx Query<'mcx>,
    target_list: &'mcx NodeList<'mcx>,
    having_qual: Option<Node<'mcx>>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    get_with_clause(query, ctx)?;
    ctx.target_list = Some(target_list);
    ctx.window_clause = Some(&query.windowClause);

    let force_colno = if let Some(setops) = query.setOperations {
        get_setop_query(setops, query, ctx)?;
        true
    } else {
        get_basic_select_query(query, target_list, having_qual, ctx)?;
        false
    };

    if !query.sortClause.is_nil() {
        append_context_keyword(ctx, " ORDER BY ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
        get_rule_orderby(&query.sortClause, target_list, force_colno, ctx)?;
    }

    if let Some(offset) = query.limitOffset {
        append_context_keyword(ctx, " OFFSET ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
        get_rule_expr(offset, ctx, false)?;
    }
    if let Some(count) = query.limitCount {
        if query.limitOption == LimitOption::LIMIT_OPTION_WITH_TIES {
            append_context_keyword(ctx, " FETCH FIRST ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
            ctx.buf.push('(');
            get_rule_expr(count, ctx, false)?;
            ctx.buf.push(')');
            ctx.buf.push_str(" ROWS WITH TIES");
        } else {
            append_context_keyword(ctx, " LIMIT ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
            match count.as_const() {
                Some(c) if c.constisnull => ctx.buf.push_str("ALL"),
                _ => get_rule_expr(count, ctx, false)?,
            }
        }
    }

    if query.hasForUpdate {
        for rc_node in query.rowMarks.iter() {
            let rc = rc_node.as_row_mark_clause().expect("rowMarks entry");
            if rc.pushedDown {
                continue;
            }
            let kw = match rc.strength {
                LockClauseStrength::LCS_FORKEYSHARE => " FOR KEY SHARE",
                LockClauseStrength::LCS_FORSHARE => " FOR SHARE",
                LockClauseStrength::LCS_FORNOKEYUPDATE => " FOR NO KEY UPDATE",
                LockClauseStrength::LCS_FORUPDATE => " FOR UPDATE",
                LockClauseStrength::LCS_NONE => {
                    panic!("unrecognized LockClauseStrength: LCS_NONE")
                }
            };
            append_context_keyword(ctx, kw, -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
            let name = get_rtable_name(rc.rti as usize, ctx).expect("locked rel has a refname");
            ctx.buf.push_str(&format!(" OF {}", quote_identifier(&name)));
            match rc.waitPolicy {
                LockWaitPolicy::LockWaitError => ctx.buf.push_str(" NOWAIT"),
                LockWaitPolicy::LockWaitSkip => ctx.buf.push_str(" SKIP LOCKED"),
                LockWaitPolicy::LockWaitBlock => {}
            }
        }
    }
    Ok(())
}

fn get_returning_clause<'mcx>(
    query: &'mcx Query<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    if query.returningList.is_nil() {
        return Ok(());
    }
    append_context_keyword(ctx, " RETURNING", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
    let mut have_with = false;
    if let Some(old_alias) = query.returningOldAlias {
        if old_alias != "old" {
            ctx.buf.push_str(&format!(" WITH (OLD AS {}", quote_identifier(old_alias)));
            have_with = true;
        }
    }
    if let Some(new_alias) = query.returningNewAlias {
        if new_alias != "new" {
            if have_with {
                ctx.buf.push_str(&format!(", NEW AS {}", quote_identifier(new_alias)));
            } else {
                ctx.buf.push_str(&format!(" WITH (NEW AS {}", quote_identifier(new_alias)));
                have_with = true;
            }
        }
    }
    if have_with {
        ctx.buf.push(')');
    }
    get_target_list(&query.returningList, ctx)
}

fn result_relation_rte<'a, 'mcx>(query: &'a Query<'mcx>) -> &'a RangeTblEntry<'mcx> {
    let rte = query
        .rtable
        .nth(query.resultRelation as usize - 1)
        .as_range_tbl_entry()
        .expect("rtable entry");
    debug_assert!(rte.rtekind == RTEKind::RTE_RELATION);
    rte
}

fn get_insert_query_def<'mcx>(
    query: &'mcx Query<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    get_with_clause(query, ctx)?;

    let mut select_rte: Option<&RangeTblEntry<'_>> = None;
    let mut values_rte: Option<&RangeTblEntry<'_>> = None;
    for n in query.rtable.iter() {
        let rte = n.as_range_tbl_entry().expect("rtable entry");
        if rte.rtekind == RTEKind::RTE_SUBQUERY {
            assert!(select_rte.is_none(), "too many subquery RTEs in INSERT");
            select_rte = Some(rte);
        }
        if rte.rtekind == RTEKind::RTE_VALUES {
            assert!(values_rte.is_none(), "too many values RTEs in INSERT");
            values_rte = Some(rte);
        }
    }
    assert!(
        select_rte.is_none() || values_rte.is_none(),
        "both subquery and values RTEs in INSERT"
    );
    let rte = result_relation_rte(query);

    if ctx.pretty_indent() {
        ctx.indent_level += PRETTYINDENT_STD;
        ctx.buf.push(' ');
    }
    let relname = generate_relation_name(ctx.mcx, rte.relid)?;
    ctx.buf.push_str(&format!("INSERT INTO {relname}"));
    get_rte_alias(rte, query.resultRelation as usize, true, ctx)?;
    ctx.buf.push(' ');

    let mut strippedexprs: Vec<Node<'mcx>> = Vec::new();
    let mut sep = "";
    if !query.targetList.is_nil() {
        ctx.buf.push('(');
    }
    for tle_node in query.targetList.iter() {
        let tle = tle_node.as_target_entry().expect("targetList entry");
        if tle.resjunk {
            continue;
        }
        ctx.buf.push_str(sep);
        sep = ", ";
        let attname = lsyscache::get_attname(ctx.mcx, rte.relid, tle.resno, false)?
            .expect("get_attname missing_ok=false");
        ctx.buf.push_str(&quote_identifier(attname.as_str()));
        strippedexprs.push(process_indirection(tle.expr, ctx)?);
    }
    if !query.targetList.is_nil() {
        ctx.buf.push_str(") ");
    }

    match query.r#override {
        OverridingKind::OVERRIDING_SYSTEM_VALUE => {
            ctx.buf.push_str("OVERRIDING SYSTEM VALUE ")
        }
        OverridingKind::OVERRIDING_USER_VALUE => ctx.buf.push_str("OVERRIDING USER VALUE "),
        OverridingKind::OVERRIDING_NOT_SET => {}
    }

    if let Some(srte) = select_rte {
        get_query_def(srte.subquery.expect("subquery RTE has a subquery"), ctx, None, false)?;
    } else if let Some(vrte) = values_rte {
        get_values_def(&vrte.values_lists, ctx)?;
    } else if !strippedexprs.is_empty() {
        append_context_keyword(ctx, "VALUES (", -PRETTYINDENT_STD, PRETTYINDENT_STD, 2);
        let mut first = true;
        for e in &strippedexprs {
            if !first {
                ctx.buf.push_str(", ");
            }
            first = false;
            get_rule_expr_toplevel(*e, ctx, false)?;
        }
        ctx.buf.push(')');
    } else {
        ctx.buf.push_str("DEFAULT VALUES");
    }

    if let Some(confl_node) = query.onConflict {
        let confl = confl_node.as_on_conflict_expr().expect("Query.onConflict");
        ctx.buf.push_str(" ON CONFLICT");
        if !confl.arbiterElems.is_nil() {
            ctx.buf.push('(');
            let mut first = true;
            for e in confl.arbiterElems.iter() {
                if !first {
                    ctx.buf.push_str(", ");
                }
                first = false;
                get_rule_expr(e, ctx, false)?;
            }
            ctx.buf.push(')');
            if let Some(arbiter_where) = confl.arbiterWhere {
                // C: force non-prefixed Vars; the parser binds them to the
                // target relation and this clause has no InferenceElem wrap.
                let save_varprefix = ctx.varprefix;
                ctx.varprefix = false;
                append_context_keyword(ctx, " WHERE ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
                get_rule_expr(arbiter_where, ctx, false)?;
                ctx.varprefix = save_varprefix;
            }
        } else if confl.constraint != types_core::InvalidOid {
            let constraint = lsyscache::get_constraint_name(ctx.mcx, confl.constraint)?
                .unwrap_or_else(|| {
                    panic!("cache lookup failed for constraint {}", confl.constraint)
                });
            ctx.buf.push_str(&format!(
                " ON CONSTRAINT {}",
                quote_identifier(constraint.as_str())
            ));
        }
        if confl.action == types_nodes::OnConflictAction::ONCONFLICT_NOTHING {
            ctx.buf.push_str(" DO NOTHING");
        } else {
            ctx.buf.push_str(" DO UPDATE SET ");
            get_update_query_targetlist_def(query, &confl.onConflictSet, rte, ctx)?;
            if let Some(conflict_where) = confl.onConflictWhere {
                append_context_keyword(ctx, " WHERE ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
                get_rule_expr(conflict_where, ctx, false)?;
            }
        }
    }
    get_returning_clause(query, ctx)
}

fn get_update_query_def<'mcx>(
    query: &'mcx Query<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    get_with_clause(query, ctx)?;
    let rte = result_relation_rte(query);
    if ctx.pretty_indent() {
        ctx.buf.push(' ');
        ctx.indent_level += PRETTYINDENT_STD;
    }
    let relname = generate_relation_name(ctx.mcx, rte.relid)?;
    ctx.buf.push_str(&format!(
        "UPDATE {}{relname}",
        if !rte.inh { "ONLY " } else { "" }
    ));
    get_rte_alias(rte, query.resultRelation as usize, false, ctx)?;
    ctx.buf.push_str(" SET ");
    get_update_query_targetlist_def(query, &query.targetList, rte, ctx)?;
    get_from_clause(query, " FROM ", ctx)?;
    if let Some(quals) = query.jointree.and_then(|jt| jt.quals) {
        append_context_keyword(ctx, " WHERE ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
        get_rule_expr(quals, ctx, false)?;
    }
    get_returning_clause(query, ctx)
}

fn get_update_query_targetlist_def<'mcx>(
    query: &'mcx Query<'mcx>,
    target_list: &'mcx NodeList<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    // MULTIEXPR source SubLinks appear, in subLinkId order, as resjunk tlist
    // entries (C collects them the same way before the main loop).
    let mut ma_sublinks: Vec<Node<'mcx>> = Vec::new();
    if query.hasSubLinks {
        for tle_node in target_list.iter() {
            let tle = tle_node.as_target_entry().expect("targetList entry");
            if tle.resjunk {
                if let Some(sl) = tle.expr.as_sub_link() {
                    if sl.subLinkType == SubLinkType::MULTIEXPR_SUBLINK {
                        ma_sublinks.push(tle.expr);
                        assert_eq!(sl.subLinkId as usize, ma_sublinks.len());
                    }
                }
            }
        }
    }
    let mut next_ma = 0usize;
    let mut cur_ma_sublink: Option<Node<'mcx>> = None;
    let mut remaining_ma_columns = 0i32;

    let mut sep = "";
    for tle_node in target_list.iter() {
        let tle = tle_node.as_target_entry().expect("targetList entry");
        if tle.resjunk {
            continue;
        }
        ctx.buf.push_str(sep);
        sep = ", ";

        if next_ma < ma_sublinks.len() && cur_ma_sublink.is_none() {
            // Dig for a PARAM_MULTIEXPR Param under assignment decoration and
            // implicit coercions (C tolerates FieldStores here; that
            // vocabulary is absent, so only the two live wrappers descend).
            let mut expr = Some(tle.expr);
            while let Some(e) = expr {
                match e.node_tag() {
                    NodeTag::T_SubscriptingRef => {
                        let sbsref = e.as_subscripting_ref().unwrap();
                        match sbsref.refassgnexpr {
                            Some(a) => expr = Some(a),
                            None => break,
                        }
                    }
                    NodeTag::T_CoerceToDomain => {
                        let cd = e.as_coerce_to_domain().unwrap();
                        if cd.coercionformat != CoercionForm::COERCE_IMPLICIT_CAST {
                            break;
                        }
                        expr = Some(cd.arg);
                    }
                    _ => break,
                }
            }
            let expr = expr.map(crate::deparse::strip_implicit_coercions);
            if let Some(p) = expr.and_then(|e| e.as_param()) {
                if p.paramkind == types_nodes::ParamKind::PARAM_MULTIEXPR {
                    let sl_node = ma_sublinks[next_ma];
                    let sl = sl_node.as_sub_link().unwrap();
                    cur_ma_sublink = Some(sl_node);
                    next_ma += 1;
                    remaining_ma_columns = sl
                        .subselect
                        .as_query()
                        .expect("MULTIEXPR subselect is a Query")
                        .targetList
                        .iter()
                        .filter(|n| !n.as_target_entry().expect("tlist entry").resjunk)
                        .count() as i32;
                    assert_eq!(p.paramid, (sl.subLinkId << 16) | 1);
                    ctx.buf.push('(');
                }
            }
        }

        let attname = lsyscache::get_attname(ctx.mcx, rte.relid, tle.resno, false)?
            .expect("get_attname missing_ok=false");
        ctx.buf.push_str(&quote_identifier(attname.as_str()));
        let mut expr = process_indirection(tle.expr, ctx)?;

        if let Some(sl_node) = cur_ma_sublink {
            remaining_ma_columns -= 1;
            if remaining_ma_columns > 0 {
                continue;
            }
            ctx.buf.push(')');
            expr = sl_node;
            cur_ma_sublink = None;
        }

        ctx.buf.push_str(" = ");
        get_rule_expr(expr, ctx, false)?;
    }
    Ok(())
}

fn get_delete_query_def<'mcx>(
    query: &'mcx Query<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    get_with_clause(query, ctx)?;
    let rte = result_relation_rte(query);
    if ctx.pretty_indent() {
        ctx.buf.push(' ');
        ctx.indent_level += PRETTYINDENT_STD;
    }
    let relname = generate_relation_name(ctx.mcx, rte.relid)?;
    ctx.buf.push_str(&format!(
        "DELETE FROM {}{relname}",
        if !rte.inh { "ONLY " } else { "" }
    ));
    get_rte_alias(rte, query.resultRelation as usize, false, ctx)?;
    get_from_clause(query, " USING ", ctx)?;
    if let Some(quals) = query.jointree.and_then(|jt| jt.quals) {
        append_context_keyword(ctx, " WHERE ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
        get_rule_expr(quals, ctx, false)?;
    }
    get_returning_clause(query, ctx)
}

fn get_merge_query_def<'mcx>(
    query: &'mcx Query<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    use types_nodes::primnodes::MergeMatchKind;

    get_with_clause(query, ctx)?;
    let rte = result_relation_rte(query);
    if ctx.pretty_indent() {
        ctx.buf.push(' ');
        ctx.indent_level += PRETTYINDENT_STD;
    }
    let relname = generate_relation_name(ctx.mcx, rte.relid)?;
    ctx.buf.push_str(&format!(
        "MERGE INTO {}{relname}",
        if !rte.inh { "ONLY " } else { "" }
    ));
    get_rte_alias(rte, query.resultRelation as usize, false, ctx)?;

    get_from_clause(query, " USING ", ctx)?;
    append_context_keyword(ctx, " ON ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 2);
    get_rule_expr(query.mergeJoinCondition.expect("MERGE has a join condition"), ctx, false)?;

    let have_not_matched_by_source = query.mergeActionList.iter().any(|n| {
        n.as_merge_action().expect("mergeActionList entry").matchKind
            == MergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_SOURCE
    });

    for action_node in query.mergeActionList.iter() {
        let action = action_node.as_merge_action().expect("mergeActionList entry");
        append_context_keyword(ctx, " WHEN ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 2);
        ctx.buf.push_str(match action.matchKind {
            MergeMatchKind::MERGE_WHEN_MATCHED => "MATCHED",
            MergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_SOURCE => "NOT MATCHED BY SOURCE",
            MergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_TARGET => {
                if have_not_matched_by_source {
                    "NOT MATCHED BY TARGET"
                } else {
                    "NOT MATCHED"
                }
            }
        });
        if let Some(qual) = action.qual {
            append_context_keyword(ctx, " AND ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 3);
            get_rule_expr(qual, ctx, false)?;
        }
        append_context_keyword(ctx, " THEN ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 3);

        match action.commandType {
            CmdType::CMD_INSERT => {
                ctx.buf.push_str("INSERT");
                let mut strippedexprs: Vec<Node<'mcx>> = Vec::new();
                let mut sep = "";
                if !action.targetList.is_nil() {
                    ctx.buf.push_str(" (");
                }
                for tle_node in action.targetList.iter() {
                    let tle = tle_node.as_target_entry().expect("targetList entry");
                    debug_assert!(!tle.resjunk);
                    ctx.buf.push_str(sep);
                    sep = ", ";
                    let attname = lsyscache::get_attname(ctx.mcx, rte.relid, tle.resno, false)?
                        .expect("get_attname missing_ok=false");
                    ctx.buf.push_str(&quote_identifier(attname.as_str()));
                    strippedexprs.push(process_indirection(tle.expr, ctx)?);
                }
                if !action.targetList.is_nil() {
                    ctx.buf.push(')');
                }
                match action.r#override {
                    OverridingKind::OVERRIDING_SYSTEM_VALUE => {
                        ctx.buf.push_str(" OVERRIDING SYSTEM VALUE")
                    }
                    OverridingKind::OVERRIDING_USER_VALUE => {
                        ctx.buf.push_str(" OVERRIDING USER VALUE")
                    }
                    OverridingKind::OVERRIDING_NOT_SET => {}
                }
                if !strippedexprs.is_empty() {
                    append_context_keyword(
                        ctx,
                        " VALUES (",
                        -PRETTYINDENT_STD,
                        PRETTYINDENT_STD,
                        4,
                    );
                    let mut first = true;
                    for e in &strippedexprs {
                        if !first {
                            ctx.buf.push_str(", ");
                        }
                        first = false;
                        get_rule_expr_toplevel(*e, ctx, false)?;
                    }
                    ctx.buf.push(')');
                } else {
                    ctx.buf.push_str(" DEFAULT VALUES");
                }
            }
            CmdType::CMD_UPDATE => {
                ctx.buf.push_str("UPDATE SET ");
                get_update_query_targetlist_def(query, &action.targetList, rte, ctx)?;
            }
            CmdType::CMD_DELETE => ctx.buf.push_str("DELETE"),
            CmdType::CMD_NOTHING => ctx.buf.push_str("DO NOTHING"),
            other => gap("get_merge_query_def", &format!("{other:?} action")),
        }
    }
    get_returning_clause(query, ctx)
}

pub(crate) fn process_indirection<'mcx>(
    node: Node<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<Node<'mcx>> {
    let mut node = node;
    let mut cdomain: Option<Node<'mcx>> = None;
    loop {
        match node.node_tag() {
            NodeTag::T_FieldStore => {
                let fstore = node.as_field_store().unwrap();
                let typrelid = lsyscache::get_typ_typrelid(fstore.resulttype)?;
                if typrelid == types_core::InvalidOid {
                    panic!(
                        "argument type {} of FieldStore is not a tuple type",
                        fstore.resulttype
                    );
                }
                // Stored rules carry exactly one target field.
                debug_assert!(fstore.fieldnums.len() == 1);
                let fieldname = lsyscache::get_attname(
                    ctx.mcx,
                    typrelid,
                    fstore.fieldnums.nth(0) as i16,
                    false,
                )?
                .expect("get_attname missing_ok=false");
                ctx.buf.push('.');
                ctx.buf.push_str(&quote_identifier(fieldname.as_str()));
                node = fstore.newvals.nth(0);
            }
            NodeTag::T_SubscriptingRef => {
                let sbsref = node.as_subscripting_ref().unwrap();
                let Some(refassgnexpr) = sbsref.refassgnexpr else {
                    break;
                };
                crate::deparse::print_subscripts(sbsref, ctx)?;
                node = refassgnexpr;
            }
            NodeTag::T_CoerceToDomain => {
                let cd = node.as_coerce_to_domain().unwrap();
                if cd.coercionformat != CoercionForm::COERCE_IMPLICIT_CAST {
                    break;
                }
                cdomain = Some(node);
                node = cd.arg;
            }
            _ => break,
        }
    }
    if let Some(cd_node) = cdomain {
        if cd_node.as_coerce_to_domain().unwrap().arg.ptr_eq(node) {
            node = cd_node;
        }
    }
    Ok(node)
}

fn get_simple_values_rte<'a, 'mcx>(
    query: &'a Query<'mcx>,
    ctx: &DeparseContext<'mcx>,
) -> Option<&'a RangeTblEntry<'mcx>> {
    let mut result: Option<&RangeTblEntry<'_>> = None;
    for n in query.rtable.iter() {
        let rte = n.as_range_tbl_entry().expect("rtable entry");
        if rte.rtekind == RTEKind::RTE_VALUES && rte.inFromCl {
            if result.is_some() {
                return None;
            }
            result = Some(rte);
        } else if rte.rtekind == RTEKind::RTE_RELATION && !rte.inFromCl {
            continue;
        } else {
            return None;
        }
    }
    let rte = result?;
    let eref_colnames = &rte.eref?.colnames;
    if query.targetList.len() != eref_colnames.len() {
        return None;
    }
    let mut colno = 0usize;
    for (tle_node, cname_node) in query.targetList.iter().zip(eref_colnames.iter()) {
        let tle = tle_node.as_target_entry().expect("targetList entry");
        let cname = cname_node.as_string().expect("eref colname").sval;
        if tle.resjunk {
            return None;
        }
        colno += 1;
        let colname: Option<&str> = match &ctx.result_desc {
            Some(rd) if colno <= rd.len() => Some(rd[colno - 1].as_str()),
            _ => tle.resname,
        };
        if colname != Some(cname) {
            return None;
        }
    }
    result
}

fn get_basic_select_query<'mcx>(
    query: &'mcx Query<'mcx>,
    target_list: &'mcx NodeList<'mcx>,
    having_qual: Option<Node<'mcx>>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    if ctx.pretty_indent() {
        ctx.indent_level += PRETTYINDENT_STD;
        ctx.buf.push(' ');
    }

    if let Some(values_rte) = get_simple_values_rte(query, ctx) {
        return get_values_def(&values_rte.values_lists, ctx);
    }

    ctx.buf.push_str(if query.isReturn { "RETURN" } else { "SELECT" });

    if !query.distinctClause.is_nil() {
        if query.hasDistinctOn {
            ctx.buf.push_str(" DISTINCT ON (");
            let mut sep = "";
            for c in query.distinctClause.iter() {
                let srt = c.as_sort_group_clause().expect("distinctClause entry");
                ctx.buf.push_str(sep);
                get_rule_sortgroupclause(srt.tleSortGroupRef, target_list, false, ctx)?;
                sep = ", ";
            }
            ctx.buf.push(')');
        } else {
            ctx.buf.push_str(" DISTINCT");
        }
    }

    get_target_list(target_list, ctx)?;

    get_from_clause(query, " FROM ", ctx)?;

    if let Some(jt) = query.jointree {
        if let Some(quals) = jt.quals {
            append_context_keyword(ctx, " WHERE ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
            get_rule_expr(quals, ctx, false)?;
        }
    }

    if !query.groupClause.is_nil() || !query.groupingSets.is_nil() {
        append_context_keyword(ctx, " GROUP BY ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
        if query.groupDistinct {
            ctx.buf.push_str("DISTINCT ");
        }
        let save = ctx.in_group_by;
        ctx.in_group_by = true;
        let mut sep = "";
        if query.groupingSets.is_nil() {
            for c in query.groupClause.iter() {
                let grp = c.as_sort_group_clause().expect("groupClause entry");
                ctx.buf.push_str(sep);
                get_rule_sortgroupclause(grp.tleSortGroupRef, target_list, false, ctx)?;
                sep = ", ";
            }
        } else {
            for g in query.groupingSets.iter() {
                let grp = g.as_grouping_set().expect("groupingSets entry");
                ctx.buf.push_str(sep);
                get_rule_groupingset(grp, target_list, true, ctx)?;
                sep = ", ";
            }
        }
        ctx.in_group_by = save;
    }

    if let Some(having) = having_qual {
        append_context_keyword(ctx, " HAVING ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
        get_rule_expr(having, ctx, false)?;
    }

    if !query.windowClause.is_nil() {
        get_rule_windowclause(query, target_list, ctx)?;
    }
    Ok(())
}

fn get_rule_windowclause<'mcx>(
    query: &'mcx Query<'mcx>,
    target_list: &'mcx NodeList<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    let mut sep: Option<&str> = None;
    for wc_node in query.windowClause.iter() {
        let wc = wc_node.as_window_clause().expect("windowClause entry");
        let Some(name) = wc.name else { continue };
        match sep {
            None => {
                append_context_keyword(ctx, " WINDOW ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1)
            }
            Some(s) => ctx.buf.push_str(s),
        }
        sep = Some(", ");
        ctx.buf.push_str(&format!("{} AS ", quote_identifier(name)));
        get_rule_windowspec(wc, target_list, ctx)?;
    }
    Ok(())
}

pub(crate) fn get_rule_windowspec<'mcx>(
    wc: &'mcx WindowClause<'mcx>,
    target_list: &'mcx NodeList<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    let mut needspace = false;
    ctx.buf.push('(');
    if let Some(refname) = wc.refname {
        ctx.buf.push_str(&quote_identifier(refname));
        needspace = true;
    }
    if !wc.partitionClause.is_nil() && wc.refname.is_none() {
        if needspace {
            ctx.buf.push(' ');
        }
        ctx.buf.push_str("PARTITION BY ");
        let mut sep = "";
        for c in wc.partitionClause.iter() {
            let grp = c.as_sort_group_clause().expect("partitionClause entry");
            ctx.buf.push_str(sep);
            get_rule_sortgroupclause(grp.tleSortGroupRef, target_list, false, ctx)?;
            sep = ", ";
        }
        needspace = true;
    }
    if !wc.orderClause.is_nil() && !wc.copiedOrder {
        if needspace {
            ctx.buf.push(' ');
        }
        ctx.buf.push_str("ORDER BY ");
        get_rule_orderby(&wc.orderClause, target_list, false, ctx)?;
        needspace = true;
    }
    if wc.frameOptions & FRAMEOPTION_NONDEFAULT != 0 {
        if needspace {
            ctx.buf.push(' ');
        }
        get_window_frame_options(wc.frameOptions, wc.startOffset, wc.endOffset, ctx)?;
    }
    ctx.buf.push(')');
    Ok(())
}

fn get_window_frame_options<'mcx>(
    frame_options: i32,
    start_offset: Option<Node<'mcx>>,
    end_offset: Option<Node<'mcx>>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    if frame_options & FRAMEOPTION_NONDEFAULT == 0 {
        return Ok(());
    }
    if frame_options & FRAMEOPTION_RANGE != 0 {
        ctx.buf.push_str("RANGE ");
    } else if frame_options & FRAMEOPTION_ROWS != 0 {
        ctx.buf.push_str("ROWS ");
    } else if frame_options & FRAMEOPTION_GROUPS != 0 {
        ctx.buf.push_str("GROUPS ");
    } else {
        debug_assert!(false);
    }
    if frame_options & FRAMEOPTION_BETWEEN != 0 {
        ctx.buf.push_str("BETWEEN ");
    }
    if frame_options & FRAMEOPTION_START_UNBOUNDED_PRECEDING != 0 {
        ctx.buf.push_str("UNBOUNDED PRECEDING ");
    } else if frame_options & FRAMEOPTION_START_CURRENT_ROW != 0 {
        ctx.buf.push_str("CURRENT ROW ");
    } else if frame_options & FRAMEOPTION_START_OFFSET != 0 {
        get_rule_expr(start_offset.expect("frame start offset"), ctx, false)?;
        if frame_options & FRAMEOPTION_START_OFFSET_PRECEDING != 0 {
            ctx.buf.push_str(" PRECEDING ");
        } else if frame_options & FRAMEOPTION_START_OFFSET_FOLLOWING != 0 {
            ctx.buf.push_str(" FOLLOWING ");
        } else {
            debug_assert!(false);
        }
    } else {
        debug_assert!(false);
    }
    if frame_options & FRAMEOPTION_BETWEEN != 0 {
        ctx.buf.push_str("AND ");
        if frame_options & FRAMEOPTION_END_UNBOUNDED_FOLLOWING != 0 {
            ctx.buf.push_str("UNBOUNDED FOLLOWING ");
        } else if frame_options & FRAMEOPTION_END_CURRENT_ROW != 0 {
            ctx.buf.push_str("CURRENT ROW ");
        } else if frame_options & FRAMEOPTION_END_OFFSET != 0 {
            get_rule_expr(end_offset.expect("frame end offset"), ctx, false)?;
            if frame_options & FRAMEOPTION_END_OFFSET_PRECEDING != 0 {
                ctx.buf.push_str(" PRECEDING ");
            } else if frame_options & FRAMEOPTION_END_OFFSET_FOLLOWING != 0 {
                ctx.buf.push_str(" FOLLOWING ");
            } else {
                debug_assert!(false);
            }
        } else {
            debug_assert!(false);
        }
    }
    if frame_options & FRAMEOPTION_EXCLUDE_CURRENT_ROW != 0 {
        ctx.buf.push_str("EXCLUDE CURRENT ROW ");
    } else if frame_options & FRAMEOPTION_EXCLUDE_GROUP != 0 {
        ctx.buf.push_str("EXCLUDE GROUP ");
    } else if frame_options & FRAMEOPTION_EXCLUDE_TIES != 0 {
        ctx.buf.push_str("EXCLUDE TIES ");
    }
    debug_assert!(ctx.buf.ends_with(' '));
    ctx.buf.pop();
    Ok(())
}

fn get_target_list<'mcx>(
    target_list: &'mcx NodeList<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    let mut sep = " ";
    let mut colno = 0usize;
    let mut last_was_multiline = false;

    for tle_node in target_list.iter() {
        let tle = tle_node.as_target_entry().expect("targetList entry");
        if tle.resjunk {
            continue;
        }
        ctx.buf.push_str(sep);
        sep = ", ";
        colno += 1;

        let saved_buf = std::mem::take(&mut ctx.buf);
        let attname: Option<String> = match tle.expr.as_var() {
            Some(var) => get_variable(tle.expr, var, 0, true, ctx)?,
            None => {
                get_rule_expr(tle.expr, ctx, true)?;
                if ctx.colnames_visible { None } else { Some("?column?".to_string()) }
            }
        };

        let colname: Option<String> = match &ctx.result_desc {
            Some(rd) if colno <= rd.len() => Some(rd[colno - 1].clone()),
            _ => tle.resname.map(str::to_owned),
        };
        if let Some(cn) = &colname {
            if attname.as_deref() != Some(cn.as_str()) {
                ctx.buf.push_str(&format!(" AS {}", quote_identifier(cn)));
            }
        }

        let targetbuf = std::mem::replace(&mut ctx.buf, saved_buf);

        if ctx.pretty_indent() && ctx.wrap_column >= 0 {
            let leading_nl = targetbuf.starts_with('\n');
            if leading_nl {
                remove_trailing_spaces(&mut ctx.buf);
            } else {
                let trailing_len = match ctx.buf.rfind('\n') {
                    Some(p) => ctx.buf.len() - (p + 1),
                    None => ctx.buf.len(),
                };
                if colno > 1
                    && (trailing_len + targetbuf.len() > ctx.wrap_column as usize
                        || last_was_multiline)
                {
                    append_context_keyword(
                        ctx,
                        "",
                        -PRETTYINDENT_STD,
                        PRETTYINDENT_STD,
                        PRETTYINDENT_VAR,
                    );
                }
            }
            let scan_from = if leading_nl { 1 } else { 0 };
            last_was_multiline = targetbuf[scan_from.min(targetbuf.len())..].contains('\n');
        }

        ctx.buf.push_str(&targetbuf);
    }
    Ok(())
}

fn get_setop_query<'mcx>(
    set_op: Node<'mcx>,
    query: &'mcx Query<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    match set_op.node_tag() {
        NodeTag::T_RangeTblRef => {
            let rtr = set_op.as_range_tbl_ref().unwrap();
            let rte = query
                .rtable
                .nth(rtr.rtindex as usize - 1)
                .as_range_tbl_entry()
                .expect("rtable entry");
            let subquery = rte.subquery.expect("setop leaf is a subquery RTE");
            let need_paren = !subquery.cteList.is_nil()
                || !subquery.sortClause.is_nil()
                || !subquery.rowMarks.is_nil()
                || subquery.limitOffset.is_some()
                || subquery.limitCount.is_some()
                || subquery.setOperations.is_some();
            if need_paren {
                ctx.buf.push('(');
            }
            get_query_def(subquery, ctx, ctx.result_desc.clone(), ctx.colnames_visible)?;
            if need_paren {
                ctx.buf.push(')');
            }
            Ok(())
        }
        NodeTag::T_SetOperationStmt => {
            let op = set_op.as_set_operation_stmt().unwrap();
            let larg = op.larg.expect("setop has a larg");
            let rarg = op.rarg.expect("setop has a rarg");

            let need_paren = match larg.as_set_operation_stmt() {
                Some(lop) => !(lop.op == op.op && lop.all == op.all),
                None => false,
            };
            let subindent = if need_paren {
                ctx.buf.push('(');
                append_context_keyword(ctx, "", PRETTYINDENT_STD, 0, 0);
                PRETTYINDENT_STD
            } else {
                0
            };

            get_setop_query(larg, query, ctx)?;

            if need_paren {
                append_context_keyword(ctx, ") ", -subindent, 0, 0);
            } else if ctx.pretty_indent() {
                append_context_keyword(ctx, "", -subindent, 0, 0);
            } else {
                ctx.buf.push(' ');
            }

            ctx.buf.push_str(match op.op {
                SetOperation::SETOP_UNION => "UNION ",
                SetOperation::SETOP_INTERSECT => "INTERSECT ",
                SetOperation::SETOP_EXCEPT => "EXCEPT ",
                SetOperation::SETOP_NONE => panic!("unrecognized set op: SETOP_NONE"),
            });
            if op.all {
                ctx.buf.push_str("ALL ");
            }

            let need_paren = rarg.node_tag() == NodeTag::T_SetOperationStmt;
            let subindent = if need_paren {
                ctx.buf.push('(');
                PRETTYINDENT_STD
            } else {
                0
            };
            append_context_keyword(ctx, "", subindent, 0, 0);

            let save_visible = ctx.colnames_visible;
            ctx.colnames_visible = false;
            get_setop_query(rarg, query, ctx)?;
            ctx.colnames_visible = save_visible;

            if ctx.pretty_indent() {
                ctx.indent_level -= subindent;
            }
            if need_paren {
                append_context_keyword(ctx, ")", 0, 0, 0);
            }
            Ok(())
        }
        other => panic!("get_setop_query: unrecognized node type {other:?}"),
    }
}

fn get_sortgroupref_tle<'mcx>(
    sortref: u32,
    target_list: &'mcx NodeList<'mcx>,
) -> &'mcx types_nodes::TargetEntry<'mcx> {
    for n in target_list.iter() {
        let tle = n.as_target_entry().expect("targetList entry");
        if tle.ressortgroupref == sortref {
            return tle;
        }
    }
    panic!("ORDER/GROUP BY expression not found in targetlist");
}

fn get_tablesample_def<'mcx>(
    tablesample: &'mcx types_nodes::parsenodes::TableSampleClause<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    const INTERNALOID: types_core::Oid = 2281;
    let fname = crate::generate_function_name(
        ctx.mcx,
        tablesample.tsmhandler,
        &[INTERNALOID],
        &[],
        false,
    )?;
    ctx.buf.push_str(&format!(" TABLESAMPLE {fname} ("));
    let mut nargs = 0;
    for arg in tablesample.args.iter() {
        if nargs > 0 {
            ctx.buf.push_str(", ");
        }
        nargs += 1;
        get_rule_expr(arg, ctx, false)?;
    }
    ctx.buf.push(')');
    if let Some(repeatable) = tablesample.repeatable {
        ctx.buf.push_str(" REPEATABLE (");
        get_rule_expr(repeatable, ctx, false)?;
        ctx.buf.push(')');
    }
    Ok(())
}

// SIMPLE content carries Integer nodes (C: an int list of sortgrouprefs).
fn get_rule_groupingset<'mcx>(
    gset: &'mcx types_nodes::parsenodes::GroupingSet<'mcx>,
    target_list: &'mcx NodeList<'mcx>,
    omit_parens: bool,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    use types_nodes::parsenodes::GroupingSetKind::*;
    let mut omit_child_parens = true;
    let mut sep = "";
    match gset.kind {
        GROUPING_SET_EMPTY => {
            ctx.buf.push_str("()");
            return Ok(());
        }
        GROUPING_SET_SIMPLE => {
            let parens = !omit_parens || gset.content.len() != 1;
            if parens {
                ctx.buf.push('(');
            }
            for n in gset.content.iter() {
                let sortref = n.as_integer().expect("SIMPLE grouping-set ref").ival as u32;
                ctx.buf.push_str(sep);
                get_rule_sortgroupclause(sortref, target_list, false, ctx)?;
                sep = ", ";
            }
            if parens {
                ctx.buf.push(')');
            }
            return Ok(());
        }
        GROUPING_SET_ROLLUP => ctx.buf.push_str("ROLLUP("),
        GROUPING_SET_CUBE => ctx.buf.push_str("CUBE("),
        GROUPING_SET_SETS => {
            ctx.buf.push_str("GROUPING SETS (");
            omit_child_parens = false;
        }
    }
    for n in gset.content.iter() {
        ctx.buf.push_str(sep);
        let child = n.as_grouping_set().expect("nested GroupingSet");
        get_rule_groupingset(child, target_list, omit_child_parens, ctx)?;
        sep = ", ";
    }
    ctx.buf.push(')');
    Ok(())
}

fn get_rule_sortgroupclause<'mcx>(
    sortref: u32,
    target_list: &'mcx NodeList<'mcx>,
    force_colno: bool,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<Node<'mcx>> {
    let tle = get_sortgroupref_tle(sortref, target_list);
    let expr = tle.expr;

    if force_colno {
        debug_assert!(!tle.resjunk);
        ctx.buf.push_str(&format!("{}", tle.resno));
    } else if let Some(c) = expr.as_const() {
        get_const_expr(c, ctx, 1)?;
    } else if let Some(v) = expr.as_var() {
        let save = ctx.var_in_order_by;
        ctx.var_in_order_by = true;
        get_variable(expr, v, 0, false, ctx)?;
        ctx.var_in_order_by = save;
    } else {
        let need_paren = ctx.pretty_paren()
            || matches!(
                expr.node_tag(),
                NodeTag::T_FuncExpr | NodeTag::T_Aggref | NodeTag::T_WindowFunc
            );
        if need_paren {
            ctx.buf.push('(');
        }
        get_rule_expr(expr, ctx, true)?;
        if need_paren {
            ctx.buf.push(')');
        }
    }
    Ok(expr)
}

pub(crate) fn get_rule_orderby<'mcx>(
    order_list: &'mcx NodeList<'mcx>,
    target_list: &'mcx NodeList<'mcx>,
    force_colno: bool,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    let mut sep = "";
    for n in order_list.iter() {
        let srt = n.as_sort_group_clause().expect("sortClause entry");
        ctx.buf.push_str(sep);
        let sortexpr = get_rule_sortgroupclause(srt.tleSortGroupRef, target_list, force_colno, ctx)?;
        let sortcoltype = parse_expr::expr_type(sortexpr);
        let typentry = typcache::lookup_type_cache(
            sortcoltype,
            typcache::TYPECACHE_LT_OPR | typcache::TYPECACHE_GT_OPR,
        )?;
        if srt.sortop == typentry.lt_opr() {
            if srt.nulls_first {
                ctx.buf.push_str(" NULLS FIRST");
            }
        } else if srt.sortop == typentry.gt_opr() {
            ctx.buf.push_str(" DESC");
            if !srt.nulls_first {
                ctx.buf.push_str(" NULLS LAST");
            }
        } else {
            let opname = generate_operator_name(ctx.mcx, srt.sortop, sortcoltype, sortcoltype)?;
            ctx.buf.push_str(&format!(" USING {opname}"));
            ctx.buf.push_str(if srt.nulls_first { " NULLS FIRST" } else { " NULLS LAST" });
        }
        sep = ", ";
    }
    Ok(())
}

fn get_from_clause<'mcx>(
    query: &'mcx Query<'mcx>,
    prefix: &str,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    let Some(jt) = query.jointree else { return Ok(()) };
    let mut first = true;
    for jtnode in jt.fromlist.iter() {
        if let Some(rtr) = jtnode.as_range_tbl_ref() {
            let rte = query
                .rtable
                .nth(rtr.rtindex as usize - 1)
                .as_range_tbl_entry()
                .expect("rtable entry");
            if !rte.inFromCl {
                continue;
            }
        }
        if first {
            append_context_keyword(ctx, prefix, -PRETTYINDENT_STD, PRETTYINDENT_STD, 2);
            first = false;
            get_from_clause_item(jtnode, query, ctx)?;
        } else {
            ctx.buf.push_str(", ");
            let saved_buf = std::mem::take(&mut ctx.buf);
            get_from_clause_item(jtnode, query, ctx)?;
            let itembuf = std::mem::replace(&mut ctx.buf, saved_buf);

            if ctx.pretty_indent() && ctx.wrap_column >= 0 {
                if itembuf.starts_with('\n') {
                    remove_trailing_spaces(&mut ctx.buf);
                } else {
                    let trailing_len = match ctx.buf.rfind('\n') {
                        Some(p) => ctx.buf.len() - (p + 1),
                        None => ctx.buf.len(),
                    };
                    if trailing_len + itembuf.len() > ctx.wrap_column as usize {
                        append_context_keyword(
                            ctx,
                            "",
                            -PRETTYINDENT_STD,
                            PRETTYINDENT_STD,
                            PRETTYINDENT_VAR,
                        );
                    }
                }
            }
            ctx.buf.push_str(&itembuf);
        }
    }
    Ok(())
}

fn get_from_clause_item<'mcx>(
    jtnode: Node<'mcx>,
    query: &'mcx Query<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    match jtnode.node_tag() {
        NodeTag::T_RangeTblRef => {
            let varno = jtnode.as_range_tbl_ref().unwrap().rtindex as usize;
            let rte = query
                .rtable
                .nth(varno - 1)
                .as_range_tbl_entry()
                .expect("rtable entry");
            if rte.lateral {
                ctx.buf.push_str("LATERAL ");
            }
            let mut rtfunc1: Option<&RangeTblFunction<'_>> = None;
            match rte.rtekind {
                RTEKind::RTE_RELATION => {
                    if !rte.inh {
                        ctx.buf.push_str("ONLY ");
                    }
                    let name = generate_relation_name(ctx.mcx, rte.relid)?;
                    ctx.buf.push_str(&name);
                }
                RTEKind::RTE_SUBQUERY => {
                    ctx.buf.push('(');
                    let sub = rte.subquery.expect("subquery RTE has a subquery");
                    get_query_def(sub, ctx, None, true)?;
                    ctx.buf.push(')');
                }
                RTEKind::RTE_FUNCTION => {
                    let first = rte
                        .functions
                        .nth(0)
                        .as_range_tbl_function()
                        .expect("functions entry");
                    if rte.functions.len() == 1
                        && (first.funccolnames.is_nil() || !rte.funcordinality)
                    {
                        rtfunc1 = Some(first);
                        get_rule_expr_funccall(
                            first.funcexpr.expect("RangeTblFunction has a funcexpr"),
                            ctx,
                            true,
                        )?;
                    } else {
                        let all_unnest = rte.functions.iter().all(|f| {
                            let rtfunc = f.as_range_tbl_function().expect("functions entry");
                            rtfunc.funccolnames.is_nil()
                                && rtfunc
                                    .funcexpr
                                    .and_then(|e| e.as_func_expr())
                                    .is_some_and(|fe| fe.funcid == F_UNNEST_ANYARRAY)
                        });
                        if all_unnest {
                            ctx.buf.push_str("UNNEST(");
                            let mut first_arg = true;
                            for f in rte.functions.iter() {
                                let fe = f
                                    .as_range_tbl_function()
                                    .unwrap()
                                    .funcexpr
                                    .unwrap()
                                    .as_func_expr()
                                    .unwrap();
                                for arg in fe.args.iter() {
                                    if !first_arg {
                                        ctx.buf.push_str(", ");
                                    }
                                    first_arg = false;
                                    get_rule_expr(arg, ctx, true)?;
                                }
                            }
                            ctx.buf.push(')');
                        } else {
                            ctx.buf.push_str("ROWS FROM(");
                            let mut funcno = 0;
                            for f in rte.functions.iter() {
                                let rtfunc =
                                    f.as_range_tbl_function().expect("functions entry");
                                if funcno > 0 {
                                    ctx.buf.push_str(", ");
                                }
                                get_rule_expr_funccall(
                                    rtfunc.funcexpr.expect("RangeTblFunction has a funcexpr"),
                                    ctx,
                                    true,
                                )?;
                                if !rtfunc.funccolnames.is_nil() {
                                    ctx.buf.push_str(" AS ");
                                    get_from_clause_coldeflist(rtfunc, ctx)?;
                                }
                                funcno += 1;
                            }
                            ctx.buf.push(')');
                        }
                    }
                    if rte.funcordinality {
                        ctx.buf.push_str(" WITH ORDINALITY");
                    }
                }
                RTEKind::RTE_TABLEFUNC => {
                    let tf = rte
                        .tablefunc
                        .and_then(|n| n.as_table_func())
                        .expect("RTE_TABLEFUNC holds a TableFunc");
                    crate::deparse::get_tablefunc(tf, ctx, true)?;
                }
                RTEKind::RTE_VALUES => {
                    ctx.buf.push('(');
                    get_values_def(&rte.values_lists, ctx)?;
                    ctx.buf.push(')');
                }
                RTEKind::RTE_CTE => {
                    ctx.buf
                        .push_str(&quote_identifier(rte.ctename.expect("CTE RTE has a name")));
                }
                other => gap("get_from_clause_item", &format!("{other:?} RTE deparse")),
            }
            get_rte_alias(rte, varno, false, ctx)?;
            match rtfunc1 {
                Some(rtfunc) if !rtfunc.funccolnames.is_nil() => {
                    let colinfo_names =
                        ctx.namespaces[0].rtable_columns[varno - 1].colnames.clone();
                    get_from_clause_coldeflist_named(rtfunc, &colinfo_names, ctx)?;
                }
                _ => get_column_alias_list(varno, ctx),
            }
            if rte.rtekind == RTEKind::RTE_RELATION {
                if let Some(ts) = rte.tablesample {
                    let ts =
                        ts.as_table_sample_clause().expect("tablesample is a TableSampleClause");
                    get_tablesample_def(ts, ctx)?;
                }
            }
            Ok(())
        }
        NodeTag::T_JoinExpr => {
            let j = jtnode.as_join_expr().unwrap();
            let need_paren_on_right = ctx.pretty_paren()
                && j.rarg.node_tag() != NodeTag::T_RangeTblRef
                && !(j.rarg.as_join_expr().is_some_and(|rj| rj.alias.is_some()));

            if !ctx.pretty_paren() || j.alias.is_some() {
                ctx.buf.push('(');
            }

            get_from_clause_item(j.larg, query, ctx)?;

            match j.jointype {
                JoinType::JOIN_INNER => {
                    if j.quals.is_some() || !j.usingClause.is_nil() {
                        append_context_keyword(
                            ctx,
                            " JOIN ",
                            -PRETTYINDENT_STD,
                            PRETTYINDENT_STD,
                            PRETTYINDENT_JOIN,
                        );
                    } else {
                        append_context_keyword(
                            ctx,
                            " CROSS JOIN ",
                            -PRETTYINDENT_STD,
                            PRETTYINDENT_STD,
                            PRETTYINDENT_JOIN,
                        );
                    }
                }
                JoinType::JOIN_LEFT => append_context_keyword(
                    ctx,
                    " LEFT JOIN ",
                    -PRETTYINDENT_STD,
                    PRETTYINDENT_STD,
                    PRETTYINDENT_JOIN,
                ),
                JoinType::JOIN_FULL => append_context_keyword(
                    ctx,
                    " FULL JOIN ",
                    -PRETTYINDENT_STD,
                    PRETTYINDENT_STD,
                    PRETTYINDENT_JOIN,
                ),
                JoinType::JOIN_RIGHT => append_context_keyword(
                    ctx,
                    " RIGHT JOIN ",
                    -PRETTYINDENT_STD,
                    PRETTYINDENT_STD,
                    PRETTYINDENT_JOIN,
                ),
                other => panic!("unrecognized join type: {other:?}"),
            }

            if need_paren_on_right {
                ctx.buf.push('(');
            }
            get_from_clause_item(j.rarg, query, ctx)?;
            if need_paren_on_right {
                ctx.buf.push(')');
            }

            if !j.usingClause.is_nil() {
                ctx.buf.push_str(" USING (");
                let using_names =
                    ctx.namespaces[0].rtable_columns[j.rtindex as usize - 1].using_names.clone();
                let mut first = true;
                for name in &using_names {
                    if !first {
                        ctx.buf.push_str(", ");
                    }
                    first = false;
                    ctx.buf.push_str(&quote_identifier(name));
                }
                ctx.buf.push(')');
                if let Some(jua) = j.join_using_alias {
                    ctx.buf.push_str(&format!(
                        " AS {}",
                        quote_identifier(jua.aliasname.expect("USING alias has a name"))
                    ));
                }
            } else if let Some(quals) = j.quals {
                ctx.buf.push_str(" ON ");
                if !ctx.pretty_paren() {
                    ctx.buf.push('(');
                }
                get_rule_expr(quals, ctx, false)?;
                if !ctx.pretty_paren() {
                    ctx.buf.push(')');
                }
            } else if j.jointype != JoinType::JOIN_INNER {
                ctx.buf.push_str(" ON TRUE");
            }

            if !ctx.pretty_paren() || j.alias.is_some() {
                ctx.buf.push(')');
            }

            if j.alias.is_some() {
                let name = get_rtable_name(j.rtindex as usize, ctx)
                    .expect("aliased join has a refname");
                ctx.buf.push_str(&format!(" {}", quote_identifier(&name)));
                get_column_alias_list(j.rtindex as usize, ctx);
            }
            Ok(())
        }
        other => panic!("get_from_clause_item: unrecognized node type {other:?}"),
    }
}

fn get_rte_alias(
    rte: &RangeTblEntry<'_>,
    varno: usize,
    use_as: bool,
    ctx: &mut DeparseContext<'_>,
) -> PgResult<()> {
    let refname = get_rtable_name(varno, ctx);
    let printalias = if rte.alias.is_some() {
        true
    } else if ctx.namespaces[0].rtable_columns[varno - 1].printaliases {
        true
    } else if rte.rtekind == RTEKind::RTE_RELATION {
        let relname = lsyscache::get_rel_name(ctx.mcx, rte.relid)?
            .expect("get_relation_name: relation exists")
            .as_str()
            .to_owned();
        refname.as_deref() != Some(relname.as_str())
    } else if rte.rtekind == RTEKind::RTE_CTE {
        refname.as_deref() != rte.ctename
    } else {
        matches!(
            rte.rtekind,
            RTEKind::RTE_FUNCTION | RTEKind::RTE_SUBQUERY | RTEKind::RTE_VALUES
        )
    };
    if printalias {
        let name = refname.expect("printed alias has a refname");
        ctx.buf.push_str(if use_as { " AS " } else { " " });
        ctx.buf.push_str(&quote_identifier(&name));
    }
    Ok(())
}

const F_UNNEST_ANYARRAY: types_core::Oid = 2331;

pub(crate) fn looks_like_function(node: Node<'_>) -> bool {
    match node.node_tag() {
        NodeTag::T_FuncExpr => matches!(
            node.as_func_expr().unwrap().funcformat,
            CoercionForm::COERCE_EXPLICIT_CALL | CoercionForm::COERCE_SQL_SYNTAX
        ),
        NodeTag::T_NullIfExpr
        | NodeTag::T_CoalesceExpr
        | NodeTag::T_MinMaxExpr
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_XmlExpr
        | NodeTag::T_JsonExpr => true,
        _ => false,
    }
}

fn get_rule_expr_funccall<'mcx>(
    node: Node<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
    showimplicit: bool,
) -> PgResult<()> {
    if looks_like_function(node) {
        get_rule_expr(node, ctx, showimplicit)
    } else {
        ctx.buf.push_str("CAST(");
        get_rule_expr(node, ctx, false)?;
        ctx.buf.push_str(&format!(
            " AS {})",
            format_type::format_type_with_typemod(
                parse_expr::expr_type(node),
                parse_expr::expr_typmod(node)
            )?
        ));
        Ok(())
    }
}

fn coldeflist_body<'mcx>(
    rtfunc: &RangeTblFunction<'mcx>,
    names: &[String],
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    ctx.buf.push('(');
    let typmods: Vec<i32> = rtfunc.funccoltypmods.iter().collect();
    let collations: Vec<types_core::Oid> = rtfunc.funccolcollations.iter().collect();
    for (i, atttypid) in rtfunc.funccoltypes.iter().enumerate() {
        if i > 0 {
            ctx.buf.push_str(", ");
        }
        ctx.buf.push_str(&format!(
            "{} {}",
            quote_identifier(&names[i]),
            format_type::format_type_with_typemod(atttypid, typmods[i])?
        ));
        let attcollation = collations[i];
        if attcollation != types_core::InvalidOid
            && attcollation != lsyscache::get_typcollation(atttypid)?
        {
            let collname = crate::generate_collation_name(ctx.mcx, attcollation)?;
            ctx.buf.push_str(&format!(" COLLATE {collname}"));
        }
    }
    ctx.buf.push(')');
    Ok(())
}

fn get_from_clause_coldeflist<'mcx>(
    rtfunc: &RangeTblFunction<'mcx>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    let names: Vec<String> = rtfunc
        .funccolnames
        .iter()
        .map(|n| n.as_string().expect("funccolname").sval.to_owned())
        .collect();
    coldeflist_body(rtfunc, &names, ctx)
}

fn get_from_clause_coldeflist_named<'mcx>(
    rtfunc: &RangeTblFunction<'mcx>,
    colinfo_names: &[Option<String>],
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    let names: Vec<String> = colinfo_names
        .iter()
        .map(|n| n.clone().expect("no dropped columns in a coldeflist"))
        .collect();
    coldeflist_body(rtfunc, &names, ctx)
}

fn get_column_alias_list(varno: usize, ctx: &mut DeparseContext<'_>) {
    let colinfo = &ctx.namespaces[0].rtable_columns[varno - 1];
    if !colinfo.printaliases {
        return;
    }
    let names = colinfo.new_colnames.clone();
    let mut first = true;
    for name in &names {
        if first {
            ctx.buf.push('(');
            first = false;
        } else {
            ctx.buf.push_str(", ");
        }
        ctx.buf.push_str(&quote_identifier(name));
    }
    if !first {
        ctx.buf.push(')');
    }
}
