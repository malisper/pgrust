//! deparse_namespace + get_query_def spine for SELECT (ruleutils.c). Query
//! shapes outside the stored-view SELECT set are loud named panics.

use std::rc::Rc;

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::nodes_enums::{CmdType, LimitOption};
use types_nodes::parsenodes::SetOperation;
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
    rte.eref = Some(alias_ref);
    rte.inFromCl = true;
    let rte_ref = rte.seal_ref();

    let mut dpns = DeparseNamespace {
        rtable: vec![rte_ref],
        rtable_names: Vec::new(),
        rtable_columns: Vec::new(),
        unique_using: false,
        using_names: Vec::new(),
    };
    set_rtable_names(mcx, &mut dpns, &[])?;
    set_simple_column_names(&mut dpns)?;
    Ok(dpns)
}

fn set_rtable_names<'mcx>(
    mcx: Mcx<'mcx>,
    dpns: &mut DeparseNamespace<'mcx>,
    parents: &[Rc<DeparseNamespace<'mcx>>],
) -> PgResult<()> {
    let mut entries: Vec<(String, u32)> = Vec::new();
    for p in parents {
        for name in p.rtable_names.iter().flatten() {
            if !entries.iter().any(|(n, _)| n == name) {
                entries.push((name.clone(), 0));
            }
        }
    }
    for rte in &dpns.rtable {
        let refname: Option<String> = if let Some(alias) = rte.alias {
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
    if !query.cteList.is_nil() {
        gap("set_deparse_for_query", "WITH/CTE deparse");
    }
    let rtable: Vec<&RangeTblEntry<'_>> = query
        .rtable
        .iter()
        .map(|n| n.as_range_tbl_entry().expect("rtable entry"))
        .collect();
    let mut dpns = DeparseNamespace {
        rtable,
        rtable_names: Vec::new(),
        rtable_columns: Vec::new(),
        unique_using: false,
        using_names: Vec::new(),
    };
    set_rtable_names(mcx, &mut dpns, parents)?;
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
            set_relation_column_names(&mut dpns, i)?;
        }
    }
    Ok(dpns)
}

fn set_simple_column_names(dpns: &mut DeparseNamespace<'_>) -> PgResult<()> {
    for _ in 0..dpns.rtable.len() {
        dpns.rtable_columns.push(DeparseColumns::default());
    }
    for i in 0..dpns.rtable.len() {
        if dpns.rtable[i].rtekind != RTEKind::RTE_JOIN {
            set_relation_column_names(dpns, i)?;
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

fn set_relation_column_names(dpns: &mut DeparseNamespace<'_>, idx: usize) -> PgResult<()> {
    let rte = dpns.rtable[idx];
    let mut colinfo = std::mem::take(&mut dpns.rtable_columns[idx]);

    let real_colnames: Vec<Option<String>> = match rte.rtekind {
        RTEKind::RTE_RELATION => relation_real_colnames(rte.relid)?,
        RTEKind::RTE_FUNCTION if !rte.functions.is_nil() => {
            gap("set_relation_column_names", "function RTE (expandRTE)")
        }
        RTEKind::RTE_TABLEFUNC => gap("set_relation_column_names", "tablefunc RTE"),
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
    let mut new_colnames: Vec<String> = Vec::with_capacity(nnewcolumns);
    let mut is_new_col: Vec<bool> = Vec::with_capacity(nnewcolumns);

    let mut leftmerged = vec![false; left.colnames.len() + 1];
    let mut rightmerged = vec![false; right.colnames.len() + 1];
    let mut i = 0usize;
    while i < noldcolumns && colinfo.leftattnos[i] != 0 && colinfo.rightattnos[i] != 0 {
        new_colnames.push(colinfo.colnames[i].clone().expect("merged column name assigned"));
        is_new_col.push(false);
        if colinfo.leftattnos[i] > 0 {
            leftmerged[colinfo.leftattnos[i] as usize] = true;
        }
        if colinfo.rightattnos[i] > 0 {
            rightmerged[colinfo.rightattnos[i] as usize] = true;
        }
        i += 1;
    }

    for (child, merged, attnos) in [
        (left, &leftmerged, &colinfo.leftattnos),
        (right, &rightmerged, &colinfo.rightattnos),
    ] {
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
                new_colnames.push(colinfo.colnames[i].clone().expect("existing join column"));
                is_new_col.push(child.is_new_col[jc]);
                i += 1;
            } else {
                if rte.alias.is_some() {
                    // Aliased-join new-column unique-ification mutates colinfo
                    // while child is borrowed; the C corpus reaching it needs
                    // ALTER TABLE ADD COLUMN under an aliased join.
                    gap("set_join_column_names", "new columns under an aliased join");
                }
                new_colnames.push(child_colname.clone());
                is_new_col.push(child.is_new_col[jc]);
            }
        }
    }
    debug_assert_eq!(new_colnames.len(), nnewcolumns);
    colinfo.new_colnames = new_colnames;
    colinfo.is_new_col = is_new_col;

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
        CmdType::CMD_NOTHING => {
            ctx.buf.push_str("NOTHING");
            Ok(())
        }
        other => gap("get_query_def", &format!("{other:?} deparse")),
    };

    ctx.namespaces.remove(0);
    ctx.varprefix = save_varprefix;
    ctx.result_desc = save_result_desc;
    ctx.target_list = save_target_list;
    ctx.colnames_visible = save_colnames_visible;
    ctx.in_group_by = save_in_group_by;
    ctx.var_in_order_by = save_var_in_order_by;
    ctx.indent_level = save_indent;
    r
}

fn get_select_query_def<'mcx>(
    query: &'mcx Query<'mcx>,
    target_list: &'mcx NodeList<'mcx>,
    having_qual: Option<Node<'mcx>>,
    ctx: &mut DeparseContext<'mcx>,
) -> PgResult<()> {
    ctx.target_list = Some(target_list);

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
            gap("get_select_query_def", "FETCH FIRST ... WITH TIES");
        }
        append_context_keyword(ctx, " LIMIT ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
        match count.as_const() {
            Some(c) if c.constisnull => ctx.buf.push_str("ALL"),
            _ => get_rule_expr(count, ctx, false)?,
        }
    }

    if query.hasForUpdate {
        gap("get_select_query_def", "FOR UPDATE/SHARE deparse");
    }
    Ok(())
}

fn get_simple_values_rte<'a, 'mcx>(
    query: &'a Query<'mcx>,
    _ctx: &DeparseContext<'mcx>,
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

    if get_simple_values_rte(query, ctx).is_some() {
        gap("get_values_def", "VALUES list deparse");
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
        if !query.groupingSets.is_nil() {
            gap("get_basic_select_query", "GROUPING SETS/ROLLUP/CUBE deparse");
        }
        append_context_keyword(ctx, " GROUP BY ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
        if query.groupDistinct {
            ctx.buf.push_str("DISTINCT ");
        }
        let save = ctx.in_group_by;
        ctx.in_group_by = true;
        let mut sep = "";
        for c in query.groupClause.iter() {
            let grp = c.as_sort_group_clause().expect("groupClause entry");
            ctx.buf.push_str(sep);
            get_rule_sortgroupclause(grp.tleSortGroupRef, target_list, false, ctx)?;
            sep = ", ";
        }
        ctx.in_group_by = save;
    }

    if let Some(having) = having_qual {
        append_context_keyword(ctx, " HAVING ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
        get_rule_expr(having, ctx, false)?;
    }

    if !query.windowClause.is_nil() {
        gap("get_basic_select_query", "WINDOW clause deparse");
    }
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
            Some(var) => get_variable(var, 0, true, ctx)?,
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
        get_variable(v, 0, false, ctx)?;
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
                gap("get_from_clause_item", "LATERAL deparse");
            }
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
                other => gap("get_from_clause_item", &format!("{other:?} RTE deparse")),
            }
            if rte.tablesample.is_some() {
                gap("get_from_clause_item", "TABLESAMPLE deparse");
            }
            get_rte_alias(rte, varno, false, ctx)?;
            get_column_alias_list(varno, ctx);
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
                if j.join_using_alias.is_some() {
                    gap("get_from_clause_item", "JOIN ... USING ... AS alias");
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
