//! JSON_TABLE plan execution (jsonpath_exec.c JsonbTableRoutine half): the
//! JsonTablePlanState tree over TableFunc.plan and the row-pattern walk.
//! The executor (nodetablefuncscan) owns the per-column ExprState evaluation
//! (C JsonTableGetValue's ExecEvalExpr half) and drives this context.

use datum::Datum;
use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_nodes::Node;
use types_tuple::varatt;

use crate::{
    execute_json_path, jbv_to_jsonb_image, Jper, JsonPathVariable, JsonPathVars, JsonValueList,
};

// C JsonTablePlanState splits by IsA(plan): PathScan carries the jsonpath and
// the nested link; SiblingJoin carries left/right. Indices point into
// JsonTableExecContext::states (C's pointer graph as an index arena).
#[derive(Clone, Copy)]
enum PlanKind<'mcx> {
    PathScan {
        path: &'mcx [u8],
        error_on_error: bool,
        nested: Option<u32>,
    },
    SiblingJoin {
        left: u32,
        right: u32,
    },
}

struct JsonTablePlanState<'mcx> {
    kind: PlanKind<'mcx>,
    // Row pattern results as full on-disk jsonb varlena images, serialized
    // once per reset_row_pattern (C keeps JsonbValue* in a per-scan mcxt
    // reset per ResetRowPattern; here the images are owned and the vec is
    // cleared per reset).
    found: PgVec<'mcx, PgVec<'mcx, u8>>,
    iter_pos: usize,
    // C current.{value,isnull}: Some(i) indexes `found`; None = isnull.
    current: Option<u32>,
    ordinal: i32,
    parent: Option<u32>,
}

// C JsonTableExecContext. Divergences: the 418352867 magic sanity field is
// dropped (the context is an owned field of the scan state, never a void*
// opaque); DestroyOpaque is Drop. Row-pattern evaluation temporaries allocate
// in the scan-lifetime mcx instead of C's per-planstate reset context.
pub struct JsonTableExecContext<'mcx> {
    mcx: Mcx<'mcx>,
    states: PgVec<'mcx, JsonTablePlanState<'mcx>>,
    root: u32,
    colplanstates: PgVec<'mcx, u32>,
    args: PgVec<'mcx, JsonPathVariable<'mcx>>,
}

// C DatumGetJsonPathP on JsonTablePath.value: jsonpath_in never toasts, but
// detoast/expand defensively; jsp_init wants the full 4B-header image.
fn jsonpath_image_from_datum<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<&'mcx [u8]> {
    let p = d.as_usize() as *const u8;
    // SAFETY: a Const jsonpath datum is a live by-ref varlena owned by the
    // plan tree, which outlives 'mcx borrowers of this context.
    let image: &'mcx [u8] = unsafe { core::slice::from_raw_parts(p, varatt::varsize_any(p)) };
    if image[0] & 0x01 == 0x01 && image[0] != 0x01 {
        let payload = &image[1..];
        let mut v: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, 4 + payload.len())?;
        mcx::vec_append_bytes(&mut v, &(((4 + payload.len()) as u32) << 2).to_ne_bytes())?;
        mcx::vec_append_bytes(&mut v, payload)?;
        return Ok(&*v.leak());
    }
    match varlena::open_image(mcx, image)? {
        varlena::VarPayload::Inline(_) => Ok(image),
        varlena::VarPayload::Detoasted(v) => Ok(&*v.leak()),
    }
}

// C JsonTableResetRowPattern over one planstate; `doc_payload` is the jsonb
// container payload of the item (C: DatumGetJsonbP(item)).
fn reset_row_pattern<'mcx>(
    mcx: Mcx<'mcx>,
    args: &[JsonPathVariable<'_>],
    st: &mut JsonTablePlanState<'mcx>,
    doc_payload: &[u8],
) -> PgResult<()> {
    let PlanKind::PathScan { path, error_on_error, .. } = st.kind else {
        panic!("JsonTableResetRowPattern on a non-PathScan plan");
    };
    st.found.clear();
    let vars = JsonPathVars::List(args);
    let mut found = JsonValueList::new(mcx)?;
    let res = execute_json_path(
        mcx,
        path,
        &vars,
        doc_payload,
        error_on_error,
        Some(&mut found),
        true,
    )?;
    if res == Jper::Error {
        debug_assert!(!error_on_error);
    } else {
        for v in found.as_slice() {
            st.found.push(jbv_to_jsonb_image(mcx, v)?);
        }
    }
    st.iter_pos = 0;
    st.current = None;
    st.ordinal = 0;
    Ok(())
}

impl<'mcx> JsonTableExecContext<'mcx> {
    /// C `JsonTableInitOpaque` minus the PASSING-argument evaluation: the
    /// executor evaluates `passingvalexprs` and hands the finished list here;
    /// every plan state shares it (C shares the List pointer).
    pub fn init(
        mcx: Mcx<'mcx>,
        rootplan: Node<'mcx>,
        args: PgVec<'mcx, JsonPathVariable<'mcx>>,
        ncols: usize,
    ) -> PgResult<Self> {
        let mut cxt = JsonTableExecContext {
            mcx,
            states: PgVec::new_in(mcx),
            root: 0,
            colplanstates: mcx::vec_from_elem_in(mcx, 0u32, ncols),
            args,
        };
        cxt.root = cxt.init_plan(rootplan, None)?;
        Ok(cxt)
    }

    // C JsonTableInitPlan. Invariant: a state is pushed before its children,
    // so parent index < child index (reset_nested_plan splits on it).
    fn init_plan(&mut self, plan: Node<'mcx>, parent: Option<u32>) -> PgResult<u32> {
        let ix = self.states.len() as u32;
        if let Some(scan) = plan.as_json_table_path_scan() {
            let jtp = scan
                .path
                .and_then(|n| n.as_json_table_path())
                .expect("JsonTablePathScan.path is JsonTablePath");
            let cnst = jtp
                .value
                .and_then(|n| n.as_const())
                .expect("JsonTablePath.value is Const");
            let path = jsonpath_image_from_datum(self.mcx, cnst.constvalue)?;
            self.states.push(JsonTablePlanState {
                kind: PlanKind::PathScan {
                    path,
                    error_on_error: scan.errorOnError,
                    nested: None,
                },
                found: PgVec::new_in(self.mcx),
                iter_pos: 0,
                current: None,
                ordinal: 0,
                parent,
            });
            let mut i = scan.colMin;
            while i >= 0 && i <= scan.colMax {
                self.colplanstates[i as usize] = ix;
                i += 1;
            }
            if let Some(child) = scan.child {
                let n = self.init_plan(child, Some(ix))?;
                let PlanKind::PathScan { nested, .. } = &mut self.states[ix as usize].kind else {
                    unreachable!()
                };
                *nested = Some(n);
            }
            Ok(ix)
        } else if let Some(join) = plan.as_json_table_sibling_join() {
            self.states.push(JsonTablePlanState {
                kind: PlanKind::SiblingJoin { left: 0, right: 0 },
                found: PgVec::new_in(self.mcx),
                iter_pos: 0,
                current: None,
                ordinal: 0,
                parent,
            });
            let l = self.init_plan(join.lplan.expect("JsonTableSiblingJoin.lplan"), parent)?;
            let r = self.init_plan(join.rplan.expect("JsonTableSiblingJoin.rplan"), parent)?;
            let PlanKind::SiblingJoin { left, right } = &mut self.states[ix as usize].kind else {
                unreachable!()
            };
            *left = l;
            *right = r;
            Ok(ix)
        } else {
            panic!("invalid JsonTablePlan {:?}", plan.node_tag());
        }
    }

    /// C `JsonTableSetDocument`: `doc_payload` is the detoasted input
    /// document's jsonb container payload.
    pub fn set_document(&mut self, doc_payload: &[u8]) -> PgResult<()> {
        let root = self.root;
        reset_row_pattern(self.mcx, &self.args, &mut self.states[root as usize], doc_payload)
    }

    /// C `JsonTableFetchRow`.
    pub fn fetch_row(&mut self) -> PgResult<bool> {
        let root = self.root;
        self.plan_next_row(root)
    }

    /// C `JsonTableGetValue`'s data half for column `colnum`: the owning plan
    /// state's current row-pattern value as a full jsonb varlena image (None =
    /// C `current.isnull`) plus its ordinal counter. The image stays live
    /// until the owning plan state's next row-pattern reset.
    pub fn current_row(&self, colnum: usize) -> (Option<&[u8]>, i32) {
        let st = &self.states[self.colplanstates[colnum] as usize];
        (st.current.map(|c| &st.found[c as usize][..]), st.ordinal)
    }

    // C JsonTablePlanNextRow.
    fn plan_next_row(&mut self, ix: u32) -> PgResult<bool> {
        match self.states[ix as usize].kind {
            PlanKind::PathScan { .. } => self.scan_next_row(ix),
            PlanKind::SiblingJoin { left, right } => {
                if self.plan_next_row(left)? {
                    return Ok(true);
                }
                self.plan_next_row(right)
            }
        }
    }

    // C JsonTablePlanScanNextRow: parent row kept while the nested plan
    // yields; a fresh parent row resets the nested plan and fetches its first
    // row ignoring the result (no match => outer-join NULLs).
    fn scan_next_row(&mut self, ix: u32) -> PgResult<bool> {
        let PlanKind::PathScan { nested, .. } = self.states[ix as usize].kind else {
            unreachable!()
        };
        if self.states[ix as usize].current.is_some() {
            if let Some(n) = nested {
                if self.plan_next_row(n)? {
                    return Ok(true);
                }
            }
        }
        {
            let st = &mut self.states[ix as usize];
            if st.iter_pos >= st.found.len() {
                st.current = None;
                return Ok(false);
            }
            st.current = Some(st.iter_pos as u32);
            st.iter_pos += 1;
            st.ordinal += 1;
        }
        if let Some(n) = nested {
            self.reset_nested_plan(n)?;
            let _ = self.plan_next_row(n)?;
        }
        Ok(true)
    }

    // C JsonTableResetNestedPlan.
    fn reset_nested_plan(&mut self, ix: u32) -> PgResult<()> {
        match self.states[ix as usize].kind {
            PlanKind::PathScan { .. } => {
                let parent = self.states[ix as usize]
                    .parent
                    .expect("nested plan has a parent");
                debug_assert!(parent < ix);
                let (head, tail) = self.states.split_at_mut(ix as usize);
                let pst = &head[parent as usize];
                if let Some(c) = pst.current {
                    let doc_payload = &pst.found[c as usize][4..];
                    reset_row_pattern(self.mcx, &self.args, &mut tail[0], doc_payload)?;
                }
            }
            PlanKind::SiblingJoin { left, right } => {
                self.reset_nested_plan(left)?;
                self.reset_nested_plan(right)?;
            }
        }
        Ok(())
    }
}
