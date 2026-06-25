# Audit: backend-rewrite-rowsecurity (rewrite/rowsecurity.c)

Independent re-derivation from C (`postgres-18.3/src/backend/rewrite/rowsecurity.c`),
c2rust (`c2rust-runs/backend-rewrite-core/src/rowsecurity.rs`), and the port
(`crates/backend-rewrite-rowsecurity/src/lib.rs`).

## Function inventory (7 functions + 2 hook globals)

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `get_row_security_policies` | 97-530 | `get_row_security_policies` | MATCH | RLS_NONE/RLS_NONE_ENV early-outs; commandType = (rt_index==resultRelation? query : SELECT); the SELECT-FOR-UPDATE pre-pass, SELECT/UPDATE/DELETE add_security_quals, UPD/DEL/MERGE-needs-SELECT pre-pass, INSERT/UPDATE WCO + RETURNING-SELECT WCO + ON CONFLICT DO UPDATE (CONFLICT_CHECK USING, UPDATE_CHECK WITH CHECK, SELECT WCOs), and the full MERGE block (MERGE_UPDATE_CHECK, UPDATE_CHECK, SELECT UPDATE_CHECK, MERGE_DELETE_CHECK, INSERT_CHECK, RETURNING INSERT_CHECK) all reproduced in order. check_enable_rls called with raw checkAsUser; setRuleCheckAsUser(checkAsUser) over each securityQual+WCO; hasRowSecurity=true at end. Query-level fields (resultRelation/commandType/returningList!=NIL/onConflict.action) passed explicitly (caller reads them off the Query). |
| `get_policies_for_relation` | 540-654 | `get_policies_for_relation` | MATCH | polcmd '*' always matches; per-cmd char match (r/a/w/d); MERGE => no match (derives from others); default => elog(ERROR "unrecognized policy command type %d"). Role gate check_role_for_policy. permissive/restrictive partition. sort_policies_by_name on restrictive. Extension hooks (619-653) are always-NULL global fn ptrs in this single-process port — unreachable, noted+elided (design-justified). |
| `sort_policies_by_name` | 664-668 | `sort_policies_by_name` | MATCH | list_sort by row_security_policy_cmp; operates on the index list, resolving to descriptors. |
| `row_security_policy_cmp` | 673-686 | `row_security_policy_cmp` | MATCH | strcmp over names. NULL-name guard unreachable (built-in policies always have a name; only ext hooks omit it, which this port never produces) — byte cmp reproduces strcmp order. |
| `add_security_quals` | 699-778 | `add_security_quals` | MATCH | permissive quals: copyObject (clone_in) + plain push (lappend), no ChangeVarNodes. If non-empty: restrictive copyObject+ChangeVarNodes(1,rt_index,0)+list_append_unique; OR-combine permissive (single => linitial) then ChangeVarNodes the combined expr + list_append_unique. Else: single makeConst(BOOLOID,-1,InvalidOid,1,false,false,true)=make_bool_const(false,false) via plain push. hassublinks ORed at each qual. |
| `add_with_check_options` | 795-909 | `add_with_check_options` | MATCH | QUAL_FOR_WCO macro = with_check_qual unless force_using or NULL, else qual. permissive: copyObject+OR-combine+ChangeVarNodes wco->qual, relname=pstrdup, polname=NULL, list_append_unique. restrictive: copyObject+ChangeVarNodes per policy, polname=policy_name, list_append_unique. Else: always-false WCO via plain push (lappend). hassublinks ORed. |
| `check_role_for_policy` | 915-932 | `check_role_for_policy` | MATCH | roles[0]==ACL_ID_PUBLIC(0) => true; else has_privs_of_role(user_id, roles[i]) over all. C reads decoded Oid[]; the relcache build already decoded ArrayType->roles to Oid[] (DatumGetArrayTypePCopy). Empty-roles handled safely (pg_policy always has >=1 role). |
| `row_security_policy_hook_permissive/_restrictive` | 86-87 | n/a | MATCH | NULL globals; always-NULL in single-process port. Their call sites in get_policies_for_relation are unreachable and elided with a note. |

## Constants verified against headers
- ACL_ID_PUBLIC=0, ACL_SELECT_CHR='r', ACL_INSERT_CHR='a', ACL_UPDATE_CHR='w', ACL_DELETE_CHR='d' (utils/acl.h) — via types_acl::acl.
- POLICY_CMD_ALL='*' (pg_policy.h).
- RELKIND_RELATION='r', RELKIND_PARTITIONED_TABLE='p' (pg_class.h).
- ACL_SELECT=1<<1, ACL_UPDATE=1<<2 (utils/acl.h) — via types_acl::acl.
- make_bool_const => BOOLOID=16, constlen=1, constbyval=true (makefuncs.c makeBoolConst).
- WCOKind variants (parsenodes.h) — via types_nodes::rawnodes.
- RLS_NONE/RLS_NONE_ENV/RLS_ENABLED via CheckEnableRlsResult (types_acl).

## Seam audit
- The crate OWNS no `-seams` crate (it is a fresh owner with no cyclic callers: rewritehandler deps it directly; cargo accepts the edge). No init_seams() required.
- OUTWARD calls (all justified cycle-free direct deps, thin):
  - `relation_row_security::call` (backend-utils-cache-relcache-seams) — the per-query rd_rsdesc->policies projection (the trimmed types_rel::Relation carries no rd_rsdesc; relcache owns the entry store). Reader seam mirrors the existing relation_rules precedent; impl + install added to the relcache owner. Thin marshal: the projection (Node::clone_in per qual) lives in the relcache owner, not the seam.
  - check_enable_rls (backend-utils-misc-more), getRTEPermissionInfo (backend-parser-relation), setRuleCheckAsUser (backend-rewrite-rewriteDefine), has_privs_of_role (backend-utils-adt-acl), GetUserId (backend-utils-init-miscinit), table_open (backend-access-table-table), ChangeVarNodes (backend-rewrite-core), make_bool_expr/make_bool_const (backend-nodes-core), equal_node (backend-nodes-equalfuncs) — all direct deps, no seam, no cycle.

## Design conformance
- Allocating fns return PgResult and take Mcx — yes (get_row_security_policies, add_*, node_to_expr_clone, append_unique_node).
- No shared statics; no ambient-global seams; no locks held across `?` (table_open handle closed on every error path before `?`-return in the sublink legs; the success path closes before the final returns).
- The always-NULL extension hooks are elided per the single-process model (no LOADable C modules); noted at call sites — not silent.

## Verdict: PASS

Every function MATCH; zero seam findings; design-conformant. The downstream
planner-side consumption of securityQuals (planner.c:1050 / prepsecurity.c) and
ModifyTable WITH CHECK OPTION (grouping_planner) are SEPARATE units, not part of
rowsecurity.c — the rewriter half is complete and verified to inject the correct
quals/WCOs (non-owner sees policy-filtered rows once the planner leg lands;
row_security=off raises the exact C error today).
