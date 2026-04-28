Goal:
Fix updatable-view privilege and security_invoker semantics.

Key decisions:
Added explicit relation privilege requirements to RTEs, planned statements, and bound DML statements. View rewrite preserves the outer view check and annotates expanded base relations with check_as_user based on security_invoker. Auto-updatable view DML now carries privilege requirements for every nested view/base layer with composed column maps.

Files touched:
Parser/AST/planner/rewrite/executor privilege plumbing, ALTER VIEW SET reloption handling, and focused database/parser tests.

Tests run:
scripts/cargo_isolated.sh check
Focused parser/database privilege tests
scripts/run_regression.sh --schedule /tmp/updatable_views_privilege.schedule --jobs 1 --results-dir /tmp/diffs/updatable_views_privilege_after_chain_fix

Remaining:
updatable_views still has unrelated failures. Privilege/security-invoker mismatches are no longer present in the targeted section; remaining permission-denied deltas are ON CONFLICT view support gaps.
