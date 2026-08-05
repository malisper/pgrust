-- Clean transactional training corpus. Statement-class list of record: every
-- statement the OLTP legs execute appears here in canonical form (the expander
-- pgo/gen-corpus.sh only varies the literals and the repetition counts).
-- Schema is pgo/corpus/oltp-schema.sql — deliberately unrelated to any
-- measurement rig's schema, in table names and in column names.
--
-- O1 single-row lookup by unique key, projecting a fixed-width string
SELECT note FROM ledger_acct WHERE acct_id = 1;
-- O2 bounded key-range scan
SELECT note FROM ledger_acct WHERE acct_id BETWEEN 1 AND 100;
-- O3 bounded key-range scan feeding a scalar aggregate
SELECT SUM(owner_id) FROM ledger_acct WHERE acct_id BETWEEN 1 AND 100;
-- O4 bounded key-range scan feeding a sort on a fixed-width string
SELECT note FROM ledger_acct WHERE acct_id BETWEEN 1 AND 100 ORDER BY note;
-- O5 bounded key-range scan feeding a sorted duplicate-elimination
SELECT DISTINCT note FROM ledger_acct WHERE acct_id BETWEEN 1 AND 100 ORDER BY note;
-- O6 secondary-index range scan + count
SELECT count(*) FROM ledger_acct WHERE owner_id BETWEEN 1 AND 100;
-- O7 in-place update of an indexed column (index maintenance path)
UPDATE ledger_acct SET owner_id = owner_id + 1 WHERE acct_id = 1;
-- O8 in-place update of a non-indexed wide column (HOT-update path)
UPDATE ledger_acct SET note = md5(1::text) WHERE acct_id = 1;
-- O9 balance-carrying update of a non-indexed narrow column
UPDATE ledger_acct SET bal = bal + 1 WHERE acct_id = 1;
-- O10 read-back of the updated row inside the same transaction
SELECT bal FROM ledger_acct WHERE acct_id = 1;
-- O11 update of a small, highly contended relation
UPDATE ledger_agent SET agent_bal = agent_bal + 1 WHERE agent_id = 1;
-- O12 update of a very small, maximally contended relation
UPDATE ledger_hub SET hub_bal = hub_bal + 1 WHERE hub_id = 1;
-- O13 append-only insert of a narrow event row
INSERT INTO ledger_event (agent_id, hub_id, acct_id, amt, at) VALUES (1, 1, 1, 1, CURRENT_TIMESTAMP);
-- O14 delete by unique key
DELETE FROM ledger_acct WHERE acct_id = 1;
-- O15 re-insert of a full row
INSERT INTO ledger_acct (acct_id, owner_id, note, tag, bal) VALUES (1, 1, 'a', 'b', 0);
-- O16 whole-relation count (sequential-scan aggregate)
SELECT count(*) FROM ledger_acct;
-- O17 whole-relation top-N on a narrow column
SELECT bal FROM ledger_acct ORDER BY bal LIMIT 25;
-- O18 protocol floor: constant projection, no relation
SELECT 7;
-- O19 multi-row VALUES insert (batch path)
INSERT INTO ledger_batch VALUES (1, 1), (2, 2);
-- O20 explicit transaction control (the statements above run inside these)
BEGIN;
COMMIT;
