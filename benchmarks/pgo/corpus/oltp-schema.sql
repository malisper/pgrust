-- Fixture for the clean transactional training corpus. Table and column names
-- are deliberately unrelated to any measurement rig's schema. Column TYPES do
-- mirror the transactional shape under test (fixed-width bpchar payloads, a
-- secondary index, a small contended relation, an append-only event table) —
-- types drive which kernels train, and that is the legitimate part of choosing
-- a training workload.
-- No PRIMARY KEY / NOT NULL on non-catalog relations: the relcache path in this
-- engine rejects them (same constraint the existing fixtures observe).
CREATE TABLE ledger_acct (acct_id int4, owner_id int4, note char(120), tag char(60), bal int8);
CREATE UNIQUE INDEX ledger_acct_key ON ledger_acct (acct_id);
CREATE INDEX ledger_acct_owner ON ledger_acct (owner_id);
CREATE TABLE ledger_agent (agent_id int4, hub_id int4, agent_bal int8, memo char(84));
CREATE UNIQUE INDEX ledger_agent_key ON ledger_agent (agent_id);
CREATE TABLE ledger_hub (hub_id int4, hub_bal int8, memo char(88));
CREATE UNIQUE INDEX ledger_hub_key ON ledger_hub (hub_id);
CREATE TABLE ledger_event (agent_id int4, hub_id int4, acct_id int4, amt int4, at timestamp, memo char(22));
CREATE TABLE ledger_batch (k int4, v int8);
CREATE TABLE ledger_bulk (k int4, v int8);
