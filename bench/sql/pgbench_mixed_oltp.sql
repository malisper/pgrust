\set op random(1, 100)
\set id random(1, :rows)
\if :op <= 70
SELECT payload FROM scanbench WHERE id = :id;
\elif :op <= 90
UPDATE scanbench SET touched = touched + 1 WHERE id = :id;
\else
INSERT INTO scanbench_events (item_id, event_type) VALUES (:id, 'mixed');
\endif
