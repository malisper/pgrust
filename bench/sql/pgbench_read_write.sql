\set id random(1, :rows)
BEGIN;
SELECT payload FROM scanbench WHERE id = :id;
UPDATE scanbench SET touched = touched + 1 WHERE id = :id;
INSERT INTO scanbench_events (item_id, event_type) VALUES (:id, 'touch');
COMMIT;
