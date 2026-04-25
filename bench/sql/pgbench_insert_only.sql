\set id random(100000000, 2000000000)
INSERT INTO scanbench_events (item_id, event_type) VALUES (:id, 'insert-only');
