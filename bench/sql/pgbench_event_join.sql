\set start_id random(1, :rows)
\set end_id :start_id + 50
SELECT count(*)
  FROM scanbench s
  JOIN scanbench_events e ON e.item_id = s.id
 WHERE s.id >= :start_id
   AND s.id < :end_id;
