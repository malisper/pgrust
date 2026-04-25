\set start_id random(1, :rows)
SELECT id, payload FROM scanbench WHERE id >= :start_id ORDER BY id LIMIT 20;
