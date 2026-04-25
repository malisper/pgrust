\set id random(1, :rows)
SELECT payload FROM scanbench WHERE id = :id;
