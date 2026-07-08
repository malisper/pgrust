-- contrib-ports: prepended CREATE EXTENSION (contrib-e2e runs each suite in its own DB; upstream relies on init.sql).
CREATE EXTENSION pgcrypto;
--
-- PGP encrypt using MD5
--

select pgp_sym_decrypt(
	pgp_sym_encrypt('Secret.', 'key', 's2k-digest-algo=md5'),
	'key', 'expect-s2k-digest-algo=md5');
