-- contrib-ports: prepended CREATE EXTENSION (ln runs each suite in its own DB; upstream relies on the shared contrib_regression DB).
CREATE EXTENSION pg_trgm;
SELECT getdatabaseencoding() <> 'UTF8' AS skip_test \gset
\if :skip_test
\quit
\endif

-- Index 50 translations of the word "Mathematics"
CREATE TEMP TABLE mb (s text);
\copy mb from 'data/trgm_utf8.data'
CREATE INDEX ON mb USING gist(s gist_trgm_ops);

-- Train-6 audit riders (blockers B1/B2): make hashed-trigram bytes and
-- threshold-boundary comparisons suite-visible against live C.
-- B1: any trigram containing a multibyte char is compact_trigram-hashed with
-- the LEGACY (reflected-table) CRC32 — show_trgm prints the raw hash bytes.
SELECT show_trgm('Математика');
SELECT show_trgm('数学の テスト');
SELECT similarity('Математика', 'Математик');
SELECT word_similarity('Математик', 'Математика прикладная');
-- B2: float4 similarity must be compared against the double threshold in
-- double (C promotes; a f32-demoted 0.7 is 0.69999999...).
SET pg_trgm.similarity_threshold = 0.5;
SELECT similarity('qwertyuiop', 'qwertyuio') AS s,
       'qwertyuiop' % 'qwertyuio' AS matches;
SET pg_trgm.word_similarity_threshold = 0.75;
SELECT word_similarity('dog', 'the doge') AS ws,
       'dog' <% 'the doge' AS ws_matches;
RESET pg_trgm.similarity_threshold;
RESET pg_trgm.word_similarity_threshold;
