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
