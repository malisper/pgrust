--Test text search dictionaries and configurations

-- Test ISpell dictionary with ispell affix file
CREATE TEXT SEARCH DICTIONARY ispell (
                        Template=ispell,
                        DictFile=ispell_sample,
                        AffFile=ispell_sample
);

-- pgrust:rowsort
SELECT ts_lexize('ispell', 'skies');
-- pgrust:rowsort
SELECT ts_lexize('ispell', 'bookings');
-- pgrust:rowsort
SELECT ts_lexize('ispell', 'booking');
-- pgrust:rowsort
SELECT ts_lexize('ispell', 'foot');
-- pgrust:rowsort
SELECT ts_lexize('ispell', 'foots');
-- pgrust:rowsort
SELECT ts_lexize('ispell', 'rebookings');
-- pgrust:rowsort
SELECT ts_lexize('ispell', 'rebooking');
-- pgrust:rowsort
SELECT ts_lexize('ispell', 'rebook');
-- pgrust:rowsort
SELECT ts_lexize('ispell', 'unbookings');
-- pgrust:rowsort
SELECT ts_lexize('ispell', 'unbooking');
-- pgrust:rowsort
SELECT ts_lexize('ispell', 'unbook');

-- pgrust:rowsort
SELECT ts_lexize('ispell', 'footklubber');
-- pgrust:rowsort
SELECT ts_lexize('ispell', 'footballklubber');
-- pgrust:rowsort
SELECT ts_lexize('ispell', 'ballyklubber');
-- pgrust:rowsort
SELECT ts_lexize('ispell', 'footballyklubber');

-- Test ISpell dictionary with hunspell affix file
CREATE TEXT SEARCH DICTIONARY hunspell (
                        Template=ispell,
                        DictFile=ispell_sample,
                        AffFile=hunspell_sample
);

-- pgrust:rowsort
SELECT ts_lexize('hunspell', 'skies');
-- pgrust:rowsort
SELECT ts_lexize('hunspell', 'bookings');
-- pgrust:rowsort
SELECT ts_lexize('hunspell', 'booking');
-- pgrust:rowsort
SELECT ts_lexize('hunspell', 'foot');
-- pgrust:rowsort
SELECT ts_lexize('hunspell', 'foots');
-- pgrust:rowsort
SELECT ts_lexize('hunspell', 'rebookings');
-- pgrust:rowsort
SELECT ts_lexize('hunspell', 'rebooking');
-- pgrust:rowsort
SELECT ts_lexize('hunspell', 'rebook');
-- pgrust:rowsort
SELECT ts_lexize('hunspell', 'unbookings');
-- pgrust:rowsort
SELECT ts_lexize('hunspell', 'unbooking');
-- pgrust:rowsort
SELECT ts_lexize('hunspell', 'unbook');

-- pgrust:rowsort
SELECT ts_lexize('hunspell', 'footklubber');
-- pgrust:rowsort
SELECT ts_lexize('hunspell', 'footballklubber');
-- pgrust:rowsort
SELECT ts_lexize('hunspell', 'ballyklubber');
-- pgrust:rowsort
SELECT ts_lexize('hunspell', 'footballyklubber');

-- Test ISpell dictionary with hunspell affix file with FLAG long parameter
CREATE TEXT SEARCH DICTIONARY hunspell_long (
                        Template=ispell,
                        DictFile=hunspell_sample_long,
                        AffFile=hunspell_sample_long
);

-- pgrust:rowsort
SELECT ts_lexize('hunspell_long', 'skies');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_long', 'bookings');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_long', 'booking');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_long', 'foot');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_long', 'foots');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_long', 'rebookings');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_long', 'rebooking');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_long', 'rebook');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_long', 'unbookings');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_long', 'unbooking');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_long', 'unbook');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_long', 'booked');

-- pgrust:rowsort
SELECT ts_lexize('hunspell_long', 'footklubber');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_long', 'footballklubber');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_long', 'ballyklubber');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_long', 'ballsklubber');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_long', 'footballyklubber');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_long', 'ex-machina');

-- Test ISpell dictionary with hunspell affix file with FLAG num parameter
CREATE TEXT SEARCH DICTIONARY hunspell_num (
                        Template=ispell,
                        DictFile=hunspell_sample_num,
                        AffFile=hunspell_sample_num
);

-- pgrust:rowsort
SELECT ts_lexize('hunspell_num', 'skies');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_num', 'sk');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_num', 'bookings');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_num', 'booking');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_num', 'foot');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_num', 'foots');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_num', 'rebookings');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_num', 'rebooking');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_num', 'rebook');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_num', 'unbookings');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_num', 'unbooking');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_num', 'unbook');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_num', 'booked');

-- pgrust:rowsort
SELECT ts_lexize('hunspell_num', 'footklubber');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_num', 'footballklubber');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_num', 'ballyklubber');
-- pgrust:rowsort
SELECT ts_lexize('hunspell_num', 'footballyklubber');

-- Test suitability of affix and dict files
CREATE TEXT SEARCH DICTIONARY hunspell_err (
						Template=ispell,
						DictFile=ispell_sample,
						AffFile=hunspell_sample_long
);

CREATE TEXT SEARCH DICTIONARY hunspell_err (
						Template=ispell,
						DictFile=ispell_sample,
						AffFile=hunspell_sample_num
);

CREATE TEXT SEARCH DICTIONARY hunspell_invalid_1 (
						Template=ispell,
						DictFile=hunspell_sample_long,
						AffFile=ispell_sample
);

CREATE TEXT SEARCH DICTIONARY hunspell_invalid_2 (
						Template=ispell,
						DictFile=hunspell_sample_long,
						AffFile=hunspell_sample_num
);

CREATE TEXT SEARCH DICTIONARY hunspell_invalid_3 (
						Template=ispell,
						DictFile=hunspell_sample_num,
						AffFile=ispell_sample
);

CREATE TEXT SEARCH DICTIONARY hunspell_err (
						Template=ispell,
						DictFile=hunspell_sample_num,
						AffFile=hunspell_sample_long
);

-- Synonym dictionary
CREATE TEXT SEARCH DICTIONARY synonym (
						Template=synonym,
						Synonyms=synonym_sample
);

-- pgrust:rowsort
SELECT ts_lexize('synonym', 'PoStGrEs');
-- pgrust:rowsort
SELECT ts_lexize('synonym', 'Gogle');
-- pgrust:rowsort
SELECT ts_lexize('synonym', 'indices');

-- test altering boolean parameters
SELECT dictinitoption FROM pg_ts_dict WHERE dictname = 'synonym';

ALTER TEXT SEARCH DICTIONARY synonym (CaseSensitive = 1);
-- pgrust:rowsort
SELECT ts_lexize('synonym', 'PoStGrEs');
SELECT dictinitoption FROM pg_ts_dict WHERE dictname = 'synonym';

ALTER TEXT SEARCH DICTIONARY synonym (CaseSensitive = 2);  -- fail

ALTER TEXT SEARCH DICTIONARY synonym (CaseSensitive = off);
-- pgrust:rowsort
SELECT ts_lexize('synonym', 'PoStGrEs');
SELECT dictinitoption FROM pg_ts_dict WHERE dictname = 'synonym';

-- Create and simple test thesaurus dictionary
-- More tests in configuration checks because ts_lexize()
-- cannot pass more than one word to thesaurus.
CREATE TEXT SEARCH DICTIONARY thesaurus (
                        Template=thesaurus,
						DictFile=thesaurus_sample,
						Dictionary=english_stem
);

-- pgrust:rowsort
SELECT ts_lexize('thesaurus', 'one');

-- Test ispell dictionary in configuration
CREATE TEXT SEARCH CONFIGURATION ispell_tst (
						COPY=english
);

ALTER TEXT SEARCH CONFIGURATION ispell_tst ALTER MAPPING FOR
	word, numword, asciiword, hword, numhword, asciihword, hword_part, hword_numpart, hword_asciipart
	WITH ispell, english_stem;

-- pgrust:rowsort
SELECT to_tsvector('ispell_tst', 'Booking the skies after rebookings for footballklubber from a foot');
-- pgrust:rowsort
SELECT to_tsquery('ispell_tst', 'footballklubber');
-- pgrust:rowsort
SELECT to_tsquery('ispell_tst', 'footballyklubber:b & rebookings:A & sky');

-- Test ispell dictionary with hunspell affix in configuration
CREATE TEXT SEARCH CONFIGURATION hunspell_tst (
						COPY=ispell_tst
);

ALTER TEXT SEARCH CONFIGURATION hunspell_tst ALTER MAPPING
	REPLACE ispell WITH hunspell;

-- pgrust:rowsort
SELECT to_tsvector('hunspell_tst', 'Booking the skies after rebookings for footballklubber from a foot');
-- pgrust:rowsort
SELECT to_tsquery('hunspell_tst', 'footballklubber');
-- pgrust:rowsort
SELECT to_tsquery('hunspell_tst', 'footballyklubber:b & rebookings:A & sky');

-- pgrust:rowsort
SELECT to_tsquery('hunspell_tst', 'footballyklubber:b <-> sky');
-- pgrust:rowsort
SELECT phraseto_tsquery('hunspell_tst', 'footballyklubber sky');

-- Test ispell dictionary with hunspell affix with FLAG long in configuration
ALTER TEXT SEARCH CONFIGURATION hunspell_tst ALTER MAPPING
	REPLACE hunspell WITH hunspell_long;

-- pgrust:rowsort
SELECT to_tsvector('hunspell_tst', 'Booking the skies after rebookings for footballklubber from a foot');
-- pgrust:rowsort
SELECT to_tsquery('hunspell_tst', 'footballklubber');
-- pgrust:rowsort
SELECT to_tsquery('hunspell_tst', 'footballyklubber:b & rebookings:A & sky');

-- Test ispell dictionary with hunspell affix with FLAG num in configuration
ALTER TEXT SEARCH CONFIGURATION hunspell_tst ALTER MAPPING
	REPLACE hunspell_long WITH hunspell_num;

-- pgrust:rowsort
SELECT to_tsvector('hunspell_tst', 'Booking the skies after rebookings for footballklubber from a foot');
-- pgrust:rowsort
SELECT to_tsquery('hunspell_tst', 'footballklubber');
-- pgrust:rowsort
SELECT to_tsquery('hunspell_tst', 'footballyklubber:b & rebookings:A & sky');

-- Test synonym dictionary in configuration
CREATE TEXT SEARCH CONFIGURATION synonym_tst (
						COPY=english
);

ALTER TEXT SEARCH CONFIGURATION synonym_tst ALTER MAPPING FOR
	asciiword, hword_asciipart, asciihword
	WITH synonym, english_stem;

-- pgrust:rowsort
SELECT to_tsvector('synonym_tst', 'Postgresql is often called as postgres or pgsql and pronounced as postgre');
-- pgrust:rowsort
SELECT to_tsvector('synonym_tst', 'Most common mistake is to write Gogle instead of Google');
-- pgrust:rowsort
SELECT to_tsvector('synonym_tst', 'Indexes or indices - Which is right plural form of index?');
-- pgrust:rowsort
SELECT to_tsquery('synonym_tst', 'Index & indices');

-- test thesaurus in configuration
-- see thesaurus_sample.ths to understand 'odd' resulting tsvector
CREATE TEXT SEARCH CONFIGURATION thesaurus_tst (
						COPY=synonym_tst
);

ALTER TEXT SEARCH CONFIGURATION thesaurus_tst ALTER MAPPING FOR
	asciiword, hword_asciipart, asciihword
	WITH synonym, thesaurus, english_stem;

-- pgrust:rowsort
SELECT to_tsvector('thesaurus_tst', 'one postgres one two one two three one');
-- pgrust:rowsort
SELECT to_tsvector('thesaurus_tst', 'Supernovae star is very new star and usually called supernovae (abbreviation SN)');
-- pgrust:rowsort
SELECT to_tsvector('thesaurus_tst', 'Booking tickets is looking like a booking a tickets');

-- invalid: non-lowercase quoted identifiers
CREATE TEXT SEARCH DICTIONARY tsdict_case
(
	Template = ispell,
	"DictFile" = ispell_sample,
	"AffFile" = ispell_sample
);

-- Test grammar for configurations
CREATE TEXT SEARCH CONFIGURATION dummy_tst (COPY=english);
-- Overridden mapping change with duplicated tokens.
ALTER TEXT SEARCH CONFIGURATION dummy_tst
  ALTER MAPPING FOR word, word WITH ispell;
-- Not a token supported by the configuration's parser, fails.
ALTER TEXT SEARCH CONFIGURATION dummy_tst
  DROP MAPPING FOR not_a_token, not_a_token;
-- Not a token supported by the configuration's parser, fails even
-- with IF EXISTS.
ALTER TEXT SEARCH CONFIGURATION dummy_tst
  DROP MAPPING IF EXISTS FOR not_a_token, not_a_token;
-- Token supported by the configuration's parser, succeeds.
ALTER TEXT SEARCH CONFIGURATION dummy_tst
  DROP MAPPING FOR word, word;
-- No mapping for token supported by the configuration's parser, fails.
ALTER TEXT SEARCH CONFIGURATION dummy_tst
  DROP MAPPING FOR word;
-- Token supported by the configuration's parser, cannot be found,
-- succeeds with IF EXISTS.
ALTER TEXT SEARCH CONFIGURATION dummy_tst
  DROP MAPPING IF EXISTS FOR word, word;
-- Re-add mapping, with duplicated tokens supported by the parser.
ALTER TEXT SEARCH CONFIGURATION dummy_tst
  ADD MAPPING FOR word, word WITH ispell;
-- Not a token supported by the configuration's parser, fails.
ALTER TEXT SEARCH CONFIGURATION dummy_tst
  ADD MAPPING FOR not_a_token WITH ispell;
DROP TEXT SEARCH CONFIGURATION dummy_tst;
