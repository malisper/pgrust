--
-- Regular expression tests
--

-- Don't want to have to double backslashes in regexes
set standard_conforming_strings = on;

-- Test simple quantified backrefs
-- pgrust:rowsort
select 'bbbbb' ~ '^([bc])\1*$' as t;
-- pgrust:rowsort
select 'ccc' ~ '^([bc])\1*$' as t;
-- pgrust:rowsort
select 'xxx' ~ '^([bc])\1*$' as f;
-- pgrust:rowsort
select 'bbc' ~ '^([bc])\1*$' as f;
-- pgrust:rowsort
select 'b' ~ '^([bc])\1*$' as t;

-- Test quantified backref within a larger expression
-- pgrust:rowsort
select 'abc abc abc' ~ '^(\w+)( \1)+$' as t;
-- pgrust:rowsort
select 'abc abd abc' ~ '^(\w+)( \1)+$' as f;
-- pgrust:rowsort
select 'abc abc abd' ~ '^(\w+)( \1)+$' as f;
-- pgrust:rowsort
select 'abc abc abc' ~ '^(.+)( \1)+$' as t;
-- pgrust:rowsort
select 'abc abd abc' ~ '^(.+)( \1)+$' as f;
-- pgrust:rowsort
select 'abc abc abd' ~ '^(.+)( \1)+$' as f;

-- Test some cases that crashed in 9.2beta1 due to pmatch[] array overrun
-- pgrust:rowsort
select substring('asd TO foo' from ' TO (([a-z0-9._]+|"([^"]+|"")+")+)');
-- pgrust:rowsort
select substring('a' from '((a))+');
-- pgrust:rowsort
select substring('a' from '((a)+)');

-- Test regexp_match()
-- pgrust:rowsort
select regexp_match('abc', '');
-- pgrust:rowsort
select regexp_match('abc', 'bc');
-- pgrust:rowsort
select regexp_match('abc', 'd') is null;
-- pgrust:rowsort
select regexp_match('abc', '(B)(c)', 'i');
select regexp_match('abc', 'Bd', 'ig'); -- error

-- Test lookahead constraints
-- pgrust:rowsort
select regexp_matches('ab', 'a(?=b)b*');
-- pgrust:rowsort
select regexp_matches('a', 'a(?=b)b*');
-- pgrust:rowsort
select regexp_matches('abc', 'a(?=b)b*(?=c)c*');
-- pgrust:rowsort
select regexp_matches('ab', 'a(?=b)b*(?=c)c*');
-- pgrust:rowsort
select regexp_matches('ab', 'a(?!b)b*');
-- pgrust:rowsort
select regexp_matches('a', 'a(?!b)b*');
-- pgrust:rowsort
select regexp_matches('b', '(?=b)b');
-- pgrust:rowsort
select regexp_matches('a', '(?=b)b');

-- Test lookbehind constraints
-- pgrust:rowsort
select regexp_matches('abb', '(?<=a)b*');
-- pgrust:rowsort
select regexp_matches('a', 'a(?<=a)b*');
-- pgrust:rowsort
select regexp_matches('abc', 'a(?<=a)b*(?<=b)c*');
-- pgrust:rowsort
select regexp_matches('ab', 'a(?<=a)b*(?<=b)c*');
-- pgrust:rowsort
select regexp_matches('ab', 'a*(?<!a)b*');
-- pgrust:rowsort
select regexp_matches('ab', 'a*(?<!a)b+');
-- pgrust:rowsort
select regexp_matches('b', 'a*(?<!a)b+');
-- pgrust:rowsort
select regexp_matches('a', 'a(?<!a)b*');
-- pgrust:rowsort
select regexp_matches('b', '(?<=b)b');
-- pgrust:rowsort
select regexp_matches('foobar', '(?<=f)b+');
-- pgrust:rowsort
select regexp_matches('foobar', '(?<=foo)b+');
-- pgrust:rowsort
select regexp_matches('foobar', '(?<=oo)b+');

-- Test optimization of single-chr-or-bracket-expression lookaround constraints
-- pgrust:rowsort
select 'xz' ~ 'x(?=[xy])';
-- pgrust:rowsort
select 'xy' ~ 'x(?=[xy])';
-- pgrust:rowsort
select 'xz' ~ 'x(?![xy])';
-- pgrust:rowsort
select 'xy' ~ 'x(?![xy])';
-- pgrust:rowsort
select 'x'  ~ 'x(?![xy])';
-- pgrust:rowsort
select 'xyy' ~ '(?<=[xy])yy+';
-- pgrust:rowsort
select 'zyy' ~ '(?<=[xy])yy+';
-- pgrust:rowsort
select 'xyy' ~ '(?<![xy])yy+';
-- pgrust:rowsort
select 'zyy' ~ '(?<![xy])yy+';

-- Test conversion of regex patterns to indexable conditions
explain (costs off) select * from pg_proc where proname ~ 'abc';
explain (costs off) select * from pg_proc where proname ~ '^abc';
explain (costs off) select * from pg_proc where proname ~ '^abc$';
explain (costs off) select * from pg_proc where proname ~ '^abcd*e';
explain (costs off) select * from pg_proc where proname ~ '^abc+d';
explain (costs off) select * from pg_proc where proname ~ '^(abc)(def)';
explain (costs off) select * from pg_proc where proname ~ '^(abc)$';
explain (costs off) select * from pg_proc where proname ~ '^(abc)?d';
explain (costs off) select * from pg_proc where proname ~ '^abcd(x|(?=\w\w)q)';

-- Test for infinite loop in pullback() (CVE-2007-4772)
-- pgrust:rowsort
select 'a' ~ '($|^)*';

-- These cases expose a bug in the original fix for CVE-2007-4772
-- pgrust:rowsort
select 'a' ~ '(^)+^';
-- pgrust:rowsort
select 'a' ~ '$($$)+';

-- More cases of infinite loop in pullback(), not fixed by CVE-2007-4772 fix
-- pgrust:rowsort
select 'a' ~ '($^)+';
-- pgrust:rowsort
select 'a' ~ '(^$)*';
-- pgrust:rowsort
select 'aa bb cc' ~ '(^(?!aa))+';
-- pgrust:rowsort
select 'aa x' ~ '(^(?!aa)(?!bb)(?!cc))+';
-- pgrust:rowsort
select 'bb x' ~ '(^(?!aa)(?!bb)(?!cc))+';
-- pgrust:rowsort
select 'cc x' ~ '(^(?!aa)(?!bb)(?!cc))+';
-- pgrust:rowsort
select 'dd x' ~ '(^(?!aa)(?!bb)(?!cc))+';

-- Test for infinite loop in fixempties() (Tcl bugs 3604074, 3606683)
-- pgrust:rowsort
select 'a' ~ '((((((a)*)*)*)*)*)*';
-- pgrust:rowsort
select 'a' ~ '((((((a+|)+|)+|)+|)+|)+|)';

-- These cases used to give too-many-states failures
-- pgrust:rowsort
select 'x' ~ 'abcd(\m)+xyz';
-- pgrust:rowsort
select 'a' ~ '^abcd*(((((^(a c(e?d)a+|)+|)+|)+|)+|a)+|)';
-- pgrust:rowsort
select 'x' ~ 'a^(^)bcd*xy(((((($a+|)+|)+|)+$|)+|)+|)^$';
-- pgrust:rowsort
select 'x' ~ 'xyz(\Y\Y)+';
-- pgrust:rowsort
select 'x' ~ 'x|(?:\M)+';

-- This generates O(N) states but O(N^2) arcs, so it causes problems
-- if arc count is not constrained
select 'x' ~ repeat('x*y*z*', 1000);

-- Test backref in combination with non-greedy quantifier
-- https://core.tcl.tk/tcl/tktview/6585b21ca8fa6f3678d442b97241fdd43dba2ec0
-- pgrust:rowsort
select 'Programmer' ~ '(\w).*?\1' as t;
-- pgrust:rowsort
select regexp_matches('Programmer', '(\w)(.*?\1)', 'g');

-- Test for proper matching of non-greedy iteration (bug #11478)
-- pgrust:rowsort
select regexp_matches('foo/bar/baz',
                      '^([^/]+?)(?:/([^/]+?))(?:/([^/]+?))?$', '');

-- Test that greediness can be overridden by outer quantifier
-- pgrust:rowsort
select regexp_matches('llmmmfff', '^(l*)(.*)(f*)$');
-- pgrust:rowsort
select regexp_matches('llmmmfff', '^(l*){1,1}(.*)(f*)$');
-- pgrust:rowsort
select regexp_matches('llmmmfff', '^(l*){1,1}?(.*)(f*)$');
-- pgrust:rowsort
select regexp_matches('llmmmfff', '^(l*){1,1}?(.*){1,1}?(f*)$');
-- pgrust:rowsort
select regexp_matches('llmmmfff', '^(l*?)(.*)(f*)$');
-- pgrust:rowsort
select regexp_matches('llmmmfff', '^(l*?){1,1}(.*)(f*)$');
-- pgrust:rowsort
select regexp_matches('llmmmfff', '^(l*?){1,1}?(.*)(f*)$');
-- pgrust:rowsort
select regexp_matches('llmmmfff', '^(l*?){1,1}?(.*){1,1}?(f*)$');

-- Test for infinite loop in cfindloop with zero-length possible match
-- but no actual match (can only happen in the presence of backrefs)
-- pgrust:rowsort
select 'a' ~ '$()|^\1';
-- pgrust:rowsort
select 'a' ~ '.. ()|\1';
-- pgrust:rowsort
select 'a' ~ '()*\1';
-- pgrust:rowsort
select 'a' ~ '()+\1';

-- Test incorrect removal of capture groups within {0}
-- pgrust:rowsort
select 'xxx' ~ '(.){0}(\1)' as f;
-- pgrust:rowsort
select 'xxx' ~ '((.)){0}(\2)' as f;
-- pgrust:rowsort
select 'xyz' ~ '((.)){0}(\2){0}' as t;

-- Test ancient oversight in when to apply zaptreesubs
-- pgrust:rowsort
select 'abcdef' ~ '^(.)\1|\1.' as f;
-- pgrust:rowsort
select 'abadef' ~ '^((.)\2|..)\2' as f;

-- Add coverage for some cases in checkmatchall
-- pgrust:rowsort
select regexp_match('xy', '.|...');
-- pgrust:rowsort
select regexp_match('xyz', '.|...');
-- pgrust:rowsort
select regexp_match('xy', '.*');
-- pgrust:rowsort
select regexp_match('fooba', '(?:..)*');
-- pgrust:rowsort
select regexp_match('xyz', repeat('.', 260));
-- pgrust:rowsort
select regexp_match('foo', '(?:.|){99}');

-- Error conditions
select 'xyz' ~ 'x(\w)(?=\1)';  -- no backrefs in LACONs
select 'xyz' ~ 'x(\w)(?=(\1))';
select 'a' ~ '\x7fffffff';  -- invalid chr code
