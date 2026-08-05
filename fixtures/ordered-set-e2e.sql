-- Ordered-set aggregates (WITHIN GROUP): parse + execution parity.
create table osa_t (ten int, hundred int, thousand int, s text, iv interval);
insert into osa_t
select i % 10, i % 100, i % 1000,
       (array['fred','jim','jack','jill','sheila'])[1 + i % 5],
       ((i % 7) || ' days ' || (i % 5) || ' hours')::interval
from generate_series(1, 1000) i;
insert into osa_t values (0, 1, null, null, null);

-- basic percentile_cont over float8, grouped
select p, percentile_cont(p) within group (order by x::float8)
from generate_series(1,5) x,
     (values (0::float8),(0.1),(0.25),(0.4),(0.5),(0.6),(0.75),(0.9),(1)) v(p)
group by p order by p;

-- errors
select p, percentile_cont(p order by p) within group (order by x)
from generate_series(1,5) x, (values (0::float8)) v(p) group by p;
select p, sum() within group (order by x::float8)
from generate_series(1,5) x, (values (0::float8)) v(p) group by p;
select p, percentile_cont(p,p)
from generate_series(1,5) x, (values (0::float8)) v(p) group by p;
select percentile_cont(0.5) from generate_series(1,5) x;
select percentile_cont(1.5) within group (order by x::float8) from generate_series(1,5) x;
select percentile_cont(-0.5) within group (order by x::float8) from generate_series(1,5) x;

-- scalar percentiles, NULL handling, shared transition state
select percentile_cont(0.5) within group (order by thousand) from osa_t;
select percentile_disc(0.5) within group (order by thousand) from osa_t;
select percentile_cont(0.25) within group (order by thousand),
       percentile_cont(0.75) within group (order by thousand),
       percentile_disc(0.25) within group (order by thousand)
from osa_t;
select percentile_disc(0.5) within group (order by thousand) from osa_t where thousand is null;
select percentile_cont(null::float8) within group (order by thousand) from osa_t;

-- interval
select percentile_cont(0.5) within group (order by iv) from osa_t;
select percentile_cont(0.33) within group (order by iv) from osa_t;
select percentile_cont(array[0.25,0.5,0.75]) within group (order by iv) from osa_t;
select percentile_disc(0.5) within group (order by iv) from osa_t;

-- hypothetical-set family
select rank(3) within group (order by x)
from (values (1),(1),(2),(2),(3),(3),(4)) v(x);
select cume_dist(3) within group (order by x)
from (values (1),(1),(2),(2),(3),(3),(4)) v(x);
select percent_rank(3) within group (order by x)
from (values (1),(1),(2),(2),(3),(3),(4),(5)) v(x);
select dense_rank(3) within group (order by x)
from (values (1),(1),(2),(2),(3),(3),(4)) v(x);
select rank(3, 'jim') within group (order by hundred, s) from osa_t;
select dense_rank(3, 'jim') within group (order by hundred, s) from osa_t;

-- percentile arrays (incl. multidim + nulls)
select percentile_disc(array[0,0.1,0.25,0.5,0.75,0.9,1]) within group (order by thousand)
from osa_t;
select percentile_cont(array[0,0.25,0.5,0.75,1]) within group (order by thousand)
from osa_t;
select percentile_disc(array[[null,1,0.5],[0.75,0.25,null]]) within group (order by thousand)
from osa_t;
select percentile_cont(array[0,1,0.25,0.75,0.5,1,0.3,0.32,0.35,0.38,0.4]) within group (order by x)
from generate_series(1,6) x;
select percentile_cont('{}'::float8[]) within group (order by x::float8)
from generate_series(1,5) x;

-- mode
select ten, mode() within group (order by s) from osa_t group by ten order by ten;
select mode() within group (order by thousand) from osa_t;

-- text percentiles (collation-sensitive sort)
select percentile_disc(array[0.25,0.5,0.75]) within group (order by x)
from unnest('{fred,jim,fred,jack,jill,fred,jill,jim,jim,sheila,jim,sheila}'::text[]) u(x);
-- pg_collation_for (oid 3162) is unported (pre-existing, out of OSA scope);
-- collation propagation is covered by the explicit-collation error case below.
select percentile_disc(1) within group (order by x collate "POSIX")
  from (values ('fred'),('jim')) v(x);

-- GROUP BY variants + FILTER
select ten,
       percentile_disc(0.5) within group (order by thousand) as p50,
       percentile_disc(0.5) within group (order by thousand)
         filter (where hundred = 1) as px
from osa_t group by ten order by ten;
select ten, rank(500) within group (order by thousand) from osa_t
group by ten order by ten;
select hundred % 3 as g, mode() within group (order by s)
from osa_t group by hundred % 3 order by g;

-- direct args using grouped columns
select ten, percentile_disc(ten / 20.0 + 0.25) within group (order by thousand)
from osa_t group by ten order by ten;

-- ordered-set aggs created with CREATE AGGREGATE
create aggregate test_percentile_disc(float8 ORDER BY anyelement) (
    stype = internal,
    sfunc = ordered_set_transition,
    finalfunc = percentile_disc_final,
    finalfunc_extra = true,
    finalfunc_modify = read_write
);
create aggregate test_rank(VARIADIC "any" ORDER BY VARIADIC "any") (
    stype = internal,
    sfunc = ordered_set_transition_multi,
    finalfunc = hypothetical_rank_final,
    finalfunc_extra = true,
    finalfunc_modify = read_write,
    hypothetical
);
select test_rank(3) within group (order by x)
from (values (1),(1),(2),(2),(3),(3),(4)) v(x);
select test_percentile_disc(0.5) within group (order by thousand) from osa_t;

-- ordered-set aggs can't use ungrouped vars in direct args:
select rank(x) within group (order by x) from generate_series(1,5) x;
-- agg in the direct args is a grouping violation, too:
select rank(sum(x)) within group (order by x) from generate_series(1,5) x;

-- hypothetical-set type unification and argument-count failures:
select rank(3) within group (order by x) from (values ('fred'),('jim')) v(x);
select rank(3) within group (order by s, ten::text) from osa_t;
select rank('fred') within group (order by x) from generate_series(1,5) x;
select rank('adam'::text collate "C") within group (order by x collate "POSIX")
  from (values ('fred'),('jim')) v(x);
-- hypothetical-set type unification successes:
select rank('adam'::varchar) within group (order by x) from (values ('fred'),('jim')) v(x);
select rank('3') within group (order by x) from generate_series(1,5) x;

-- divide by zero check
select percent_rank(0) within group (order by x) from generate_series(1,0) x;

-- deparse and multiple features:
create view aggordview1 as
select ten,
       percentile_disc(0.5) within group (order by thousand) as p50,
       percentile_disc(0.5) within group (order by thousand)
         filter (where hundred=1) as px,
       rank(5,'AZZZZ',50) within group (order by hundred, s desc, hundred)
  from osa_t
 group by ten order by ten;
select pg_get_viewdef('aggordview1');
select * from aggordview1 order by ten;
drop view aggordview1;

drop aggregate test_percentile_disc(float8 ORDER BY anyelement);
drop aggregate test_rank(VARIADIC "any" ORDER BY VARIADIC "any");
drop table osa_t;
