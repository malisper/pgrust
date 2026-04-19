with recursive points as (
  select r, c from generate_series(-2, 2, 0.1) a(r)
  cross join generate_series(-2, 1, 0.1) b(c)
  order by r desc, c asc
), iterations as (
  select r,
         c,
         0.0::float as zr,
         0.0::float as zc,
         0 as iteration
  from points
  union all
  select r,
         c,
         zr*zr - zc*zc + c as zr,
         2*zr*zc + r as zc,
         iteration+1 as iteration
  from iterations
  where zr*zr + zc*zc < 4 and iteration < 1000
), final_iteration as (
  select * from iterations where iteration = 1000
), marked_points as (
  select r,
         c,
         case when exists (
                select 1
                from final_iteration i
                where p.r = i.r and p.c = i.c
              )
              then '**'
              else '  '
         end as marker
  from points p
  order by r desc, c asc
), lines as (
  select r, string_agg(marker, '') as r_text
  from marked_points
  group by r
  order by r desc
)
select string_agg(r_text, E'\n') from lines;
