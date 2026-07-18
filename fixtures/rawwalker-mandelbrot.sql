WITH RECURSIVE points AS (
  SELECT (x::real / 20.0::real) AS r,
         (y::real / 20.0::real) AS c
  FROM generate_series(-40, 40) AS x
  CROSS JOIN generate_series(-40, 20) AS y
  ORDER BY r DESC, c ASC
), iterations AS (
     SELECT r,
            c,
            0.0::real AS zr,
            0.0::real AS zc,
            0 AS iteration
     FROM points
   UNION ALL
     SELECT r,
            c,
            zr*zr - zc*zc + c AS zr,
            2.0::real*zr*zc + r AS zc,
            iteration+1 AS iteration
     FROM iterations WHERE zr*zr + zc*zc < 4.0::real AND iteration < 100
), final_iteration AS (
  SELECT * FROM iterations WHERE iteration = 100
), marked_points AS (
   SELECT r,
          c,
          (CASE WHEN EXISTS (SELECT 1 FROM final_iteration i WHERE p.r = i.r AND p.c = i.c)
                THEN '**'
                ELSE '  '
           END) AS marker
   FROM points p
   ORDER BY r DESC, c ASC
), lines AS (
   SELECT r, string_agg(marker, '') AS r_text
   FROM marked_points
   GROUP BY r
   ORDER BY r DESC
) SELECT string_agg(r_text, E'\n') FROM lines;
