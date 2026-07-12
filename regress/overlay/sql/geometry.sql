--
-- GEOMETRY
--

-- Back off displayed precision a little bit to reduce platform-to-platform
-- variation in results.
SET extra_float_digits TO -3;

--
-- Points
--

-- pgrust:rowsort
SELECT center(f1) AS center
   FROM BOX_TBL;

-- pgrust:rowsort
SELECT (@@ f1) AS center
   FROM BOX_TBL;

-- pgrust:rowsort
SELECT point(f1) AS center
   FROM CIRCLE_TBL;

-- pgrust:rowsort
SELECT (@@ f1) AS center
   FROM CIRCLE_TBL;

-- pgrust:rowsort
SELECT (@@ f1) AS center
   FROM POLYGON_TBL
   WHERE (# f1) > 2;

-- "is horizontal" function
-- pgrust:rowsort
SELECT p1.f1
   FROM POINT_TBL p1
   WHERE ishorizontal(p1.f1, point '(0,0)');

-- "is horizontal" operator
-- pgrust:rowsort
SELECT p1.f1
   FROM POINT_TBL p1
   WHERE p1.f1 ?- point '(0,0)';

-- "is vertical" function
-- pgrust:rowsort
SELECT p1.f1
   FROM POINT_TBL p1
   WHERE isvertical(p1.f1, point '(5.1,34.5)');

-- "is vertical" operator
-- pgrust:rowsort
SELECT p1.f1
   FROM POINT_TBL p1
   WHERE p1.f1 ?| point '(5.1,34.5)';

-- Slope
-- pgrust:rowsort
SELECT p1.f1, p2.f1, slope(p1.f1, p2.f1) FROM POINT_TBL p1, POINT_TBL p2;

-- Add point
-- pgrust:rowsort
SELECT p1.f1, p2.f1, p1.f1 + p2.f1 FROM POINT_TBL p1, POINT_TBL p2;

-- Subtract point
-- pgrust:rowsort
SELECT p1.f1, p2.f1, p1.f1 - p2.f1 FROM POINT_TBL p1, POINT_TBL p2;

-- Multiply with point
-- pgrust:rowsort
SELECT p1.f1, p2.f1, p1.f1 * p2.f1 FROM POINT_TBL p1, POINT_TBL p2 WHERE p1.f1[0] BETWEEN 1 AND 1000;

-- Underflow error
SELECT p1.f1, p2.f1, p1.f1 * p2.f1 FROM POINT_TBL p1, POINT_TBL p2 WHERE p1.f1[0] < 1;

-- Divide by point
-- pgrust:rowsort
SELECT p1.f1, p2.f1, p1.f1 / p2.f1 FROM POINT_TBL p1, POINT_TBL p2 WHERE p2.f1[0] BETWEEN 1 AND 1000;

-- Overflow error
SELECT p1.f1, p2.f1, p1.f1 / p2.f1 FROM POINT_TBL p1, POINT_TBL p2 WHERE p2.f1[0] > 1000;

-- Division by 0 error
SELECT p1.f1, p2.f1, p1.f1 / p2.f1 FROM POINT_TBL p1, POINT_TBL p2 WHERE p2.f1 ~= '(0,0)'::point;

-- Distance to line
-- pgrust:rowsort
SELECT p.f1, l.s, p.f1 <-> l.s AS dist_pl, l.s <-> p.f1 AS dist_lp FROM POINT_TBL p, LINE_TBL l;

-- Distance to line segment
-- pgrust:rowsort
SELECT p.f1, l.s, p.f1 <-> l.s AS dist_ps, l.s <-> p.f1 AS dist_sp FROM POINT_TBL p, LSEG_TBL l;

-- Distance to box
-- pgrust:rowsort
SELECT p.f1, b.f1, p.f1 <-> b.f1 AS dist_pb, b.f1 <-> p.f1 AS dist_bp FROM POINT_TBL p, BOX_TBL b;

-- Distance to path
-- pgrust:rowsort
SELECT p.f1, p1.f1, p.f1 <-> p1.f1 AS dist_ppath, p1.f1 <-> p.f1 AS dist_pathp FROM POINT_TBL p, PATH_TBL p1;

-- Distance to polygon
-- pgrust:rowsort
SELECT p.f1, p1.f1, p.f1 <-> p1.f1 AS dist_ppoly, p1.f1 <-> p.f1 AS dist_polyp FROM POINT_TBL p, POLYGON_TBL p1;

-- Construct line through two points
-- pgrust:rowsort
SELECT p1.f1, p2.f1, line(p1.f1, p2.f1)
  FROM POINT_TBL p1, POINT_TBL p2 WHERE p1.f1 <> p2.f1;

-- Closest point to line
-- pgrust:rowsort
SELECT p.f1, l.s, p.f1 ## l.s FROM POINT_TBL p, LINE_TBL l;

-- Closest point to line segment
-- pgrust:rowsort
SELECT p.f1, l.s, p.f1 ## l.s FROM POINT_TBL p, LSEG_TBL l;

-- Closest point to box
-- pgrust:rowsort
SELECT p.f1, b.f1, p.f1 ## b.f1 FROM POINT_TBL p, BOX_TBL b;

-- On line
-- pgrust:rowsort
SELECT p.f1, l.s FROM POINT_TBL p, LINE_TBL l WHERE p.f1 <@ l.s;

-- On line segment
-- pgrust:rowsort
SELECT p.f1, l.s FROM POINT_TBL p, LSEG_TBL l WHERE p.f1 <@ l.s;

-- On path
-- pgrust:rowsort
SELECT p.f1, p1.f1 FROM POINT_TBL p, PATH_TBL p1 WHERE p.f1 <@ p1.f1;

--
-- Lines
--

-- Vertical
-- pgrust:rowsort
SELECT s FROM LINE_TBL WHERE ?| s;

-- Horizontal
-- pgrust:rowsort
SELECT s FROM LINE_TBL WHERE ?- s;

-- Same as line
-- pgrust:rowsort
SELECT l1.s, l2.s FROM LINE_TBL l1, LINE_TBL l2 WHERE l1.s = l2.s;

-- Parallel to line
-- pgrust:rowsort
SELECT l1.s, l2.s FROM LINE_TBL l1, LINE_TBL l2 WHERE l1.s ?|| l2.s;

-- Perpendicular to line
-- pgrust:rowsort
SELECT l1.s, l2.s FROM LINE_TBL l1, LINE_TBL l2 WHERE l1.s ?-| l2.s;

-- Distance to line
-- pgrust:rowsort
SELECT l1.s, l2.s, l1.s <-> l2.s FROM LINE_TBL l1, LINE_TBL l2;

-- Intersect with line
-- pgrust:rowsort
SELECT l1.s, l2.s FROM LINE_TBL l1, LINE_TBL l2 WHERE l1.s ?# l2.s;

-- Intersect with box
-- pgrust:rowsort
SELECT l.s, b.f1 FROM LINE_TBL l, BOX_TBL b WHERE l.s ?# b.f1;

-- Intersection point with line
-- pgrust:rowsort
SELECT l1.s, l2.s, l1.s # l2.s FROM LINE_TBL l1, LINE_TBL l2;

-- Closest point to line segment
-- pgrust:rowsort
SELECT l.s, l1.s, l.s ## l1.s FROM LINE_TBL l, LSEG_TBL l1;

--
-- Line segments
--

-- intersection
SELECT p.f1, l.s, l.s # p.f1 AS intersection
   FROM LSEG_TBL l, POINT_TBL p;

-- Length
-- pgrust:rowsort
SELECT s, @-@ s FROM LSEG_TBL;

-- Vertical
-- pgrust:rowsort
SELECT s FROM LSEG_TBL WHERE ?| s;

-- Horizontal
-- pgrust:rowsort
SELECT s FROM LSEG_TBL WHERE ?- s;

-- Center
-- pgrust:rowsort
SELECT s, @@ s FROM LSEG_TBL;

-- To point
-- pgrust:rowsort
SELECT s, s::point FROM LSEG_TBL;

-- Has points less than line segment
-- pgrust:rowsort
SELECT l1.s, l2.s FROM LSEG_TBL l1, LSEG_TBL l2 WHERE l1.s < l2.s;

-- Has points less than or equal to line segment
-- pgrust:rowsort
SELECT l1.s, l2.s FROM LSEG_TBL l1, LSEG_TBL l2 WHERE l1.s <= l2.s;

-- Has points equal to line segment
-- pgrust:rowsort
SELECT l1.s, l2.s FROM LSEG_TBL l1, LSEG_TBL l2 WHERE l1.s = l2.s;

-- Has points greater than or equal to line segment
-- pgrust:rowsort
SELECT l1.s, l2.s FROM LSEG_TBL l1, LSEG_TBL l2 WHERE l1.s >= l2.s;

-- Has points greater than line segment
-- pgrust:rowsort
SELECT l1.s, l2.s FROM LSEG_TBL l1, LSEG_TBL l2 WHERE l1.s > l2.s;

-- Has points not equal to line segment
-- pgrust:rowsort
SELECT l1.s, l2.s FROM LSEG_TBL l1, LSEG_TBL l2 WHERE l1.s != l2.s;

-- Parallel with line segment
-- pgrust:rowsort
SELECT l1.s, l2.s FROM LSEG_TBL l1, LSEG_TBL l2 WHERE l1.s ?|| l2.s;

-- Perpendicular with line segment
-- pgrust:rowsort
SELECT l1.s, l2.s FROM LSEG_TBL l1, LSEG_TBL l2 WHERE l1.s ?-| l2.s;

-- Distance to line
-- pgrust:rowsort
SELECT l.s, l1.s, l.s <-> l1.s AS dist_sl, l1.s <-> l.s AS dist_ls FROM LSEG_TBL l, LINE_TBL l1;

-- Distance to line segment
-- pgrust:rowsort
SELECT l1.s, l2.s, l1.s <-> l2.s FROM LSEG_TBL l1, LSEG_TBL l2;

-- Distance to box
-- pgrust:rowsort
SELECT l.s, b.f1, l.s <-> b.f1 AS dist_sb, b.f1 <-> l.s AS dist_bs FROM LSEG_TBL l, BOX_TBL b;

-- Intersect with line segment
-- pgrust:rowsort
SELECT l.s, l1.s FROM LSEG_TBL l, LINE_TBL l1 WHERE l.s ?# l1.s;

-- Intersect with box
-- pgrust:rowsort
SELECT l.s, b.f1 FROM LSEG_TBL l, BOX_TBL b WHERE l.s ?# b.f1;

-- Intersection point with line segment
-- pgrust:rowsort
SELECT l1.s, l2.s, l1.s # l2.s FROM LSEG_TBL l1, LSEG_TBL l2;

-- Closest point to line segment
-- pgrust:rowsort
SELECT l1.s, l2.s, l1.s ## l2.s FROM LSEG_TBL l1, LSEG_TBL l2;

-- Closest point to box
-- pgrust:rowsort
SELECT l.s, b.f1, l.s ## b.f1 FROM LSEG_TBL l, BOX_TBL b;

-- On line
-- pgrust:rowsort
SELECT l.s, l1.s FROM LSEG_TBL l, LINE_TBL l1 WHERE l.s <@ l1.s;

-- On box
-- pgrust:rowsort
SELECT l.s, b.f1 FROM LSEG_TBL l, BOX_TBL b WHERE l.s <@ b.f1;

--
-- Boxes
--

-- pgrust:rowsort
SELECT box(f1) AS box FROM CIRCLE_TBL;

-- translation
-- pgrust:rowsort
SELECT b.f1 + p.f1 AS translation
   FROM BOX_TBL b, POINT_TBL p;

-- pgrust:rowsort
SELECT b.f1 - p.f1 AS translation
   FROM BOX_TBL b, POINT_TBL p;

-- Multiply with point
-- pgrust:rowsort
SELECT b.f1, p.f1, b.f1 * p.f1 FROM BOX_TBL b, POINT_TBL p WHERE p.f1[0] BETWEEN 1 AND 1000;

-- Overflow error
-- pgrust:rowsort
SELECT b.f1, p.f1, b.f1 * p.f1 FROM BOX_TBL b, POINT_TBL p WHERE p.f1[0] > 1000;

-- Divide by point
-- pgrust:rowsort
SELECT b.f1, p.f1, b.f1 / p.f1 FROM BOX_TBL b, POINT_TBL p WHERE p.f1[0] BETWEEN 1 AND 1000;

-- To box
-- pgrust:rowsort
SELECT f1::box
	FROM POINT_TBL;

-- pgrust:rowsort
SELECT bound_box(a.f1, b.f1)
	FROM BOX_TBL a, BOX_TBL b;

-- Below box
-- pgrust:rowsort
SELECT b1.f1, b2.f1, b1.f1 <^ b2.f1 FROM BOX_TBL b1, BOX_TBL b2;

-- Above box
-- pgrust:rowsort
SELECT b1.f1, b2.f1, b1.f1 >^ b2.f1 FROM BOX_TBL b1, BOX_TBL b2;

-- Intersection point with box
-- pgrust:rowsort
SELECT b1.f1, b2.f1, b1.f1 # b2.f1 FROM BOX_TBL b1, BOX_TBL b2;

-- Diagonal
-- pgrust:rowsort
SELECT f1, diagonal(f1) FROM BOX_TBL;

-- Distance to box
-- pgrust:rowsort
SELECT b1.f1, b2.f1, b1.f1 <-> b2.f1 FROM BOX_TBL b1, BOX_TBL b2;

--
-- Paths
--

-- Points
-- pgrust:rowsort
SELECT f1, npoints(f1) FROM PATH_TBL;

-- Area
-- pgrust:rowsort
SELECT f1, area(f1) FROM PATH_TBL;

-- Length
-- pgrust:rowsort
SELECT f1, @-@ f1 FROM PATH_TBL;

-- To polygon
-- pgrust:rowsort
SELECT f1, f1::polygon FROM PATH_TBL WHERE isclosed(f1);

-- Open path cannot be converted to polygon error
SELECT f1, f1::polygon FROM PATH_TBL WHERE isopen(f1);

-- Has points less than path
-- pgrust:rowsort
SELECT p1.f1, p2.f1 FROM PATH_TBL p1, PATH_TBL p2 WHERE p1.f1 < p2.f1;

-- Has points less than or equal to path
-- pgrust:rowsort
SELECT p1.f1, p2.f1 FROM PATH_TBL p1, PATH_TBL p2 WHERE p1.f1 <= p2.f1;

-- Has points equal to path
-- pgrust:rowsort
SELECT p1.f1, p2.f1 FROM PATH_TBL p1, PATH_TBL p2 WHERE p1.f1 = p2.f1;

-- Has points greater than or equal to path
-- pgrust:rowsort
SELECT p1.f1, p2.f1 FROM PATH_TBL p1, PATH_TBL p2 WHERE p1.f1 >= p2.f1;

-- Has points greater than path
-- pgrust:rowsort
SELECT p1.f1, p2.f1 FROM PATH_TBL p1, PATH_TBL p2 WHERE p1.f1 > p2.f1;

-- Add path
-- pgrust:rowsort
SELECT p1.f1, p2.f1, p1.f1 + p2.f1 FROM PATH_TBL p1, PATH_TBL p2;

-- Add point
-- pgrust:rowsort
SELECT p.f1, p1.f1, p.f1 + p1.f1 FROM PATH_TBL p, POINT_TBL p1;

-- Subtract point
-- pgrust:rowsort
SELECT p.f1, p1.f1, p.f1 - p1.f1 FROM PATH_TBL p, POINT_TBL p1;

-- Multiply with point
-- pgrust:rowsort
SELECT p.f1, p1.f1, p.f1 * p1.f1 FROM PATH_TBL p, POINT_TBL p1;

-- Divide by point
-- pgrust:rowsort
SELECT p.f1, p1.f1, p.f1 / p1.f1 FROM PATH_TBL p, POINT_TBL p1 WHERE p1.f1[0] BETWEEN 1 AND 1000;

-- Division by 0 error
SELECT p.f1, p1.f1, p.f1 / p1.f1 FROM PATH_TBL p, POINT_TBL p1 WHERE p1.f1 ~= '(0,0)'::point;

-- Distance to path
-- pgrust:rowsort
SELECT p1.f1, p2.f1, p1.f1 <-> p2.f1 FROM PATH_TBL p1, PATH_TBL p2;

--
-- Polygons
--

-- containment
-- pgrust:rowsort
SELECT p.f1, poly.f1, poly.f1 @> p.f1 AS contains
   FROM POLYGON_TBL poly, POINT_TBL p;

-- pgrust:rowsort
SELECT p.f1, poly.f1, p.f1 <@ poly.f1 AS contained
   FROM POLYGON_TBL poly, POINT_TBL p;

-- pgrust:rowsort
SELECT npoints(f1) AS npoints, f1 AS polygon
   FROM POLYGON_TBL;

-- pgrust:rowsort
SELECT polygon(f1)
   FROM BOX_TBL;

-- pgrust:rowsort
SELECT polygon(f1)
   FROM PATH_TBL WHERE isclosed(f1);

-- pgrust:rowsort
SELECT f1 AS open_path, polygon( pclose(f1)) AS polygon
   FROM PATH_TBL
   WHERE isopen(f1);

-- To box
-- pgrust:rowsort
SELECT f1, f1::box FROM POLYGON_TBL;

-- To path
-- pgrust:rowsort
SELECT f1, f1::path FROM POLYGON_TBL;

-- Same as polygon
-- pgrust:rowsort
SELECT p1.f1, p2.f1 FROM POLYGON_TBL p1, POLYGON_TBL p2 WHERE p1.f1 ~= p2.f1;

-- Contained by polygon
-- pgrust:rowsort
SELECT p1.f1, p2.f1 FROM POLYGON_TBL p1, POLYGON_TBL p2 WHERE p1.f1 <@ p2.f1;

-- Contains polygon
-- pgrust:rowsort
SELECT p1.f1, p2.f1 FROM POLYGON_TBL p1, POLYGON_TBL p2 WHERE p1.f1 @> p2.f1;

-- Overlap with polygon
-- pgrust:rowsort
SELECT p1.f1, p2.f1 FROM POLYGON_TBL p1, POLYGON_TBL p2 WHERE p1.f1 && p2.f1;

-- Left of polygon
-- pgrust:rowsort
SELECT p1.f1, p2.f1 FROM POLYGON_TBL p1, POLYGON_TBL p2 WHERE p1.f1 << p2.f1;

-- Overlap of left of polygon
-- pgrust:rowsort
SELECT p1.f1, p2.f1 FROM POLYGON_TBL p1, POLYGON_TBL p2 WHERE p1.f1 &< p2.f1;

-- Right of polygon
-- pgrust:rowsort
SELECT p1.f1, p2.f1 FROM POLYGON_TBL p1, POLYGON_TBL p2 WHERE p1.f1 >> p2.f1;

-- Overlap of right of polygon
-- pgrust:rowsort
SELECT p1.f1, p2.f1 FROM POLYGON_TBL p1, POLYGON_TBL p2 WHERE p1.f1 &> p2.f1;

-- Below polygon
-- pgrust:rowsort
SELECT p1.f1, p2.f1 FROM POLYGON_TBL p1, POLYGON_TBL p2 WHERE p1.f1 <<| p2.f1;

-- Overlap or below polygon
-- pgrust:rowsort
SELECT p1.f1, p2.f1 FROM POLYGON_TBL p1, POLYGON_TBL p2 WHERE p1.f1 &<| p2.f1;

-- Above polygon
-- pgrust:rowsort
SELECT p1.f1, p2.f1 FROM POLYGON_TBL p1, POLYGON_TBL p2 WHERE p1.f1 |>> p2.f1;

-- Overlap or above polygon
-- pgrust:rowsort
SELECT p1.f1, p2.f1 FROM POLYGON_TBL p1, POLYGON_TBL p2 WHERE p1.f1 |&> p2.f1;

-- Distance to polygon
-- pgrust:rowsort
SELECT p1.f1, p2.f1, p1.f1 <-> p2.f1 FROM POLYGON_TBL p1, POLYGON_TBL p2;

--
-- Circles
--

-- pgrust:rowsort
SELECT circle(f1, 50.0)
   FROM POINT_TBL;

-- pgrust:rowsort
SELECT circle(f1)
   FROM BOX_TBL;

-- pgrust:rowsort
SELECT circle(f1)
   FROM POLYGON_TBL
   WHERE (# f1) >= 3;

SELECT c1.f1 AS circle, p1.f1 AS point, (p1.f1 <-> c1.f1) AS distance
   FROM CIRCLE_TBL c1, POINT_TBL p1
   WHERE (p1.f1 <-> c1.f1) > 0
   ORDER BY distance, area(c1.f1), p1.f1[0];

-- To polygon
-- pgrust:rowsort
SELECT f1, f1::polygon FROM CIRCLE_TBL WHERE f1 >= '<(0,0),1>';

-- To polygon with less points
-- pgrust:rowsort
SELECT f1, polygon(8, f1) FROM CIRCLE_TBL WHERE f1 >= '<(0,0),1>';

-- Error for insufficient number of points
SELECT f1, polygon(1, f1) FROM CIRCLE_TBL WHERE f1 >= '<(0,0),1>';

-- Zero radius error
SELECT f1, polygon(10, f1) FROM CIRCLE_TBL WHERE f1 < '<(0,0),1>';

-- Same as circle
-- pgrust:rowsort
SELECT c1.f1, c2.f1 FROM CIRCLE_TBL c1, CIRCLE_TBL c2 WHERE c1.f1 ~= c2.f1;

-- Overlap with circle
-- pgrust:rowsort
SELECT c1.f1, c2.f1 FROM CIRCLE_TBL c1, CIRCLE_TBL c2 WHERE c1.f1 && c2.f1;

-- Overlap or left of circle
-- pgrust:rowsort
SELECT c1.f1, c2.f1 FROM CIRCLE_TBL c1, CIRCLE_TBL c2 WHERE c1.f1 &< c2.f1;

-- Left of circle
-- pgrust:rowsort
SELECT c1.f1, c2.f1 FROM CIRCLE_TBL c1, CIRCLE_TBL c2 WHERE c1.f1 << c2.f1;

-- Right of circle
-- pgrust:rowsort
SELECT c1.f1, c2.f1 FROM CIRCLE_TBL c1, CIRCLE_TBL c2 WHERE c1.f1 >> c2.f1;

-- Overlap or right of circle
-- pgrust:rowsort
SELECT c1.f1, c2.f1 FROM CIRCLE_TBL c1, CIRCLE_TBL c2 WHERE c1.f1 &> c2.f1;

-- Contained by circle
-- pgrust:rowsort
SELECT c1.f1, c2.f1 FROM CIRCLE_TBL c1, CIRCLE_TBL c2 WHERE c1.f1 <@ c2.f1;

-- Contain by circle
-- pgrust:rowsort
SELECT c1.f1, c2.f1 FROM CIRCLE_TBL c1, CIRCLE_TBL c2 WHERE c1.f1 @> c2.f1;

-- Below circle
-- pgrust:rowsort
SELECT c1.f1, c2.f1 FROM CIRCLE_TBL c1, CIRCLE_TBL c2 WHERE c1.f1 <<| c2.f1;

-- Above circle
-- pgrust:rowsort
SELECT c1.f1, c2.f1 FROM CIRCLE_TBL c1, CIRCLE_TBL c2 WHERE c1.f1 |>> c2.f1;

-- Overlap or below circle
-- pgrust:rowsort
SELECT c1.f1, c2.f1 FROM CIRCLE_TBL c1, CIRCLE_TBL c2 WHERE c1.f1 &<| c2.f1;

-- Overlap or above circle
-- pgrust:rowsort
SELECT c1.f1, c2.f1 FROM CIRCLE_TBL c1, CIRCLE_TBL c2 WHERE c1.f1 |&> c2.f1;

-- Area equal with circle
-- pgrust:rowsort
SELECT c1.f1, c2.f1 FROM CIRCLE_TBL c1, CIRCLE_TBL c2 WHERE c1.f1 = c2.f1;

-- Area not equal with circle
-- pgrust:rowsort
SELECT c1.f1, c2.f1 FROM CIRCLE_TBL c1, CIRCLE_TBL c2 WHERE c1.f1 != c2.f1;

-- Area less than circle
-- pgrust:rowsort
SELECT c1.f1, c2.f1 FROM CIRCLE_TBL c1, CIRCLE_TBL c2 WHERE c1.f1 < c2.f1;

-- Area greater than circle
-- pgrust:rowsort
SELECT c1.f1, c2.f1 FROM CIRCLE_TBL c1, CIRCLE_TBL c2 WHERE c1.f1 > c2.f1;

-- Area less than or equal circle
-- pgrust:rowsort
SELECT c1.f1, c2.f1 FROM CIRCLE_TBL c1, CIRCLE_TBL c2 WHERE c1.f1 <= c2.f1;

-- Area greater than or equal circle
-- pgrust:rowsort
SELECT c1.f1, c2.f1 FROM CIRCLE_TBL c1, CIRCLE_TBL c2 WHERE c1.f1 >= c2.f1;

-- Area less than circle
-- pgrust:rowsort
SELECT c1.f1, c2.f1 FROM CIRCLE_TBL c1, CIRCLE_TBL c2 WHERE c1.f1 < c2.f1;

-- Area greater than circle
-- pgrust:rowsort
SELECT c1.f1, c2.f1 FROM CIRCLE_TBL c1, CIRCLE_TBL c2 WHERE c1.f1 < c2.f1;

-- Add point
-- pgrust:rowsort
SELECT c.f1, p.f1, c.f1 + p.f1 FROM CIRCLE_TBL c, POINT_TBL p;

-- Subtract point
-- pgrust:rowsort
SELECT c.f1, p.f1, c.f1 - p.f1 FROM CIRCLE_TBL c, POINT_TBL p;

-- Multiply with point
-- pgrust:rowsort
SELECT c.f1, p.f1, c.f1 * p.f1 FROM CIRCLE_TBL c, POINT_TBL p;

-- Divide by point
-- pgrust:rowsort
SELECT c.f1, p.f1, c.f1 / p.f1 FROM CIRCLE_TBL c, POINT_TBL p WHERE p.f1[0] BETWEEN 1 AND 1000;

-- Overflow error
SELECT c.f1, p.f1, c.f1 / p.f1 FROM CIRCLE_TBL c, POINT_TBL p WHERE p.f1[0] > 1000;

-- Division by 0 error
SELECT c.f1, p.f1, c.f1 / p.f1 FROM CIRCLE_TBL c, POINT_TBL p WHERE p.f1 ~= '(0,0)'::point;

-- Distance to polygon
-- pgrust:rowsort
SELECT c.f1, p.f1, c.f1 <-> p.f1 FROM CIRCLE_TBL c, POLYGON_TBL p;

-- Check index behavior for circles

CREATE INDEX gcircleind ON circle_tbl USING gist (f1);

SELECT * FROM circle_tbl WHERE f1 && circle(point(1,-2), 1)
    ORDER BY area(f1);

EXPLAIN (COSTS OFF)
SELECT * FROM circle_tbl WHERE f1 && circle(point(1,-2), 1)
    ORDER BY area(f1);
SELECT * FROM circle_tbl WHERE f1 && circle(point(1,-2), 1)
    ORDER BY area(f1);

-- Check index behavior for polygons

CREATE INDEX gpolygonind ON polygon_tbl USING gist (f1);

SELECT * FROM polygon_tbl WHERE f1 @> '((1,1),(2,2),(2,1))'::polygon
    ORDER BY (poly_center(f1))[0];

EXPLAIN (COSTS OFF)
SELECT * FROM polygon_tbl WHERE f1 @> '((1,1),(2,2),(2,1))'::polygon
    ORDER BY (poly_center(f1))[0];
SELECT * FROM polygon_tbl WHERE f1 @> '((1,1),(2,2),(2,1))'::polygon
    ORDER BY (poly_center(f1))[0];

-- test non-error-throwing API for some core types
SELECT pg_input_is_valid('(1', 'circle');
SELECT * FROM pg_input_error_info('1,', 'circle');
SELECT pg_input_is_valid('(1,2),-1', 'circle');
SELECT * FROM pg_input_error_info('(1,2),-1', 'circle');
