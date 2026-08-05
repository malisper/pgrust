-- Clean analytics training corpus over the wide web-log fixture (table "hits").
-- One statement per line. No line here appears in any published measurement
-- vector; non-overlap is enforced mechanically by pgo/lint-training-overlap.sh
-- against pgo/denylist/. Shape classes are documented in pgo/README.md; the
-- per-line engine assignment lives in corpus/analytics-hits-engines.tsv.
--
-- A1 whole-relation scan + trivial count
SELECT COUNT(*) FROM hits WHERE CounterClass >= 0;
-- A2 scan + smallint inequality filter + count
SELECT COUNT(*) FROM hits WHERE IsMobile <> 0;
-- A3 multi scalar aggregate over narrow numerics (sum/count/avg fusion)
SELECT SUM(SendTiming), COUNT(*), AVG(ResolutionHeight) FROM hits;
-- A4 single average over a wide integer
SELECT AVG(FUniqID) FROM hits;
-- A5 global distinct over a wide integer
SELECT COUNT(DISTINCT WatchID) FROM hits;
-- A6 global distinct over a variable-length string
SELECT COUNT(DISTINCT Params) FROM hits;
-- A7 min/max over a timestamp
SELECT MIN(ClientEventTime), MAX(ClientEventTime) FROM hits;
-- A8 low-cardinality integer group, ordered by the aggregate, unbounded
SELECT TraficSourceID, COUNT(*) FROM hits WHERE TraficSourceID <> 0 GROUP BY TraficSourceID ORDER BY COUNT(*) DESC;
-- A9 integer group + per-group distinct of a wide integer, bounded
SELECT URLRegionID, COUNT(DISTINCT FUniqID) AS n FROM hits GROUP BY URLRegionID ORDER BY n DESC LIMIT 12;
-- A10 integer group + mixed aggregate battery incl. per-group distinct
SELECT RefererRegionID, SUM(TraficSourceID), COUNT(*) AS n, AVG(ResolutionHeight), COUNT(DISTINCT FUniqID) FROM hits GROUP BY RefererRegionID ORDER BY n DESC LIMIT 12;
-- A11 short-string group + per-group distinct, non-empty filter
SELECT BrowserCountry, COUNT(DISTINCT WatchID) AS n FROM hits WHERE BrowserCountry <> '' GROUP BY BrowserCountry ORDER BY n DESC LIMIT 12;
-- A12 composite (smallint, string) group + per-group distinct
SELECT SocialSourceNetworkID, SocialSourcePage, COUNT(DISTINCT WatchID) AS n FROM hits WHERE SocialSourcePage <> '' GROUP BY SocialSourceNetworkID, SocialSourcePage ORDER BY n DESC LIMIT 12;
-- A13 string group + count, bounded top-N
SELECT OpenstatCampaignID, COUNT(*) AS n FROM hits WHERE OpenstatCampaignID <> '' GROUP BY OpenstatCampaignID ORDER BY n DESC LIMIT 12;
-- A14 string group + per-group distinct of a wide integer
SELECT UTMCampaign, COUNT(DISTINCT UserID) AS n FROM hits WHERE UTMCampaign <> '' GROUP BY UTMCampaign ORDER BY n DESC LIMIT 12;
-- A15 composite (smallint, string) group + count
SELECT URLCategoryID, UTMSource, COUNT(*) AS n FROM hits WHERE UTMSource <> '' GROUP BY URLCategoryID, UTMSource ORDER BY n DESC LIMIT 12;
-- A16 very-high-cardinality wide-integer group, bounded
SELECT FUniqID, COUNT(*) FROM hits GROUP BY FUniqID ORDER BY COUNT(*) DESC LIMIT 12;
-- A17 very-high-cardinality composite (wide integer, string) group
SELECT FUniqID, ParamOrderID, COUNT(*) FROM hits GROUP BY FUniqID, ParamOrderID ORDER BY COUNT(*) DESC LIMIT 12;
-- A18 bare LIMIT with no ORDER BY (group-admission freeze path)
SELECT FUniqID, ParamOrderID, COUNT(*) FROM hits GROUP BY FUniqID, ParamOrderID LIMIT 12;
-- A19 derived group key via extract() on a timestamp
SELECT FUniqID, extract(hour FROM ClientEventTime) AS h, ParamOrderID, COUNT(*) FROM hits GROUP BY FUniqID, h, ParamOrderID ORDER BY COUNT(*) DESC LIMIT 12;
-- A20 equality probe on a wide integer (scan + selective filter)
SELECT FUniqID FROM hits WHERE FUniqID = 2231001430434464715;
-- A21 substring match on a long string column
SELECT COUNT(*) FROM hits WHERE Referer LIKE '%yandex%';
-- A22 substring match + string group + MIN over a long string
SELECT UTMSource, MIN(Referer), COUNT(*) AS n FROM hits WHERE Referer LIKE '%yandex%' AND UTMSource <> '' GROUP BY UTMSource ORDER BY n DESC LIMIT 12;
-- A23 positive + negative substring match, two string MINs, per-group distinct
SELECT UTMSource, MIN(Referer), MIN(OriginalURL), COUNT(*) AS n, COUNT(DISTINCT WatchID) FROM hits WHERE OriginalURL LIKE '%Yandex%' AND Referer NOT LIKE '%.yandex.%' AND UTMSource <> '' GROUP BY UTMSource ORDER BY n DESC LIMIT 12;
-- A24 full-width row projection + top-N on a timestamp
SELECT * FROM hits WHERE Referer LIKE '%yandex%' ORDER BY ClientEventTime LIMIT 12;
-- A25 string projection + top-N on a timestamp
SELECT Params FROM hits WHERE Params <> '' ORDER BY ClientEventTime LIMIT 12;
-- A26 string projection + top-N on the string itself
SELECT Params FROM hits WHERE Params <> '' ORDER BY Params LIMIT 12;
-- A27 string projection + two-key top-N
SELECT Params FROM hits WHERE Params <> '' ORDER BY ClientEventTime, Params LIMIT 12;
-- A28 byte-length aggregate + HAVING threshold
SELECT RegionID, AVG(octet_length(Referer)) AS l, COUNT(*) AS n FROM hits WHERE Referer <> '' GROUP BY RegionID HAVING COUNT(*) > 50000 ORDER BY l DESC LIMIT 30;
-- A29 regexp-derived group key + byte-length aggregate + HAVING
SELECT REGEXP_REPLACE(OriginalURL, '^[a-z]+://([^/?#]+).*$', '\1') AS k, AVG(octet_length(OriginalURL)) AS l, COUNT(*) AS n, MIN(OriginalURL) FROM hits WHERE OriginalURL <> '' GROUP BY k HAVING COUNT(*) > 50000 ORDER BY l DESC LIMIT 30;
-- A30 wide projection fan-out of expression aggregates
SELECT SUM(ResolutionHeight), SUM(ResolutionHeight + 1), SUM(ResolutionHeight + 2), SUM(ResolutionHeight + 3), SUM(ResolutionHeight + 4), SUM(ResolutionHeight + 5), SUM(ResolutionHeight + 6), SUM(ResolutionHeight + 7), SUM(ResolutionHeight + 8), SUM(ResolutionHeight + 9), SUM(ResolutionHeight + 10), SUM(ResolutionHeight + 11), SUM(ResolutionHeight + 12), SUM(ResolutionHeight + 13), SUM(ResolutionHeight + 14), SUM(ResolutionHeight + 15), SUM(ResolutionHeight + 16), SUM(ResolutionHeight + 17), SUM(ResolutionHeight + 18), SUM(ResolutionHeight + 19), SUM(ResolutionHeight + 20), SUM(ResolutionHeight + 21), SUM(ResolutionHeight + 22), SUM(ResolutionHeight + 23), SUM(ResolutionHeight + 24), SUM(ResolutionHeight + 25), SUM(ResolutionHeight + 26), SUM(ResolutionHeight + 27), SUM(ResolutionHeight + 28), SUM(ResolutionHeight + 29), SUM(ResolutionHeight + 30), SUM(ResolutionHeight + 31), SUM(ResolutionHeight + 32), SUM(ResolutionHeight + 33), SUM(ResolutionHeight + 34), SUM(ResolutionHeight + 35), SUM(ResolutionHeight + 36), SUM(ResolutionHeight + 37), SUM(ResolutionHeight + 38), SUM(ResolutionHeight + 39), SUM(ResolutionHeight + 40), SUM(ResolutionHeight + 41), SUM(ResolutionHeight + 42), SUM(ResolutionHeight + 43), SUM(ResolutionHeight + 44), SUM(ResolutionHeight + 45), SUM(ResolutionHeight + 46), SUM(ResolutionHeight + 47) FROM hits;
-- A31 composite (smallint, integer) group + aggregate battery
SELECT URLCategoryID, RemoteIP, COUNT(*) AS n, SUM(IsLink), AVG(ResolutionHeight) FROM hits WHERE Params <> '' GROUP BY URLCategoryID, RemoteIP ORDER BY n DESC LIMIT 12;
-- A32 composite (wide integer, integer) group + aggregate battery, filtered
SELECT FUniqID, RemoteIP, COUNT(*) AS n, SUM(IsLink), AVG(ResolutionHeight) FROM hits WHERE Params <> '' GROUP BY FUniqID, RemoteIP ORDER BY n DESC LIMIT 12;
-- A33 composite (wide integer, integer) group + aggregate battery, unfiltered
SELECT FUniqID, RemoteIP, COUNT(*) AS n, SUM(IsLink), AVG(ResolutionHeight) FROM hits GROUP BY FUniqID, RemoteIP ORDER BY n DESC LIMIT 12;
-- A34 long-string group at very high cardinality
SELECT OriginalURL, COUNT(*) AS n FROM hits GROUP BY OriginalURL ORDER BY n DESC LIMIT 12;
-- A35 constant + long-string group (ordinal group reference)
SELECT 2, OriginalURL, COUNT(*) AS n FROM hits GROUP BY 1, OriginalURL ORDER BY n DESC LIMIT 12;
-- A36 arithmetic-expression group keys over one integer column
SELECT RemoteIP, RemoteIP - 4, RemoteIP - 5, RemoteIP - 6, COUNT(*) AS n FROM hits GROUP BY RemoteIP, RemoteIP - 4, RemoteIP - 5, RemoteIP - 6 ORDER BY n DESC LIMIT 12;
-- A37 date-range + boolean-flag conjunction + long-string group
SELECT OriginalURL, COUNT(*) AS n FROM hits WHERE EventDate >= '2013-07-02' AND EventDate <= '2013-07-29' AND IsNotBounce = 0 AND IsLink = 0 AND OriginalURL <> '' GROUP BY OriginalURL ORDER BY n DESC LIMIT 12;
-- A38 same conjunction over a different string column
SELECT FlashMinor2, COUNT(*) AS n FROM hits WHERE EventDate >= '2013-07-02' AND EventDate <= '2013-07-29' AND IsNotBounce = 0 AND IsLink = 0 AND FlashMinor2 <> '' GROUP BY FlashMinor2 ORDER BY n DESC LIMIT 12;
-- A39 bounded top-N with a deep OFFSET
SELECT OriginalURL, COUNT(*) AS n FROM hits WHERE EventDate >= '2013-07-02' AND EventDate <= '2013-07-29' AND IsNotBounce = 0 AND OriginalURL <> '' GROUP BY OriginalURL ORDER BY n DESC LIMIT 12 OFFSET 900;
-- A40 CASE-derived group key inside a five-key grouping
SELECT URLCategoryID, RefererCategoryID, SocialSourceNetworkID, CASE WHEN (URLCategoryID = 0 AND SocialSourceNetworkID = 0) THEN OriginalURL ELSE '' END AS a, Referer AS b, COUNT(*) AS n FROM hits WHERE EventDate >= '2013-07-02' AND EventDate <= '2013-07-29' AND IsNotBounce = 0 GROUP BY URLCategoryID, RefererCategoryID, SocialSourceNetworkID, a, b ORDER BY n DESC LIMIT 12 OFFSET 900;
-- A41 IN-list membership + hash inequality + composite group, deep OFFSET
SELECT RefererHash, ClientEventTime, COUNT(*) AS n FROM hits WHERE EventDate >= '2013-07-02' AND EventDate <= '2013-07-29' AND IsNotBounce = 0 AND URLCategoryID IN (0, 3, 17) AND RefererHash <> 0 GROUP BY RefererHash, ClientEventTime ORDER BY n DESC LIMIT 12 OFFSET 90;
-- A42 two narrow-integer group keys with a very deep OFFSET
SELECT ResolutionWidth, ResolutionDepth, COUNT(*) AS n FROM hits WHERE EventDate >= '2013-07-02' AND EventDate <= '2013-07-29' AND IsNotBounce = 0 AND IsLink = 0 GROUP BY ResolutionWidth, ResolutionDepth ORDER BY n DESC LIMIT 12 OFFSET 9000;
-- A43 date_trunc-derived group key ordered by the derived key
SELECT DATE_TRUNC('hour', ClientEventTime) AS h, COUNT(*) AS n FROM hits WHERE EventDate >= '2013-07-08' AND EventDate <= '2013-07-11' AND IsNotBounce = 0 AND IsLink = 0 GROUP BY DATE_TRUNC('hour', ClientEventTime) ORDER BY DATE_TRUNC('hour', ClientEventTime) LIMIT 12 OFFSET 90;
-- A44 dilution shape: sorted distinct over a low-cardinality string
SELECT DISTINCT BrowserLanguage FROM hits ORDER BY BrowserLanguage;
-- A45 dilution shape: filtered scan projecting several narrow columns, deep top-N
SELECT ResolutionWidth, ResolutionHeight, ClientTimeZone FROM hits WHERE HTTPError <> 0 ORDER BY ClientTimeZone DESC, ResolutionWidth LIMIT 40 OFFSET 200;
-- A46 dilution shape: grouped string-length statistics
SELECT PageCharset, MIN(length(Title)), MAX(length(Title)), AVG(length(Title)) FROM hits WHERE PageCharset <> '' GROUP BY PageCharset ORDER BY 4 DESC LIMIT 20;
-- A47 dilution shape: correlated-free scalar subquery over the same relation
SELECT COUNT(*) FROM hits WHERE SendTiming > (SELECT AVG(SendTiming) FROM hits);
-- A48 dilution shape: grouped aggregate feeding an outer aggregate
SELECT COUNT(*), SUM(n) FROM (SELECT ClientTimeZone, COUNT(*) AS n FROM hits GROUP BY ClientTimeZone) s;
