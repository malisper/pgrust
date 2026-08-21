SELECT IPNetworkID, COUNT(*) AS c, SUM(ResolutionWidth), AVG(ResolutionHeight) FROM hits GROUP BY IPNetworkID ORDER BY c DESC LIMIT 20;
SELECT COUNT(DISTINCT SearchPhrase) FROM hits WHERE EventDate >= '2013-07-08' AND EventDate <= '2013-07-14';
SELECT COUNT(*) FROM hits WHERE URL LIKE '%metrika%' AND OS = 44 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-15';
SELECT ClientIP, SearchPhrase FROM hits ORDER BY ClientIP LIMIT 10;
SELECT MobilePhoneModel, SearchPhrase, COUNT(*) AS c FROM hits WHERE MobilePhoneModel <> '' AND SearchPhrase <> '' GROUP BY MobilePhoneModel, SearchPhrase ORDER BY c DESC LIMIT 10;
