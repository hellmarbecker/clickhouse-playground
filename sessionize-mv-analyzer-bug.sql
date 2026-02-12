SET enable_analyzer = 0;

CREATE DATABASE IF NOT EXISTS reproduction_default;
CREATE DATABASE IF NOT EXISTS reproduction_test;

DROP TABLE IF EXISTS reproduction_default.events;
CREATE TABLE reproduction_default.events
(
    id                 String,
    visitorId          String,
    isInitialVisitorId UInt8,
    sessionId          String,
    isInitialSessionId UInt8,
    timestamp          DateTime64(3),
    referrer           String
)
    engine = MergeTree ORDER BY (timestamp)
        SETTINGS index_granularity = 8192;

DROP VIEW IF EXISTS reproduction_test.events_initial_visitor_view;
CREATE VIEW reproduction_test.events_initial_visitor_view
AS
SELECT id,
       visitorId,
       timestamp,
       referrer
FROM reproduction_default.events
WHERE isInitialVisitorId = 1;

DROP TABLE IF EXISTS reproduction_test.events_first_click;
CREATE TABLE reproduction_test.events_first_click
(
    id                 String,
    visitorId          String,
    isInitialVisitorId UInt8,
    sessionId          String,
    isInitialSessionId UInt8,
    timestamp          DateTime64(3),
    referrer           String
)
    engine = MergeTree ORDER BY (timestamp)
        SETTINGS index_granularity = 8192;

DROP VIEW IF EXISTS reproduction_test.events_first_click_mv;
CREATE MATERIALIZED VIEW reproduction_test.events_first_click_mv TO reproduction_test.events_first_click
AS
SELECT e.id as id,
       e.visitorId as visitorId,
       e.isInitialVisitorId as isInitialVisitorId,
       e.sessionId as sessionId,
       e.isInitialSessionId as isInitialSessionId,
       e.timestamp as timestamp,
       if(initialVisitor.referrer <> '', initialVisitor.referrer, e.sref) as referrer
FROM (
    SELECT 
        *,
        anyIf(referrer, isInitialSessionId = 1) OVER (PARTITION BY sessionId ORDER BY timestamp ASC ROWS UNBOUNDED PRECEDING) AS sref
    FROM reproduction_default.events
) as e
LEFT ANY JOIN (
    SELECT *
        from reproduction_test.events_initial_visitor_view
        WHERE visitorId IN (select visitorId from reproduction_default.events)
    ) AS initialVisitor
ON e.visitorId = initialVisitor.visitorId
SETTINGS enable_analyzer = 1;

INSERT INTO reproduction_default.events (id, visitorId, isInitialVisitorId, sessionId, isInitialSessionId, timestamp,
                                         referrer)
    --SETTINGS async_insert = 1
VALUES ('id11', '7fRCSZt27o_kBoF8JT1F2', 1, 'QrDgfP_jbZn93goBiZ_nm', 1, '2025-09-22 22:33:51.536', 'google.com'),
    ('id10', '7fRCSZt27o_kBoF8JT1F2', 0, 'QrDgfP_jbZn93goBiZ_nm', 0, '2025-09-22 22:34:03.506', ''),
    ('id9', '7fRCSZt27o_kBoF8JT1F2', 0, 'QrDgfP_jbZn93goBiZ_nm', 0, '2025-09-22 22:34:09.921', ''),
    ('id8', '7fRCSZt27o_kBoF8JT1F2', 0, 'QrDgfP_jbZn93goBiZ_nm', 0, '2025-09-22 22:34:27.056', '');

INSERT INTO reproduction_default.events (id, visitorId, isInitialVisitorId, sessionId, isInitialSessionId, timestamp,
                                         referrer)
    --SETTINGS async_insert = 1
VALUES
    ('id7', '7fRCSZt27o_kBoF8JT1F2', 0, 'QrDgfP_jbZn93goBiZ_nm', 0, '2025-09-22 22:35:19.353', ''),
    ('id6', '7fRCSZt27o_kBoF8JT1F2', 0, 'QrDgfP_jbZn93goBiZ_nm', 0, '2025-09-22 22:35:36.786', ''),
    ('id5', '7fRCSZt27o_kBoF8JT1F2', 0, 'QrDgfP_jbZn93goBiZ_nm', 0, '2025-09-22 22:36:33.896', ''),
    ('id4', '7fRCSZt27o_kBoF8JT1F2', 0, 'QrDgfP_jbZn93goBiZ_nm', 0, '2025-09-22 22:36:38.570', ''),
    ('id3', '7fRCSZt27o_kBoF8JT1F2', 0, 'QrDgfP_jbZn93goBiZ_nm', 0, '2025-09-22 22:37:08.867', ''),
    ('id2', '7fRCSZt27o_kBoF8JT1F2', 0, 'QrDgfP_jbZn93goBiZ_nm', 0, '2025-09-22 22:37:24.307', ''),
    ('id1', '7fRCSZt27o_kBoF8JT1F2', 0, 'QrDgfP_jbZn93goBiZ_nm', 0, '2025-09-22 22:38:11.603', '');

SELECT *
FROM reproduction_test.events_first_click
ORDER BY timestamp DESC;