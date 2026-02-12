-- table world-news

CREATE TABLE default.`world-news`
(
    `affiliateLink` Int64,
    `age` String,
    `campaign` String,
    `channel` String,
    `clickbait` Int64,
    `content` Int64,
    `country_code` String,
    `exitSession` Int64,
    `gender` String,
    `home` Int64,
    `isSubscriber` Int64,
    `latitude` Float64,
    `longitude` Float64,
    `place_name` String,
    `plusContent` Int64,
    `recordType` String,
    `sid` String,
    `statesVisited` Array(String),
    `subscribe` Int64,
    `timestamp` DateTime,
    `timezone` String,
    `uid` String,
    `useragent` String,
    `_key` String,
    `_timestamp` DateTime64(3),
    `_partition` Int32,
    `_offset` Int64,
    `_topic` String,
    `_header_keys` Array(String),
    `_header_values` Array(String),
    `contentId` String,
    `state` String,
    `statuscode` Int64,
    `subContentId` String,
    `url` String
)
ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (toStartOfDay(timestamp), uid)
SETTINGS index_granularity = 8192;

-- aggregate table

CREATE TABLE `agg-news-2` (
    `hour` Timestamp, 
    `channel` String,
    `campaign` String,
    numClicks AggregateFunction(countIf, UInt8),
    numSessions AggregateFunction(uniq, String),
    numConversions AggregateFunction(uniqIf, String, UInt8)
)
ENGINE = AggregatingMergeTree
ORDER BY (channel, campaign, hour);

-- Materialized View

drop view `mv-news-2`;

CREATE MATERIALIZED VIEW `mv-news-2`
TO `agg-news-2`
AS SELECT 
    toStartOfHour(timestamp) as hour,
    channel,
    campaign, 
    countIfState(recordType = 'click') as numClicks,
    uniqState(sid) as numSessions,
    uniqIfState(sid, has(statesVisited, 'subscribe')) as numConversions
FROM `world-news`
GROUP BY hour, channel, campaign;

SELECT count() FROM `world-news`;

SELECT count() FROM `agg-news`;

-- Analytical query

SELECT
  toStartOfDay(hour),
  channel,
  campaign,
  countIfMerge(numClicks) AS numClicks,
  uniqMerge(numSessions) AS numSessions,
  uniqIfMerge(numConversions) AS numConversions,
  round(100.0 * numConversions / numSessions, 2) || '%' AS convRate
FROM `agg-news-2`
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

SELECT
  channel,
  campaign,
  countIfMerge(numClicks) AS numClicks,
  uniqMerge(numSessions) AS numSessions,
  uniqIfMerge(numConversions) AS numConversions,
  round(100.0 * numConversions / numSessions, 2) || '%' AS convRate
FROM `agg-news`
GROUP BY 1, 2
ORDER BY 1, 2;