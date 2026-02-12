SELECT
   event_time,
   query
FROM clusterAllReplicas(default, merge('system', '^query_log'))
ORDER BY event_time DESC
LIMIT 10;