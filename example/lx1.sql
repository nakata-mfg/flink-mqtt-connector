/*
yin@rss:~/flink-1.19.1$ ./bin/sql-client.sh  myjob.sql"
*/

CREATE TABLE source(
 floatVal FLOAT
 ) WITH (
 'connector' = 'mqtt',
 'hostUrl' = 'tcp://localhost:1883',
 'username' = '',
 'password' = '',
 'sourceTopics' = 'FFX/BD1/UpperRoll/Load',
  'format' = 'json'
 );


CREATE TABLE sink (
level INTEGER,
code INTEGER,
src STRING,
`time` STRING
) WITH (
'connector' = 'mqtt',
'hostUrl' = 'tcp://localhost:1883',
'username' = '',
'password' = '',
'sinkTopics' = 'ALARM',
'format' = 'json'
);

SET 'pipeline.name' = 'BD1-UpperRoll-Load-Threshold-UB-check';
INSERT INTO sink SELECT 3 AS level ,20225 AS code , 'lx6' AS src, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd''T''HH:mm:ss') AS `time`  FROM source WHERE floatVal>1.0;

SET 'pipeline.name' = 'BD1-UpperRoll-Load-Threshold-LB-check';
INSERT INTO sink SELECT 5 AS level ,20226 AS code , 'lx6' AS src, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd''T''HH:mm:ss') AS `time`  FROM source WHERE floatVal<-1.0;

