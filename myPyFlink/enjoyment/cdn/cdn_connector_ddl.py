
kafka_source_ddl = """
CREATE TABLE cdn_access_log (
 uuid VARCHAR,
 client_ip VARCHAR,
 request_time BIGINT,
 response_size BIGINT,
 uri VARCHAR
) WITH (
 'connector.type' = 'kafka',
 'connector.version' = 'universal',
 'connector.topic' = 'access_log',
 'connector.properties.zookeeper.connect' = 'localhost:2181',
 'connector.properties.bootstrap.servers' = 'localhost:9092',
 'format.type' = 'csv',
 'format.ignore-parse-errors' = 'true'
)
"""

mysql_sink_ddl = """
CREATE TABLE cdn_access_statistic (
 province VARCHAR,
 access_count BIGINT,
 total_download BIGINT,
 download_speed DOUBLE
) WITH (
 'connector.type' = 'jdbc',
 'connector.url' = 'jdbc:mysql://localhost:3306/flink',
 'connector.table' = 'access_statistic',
 'connector.username' = 'root',
 'connector.password' = 'root',
 'connector.write.flush.interval' = '1s'
)
"""