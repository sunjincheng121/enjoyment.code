#!/usr/bin/env python
import os

from pyflink_dashboard.http_server_util import find_free_port, start_chart_server
from pyflink_dashboard.kafka_producer import get_kafka_producer
from pyflink_dashboard.mysql_reader import MysqlDataProvider

if __name__ == "__main__":
    host = "localhost"
    kafka_port = 9092
    kafka_topic = "cdn_access_log"
    mysql_port = 3306
    mysql_user = "root"
    mysql_password = "root"
    mysql_database = "flink"
    mysql_table = "cdn_access_statistic"
    port = find_free_port()
    print("--------------------------------------environment config--------------------------------------")
    print("Target kafka port: %s:%d" % (host, kafka_port))
    print("Target kafka topic: %s" % kafka_topic)
    print("Target mysql://%s:%d/%s" % (host, mysql_port, mysql_database))
    print("Target mysql table: %s" % mysql_table)
    print("Listen at: http://localhost:%d" % port)
    print("----------------------------------------------------------------------------------------------")
    print("To change above environment config, edit this file: " + os.path.abspath(__file__))
    data_provider = MysqlDataProvider(host, mysql_port, mysql_user, mysql_password, mysql_database, mysql_table)
    kafka_producer = get_kafka_producer(kafka_port, kafka_topic)
    start_chart_server(data_provider, kafka_producer, port, "localhost")
