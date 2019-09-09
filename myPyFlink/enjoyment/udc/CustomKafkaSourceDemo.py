import os

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes, CsvTableSink
from pyflink.table.descriptors import Schema, CustomFormatDescriptor, CustomConnectorDescriptor, Json
from pyflink.table.window import Tumble

if __name__ == '__main__':
    custom_connector = CustomConnectorDescriptor('kafka', 1, True) \
        .property('connector.topic', 'user') \
        .property('connector.properties.0.key', 'zookeeper.connect') \
        .property('connector.properties.0.value', 'localhost:2181') \
        .property('connector.properties.1.key', 'bootstrap.servers') \
        .property('connector.properties.1.value', 'localhost:9092') \
        .properties({'connector.version': '0.11', 'connector.startup-mode': 'earliest-offset'})

    # the key is 'format.json-schema'
    custom_format = CustomFormatDescriptor('json', 1) \
        .property('format.json-schema',
                  "{"
                  "  type: 'object',"
                  "  properties: {"
                  "    a: {"
                  "      type: 'string'"
                  "    },"
                  "    b: {"
                  "      type: 'string'"
                  "    },"
                  "    c: {"
                  "      type: 'string'"
                  "    },"
                  "    time: {"
                  "      type: 'string',"
                  "      format: 'date-time'"
                  "    }"
                  "  }"
                  "}") \
        .properties({'format.fail-on-missing-field': 'true'})

    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    s_env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)
    st_env = StreamTableEnvironment.create(s_env)
    result_file = "/tmp/custom_kafka_source_demo.csv"

    if os.path.exists(result_file):
        os.remove(result_file)

    st_env \
        .connect(custom_connector) \
        .with_format(
        custom_format
    ) \
        .with_schema(  # declare the schema of the table
        Schema()
            .field("proctime", DataTypes.TIMESTAMP())
            .proctime()
            .field("a", DataTypes.STRING())
            .field("b", DataTypes.STRING())
            .field("c", DataTypes.STRING())
    ) \
        .in_append_mode() \
        .register_table_source("source")

    st_env.register_table_sink("result",
                               CsvTableSink(["a", "b"],
                                            [DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file))

    st_env.scan("source").window(Tumble.over("2.rows").on("proctime").alias("w")) \
        .group_by("w, a") \
        .select("a, max(b)").insert_into("result")

    st_env.execute("custom kafka source demo")
    # cat /tmp/custom_kafka_source_demo.csv
    # a,3
    # b,4
    # a 5
