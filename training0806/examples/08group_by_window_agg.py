import os

from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment, CsvTableSource, CsvTableSink, DataTypes
from pyflink.table.window import Tumble


def group_by_window_agg():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    source_file = os.getcwd() + "/../resources/table_orders.csv"
    result_file = "/tmp/table_group_by_window_agg.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    bt_env.register_table_source("Orders",
                                 CsvTableSource(source_file,
                                                ["a", "b", "c", "rowtime"],
                                                [DataTypes.STRING(),
                                                 DataTypes.INT(),
                                                 DataTypes.INT(),
                                                 DataTypes.TIMESTAMP()]))
    bt_env.register_table_sink("result",
                               CsvTableSink(["a", "start", "end", "rowtime", "d"],
                                            [DataTypes.STRING(),
                                             DataTypes.TIMESTAMP(),
                                             DataTypes.TIMESTAMP(),
                                             DataTypes.TIMESTAMP(),
                                             DataTypes.INT()],
                                            result_file))
    orders = bt_env.scan("Orders")
    result = orders.window(Tumble.over("1.hours").on("rowtime").alias("w")) \
        .group_by("a, w") \
        .select("a, w.start, w.end, w.rowtime, b.sum as d")
    result.insert_into("result")
    bt_env.execute("group by agg")

    with open(result_file, 'r') as f:
        print(f.read())

    # cat /tmp/table_group_by_window_agg.csv
    # a,2013-01-01 00:00:00.0,2013-01-01 01:00:00.0,2013-01-01 00:59:59.999,4
    # a,2013-01-01 01:00:00.0,2013-01-01 02:00:00.0,2013-01-01 01:59:59.999,9
    # b,2013-01-01 00:00:00.0,2013-01-01 01:00:00.0,2013-01-01 00:59:59.999,2
    # b,2013-01-01 01:00:00.0,2013-01-01 02:00:00.0,2013-01-01 01:59:59.999,4


if __name__ == '__main__':
    group_by_window_agg()
