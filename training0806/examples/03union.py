import os

from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment, CsvTableSink, DataTypes


def union():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = os.getcwd() + "/tmp/table_union_batch.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    left = bt_env.from_elements(
        [(1, "1b", "1bb"),
         (2, "2a", "2aa"),
         (3, None, "3aa"),
         (1, "1a", "1laa"),
         (1, "1b", "1bb")],
        ["a", "b", "c"]).select("a, b, c")
    right = bt_env.from_elements([
        (1, "1b", "1bb"),
        (2, None, "2bb"),
        (1, "3b", "3bb"),
        (4, "4b", "4bb")],
        ["a", "b", "c"]).select("a, b, c")
    bt_env.register_table_sink("result",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.BIGINT(),
                                             DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file))

    result = left.union(right)
    #result = left.union_all(right)
    result.insert_into("result")
    bt_env.execute("union")

    with open(result_file, 'r') as f:
        print(f.read())

    # cat /tmp/table_union_b.csv
    # 1,1b,1bb
    # 1,3b,3bb
    # 2,,2bb
    # 2,2a,2aa
    # 3,,3aa
    # 4,4b,4bb
    # note : Unions two tables with duplicate records removed whatever the duplicate record from
    # the same table or the other.


if __name__ == '__main__':
    union()
