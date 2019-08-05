import os

from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment, CsvTableSink, DataTypes


def intersect():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_intersect.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    left = bt_env.from_elements([
        (1, "ra", "raa"),
        (2, "lb", "lbb"),
        (3, "", "lcc"),
        (1, "ra", "raa")],
        ["a", "b", "c"]).select("a, b, c")
    right = bt_env.from_elements([
        (1, "ra", "raa"),
        (2, "", "rbb"),
        (3, "rc", "rcc"),
        (1, "ra", "raa")],
        ["a", "b", "c"]).select("a, b, c")

    bt_env.register_table_sink("result",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.BIGINT(),
                                             DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file))

    result = left.intersect(right)
    # result = left.intersect_all(right)
    result.insert_into("result")
    bt_env.execute("intersect")

    with open(result_file, 'r') as f:
        print(f.read())

    # cat /tmp/table_intersect_batch.csv
    # 1,ra,raa


if __name__ == '__main__':
    intersect()
