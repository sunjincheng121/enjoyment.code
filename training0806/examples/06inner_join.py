import os

from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment, CsvTableSink, DataTypes


def inner_join():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_inner_join.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    left = bt_env.from_elements(
        [(1, "1a", "1laa"),
         (2, "2a", "2aa"),
         (3, None, "3aa"),
         (2, "4b", "4bb"),
         (5, "5a", "5aa")],
        ["a", "b", "c"]).select("a, b, c")
    right = bt_env.from_elements([
        (1, "1b", "1bb"),
        (2, None, "2bb"),
        (1, "3b", "3bb"),
        (4, "4b", "4bb")],
        ["d", "e", "f"]).select("d, e, f")
    bt_env.register_table_sink("result",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.BIGINT(),
                                             DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file))

    result = left.join(right).where("a = d").select("a, b, e")
    result.insert_into("result")
    bt_env.execute("inner join")

    with open(result_file, 'r') as f:
        print(f.read())

    # cat table/result/table_inner_join.csv
    # 1,1a,1b
    # 2,2a,
    # 2,4b,
    # 1,1a,3b


if __name__ == '__main__':
    inner_join()
