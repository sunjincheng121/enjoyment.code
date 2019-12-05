from pyflink.datastream import StreamExecutionEnvironment
from pyflink.demo import ChartConnector, SocketTableSource
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.descriptors import Schema
from pyflink.table.udf import udf

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(
    env,
    environment_settings=EnvironmentSettings.new_instance().use_blink_planner().build())
t_env.connect(ChartConnector())\
    .with_schema(Schema()
                 .field("city", DataTypes.STRING())
                 .field("sales_volume", DataTypes.BIGINT())
                 .field("sales", DataTypes.BIGINT()))\
    .register_table_sink("sink")


@udf(input_types=[DataTypes.STRING()],
     result_type=DataTypes.ARRAY(DataTypes.STRING()))
def split(line):
    return line.split(",")


@udf(input_types=[DataTypes.ARRAY(DataTypes.STRING()), DataTypes.INT()],
     result_type=DataTypes.STRING())
def get(array, index):
    return array[index]

t_env.get_config().set_python_executable("python3")
t_env.register_function("split", split)
t_env.register_function("get", get)
t_env.from_table_source(SocketTableSource(port=6666))\
    .alias("line")\
    .select("split(line) as str_array")\
    .select("get(str_array, 3) as city, "
            "get(str_array, 1).cast(LONG) as count, "
            "get(str_array, 2).cast(LONG) as unit_price")\
    .select("city, count, count * unit_price as total_price")\
    .group_by("city")\
    .select("city, "
            "sum(count) as sales_volume, "
            "sum(total_price) as sales")\
    .insert_into("sink")

t_env.execute("Sales Statistic")

