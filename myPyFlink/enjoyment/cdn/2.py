import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from enjoyment.cdn.cdn_udf import ip_to_province
from enjoyment.cdn.cdn_connector_ddl import kafka_source_ddl, mysql_sink_ddl


# 创建ENV
env = StreamExecutionEnvironment\
    .get_execution_environment()
t_env = StreamTableEnvironment.create(...)
...
# 注册IP转换地区名称的UDF
t_env.register_function(...)
# 添加依赖的Python文件
t_env.add_python_file(...)
# 核心的统计逻辑
t_env.from_path(...)\
   .group_by(...)\
   .select(...) \
   .insert_into(...)
# 执行作业
t_env.execute(...)













