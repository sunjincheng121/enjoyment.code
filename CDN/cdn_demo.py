import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from cdn_udf import ip_to_province
from cdn_connector_ddl import kafka_source_ddl, mysql_sink_ddl

# 创建Table Environment， 并选择使用的Planner
env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(
   env,
   environment_settings=EnvironmentSettings.new_instance().use_blink_planner().build())

# 创建Kafka数据源表
t_env.sql_update(kafka_source_ddl)
# 创建MySql结果表
t_env.sql_update(mysql_sink_ddl)

# 注册IP转换地区名称的UDF
t_env.register_function("ip_to_province", ip_to_province)

# 添加依赖的Python文件
t_env.add_python_file(
    os.path.dirname(os.path.abspath(__file__)) + "/cdn_udf.py")
t_env.add_python_file(os.path.dirname(
    os.path.abspath(__file__)) + "/cdn_connector_ddl.py")

# 核心的统计逻辑
t_env.from_path("cdn_access_log")\
   .select("uuid, "
           "ip_to_province(client_ip) as province, " # IP 转换为地区名称
           "response_size, request_time")\
   .group_by("province")\
   .select( # 计算访问量
           "province, count(uuid) as access_count, " 
           # 计算下载总量 
           "sum(response_size) as total_download,  " 
           # 计算下载速度
           "sum(response_size) * 1.0 / sum(request_time) as download_speed") \
   .insert_into("cdn_access_statistic")

# 执行作业
t_env.execute("pyflink_parse_cdn_log")











