# -*- coding: utf-8 -*-

from pyflink.dataset import ExecutionEnvironment
from pyflink.table import TableConfig, DataTypes, BatchTableEnvironment
from pyflink.table.descriptors import Schema, OldCsv, FileSystem

import os

# 数据源文件
source_file = 'source.csv'
#计算结果文件
sink_file = 'sink.csv'

# 创建执行环境
exec_env = ExecutionEnvironment.get_execution_environment()
# 设置并发为1 方便调试
exec_env.set_parallelism(1)
t_config = TableConfig()
t_env = BatchTableEnvironment.create(exec_env, t_config)

# 如果结果文件已经存在,就先删除
if os.path.exists(sink_file):
    os.remove(sink_file)

# 创建数据源表
t_env.connect(FileSystem().path(source_file)) \
    .with_format(OldCsv()
                 .line_delimiter(',')
                 .field('word', DataTypes.STRING())) \
    .with_schema(Schema()
                 .field('word', DataTypes.STRING())) \
    .register_table_source('mySource')

# 创建结果表
t_env.connect(FileSystem().path(sink_file)) \
    .with_format(OldCsv()
                 .field_delimiter(',')
                 .field('word', DataTypes.STRING())
                 .field('count', DataTypes.BIGINT())) \
    .with_schema(Schema()
                 .field('word', DataTypes.STRING())
                 .field('count', DataTypes.BIGINT())) \
    .register_table_sink('mySink')

# 非常简单的word_count计算逻辑
t_env.scan('mySource') \
    .group_by('word') \
    .select('word, count(1)') \
    .insert_into('mySink')

# 执行Job
t_env.execute("wordcount")