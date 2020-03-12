# -*- coding: utf-8 -*-
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.descriptors import FileSystem, OldCsv, Schema
from enjoyment.three_minutes.myudfs import add1, add2, add3, add4

import tempfile
sink_path = tempfile.gettempdir() + '/streaming.csv'

# init env
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
t_env = StreamTableEnvironment.create(env)

# register function
t_env.register_function("add1", add1)
t_env.register_function("add2", add2)
t_env.register_function("add3", add3)
t_env.register_function("add4", add4)

t = t_env.from_elements([(1, 2, 'Welcome'), (2, 3, 'To'), (3, 4, 'PyFlink')], ['a', 'b', 'c'])

t_env.connect(FileSystem().path(sink_path))\
    .with_format(OldCsv()
        .field_delimiter(',')
        .field("add1", DataTypes.BIGINT())
        .field("add2", DataTypes.BIGINT())
        .field("add3", DataTypes.BIGINT())
        .field("add4", DataTypes.BIGINT())
        .field("b", DataTypes.BIGINT())
        .field("c", DataTypes.STRING()))\
    .with_schema(Schema()
        .field("add1", DataTypes.BIGINT())
        .field("add2", DataTypes.BIGINT())
        .field("add3", DataTypes.BIGINT())
        .field("add4", DataTypes.BIGINT())
        .field("b", DataTypes.BIGINT())
        .field("c", DataTypes.STRING()))\
    .register_table_sink("pyflink_sink")

t.select("add1(a, b), add2(a, b), add3(a, b), add4(a, b), b, c").insert_into("pyflink_sink")

t_env.execute("pyflink_udf")


import os
import shutil

with open(sink_path, 'r') as f:
    print(f.read())

if os.path.exists(sink_path):
    if os.path.isfile(sink_path):
        os.remove(sink_path)
    else:
        shutil.rmtree(sink_path)


