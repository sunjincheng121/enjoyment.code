import tempfile
import os
import shutil
sink_path = tempfile.gettempdir() + '/streaming.csv'
if os.path.exists(sink_path):
    if os.path.isfile(sink_path):
        os.remove(sink_path)
    else:
        shutil.rmtree(sink_path)

s_env.set_parallelism(1)
t = st_env.from_elements([(1, 'hi', 'hello'), (2, 'hi', 'hello')], ['a', 'b', 'c'])
st_env.connect(FileSystem().path(sink_path))\
    .with_format(OldCsv()
        .field_delimiter(',')
        .field("a", DataTypes.BIGINT())
        .field("b", DataTypes.STRING())
        .field("c", DataTypes.STRING()))\
    .with_schema(Schema()
        .field("a", DataTypes.BIGINT())
        .field("b", DataTypes.STRING())
        .field("c", DataTypes.STRING()))\
    .register_table_sink("stream_sink")

t.select("a + 1, b, c")\
    .insert_into("stream_sink")
st_env.execute("stream_job")
with open(sink_path, 'r') as f:
    print(f.read())
