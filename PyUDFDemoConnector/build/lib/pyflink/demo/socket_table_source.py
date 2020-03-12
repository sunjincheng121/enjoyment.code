from pyflink.java_gateway import get_gateway
from pyflink.table import TableSource
from pyflink.table.types import _to_java_type
from pyflink.util.utils import to_jarray


class SocketTableSource(TableSource):

    def __init__(self,
                 hostname=None,
                 port=None,
                 line_delimiter=None,
                 field_delimiter=None,
                 field_names=None,
                 field_types=None,
                 append_proctime=None):
        gateway = get_gateway()
        j_builder = gateway.jvm.org.apache.flink.python.connector.SocketTableSource.Builder()
        if hostname is not None:
            j_builder.withHostname(hostname)
        if port is not None:
            j_builder.withPort(port)
        if line_delimiter is not None:
            j_builder.withLineDelimiter(line_delimiter)
        if field_delimiter is not None:
            j_builder.withFieldDelimiter(field_delimiter)
        if field_names is not None and field_types is not None:
            j_field_names = to_jarray(gateway.jvm.String, field_names)
            j_field_types = to_jarray(gateway.jvm.TypeInformation,
                                      [_to_java_type(field_type) for field_type in field_types])
            j_builder.withSchema(j_field_names, j_field_types)
        if append_proctime is not None:
            j_builder.appendProctime(append_proctime)
        super(SocketTableSource, self).__init__(j_builder.build())
