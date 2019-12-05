from pyflink.java_gateway import get_gateway
from pyflink.table import TableSink
from pyflink.table.types import _to_java_type
from pyflink.util.utils import to_jarray


class PrintTableSink(TableSink):

    def __init__(self, field_names, field_types):
        gateway = get_gateway()
        j_print_table_sink = gateway.jvm.org.apache.flink.python.connector.PrintTableSink()
        j_field_names = to_jarray(gateway.jvm.String, field_names)
        j_field_types = to_jarray(gateway.jvm.TypeInformation,
                                  [_to_java_type(field_type) for field_type in field_types])
        j_print_table_sink = j_print_table_sink.configure(j_field_names, j_field_types)
        super(PrintTableSink, self).__init__(j_print_table_sink)
