from pyflink.java_gateway import get_gateway
from pyflink.table import TableSink
from pyflink.table.descriptors import CustomFormatDescriptor
from pyflink.table.types import _to_java_type
from pyflink.util.utils import to_jarray


class CsvRetractTableSink(TableSink):

    def __init__(self, file_path, field_names, field_types, field_delimiter=None, line_delimiter=None):
        gateway = get_gateway()
        j_builder = gateway.jvm.org.apache.flink.python.connector.CsvRetractTableSink.Builder()
        j_builder.withFilePath(file_path)
        j_field_names = to_jarray(gateway.jvm.String, field_names)
        j_field_types = to_jarray(gateway.jvm.TypeInformation,
                                  [_to_java_type(field_type) for field_type in field_types])
        j_builder.withSchema(j_field_names, j_field_types)
        if field_delimiter is not None:
            j_builder.withFieldDelimiter(field_delimiter)
        if line_delimiter is not None:
            j_builder.withLineDelimiter(line_delimiter)
        super(CsvRetractTableSink, self).__init__(j_builder.build())


class RetractCsv(CustomFormatDescriptor):

    def __init__(self):
        gateway = get_gateway()
        format_type = \
            gateway.jvm.org.apache.flink.python.connector.CsvRetractTableSinkFactory.RETRACT_CSV
        super(RetractCsv, self).__init__(format_type, 1)

    def field_delimiter(self, delimiter):
        gateway = get_gateway()
        FORMAT_FIELD_DELIMITER = \
            gateway.jvm.org.apache.flink.python.connector.CsvRetractTableSinkFactory\
            .FORMAT_FIELD_DELIMITER
        self.property(FORMAT_FIELD_DELIMITER, delimiter)
        return self

    def line_delimiter(self, delimiter):
        gateway = get_gateway()
        FORMAT_LINE_DELIMITER = \
            gateway.jvm.org.apache.flink.python.connector.CsvRetractTableSinkFactory \
            .FORMAT_LINE_DELIMITER
        self.property(FORMAT_LINE_DELIMITER, delimiter)
        return self
