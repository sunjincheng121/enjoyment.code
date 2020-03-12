import os
import posixpath
import socket
import tempfile
import threading
from atexit import register
from contextlib import closing
from http.server import HTTPServer

from future.backports.urllib import parse as urllib_parse
from future.backports.http.server import SimpleHTTPRequestHandler
from pyflink.java_gateway import get_gateway
from pyflink.table.descriptors import CustomConnectorDescriptor

from pyflink.demo.csv_retract_table_sink import CsvRetractTableSink
from pyflink.table import TableSink


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def get_temp_file_path():
    file_path = tempfile.mktemp()

    @register
    def remove_tmp(*args, **kwargs):
        if os.path.exists(file_path):
            os.remove(file_path)

    return file_path


class SimpleHttpSeverWithData(SimpleHTTPRequestHandler):
    data_path = None

    def translate_path(self, path):
        """Translate a /-separated PATH to the local filename syntax.

        Components that mean special things to the local file system
        (e.g. drive or directory names) are ignored.  (XXX They should
        probably be diagnosed.)

        """
        # abandon query parameters
        path = path.split('?', 1)[0]
        path = path.split('#', 1)[0]
        path = posixpath.normpath(urllib_parse.unquote(path))
        words = path.split('/')
        words = filter(None, words)
        base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "chart")
        path = base_dir
        for word in words:
            drive, word = os.path.splitdrive(word)
            head, word = os.path.split(word)
            if word in (os.curdir, os.pardir):
                continue
            path = os.path.join(path, word)
        if path == os.path.join(base_dir, "data"):
            path = self.data_path
        return path

    def log_message(self, format, *args):
        """
        make it quiet.
        """
        pass


HOSTNAME = "localhost"


def start_chart_server(middle_file_path, server_port, hostname=None):
    if hostname is None:
        hostname = HOSTNAME

    SimpleHttpSeverWithData.data_path = middle_file_path
    server_address = (hostname, server_port)
    with HTTPServer(server_address, SimpleHttpSeverWithData) as httpd:
        httpd.serve_forever()


def start_chart_server_async(middle_file_path, server_port, hostname=None):
    thread = threading.Thread(target=start_chart_server, name="chart-http-server",
                              args=(middle_file_path, server_port, hostname))
    thread.setDaemon(True)
    thread.start()


class ChartTableSink(TableSink):
    """
    only used in local demo, first field must be a label, second field must be a number.
    """

    def __init__(self, field_names=None, field_types=None, middle_file_path=None, server_port=None):
        if middle_file_path is None:
            middle_file_path = tempfile.mktemp()

            @register
            def remove_tmp(*args, **kwargs):
                os.remove(middle_file_path)

        if server_port is None:
            server_port = find_free_port()
        file_sink = CsvRetractTableSink(middle_file_path, field_names, field_types)
        start_chart_server_async(middle_file_path, server_port)
        print("The chart URL is: http://%s:%d/" % (HOSTNAME, server_port))
        super(ChartTableSink, self).__init__(file_sink._j_table_sink)


class ChartConnector(CustomConnectorDescriptor):

    def __init__(self):
        gateway = get_gateway()
        connector_type = \
            gateway.jvm.org.apache.flink.python.connector.CsvRetractTableSinkFactory.RETRACT_CSV
        super(ChartConnector, self).__init__(connector_type, 1, False)
        connector_path = \
            gateway.jvm.org.apache.flink.table.descriptors.FileSystemValidator.CONNECTOR_PATH
        middle_file_path = get_temp_file_path()
        self.property(connector_path, middle_file_path)
        server_port = find_free_port()
        start_chart_server_async(middle_file_path, server_port)
        print("The chart URL is: http://%s:%d/" % (HOSTNAME, server_port))
