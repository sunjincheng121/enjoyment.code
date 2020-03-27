import os
import posixpath
import socket
import tempfile
import threading
import urllib.parse
from atexit import register
from contextlib import closing
from http.server import HTTPServer

from urllib import parse as urllib_parse
from http.server import SimpleHTTPRequestHandler

from pyflink_dashboard.kafka_producer import KafkaMsgProducer

from pyflink_dashboard.mysql_reader import MysqlDataProvider


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
    data_path = get_temp_file_path()
    data_provider = None  # type: MysqlDataProvider
    kafka_producer = None  # type: KafkaMsgProducer

    def translate_path(self, path):
        """Translate a /-separated PATH to the local filename syntax.

        Components that mean special things to the local file system
        (e.g. drive or directory names) are ignored.  (XXX They should
        probably be diagnosed.)

        """
        # abandon query parameters
        init_path = path
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
            if os.path.exists(self.data_path):
                os.remove(self.data_path)
            data = self.data_provider.next()
            f = open(self.data_path, "w+")
            f.write(data)
            f.close()
        if path == os.path.join(base_dir, "send"):
            param = init_path.split('?', 1)[1]
            param = param.split('&', 1)[0]
            param = param.split("=", 1)[1]
            param = urllib.parse.unquote(param)
            self.kafka_producer.send(param)
            path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "__init__.py")
        if path == os.path.join(base_dir, "clear"):
            self.data_provider.clear()
            path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "__init__.py")
        return path

    def log_message(self, format, *args):
        """
        make it quiet.
        """
        pass


HOSTNAME = "localhost"


def start_chart_server(data_provider, kafka_producer, server_port, hostname=None):
    if hostname is None:
        hostname = HOSTNAME

    SimpleHttpSeverWithData.data_provider = data_provider
    SimpleHttpSeverWithData.kafka_producer = kafka_producer
    server_address = (hostname, server_port)
    with HTTPServer(server_address, SimpleHttpSeverWithData) as httpd:
        httpd.serve_forever()


def start_chart_server_async(data_provider, kafka_producer, server_port, hostname=None):
    thread = threading.Thread(target=start_chart_server, name="chart-http-server",
                              args=(data_provider, kafka_producer, server_port, hostname))
    thread.setDaemon(True)
    thread.start()
