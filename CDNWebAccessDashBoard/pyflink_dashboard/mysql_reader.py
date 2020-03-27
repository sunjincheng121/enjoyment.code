import pymysql


class MysqlDataProvider(object):

    def __init__(self, host, port, user, passwd, db, table):
        self.table = table
        self.conn = \
            pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db, autocommit=True)

    def __next__(self):
        cursor = self.conn.cursor()
        cursor.execute("select * from %s" % self.table)
        data = cursor.fetchall()
        cursor.close()
        csv = ""
        for row in data:
            csv += ",".join([str(field) for field in row]) + "\n"
        return csv

    def next(self):
        return self.__next__()

    def clear(self):
        cursor = self.conn.cursor()
        cursor.execute("truncate %s" % self.table)
        cursor.close()


if __name__ == "__main__":
    data_provider = MysqlDataProvider()
    data = data_provider.next()
    print(data)

