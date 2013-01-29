__author__ = 'zeph'
THRIFT_PORT = 10000

from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from sqlhbase.borgs import *

class Hive(Borg):

    _db = None # DB connection

    def link(self):
        if self._db is None:
            transport = TSocket.TSocket(CLUSTER_HOST, THRIFT_PORT)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            self._db = ThriftHive.Client(protocol)
            transport.open()
        return self._db

    def __str__(self):
        return id(self._db)

    def exec_stmt(self, sql):
        try:
            self._db.execute(sql)
            return self._db.fetchAll()

        except Thrift.TException, tx:
            print '%s' % (tx.message)

        except HiveServerException:
            print "HiveServerException>", sql.strip()