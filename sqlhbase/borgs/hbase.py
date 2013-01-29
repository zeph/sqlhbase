__author__ = 'zeph'

from happybase import Connection
from sqlhbase.borgs import *

class HBase(Borg):

    _db = {} # all the connections

    def link(self, ns):
        if ns not in self._db:
            try:
                self._db[ns] = Connection(CLUSTER_HOST, table_prefix=ns)
            except Exception, e:
                print e
                print 'export CLUSTER_HOST="yourserver-hostname", please'
        return self._db[ns]

    def __str__(self):
        return id(self._db)