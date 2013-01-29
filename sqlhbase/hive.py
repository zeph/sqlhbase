__author__ = 'zeph'
__hive__ = 'localhost'
__hive_port__ = 10000
__mysql_family__ = "mysql"

# https://cwiki.apache.org/Hive/languagemanual-types.html
__hive_t__ = {
    'tinyint' 	: 'tinyint',
    'smallint' 	: 'smallint',
    'int'		: 'int',
    'mediumint' : 'int',
    'bigint'    : 'bigint',
    # 		    : 'boolean',
    'float'     : 'float',
    'double'	: 'double',
    'set'       : 'string',
    'tinytext'  : 'string',
    'varchar'	: 'string',
    'mediumtext': 'string',
    'text'		: 'string',
    'enum'		: 'string',
    'time'		: 'string',
    'date'      : 'string',
    'datetime'	: 'string',
    'timestamp'	: 'string',
    'decimal'   : 'string', # FIXME -> HIVE-3976
    'longblob'  : 'binary',	# from Hive 0.8
    #'datetime' : 'timestamp',	# from Hive 0.8 - FIXME : NOT WORKING
}

import os
from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

if os.environ.get('HIVE_HOST') is not None:
    __hive__ = os.environ.get('HIVE_HOST')

class HBaseHive():

    def __init__(self):
        transport = TSocket.TSocket(__hive__, __hive_port__)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        self._hive = ThriftHive.Client(protocol)
        transport.open()

    def exec_stmt(self, sql):
        try:
            self._hive.execute(sql)
            return self._hive.fetchAll()

        except Thrift.TException, tx:
            print '%s' % (tx.message)

        except HiveServerException:
            print "HiveServerException> ",sql.strip()

    def create_hive(self, namespace, tbl_name, tbl_def):
        try: self.drop_hive(namespace, tbl_name)
        except: print "NO-DROP> ", namespace, tbl_name

        tbl_namespace = namespace+"_"+tbl_name

        sql = "CREATE EXTERNAL TABLE "+tbl_namespace
        sql+= " (id int"+self.type_hive(tbl_def)+")"
        sql+= " STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'"
        sql+= ' WITH SERDEPROPERTIES ("hbase.columns.mapping" = '
        sql+= ' ":key'+self.type_hbase(tbl_def)+'")'
        sql+= ' TBLPROPERTIES ("hbase.table.name" = "'+tbl_namespace+'")'

        self.exec_stmt(sql)

    def drop_hive(self, namespace, tbl_name):
        tbl_namespace = namespace+"_"+tbl_name
        sql = "DROP TABLE `"+tbl_namespace+"`"
        self.exec_stmt(sql)


    def type_hive(self, tbl_def):
        #, title string, active tinyint
        elements = list(tbl_def)
        elements.pop(0)
        out=""
        for k,t in elements:
            out+=", `"+k+"` "+__hive_t__[t]
        return out

    def type_hbase(self, tbl_def):
        #,mysql:title,mysql:active
        elements = list(tbl_def)
        elements.pop(0)
        out=""
        for k,t in elements:
            out+=","+__mysql_family__+":"+k
        return out