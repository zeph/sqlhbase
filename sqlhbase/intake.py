__author__ = 'zeph'
__hbase__ = "localhost"
__createtable__ = 'create'
__valuestable__ = 'values'

__compressed_nohistory__ = dict(
    max_versions=1,
    compression="gz", # FIXME https://github.com/twitter/hadoop-lzo/issues/56
)

import happybase
import time
import os
from datetime import datetime
from sqlhbase.populate import HBaseParse

if os.environ.get('HBASE_HOST') is not None:
    __hbase__ = os.environ.get('HBASE_HOST')

class HBaseIntake():

    _tables = {} # create TABLE statements
    _views = {}  # create VIEW  statements
    _hashes = {} # INSERT statements
    _meta = {"status": "to_parse"}
    _results = "" # to be used with a JSON parser, for debugging
    _connection = None

    def __init__(self, namespace=""):
        self._namespace = namespace

    def connect(self):
        if self._namespace == "": raise RuntimeError("Hey! U shall select a DB first")
        self._connection = happybase.Connection(__hbase__, table_prefix="_"+self._namespace)
        self._connection.open()
        if __createtable__ not in self._connection.tables():
            # :key == timestamp (at "tail -1"), to have a sorted table
            self._connection.create_table( __createtable__,
                {'tables': __compressed_nohistory__, # tbl_name => statement
                 'views' : __compressed_nohistory__, # viewname => statement
                 'hashes': __compressed_nohistory__, # tbl_name => {insert hashes}
                 'meta'  : __compressed_nohistory__} # rowcount, parsetime, md5
                # additional 'meta': status (to_parse, skip, parsing, ingested)
            )

        if __valuestable__ not in self._connection.tables():
            self._connection.create_table( __valuestable__,
                # the md5 on the stmt is going to be the hbase's key
                {'values': __compressed_nohistory__} # tbl_name => insert statement
            )

        self._create_tbl = self._connection.table(__createtable__)
        self._values_tbl = self._connection.table(__valuestable__)

    def send(self, sql_row):
        tbl_name = sql_row.tbl_name()
        if os.environ.get('NOSEND') is None:
            self._values_tbl.put(str(sql_row), {'values:'+tbl_name : sql_row.raw_sets()})
        if tbl_name not in self._hashes:
            self._hashes[tbl_name] = []
        self._hashes[tbl_name].append(str(sql_row))

    def commit(self, sql_dump):
        tables = dict(('tables:'+k, str(self._tables[k])) for k in self._tables)
        views  = dict(('views:' +k, str(self._views[k] )) for k in self._views)
        hashes = dict(('hashes:'+k, str(self._hashes[k])) for k in self._hashes)
        meta   = dict(('meta:'  +k, str(self._meta[k]  )) for k in self._meta)
        data = dict(dict(dict(tables, **views), **hashes), **meta)
        #if os.environ.get('DEBUG') is not None: print >> sys.stderr, data
        self._create_tbl.put( str(sql_dump.timestamp()), data )

    def set_row_count(self, row_count):
        self._meta["rowcount"] = row_count

    def set_md5(self, md5_hex):
        self._meta["md5"] = md5_hex

    def set_create_tbl(self, tbl_name, create_stmt):
        self._tables[tbl_name] = create_stmt

    def set_view(self, tbl_name, create_stmt):
        self._views[tbl_name] = create_stmt

    def set_parse_time(self, enlapsed_time):
        self._meta["parsetime"] = round(enlapsed_time,1)

    def get_dumps(self):
        self._results = {}
        for k,v in self._create_tbl.scan(columns=["meta"]):
            self._results[k] = v
        readable = [(k, datetime.fromtimestamp(int(k)).isoformat(' ')) for k in self._results.keys()]
        return sorted([(k,v) for k,v in readable])

    def get_namespaces(self):
        connection = happybase.Connection(__hbase__)
        connection.open()
        n_spaces = []
        for t in connection.tables():
            chunks = t.split("_")
            if (len(chunks)>0) and (chunks[0] == "") and (chunks[1] != ""):
                ns = chunks[1]
                if ns not in n_spaces: n_spaces += [ns]
        connection.close()
        return n_spaces

    def parse(self, row_key, exclude_filename="", include_filename=""):
        start_time = time.time()
        exclude = self.read_list(exclude_filename)
        include = self.read_list(include_filename)
        parser = self.cls_parser()
        if len(include) != 0: parser.desired_tables(row_key, include)
        else: parser.all_except_some(row_key, exclude)
        due_time = round(time.time() - start_time,1)
        parser.__del__() # ensuring we take it down, connection included
        return "Parsing took " + str(due_time) + " secs"

    def read_list(self, filename):
        if filename == "": return []
        tables = []
        try:
            f = open(filename)
            for table in f:
                tables.append(table.strip())
            f.close()
        except:
            raise RuntimeError("something went from with: "+filename)
        return tables

    def cls_parser(self):
        return HBaseParse(
            happybase.Connection(__hbase__, table_prefix=self._namespace),
            self._create_tbl,
            self._values_tbl,
        )
