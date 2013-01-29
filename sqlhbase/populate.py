__author__ = 'zeph'
__compressed_historize__ = dict(
    max_versions=999,
    compression="gz", # FIXME https://github.com/twitter/hadoop-lzo/issues/56
    bloom_filter_type="ROW",
    block_cache_enabled=True
)

NULL = "NULL"
KEYDIGITS = 9  # FIXME : hbase rowkey lexicographic

import os
import sys
import time
from sqlhbase.merge.stone import *
from sqlhbase.merge.diff import *
from sqlhbase.hive import HBaseHive
from sqlhbase.rowvalue import *

class HBaseParse:

    _create_tbl = None
    _values_tbl = None
    _connection = None  # bare connection, no reference to the namespace
    _hbase_time = 0

    def __init__(self, connection, create_tbl, values_tbl):
        self._connection = connection
        self._create_tbl = create_tbl
        self._values_tbl = values_tbl
        self._hive = HBaseHive()

    def __del__(self):
        self._connection.close()

    def create_stmt(self, sqldump_key, tbl_name):
        row = self._create_tbl.row(sqldump_key, ["tables:"+tbl_name])
        return row["tables:"+tbl_name]

    def view_stmt(self, sqldump_key, view_name):
        row = self._create_tbl.row(sqldump_key, ["views:"+view_name])
        return row["views:"+view_name]

    def inserts(self, sqldump_key, tbl_name):
        row = self._create_tbl.row(sqldump_key, ["hashes:"+tbl_name])
        if "hashes:"+tbl_name not in row:
            print >> sys.stderr, " - MISSING DATA",
            return []
        return eval(row["hashes:"+tbl_name])

    def all_inserts(self, tbl_name):
        s = self._create_tbl.scan(columns=["hashes:"+tbl_name])
        return sorted([ (k, eval(v["hashes:"+tbl_name])) for k,v in s])

    def get_tables(self, sqldump_key):
        row = self._create_tbl.row(sqldump_key, ["tables"])
        # getting the list of CREATE statements, I'd expect to
        # step on an exception when then accessing the HASHES
        # this shall work out as a sort of consistency check
        return sorted([k.split(":")[1] for k in row.keys()])

    def get_views(self, sqldump_key):
        row = self._create_tbl.row(sqldump_key, ["views"])
        return sorted([k.split(":")[1] for k in row.keys()])

    def values(self, insert_key, tbl_name):
        row = self._values_tbl.row(insert_key, ["values:"+tbl_name])
        return eval("["+row["values:"+tbl_name]+"]") # a list of tuples

    def desired_tables(self, sqldump_key, include):
        for tbl_name in self.get_tables(sqldump_key):
            if tbl_name in include:
                self.ingest(sqldump_key, tbl_name)

    def all_except_some(self, sqldump_key, exclude):
        for tbl_name in self.get_tables(sqldump_key):
            if tbl_name not in exclude:
                self.ingest(sqldump_key, tbl_name)

    def ingest(self, sqldump_key, tbl_name):
        tbl_def = schema(self.create_stmt(sqldump_key, tbl_name))

        self._hive.create_hive(self._connection.table_prefix, tbl_name, tbl_def)
        if os.environ.get('ONLYHIVE') is not None: return

        if tbl_name not in self._connection.tables():
            print "OVER-WRITING>", tbl_name, sqldump_key; sys.stdout.flush()
            self.drop_create(tbl_name)  # FIXME! we to handle it differently

        if not self.has_data(tbl_name):
            # there are no data* => RE-INGEST, due to HBASE-5241
            # * we are looking in the past, there might be
            # data if we look ahead (more recent timestamp)
            # but we want to PRESERVE those data...
            # an alternative would be to WIPE the table
            self.bulk_intake(sqldump_key, tbl_name, tbl_def)

        else: self.incremental_intake(sqldump_key, tbl_name, tbl_def)

    def bulk_intake(self, sqldump_key, tbl_name, tbl_def):
        print " --- each . (dot) might be a 16Mb chunk of data",
        print "<", "%s[%s]" % (tbl_name, sqldump_key)

        for mysql_insert_md5 in self.inserts(sqldump_key, tbl_name):
            print ".",
            sys.stdout.flush()
            b = self._connection.table(tbl_name).batch(timestamp=int(sqldump_key))
            mysql_insert_data = self.values(mysql_insert_md5, tbl_name)
            data = [map_hbase(tbl_def, row) for row in mysql_insert_data]
            self.batch(b, data)
        print ""

    def batch(self, hbase_batch, data, deletes={}):
        for row_key, columns in data:
            hbase_batch.put(row_key.zfill(KEYDIGITS), columns)

        # here are separate batches for the deletes
        #for timestamp_, list_ in deletes.iteritems():
        #    stone_batch = self._connection.table(
        #        hbase_batch.table.name, use_prefix=False
        #            ).batch(timestamp=int(timestamp_))
        #    for row in list_:
        #        row_key, columns_ = row
        #        stone_batch.delete(row_key, columns=columns_)
        #    if os.environ.get('NOSEND') is None: stone_batch.send()
        # FIXME! disabled until HBASE-5241 gets fixed

        # when moving data towards the past, we better first delete, then write
        if os.environ.get('NOSEND') is not None: print "- NOT sent! -" ; return
        else: hbase_batch.send()

    def incremental_intake(self, sqldump_key, tbl_name, tbl_def):
        stones = milestones(self.all_inserts(tbl_name))
        timestamps = [ stone[0] for stone in stones ]

        i = timestamps.index(sqldump_key)
        prev_, next_ = in_between(timestamps, i)
        print tbl_name, (i, len(timestamps)-1), (prev_, next_), stones[i]
        sys.stdout.flush()  # weird, on Jenkins stdout gets buffered

        # k, let's operate...
        table = self._connection.table(tbl_name)

        for mysql_insert_md5 in stones[i][1]:
            mysql_insert_data = self.values(mysql_insert_md5, tbl_name)
            data = [map_hbase(tbl_def, row) for row in mysql_insert_data]
            id_first, id_last =self.id_tuples(data)
            hbase_time = time.time()

            scan_prev = table.scan(row_start=str(id_first).zfill(KEYDIGITS),
                                   row_stop=str(id_last+1).zfill(KEYDIGITS),
                                   timestamp=int(prev_)+1,
                                   include_timestamp=True)
            data_prev = [(k,v) for k,v in scan_prev]

            scan_next = table.scan(row_start=str(id_first).zfill(KEYDIGITS),
                                   row_stop=str(id_last+1).zfill(KEYDIGITS),
                                   timestamp=int(next_)+1,
                                   include_timestamp=True)
            data_next = [(k,v) for k,v in scan_next]

            delta_time = int(time.time() - hbase_time)
            self._hbase_time += delta_time
            if os.environ.get('DEBUG') is not None:
                print >> sys.stderr, "\t - spent at HBASE (delta, overall) seconds:",
                print >> sys.stderr, (delta_time, self._hbase_time)
            data_diff, deletes = diff_datasets((sqldump_key,data),
                                               (prev_, data_prev),
                                               (next_, data_next))
            b = self._connection.table(tbl_name).batch(timestamp=int(sqldump_key))
            self.batch(b, data_diff, deletes)

            # weird, on Jenkins stdout gets buffered
            sys.stdout.flush()
            sys.stderr.flush()

    def id_tuples(self, data):
        if len(data) == 0: raise RuntimeError("NO DATA???")
        sorted_ids = tuple(sorted(int(d[0]) for d in data))
        return (sorted_ids[0], sorted_ids[-1])

    def has_data(self, tbl_name):
        data_scanner = self._connection.table(tbl_name).scan(
            filter="KeyOnlyFilter()",
            include_timestamp=True,
            limit=1)
        data = [(k,v) for k,v in data_scanner]
        if len(data) != 0: return True
        return False

    def drop_create(self, tbl_name):
        try: self.create(tbl_name)
        except:
            if os.environ.get('DEBUG') is not None:
                print >> sys.stderr, " - DROP/CREATE",
            self.drop(tbl_name)
            self.create(tbl_name)

    def create(self, tbl_name):
        self._connection.create_table(tbl_name,
            {'mysql': __compressed_historize__}
        )

    def drop(self, tbl_name):
        try: self._connection.disable_table(tbl_name)
        except: print tbl_name, "... already disabled, deleting"
        finally: self._connection.delete_table(tbl_name)

