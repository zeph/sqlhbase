__author__ = 'zeph'
__tbl_name__ = "MapR"
__namespace__ = "TESTING"
__rowkey__ = '1'.zfill(9)   # due to the lexicographic ordering
__family__ = 'mysql'
__column__ = __family__+':col1'
__tpad__ = 1000000000  # timestamp padding, to simulate a real one

from sqlhbase.borgs.hbase import *
import unittest

class TestThriftSimple(unittest.TestCase):

    def test_simple_create(self):
        tbl_name = "whatever"; expected_value = 'a'  # test data/values

        db = HBase().link(__namespace__)
        try: db.create_table(tbl_name, {__family__:{}})
        except: pass  # yes I know, the table is already there

        t = db.table(tbl_name)
        t.put(__rowkey__, {__column__: expected_value})  # attempting to write something
        assert t.row(__rowkey__)[__column__] == expected_value


class TestThrift(unittest.TestCase):

    def setUp(self):
        db = HBase().link(__namespace__)
        #lzo_options = dict(
        #    compression="lzo",
        #)
        gz_multiple = dict(
            max_versions=999,
            compression="gz",
            bloom_filter_type="ROW",
            block_cache_enabled=True
        )

        try: db.create_table(__tbl_name__, {__family__: gz_multiple})
        except: pass

        t = db.table(__tbl_name__)
        t.put(__rowkey__, {__column__: 'a'}, timestamp=(__tpad__+1))
        t.put(__rowkey__, {__column__: 'b'}, timestamp=(__tpad__+2))
        t.put(__rowkey__, {__column__: 'c'}, timestamp=(__tpad__+3))
        t.put(__rowkey__, {__column__: 'd'}, timestamp=(__tpad__+4))
        t.put(__rowkey__, {__column__: 'e'}, timestamp=(__tpad__+5))
        t.put(__rowkey__, {__column__: 'f'}, timestamp=(__tpad__+6))

    def tearDown(self):
        db = HBase().link(__namespace__)
        db.disable_table(__tbl_name__)
        db.delete_table(__tbl_name__)

    def test_timestamps_inconsistencies(self):
        expected_versions = 6  # as the amount we setUp()

        t = HBase().link(__namespace__).table(__tbl_name__)

        # checking that all the versions are there
        elements = t.cells(__rowkey__,
                           __column__,
                           versions=expected_versions,
                           include_timestamp=True)
        assert len(elements) == expected_versions

        # checking that we can get a version
        scanner = t.scan(timestamp=(__tpad__+3+1))  # +1
        # ... due to the timeRange() upper bound exclusion
        data = [(k,v) for k, v in scanner]
        assert len(data) == 1

        t.delete(__rowkey__,
                 columns=[__column__],
                 timestamp=(__tpad__+3))

        # checking that all the versions are there
        elements = t.cells(__rowkey__,
                           __column__,
                           versions=expected_versions,
                           include_timestamp=True)
        assert not len(elements) == (expected_versions -1)  # FAIL

        # checking that we can't find t_3
        scanner = t.scan(timestamp=(__tpad__+3)+1)  # +1
        data = [(k,v) for k, v in scanner]
        assert not len(data) == 1  # FAIL

        # checking that we can find t_4
        scanner = t.scan(timestamp=(__tpad__+4)+1)  # +1
        data = [(k,v) for k, v in scanner]
        assert len(data) == 1  # we know it will SUCCEED

        # checking that we can't find t_2
        scanner = t.scan(timestamp=(__tpad__+2)+1)  # +1
        data = [(k,v) for k, v in scanner]
        assert not len(data) == 1  # FAIL

        row = t.row(__rowkey__, timestamp=(__tpad__+4)+1, include_timestamp=True)
        assert row[__column__][0] == 'd'
        assert row[__column__][1] == __tpad__+4
        row = t.row(__rowkey__, timestamp=(__tpad__+5)+1, include_timestamp=True)
        assert row[__column__][0] == 'e'
        assert row[__column__][1] == __tpad__+5
        row = t.row(__rowkey__, timestamp=(__tpad__+6)+1, include_timestamp=True)
        assert row[__column__][0] == 'f'
        assert row[__column__][1] == __tpad__+6

        row = t.row(__rowkey__, timestamp=(__tpad__+7)+1, include_timestamp=True)  # future!
        assert row[__column__][0] == 'f'
        assert not row[__column__][1] == __tpad__+7  # FAIL

        # this is OKish, Thrift has been implemented with a scan from epoch to "until"
        # "until" excluded, this is why we +1 to include it
        row = t.row(__rowkey__, timestamp=(__tpad__+3)+1, include_timestamp=True)
        assert __column__ not in row  # FAIL

        # but these other values, at t_1, t_2 shall be returned, instead they do not
        # due to a design_bug on HBASE (DELETEs covers PUT events) -> HBASE-5154, HBASE-5241
        row = t.row(__rowkey__, timestamp=(__tpad__+2)+1, include_timestamp=True)
        assert __column__ not in row  # FAIL
        row = t.row(__rowkey__, timestamp=(__tpad__+1)+1, include_timestamp=True)
        assert __column__ not in row  # FAIL