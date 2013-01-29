__author__ = 'zeph'
import hashlib
import time
import sys
import os
import re
from datetime import datetime
from sqlhbase.intake import HBaseIntake

class MySQLRow():

    _tbl_name = ""
    _idx = -1 # used to cut off the VALUES on INSERTs
    _timestamp = 0

    def __init__(self, line):
        self._raw = line.strip()
        self._md5 =  hashlib.md5(line)
        self._action = line[:6].strip() # avoid \n of empty lines
        if line[:13] == '/*!50001 VIEW':
            self._action = line[:13].split()[1]

    def append(self, line):
        self._raw += line.strip()
        self._md5.update(line)

    def stmt(self):
        return self._raw.strip()

    def raw(self):
        idx = self._raw.rfind(";") # truncating unneeded stuff
        payload = self._raw[:idx+1]
        return payload

    def raw_sets(self):
        if not self.is_insert(): return ""
        idx_l = self._raw.find("(") # see where the VALUEs start
        idx_r = self._raw.rfind(";") # truncating unneeded stuff
        payload = self._raw[idx_l:idx_r] # leaving out the ; as well
        return payload

    def payload(self):
        no_slash_n = re.sub(r'\n ', '', self._raw[self._idx:])
        idx = no_slash_n.rfind(";") # truncating unneeded stuff
        # the trailing string ' */;' is related to the VIEW stmt
        payload = no_slash_n[:idx+1].rstrip(" */;").lstrip("AS ")
        return payload

    def __str__(self):
        return self._md5.hexdigest()

    def is_useless(self):
        if self._action[:2] == "--": return True
        if self._action[:2] == "/*": return True
        if self._action == "": return True
        return False

    def is_insert(self):
        if self._action == "INSERT": return True
        return False

    def is_create(self):
        if self._action == "CREATE": return True
        return False

    def is_view(self):
        if self._action == "VIEW": return True
        return False

    def tbl_name(self):
        if not (self.is_insert()
                or self.is_create()
                or self.is_view()): return ""

        if self.is_view(): self._idx = self._raw.find("AS")
        else: self._idx = self._raw.find("(")

        if self._tbl_name == "":
            try:
                tokens = self._raw[:self._idx].split()
                # this shall work fine as well for the VIEW
                # as the CREATE is '/*!50001'
                # with the tbl_name at the 3rd position
                self._tbl_name = tokens[2].strip("`")
            except:
                print self._raw[:self._idx].split()
                sys.exit(2)
        return self._tbl_name

    def timestamp(self):
        if self._action[:2] == "--":
            seek = "Dump completed on "
            idx = self._raw.find(seek)
            dump_time = self._raw[idx+len(seek):].strip()
            epoch = datetime.strptime(dump_time, "%Y-%m-%d %H:%M:%S").strftime("%s")
            print "TIME>", dump_time, epoch
            return epoch
        return None

class MySQLDump():
    # this is a state machine reading through the file
    _md5 = None # md5 object associated to the mysql input stream
    _row_counter = 0 # I want to know where I'm
    _forced_timestamp = "" # just in case

    def __init__(self, f_descriptor, namespace, skiptables=[], forced_timestamp = ""):
        self._skip_tables = skiptables
        self._forced_timestamp = forced_timestamp
        self._hbase = HBaseIntake(namespace)
        self._hbase.connect()
        self._md5 = hashlib.md5()
        self._namespace = namespace
        start_time = time.time()
        self.run(f_descriptor)
        # using past_row, instead of curr_row, due the variable's scope
        self._hbase.set_row_count(self._row_counter)
        self._hbase.set_md5(self._md5.hexdigest())
        self._hbase.set_parse_time(time.time() - start_time)
        self._hbase.commit(self)

    def __str__(self):
        return "MD5> " + self._md5.hexdigest() + "\nROWs> " + str(self._row_counter)

    def timestamp(self):
        if self._forced_timestamp != "":
            self._timestamp = datetime.strptime(
                self._forced_timestamp, "%Y-%m-%d").strftime("%s")
        if self._timestamp is None or self._timestamp == 0:
            raise RuntimeError("we've not extracted the timestamp of the mysqldump")
        return self._timestamp

    def run(self, f_descriptor):
        past_row = MySQLRow("") # useless line to start with
        for line in f_descriptor:
            self._row_counter += 1
            curr_row = MySQLRow(line)

            if past_row.is_create():

                if curr_row.is_useless(): # most likely the CREATE is complete
                    if os.environ.get('DEBUG') is not None:
                        print >> sys.stderr, past_row, past_row.stmt()
                    if not past_row.tbl_name() in self._skip_tables:
                        self._hbase.set_create_tbl(past_row.tbl_name(), past_row.payload())
                    elif os.environ.get('DEBUG') is not None:
                        print >> sys.stderr, "SKIP> CREATE", past_row.tbl_name()

                if (not curr_row.is_useless() and
                    not curr_row.is_create() and
                    not curr_row.is_insert()
                    ):
                    past_row.append(line)
                    curr_row = past_row

            if curr_row.is_view():
                if os.environ.get('DEBUG') is not None:
                    print >> sys.stderr, curr_row.raw()
                self._hbase.set_view(curr_row.tbl_name(), curr_row.payload())

            if curr_row.is_insert():
                if not curr_row.tbl_name() in self._skip_tables:
                    self._hbase.send(curr_row)
                elif os.environ.get('DEBUG') is not None:
                    print >> sys.stderr, "SKIP> INSERT", past_row.tbl_name()

            if os.environ.get('DEBUG') is not None:
                print >> sys.stderr, self._row_counter, curr_row, curr_row.tbl_name()
                sys.stderr.flush()

            self._md5.update(line)
            past_row = curr_row # for next line's parsing

        # last parsed line, we have (hopefully) the timestamp in it
        self._timestamp = past_row.timestamp()
