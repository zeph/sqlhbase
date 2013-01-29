========
SqlHBase
========

SqlHBase is an HBase ingestion tool for MySQL generated dumps

The aim of this tool is to provide a 1:1 mapping of a MySQL table
into an HBase table, mapped on Hive (schema is handled too)

To run this requires a working HBase with Thrift enabled,
and a Hive instance, with metastore properly configured and
Thrift enabled as well. If u need I/O performance, I recommend to
look into Pig or Jython, or directly a native Map Reduce job.

SQOOP was discarded as an option, as it doesn't cope with dump files
and it does not compute the difference between dumps before ingestion.

SqlHBase does a 2 level ingestion process, described below.

"INSERT INTO `table_name` VALUE (), ()" statements are hashed
and stored (dropping anything at the left side of the first open
round bracket) as a single row into a staging table on HBase (the
md5 hash of the row is the row_key on HBase).
When multiple dumps of the same table/database are inserted, this
prevents (or at least reduce) the duplication of data on HBase side.

MySQL by default chunks rows as tuples, up to 16Mb, in a single
INSERT statement. Given that, we basically have a list of tuples:

    [(1, "c1", "c2", "c3"), (2, "c1", "c2", "c3"), ... ]

Initial attempt of parsing/splitting such a string with a regexp
failed, of course. Since a column value could contain ANYTHING,
even round brackets and quotes. This kind of language is not
recognizable by a Finite State Automata, so something else had to
be implemented, to keep track of the nested brackets for example.
A PDA (push down automata) would have helped but... as u can
look above, the syntax is exactly the one from a list of tuples
in python.... an eval() is all we needed in such a case.
(and it is also, I guess, optimized on C level by the interpreter)

To be taken in consideration that the IDs of the rows are integers
while HBase wants a string... plus, we need to do some zero padding
due to the fact that HBase does lexicographic sorting of its keys.

There are tons of threads on forums about how bad is to use a
monotonically incrementing key on HBase, but... this is what we needed.

[...]

A 2-level Ingestion Process
===========================

A staging,     -> (bin/sqlhbase-mysqlimport)
--------------------------------------------
without any kind of interpretation of the content of the MySQL dump
file apart of the splitting between schema data and raw data (INSERTs).
2 tables are created _"namespace"_creates, _"namespace"_values
The first table contains an entry/row for each dumpfile ingested,
having as a rowkey the timestamp of the day at the bottom of the dumpfile
(or a command line provided one, in case that information is missing).
Such row contains the list of hashes that for a table (see below),
a create statement for each table, and a create statement for each view,
plus some statistics related to the time of parsing of the file,
and the amount of rows it was containing, and the overall md5 hash.

A publishing,  -> (bin/sqlhbase-populate)
-----------------------------------------
given a namespace (as of initial import) and a timestamp (from a list):
 - the content of the table CREATE statement gets interpreted, the data
   types mapped from MySQL to HIVE, and the table created on HIVE.
 - if not existing, the table gets created fully, reading each 16Mb chunk
 - the table gets created with such convention: "namespace"_"table_name"
 - if the table exists, and it contains data, we compute the difference
   between the 2 lists of hashes that were created at ingestion time
 -- then we check what has already been ingested in the range of row ids
    which is contained in the mysql chunk (we took the assumption that
    mysql is sequentially dumping a table, hopefully)
 -- if a row id which is in the sequence in the database is not in the
    sequence from the chunk we are ingesting, than we might have a DELETE
    (DELETE that we do not execute on HBase due to HBASE-5154, HBASE-5241)
 -- if a row id is also in our chunk, we check each column for changes
 -- duplicated columns are removed from the list that is going to be sent
    to the server, this to avoid waste of bandwidth consumption
 - at this stage, we get a copy of the data on the next known ingestion
   date (dates are known from the list of dumps in the meta table)
 -- if data are found, each row gets diffed with the data to be ingested
    that are left from the previous cleaning... if there are real changes
    those are kept and will be sent to the HBase server for writing
    (timestamps are verified at this stage, to avoid to resend data
    that have already been written previously)

FIXME: ingesting data, skipping a day, will need proper recalculation
       of the difference of the hashes list...
       ingesting data, from a backup that was not previously ingested
       (while we kept ingesting data in the tables) will cause some
       redundant data duplicated in HBase, simply cause we do not dare
       to delete the duplicate that are "in the future"

      ...anyway, it is pretty easy to delete a table and reconstruct it
      having all the history into the staging level of HBase

Last but not least, we do parse VIEWs and apply them on HIVE
... be careful about https://issues.apache.org/jira/browse/HIVE-2055 !!!
