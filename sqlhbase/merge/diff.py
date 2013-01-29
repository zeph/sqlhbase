__author__ = 'zeph'
import os

def viewitems(d1, d2, op):
    """
    http://docs.python.org/2/library/stdtypes.html#dict.viewitems
    this method is available only from python 2.7 ...
    so here is a detection of its availability and a workaround
    """
    if hasattr(d1,"viewitems"):
        return eval("d1.viewitems() " + op + " d2.viewitems()")

    d1_j = set([ x+"|"+y for x,y in d1.iteritems()])
    d2_j = set([ x+"|"+y for x,y in d2.iteritems()])
    result_set = eval("d1_j " + op + " d2_j")
    return set(dict(z.split("|", 1) for z in result_set).iteritems())


def diff_datasets( n_, n_0, n_2):
    """
         IN: data from the mysql_dump insert statement
             data_prev, data_next contain:
              - all possible IDs in the range considered
              - pre and post milestones timings
              - timestamps associated to values

        OUT: a data epurated by values already in data_prev
             a deletion structure, with:
               - ids that were in data_prev, and are no more
               - ids that were in data_next, and shall not be*

        * apart if it matches by error the timestamp considered

    """
    when, data = n_; when = int(when)  # casting, sort of
    prev_, data_prev = n_0
    next_, data_next = n_2
    print "\t - rows", len(data),
    if (len(data) != len(data_prev)) or (len(data) != len(data_next)):
        print (len(data_prev), len(data_next)),
    d_ = dict(data); d_p = dict(data_prev); d_n = dict(data_next)

    writeme = {}
    if len(d_p) == 0: writeme = d_  # it is a big bunch of new IDs

    doomed = {}
    for k,v in d_p.iteritems():  # comparing on past_
        k_ = str(int(k))  # removing the zeroes padding
        updates = set()
        if len(v) == 0: break  # no columns/families???

        if k_ in d_:
            no_time = dict([(f, x[0]) for f,x in v.iteritems()])
            updates = viewitems(d_[k_], no_time, "-")  # difference

        else:  # we got a DELETE, this ID is not in the sequence

            if str(when) not in doomed:
                doomed[str(when)] = []
            doomed[str(when)].append((k_, None))

        """
        to be noted that this script will not identify
        the deletion of the first and the last row of a table
        since it is based on the sequence deducted from the
        first and last ID in the INSERT INTO statement we hashed
        """

        # ok to pile up here the UPDATE/INSERT, we are going to
        # remove, in the second loop, all the duplicates
        if len(updates)!=0: writeme[k_] = dict(updates)

    if len(writeme) != 0:
        print ", changed:", len(writeme), writeme.keys()[:3],

    if os.environ.get('DEBUG') is not None: print writeme

    for k,v in d_n.iteritems():  # comparing on next_
        k_ = str(int(k))  # removing the zeroes padding
        duplicates = []
        if len(v) == 0: break  # no columns/families???
        no_time = dict([(f, x[0]) for f,x in v.iteritems()])
        if k_ in d_: duplicates = viewitems(no_time, d_[k_], "&")  # intersection

        if len(duplicates) == len(v):
            # ALL COLUMNS (in this row) ARE IDENTICAL!

            column = duplicates.pop()[0]
            timestamp = v[column][1]
            if when < timestamp:

                if str(timestamp) not in doomed:
                    doomed[str(timestamp)] = []
                # we have a ROW, in the future, identical
                doomed[str(timestamp)].append((k_, None))

            if k_ in writeme:
                del writeme[k_]  # avoid DUPLICATEs

        else:
            for column, value in duplicates:
                timestamp = v[column][1]
                if when < timestamp:

                    if str(timestamp) not in doomed:
                        doomed[str(timestamp)] = []
                    # we have a value, in the future, identical
                    doomed[str(timestamp)].append((k_, [column]))

    if len(doomed) != 0:
        print ", DEL:", dict((t,len(v)) for t,v in doomed.iteritems()),

    print ", I/U:", len(writeme)
    # http://happybase.readthedocs.org/en/latest/api.html#happybase.Table.delete

    return (writeme.items(), doomed)
