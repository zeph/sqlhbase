__author__ = 'zeph'
import time

def milestones(days):
    """
     IN: an array of tuples (timestamp, hashes)
    OUT: an array of tuples (timestamp, hashes) containing only
         the events that brought different hashes (t - t_-1)
         - we shall also compute t_-1 -t to identify DELETEs
    """
    yesterday_hashes = []
    calendar = []
    for day in days:
        timestamp,hashes = day
        print ">", timestamp,
        if hashes!=yesterday_hashes:
            # FIXME! this is actually a BUG, we need to check what is in the DB
            # ...we can't just compute the difference between 2 assumptions
            stone = list(set(hashes)-set(yesterday_hashes))
            if len(stone) != 0 : calendar.append((timestamp, stone)); print "*",
            print ""
        yesterday_hashes = hashes
    return calendar

def in_between(t_list, i):
    """
        IN: an ordered array of timestamps, index position to be considered
            (timestamps are represented as strings, since they are HBase's keys)
       OUT: timestamp preceding the index, timestamp following
            ...using (epoch, now) as extremes

    """
    now = int(time.time())
    epoch = 0
    next_ = t_list[i+1] if len(t_list)>i+1 else str(now)
    prev_ = t_list[i-1] if i-1 > -1 else str(epoch)
    return (prev_, next_)