__author__ = 'zeph'
CLUSTER_HOST = 'localhost'

import os
if os.environ.get('CLUSTER_HOST') is not None:
    CLUSTER_HOST = os.environ.get('CLUSTER_HOST')

# http://code.activestate.com/recipes/66531/
class Borg:
    __shared_state = {}
    def __init__(self):
        self.__dict__ = self.__shared_state