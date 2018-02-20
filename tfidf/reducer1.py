#!/usr/bin/env python
# reducer1.py
# A simple implementation of a SumReducer.
#
# Author:   Benjamin Bengfort <benjamin@bengfort.com>
# Created:  Tue Dec 01 12:55:29 2015 -0500

"""
The sum reducer simply adds all of the values associated with a key.

To execute this mapper and associated reducer on Hadoop Streaming use the
following command:

    $ hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
        -input reuters \
        -output reuters_term_frequency \
        -mapper mapper1.py \
        -reducer reducer1.py \
        -file mapper1.py \
        -file reducer1.py \
        -file stopwords.txt \
        -file framework.py

The final result should be a sum by key for the emitted mapper.
"""

##########################################################################
## Imports
##########################################################################

from framework import Reducer

##########################################################################
## Reducing Functionality
##########################################################################

class SumReducer(Reducer):

    def reduce(self):
        for key, values in self:
            total = sum(int(count[1]) for count in values)
            self.emit(key, total)

##########################################################################
## Main Method
##########################################################################

if __name__ == '__main__':
    import sys

    reducer = SumReducer(sys.stdin)
    reducer.reduce()
