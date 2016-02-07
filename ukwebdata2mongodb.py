#!/usr/bin/env python2
import csv
import sys
import os
from multiprocessing import Pool

def process(fn):
    from warc2mongodb import *
    cf = ContentFilterUrlSet(s)
    processorbs = WARCMongoDBProcessorBS(contentfilter=cf)
    processorrb = WARCMongoDBProcessorReadability(contentfilter=cf)
    processorbs.process(fn)
    processorrb.process(fn)

sys.stderr.write("Preparing content filter set...")
s = set()
with open('output/nodemap-sorted-filtered.tsv', 'rb') as f:
    for line in csv.reader(f, dialect='excel-tab'):
        s.update({unicode(line[0])})
sys.stderr.write(" done.\n")

fns = [x for infn in sys.stdin]
p = Pool(None)
p.map(process, fns)
print("Done!")

"""
children = 0
dataln = [[],[],[],[]]
for i, infn in enumerate(sys.stdin):
    dataln[i%4].append(infn.rstrip())

# this is stupid, but multiprocessing won't handle class
# instance methods (pickling problems) and this should
# work without refactoring the entire warc2mongodb library
# and allow us to avoid reprocessing the nodemap file each
# time (which is expensive)
for i in range(4):
    r = os.fork()
    if not r:
        # child process
        from warc2mongodb import *
        cf = contentfilterurlset(s)
        sys.stderr.write("starting beautifulsoup mapper.\n")
        processorbs = warcmongodbprocessorbs(contentfilter=cf)
        for d in dataln[0]:
            processorbs.process(d)
        sys.stderr.write("starting readability mapper.\n")
        processorrb = warcmongodbprocessorreadability(contentfilter=cf)
        for d in dataln[0]:
            processorrb.process(d)
        sys.stderr.write("done!\n")
        exit(0)
    children += 1
    # we've "given away" dataln[0]
    dataln.pop(0)

# avoid orphaning the subprocesses
while true:
    os.wait()
    sys.stderr.write("child process exited\n")
    children -= 1
    if children <= 0:
        exit(0)
"""
