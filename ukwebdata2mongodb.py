#!/usr/bin/env python2
import csv
import sys
import os
from multiprocessing import Pool
from warc2mongodb import *
from functools import partial
from itertools import imap
import pprint
import pymongo

sys.stderr.write("Preparing content filter set...")
s = set()
with open('output/nodemap-sorted-filtered.tsv', 'rb') as f:
    for line in csv.reader(f, dialect='excel-tab'):
        s.update({md5_hash(unicode(line[0].rstrip()))})
sys.stderr.write(" done.\n")

files = [infn.rstrip() for infn in sys.stdin]

mongoclient = pymongo.mongo_client.MongoClient()

# XXX Avoid re-processing in the event of errors
for i in xrange(243):
    files.pop(0)

p = Pool(8)
process = partial(warc_to_text, discardfilter=get_content_filter_keepset(s))


p.map(process, files, 1)
#for tup in imap(process, files):
#for tup in p.imap(process, files):
#    url, text = tup
#    mongoclient.warctext.bs.save({'_id' : url, 'value' :text})
sys.stderr.write("Done!\n")


