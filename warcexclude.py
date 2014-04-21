#!/usr/bin/env python
"""Copyright 2014 Tom Nicholls

Process a list of Web ARChive files, applying various filters to exclude
records which are no longer wanted. The principal use case is to exclude junk
records where e.g. pathological recursion is detected during a crawl. A filter
attached to the crawl prevents new junk from being fetched, but this script
strips the junk out from the files already fetched.

This work is available under the terms of the GNU General Purpose Licence
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>
"""

#####
#SETUP
#####

import sys
from hanzo.warctools import WarcRecord
from warcresponseparse import *
import re

exclist = []

if len(sys.argv) < 2:
    sys.exit ("Syntax: warcexclude pattern [pattern] [...] [pattern]\n\n"
              "Where pattern is of the form field/regexp, with field being a "
              "WARC header and\nregexp being a pattern to match against. If "
              "the header's content matches the\npattern, the record is "
              "excluded. If multiple patterns are given, the record is\n"
              "excluded if all patterns match.\n\n"
              "Example pattern: "
              "WARC-Target-URI/^https?://www.example.com/.*$\n\n"
              "In addition to the WARC headers, if the payload is an HTTP "
              "response, three\nother fields are exposed: XHTTP-Response-Code "
              "contains the HTTP status code\nfrom the record, "
              "XHTTP-Content-Type contains the value of the HTTP Content-Type"
              "\nheader, and XHTTP-Body contains the full content body.\n")

for arg in sys.argv[1:]:
    if '/' not in arg:
        sys.exit("Not a valid exclusion pattern: "+str(arg))
    exclist.append(tuple(arg.split('/', 1)))
    sys.stderr.write("Excluding "+str(exclist[-1][0])+" matching "+
                     str(exclist[-1][1])+'\n')

# In theory this could be agnostic as to whether the stream is compressed or
# not. In practice, the gzip guessing code reads the stream for marker bytes
# and then attempts to rewind, which fails for stdin unless an elaborate
# stream wrapping class is set up.
inwf = WarcRecord.open_archive(file_handle=sys.stdin, mode='rb', gzip='record')
outf = sys.stdout

for record in inwf:
    # Count down matches made
    write = len(exclist)
    for tup in exclist:
        heads = [h for h in record.headers if h[0] == tup[0]]
        if (record.type == WarcRecord.RESPONSE
                and record.url.startswith('http')):
            ccode, cmime, cbody = parse_http_response(record)
            heads.append( ("XHTTP-Response-Code", ccode) )
            heads.append( ("XHTTP-Content-Type", cmime) )
            heads.append( ("XHTTP-Body", cbody) )
        for head in heads:
            if re.search(tup[1], head[1]):
                write -= 1
    if write > 0:
        # gzip could theoretically be optional, but this is normally much
        # more useful as retrospectively creating the WARC format of multiple
        # concatenated gzipped records is a pain with command-line tools.
        record.write_to(outf, gzip=True)

