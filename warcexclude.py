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
import argparse
import time

def parse_exc_args(argl, exclist=list()):
    """Given a list of patterns and an optional list of 2-tuples. If given
       a pattern of the form "XFile/filename", fetches the file and recurses
       throught it, treating each line in the file as a pattern.

       Returns: a list containing ("Header",<compiled regex>) for each item"""
    print argl
    for arg in argl:
        if '/' not in arg:
            sys.exit("Invalid exclusion pattern: "+str(arg))
        if arg.startswith('XFile/'):
            # Read file and recurse to parse it
            exclist = parse_exc_args([line.rstrip('\n')
                                        for line in open(arg[6:])],
                                     exclist)
            continue
        # Extract two parts of pattern, compile regex and write to exclist
        items = arg.split('/', 1)
        items[1] = re.compile(items[1])
        exclist.append(tuple(items))
    return exclist

def check_headers(exclist, record, just_one=False):
    """Tests the given record against the list of exclusion patterns given in
       exclist. If just_one is True, testing is optimised by returning after
       any match has been made.

       Returns: The number of matches that have been made"""
    matches = 0
    for tup in exclist:
        heads = [h for h in record.headers if h[0] == tup[0]]
        # Try to avoid processing the HTTP Response content unless we have
        # a pattern which requires it, as it's expensive.
        # This could be further optimised by caching the body etc. if
        # calculated once.
        if (record.type == WarcRecord.RESPONSE
                and record.url.startswith('http')
                and not args.do_not_expose_http_headers):
            if tup[0] == "XHTTP-Response-Code":
                ccode, _, _ = parse_http_response(record)
                heads.append( ("XHTTP-Response-Code", ccode) )
            elif tup[0] == "XHTTP-Content-Type":
                _, cmime, _ = parse_http_response(record)
                heads.append( ("XHTTP-Content-Type", cmime) )
            elif tup[0] == "XHTTP-Body":
                _, _, cbody = parse_http_response(record)
                heads.append( ("XHTTP-Body", cbody) )
        for head in heads:
            # Do the actual match
            match = tup[1].match(str(head[1]))
            if match:
                matches += 1
                # Avoid re-matching if one match hits and that's sufficient
                if just_one:
                    return matches
    return matches

#####
#ARGUMENT PARSER
#####

parser = argparse.ArgumentParser(description='Recreate a WARC record, '
           'optionally excluding records which match an arbitrary number of '
           'given header/regex pairs. If multiple patterns are given, '
           'by default exclude only if all patterns match.')
parser.add_argument("-e", "--do-not-expose-http-headers",
                    help="Don't expose additional headers if the record "
                         "payload is an HTTP response. Normally, "
                         "XHTTP-Response-Code contains the HTTP status "
                         "code from the record, XHTTP-Content-Type "
                         "contains the value of the HTTP Content-Type "
                         "header, and XHTTP-Body contains the full "
                         "content body.",
                    action="store_true") 
parser.add_argument('-i', '--in-filename', metavar='inwf',
                    help='Input WARC filename. Default: stdin.')
parser.add_argument('-o', '--out-filename', metavar='outwf',
                    help='Output WARC filename. Default: stdout.')


gzinput = parser.add_mutually_exclusive_group()
gzinput.add_argument('-gz', '--gzipped-input', action="store_true",
                    help='Treat input stream as gzipped. Default: guess, '
                         'which fails on stdin.')
gzinput.add_argument('-gp', '--plain-input', action="store_true",
                    help='Treat input stream as plain text.')

parser.add_argument('-G', '--gzipped-output', action="store_true", 
                    help='Gzip the output stream (record-wise).')

parser.add_argument('-a', '--match-any', action="store_true",
                    help='Exclude if any one pattern is matched. '
                         'Default: all')

parser.add_argument('pattern', metavar='patt', nargs='+',
               help="field/regexp, where field is a "
              "WARC header and regexp is a pattern to match against. "
              "Example pattern: WARC-Target-URI/^https?://www.example.com/.*$"
              "If the field is of the format XFile/filepath, then the given "
              "file will be loaded and each line interpreted as a pattern.")



args = parser.parse_args()

uuidsexcluded = set()

exclist = parse_exc_args(args.pattern)

# In theory this could be agnostic as to whether the stream is compressed or
# not. In practice, the gzip guessing code reads the stream for marker bytes
# and then attempts to rewind, which fails for stdin unless an elaborate
# stream wrapping class is set up.
gzi = 'auto'
if args.gzipped_input:
    gzi = 'record'
elif args.plain_input:
    gzi = False

if args.in_filename is None:
    inwf = WarcRecord.open_archive(file_handle=sys.stdin,
                                   mode='rb', gzip=gzi)
else:
    inwf = WarcRecord.open_archive(filename=args.in_filename,
                                   mode='rb', gzip=gzi)

#####
#MAIN
#####

outf = sys.stdout
if args.out_filename is not None:
    outf = open(args.out_filename, 'wb')

for record in inwf:
    # How many matches constitutes failure?
    write = len(exclist)
    if args.match_any:
        match_target = 0
    else:
        match_target = len(exclist) - 1

    # Extract "WARC-Concurrent-To" headers
    concurrentheads = {h[1] for h in record.headers
                       if h[0] == WarcRecord.CONCURRENT_TO}
    if uuidsexcluded.intersection(concurrentheads):
        # Skip records which are derivative of those excluded
        sys.stderr.write('.')
        continue
   
    matches = check_headers(exclist, record, args.match_any)

    if matches <= match_target:
        record.write_to(outf, gzip=args.gzipped_output)
        sys.stderr.write('#')
    else:
        # Don't write. Additionally, exclude all derivative records.
        sys.stderr.write('-')
        uuidsexcluded.add(record.id)
sys.stderr.write("Done.\n")

