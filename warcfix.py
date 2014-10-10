#!/usr/bin/env python
"""Copyright 2014 Tom Nicholls

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

from __future__ import print_function

import sys
from hanzo.warctools import WarcRecord
import argparse

parser = argparse.ArgumentParser(description='Attempt to fix WARC files with '
    'a broken gzipped record. Most WARC tools use the iterator reader, which '
    'fails if any one of the gzip records is damaged.')
parser.add_argument('infn', help='Input gzipped WARC filename.')
parser.add_argument('outfn', help='Output gzipped WARC filename.')

args = parser.parse_args()

inwf = WarcRecord.open_archive(args.infn, gzip="auto")
outwf = open(args.outfn, 'wb')
for (offset, record, errors) in inwf.read_records(limit=None):
    # Generates an offset (or None) plus *either* a valid record (and empty
    # list for errors, *or* a list of errors (and None for record).
    if errors:
        print("warc errors at %s:%d"%(args.infn, offset), file=sys.stderr)
        print(errors, file=sys.stderr)
        break
    elif record is not None and record.validate(): # ugh name, returns errorsa
        print("warc errors at %s:%d"%(args.infn, offset), file=sys.stderr)
        print(record.validate(), file=sys.stderr)
        break
    try:
        record.validate()
        record.write_to(outwf, gzip=True)
    except IOError:
        print("Failed to read content for record. Skipping.")
inwf.close()
outwf.close()
