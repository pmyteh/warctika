#!/usr/bin/env python
"""Copyright 2014 Tom Nicholls

Unpacks a warc file (optionally gzipped) into a series of uncompressed
warc files, one per each record in the original.
The aim of this process is to obtain an easily diff-able set of files.

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

from __future__ import print_function

import sys
import os
import warctika
import re
import string

def warning(*objs):
    print("WARNING: ", *objs, file=sys.stderr)

def sanitise_url(s):
    return s.translate(string.maketrans('/*#', '___'))[:200]

if len(sys.argv) < 2:
    sys.exit("Must give name of file to unpack.")

wf = warctika.WARCFile(sys.argv[1], 'rb')
for record in wf:
    if (record.type == 'metadata' or
        record.type == 'request' or
        record.type == 'warcinfo'):
        continue
    outwn = sanitise_url(record.url)+'.warc'
    outwarc = warctika.WARCFile(outwn, 'wb')
    outwarc.write_record(
			warctika.WARCRecord(header=record.header,
            payload=record.payload,
            defaults=False))
    outwarc.close()
wf.close()
