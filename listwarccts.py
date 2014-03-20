#!/usr/bin/env python
"""Copyright 2014 Tom Nicholls

Process a directory of Web ARChive files through the warctika library to
reduce binary document formats to plain text 'conversion' records.

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

def warning(*objs):
    print("WARNING: ", *objs, file=sys.stderr)


if len(sys.argv) < 2:
    sys.exit("Must give name of file to list contents of.")

print("Type, URL, Underlying Content-Type")
wf = warctika.WARCFile(sys.argv[1], 'rb')
for record in wf:
    rtype = record.type
    try:
        rurl = record.url
    except Exception:
        rurl = None
    try:
        rmime = record.get_underlying_mimetype()
    except Exception:
        rmime = None
    print(rtype, rurl, rmime, sep=", ")
wf.close()
