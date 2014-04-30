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

import sys
import os
from warctika import *
import re
import time

if len(sys.argv) < 2:
    print "Must give name of WARC directory to watch"
    sys.exit(1)

dirname = sys.argv[1]

warcprocessor = WARCTikaProcessor()
oldsuffix = '.warc.gz'
newsuffix = '-ViaTika.warc.gz'

while True:
    for fn in os.listdir(dirname):
        if fn.endswith(oldsuffix) and not fn.endswith(newsuffix):
            infn = dirname+"/"+fn
            outfn = re.sub(oldsuffix+'$', newsuffix, infn)
            if os.path.exists(outfn):
                print "File", infn, "has already been processed. Skipping."
                continue
            warcprocessor.process(infn=infn, outfn=outfn, delete=True)
            print "Done."
    time.sleep(15) 

