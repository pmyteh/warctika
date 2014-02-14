#!/usr/bin/env python
"""Watch a directory for new WARC files, then process them by extracting
non-text content with Apache Tika and re-writing a WARC file with
transformation records in place of the original.

Requirements: TikaJAXRS running on a given port and auto-reloaded.

Copyright 2014 Tom Nicholls

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
import pyinotify
import warc
import warctika
import re


if len(sys.argv) < 2:
    print "Must give name of WARC directory to watch"
    sys.exit(1)

dirname = sys.argv[1]

# Watch the WARC directory for file creation and deletion
#log.setLevel(10)
wm = pyinotify.WatchManager() # Watch Manager
# watched events
# TODO: Consider if we also want IN_CLOSE_WRITE (depends on the order that
# heritrix finishes writing, closes and renames the file. 
mask = pyinotify.IN_CREATE | pyinotify.IN_MOVED_TO
wm.add_watch(dirname, mask)
warcprocessor = warctika.WARCTikaProcessor()
oldsuffix = 'warc.gz'
newsuffix = '-ViaTika.warc.gz'
handler = warctika.WARCNotifyHandler(warcprocessor=warcprocessor,
                                     oldsuffix=oldsuffix,
                                     newsuffix=newsuffix)
notifier = pyinotify.Notifier(wm, handler)

# On first run, loop through watched directory and handle all existing
# files, in case we restarted part-way through a crawl.
for fn in os.listdir(dirname):
    if fn.endswith(oldsuffix) and not fn.endswith(newsuffix):
        print "Processing existing file:"+dirname+"/"+fn
#            try:
        print dirname
        warcprocessor.process(
            infn=dirname+"/"+fn,
            outfn=re.sub(oldsuffix+'$', newsuffix, dirname+"/"+fn) )
#            except Exception as e:
#                print ("Warning: Startup processor failed to process "+
#                       "file "+fn+": "+str(e)+str(e.args)+
#                       "\n\tGiving up on it.")
#                raise e

# Run forever
notifier.loop()

