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
import pyinotify
import warctika
import re
import time

if len(sys.argv) < 2:
    print "Must give name of WARC directory to watch"
    sys.exit(1)

dirname = sys.argv[1]

# Watch the WARC directory for file creation and deletion
#log.setLevel(10)
#wm = pyinotify.WatchManager() # Watch Manager
# watched events
# TODO: Consider if we also want IN_CLOSE_WRITE (depends on the order that
# heritrix finishes writing, closes and renames the file. 
#mask = pyinotify.IN_CREATE | pyinotify.IN_MOVED_TO | pyinotify.IN_MOVED_FROM
#wm.add_watch(dirname, mask)
warcprocessor = warctika.WARCTikaProcessor()
oldsuffix = '.warc.gz'
newsuffix = '-ViaTika.warc.gz'
#handler = warctika.WARCNotifyHandler(warcprocessor=warcprocessor,
#                                     oldsuffix=oldsuffix,
#                                     newsuffix=newsuffix)
#notifier = pyinotify.Notifier(wm, handler)

# On first run,
# loop through watched directory and handle all existing
# files, in case we restarted part-way through a crawl.
# Then check forever.
while True:
    for fn in os.listdir(dirname):
        if fn.endswith(oldsuffix) and not fn.endswith(newsuffix):
            infn = dirname+"/"+fn
            outfn = re.sub(oldsuffix+'$', newsuffix, infn)
    #        if os.path.exists(outfn):
    #            print "Existing file", infn, "has already been processed. Skipping."
    #            continue
            print "Processing existing file:", infn
    #            try:
            warcprocessor.process(infn=infn, outfn=outfn)
    #        print "Not deleting:", infn
            os.unlink(infn)
    #            except Exception as e:
    #               XXX cleanup: delete -ViaTika.warc.gz file if present.
    #                print ("Warning: Startup processor failed to process "+
    #                       "file "+fn+": "+str(e)+str(e.args)+
    #                       "\n\tGiving up on it.")
    #                raise e
            print "Done."
    time.sleep(15) 

#print "Finished processing existing files. Now watching for new WARC files."
# Run forever
#notifier.loop()

