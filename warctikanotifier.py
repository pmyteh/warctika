#!/usr/bin/env python
"""Watch/notify module for use with warctika

Copyright 2014 Tom Nicholls

This work is available under the terms of the GNU General Purpose Licence
This program is free software: you can redistribute it and/or modify
it under the terms of version 2 of the GNU General Public License as published
by the Free Software Foundation.

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

#import sys
import os
import pyinotify
import re
from .warctikahanzo import WARCTikaProcessor

class WARCNotifyHandler(pyinotify.ProcessEvent):
    """Handler for pyinotify created/deleted WARC notifications."""
    def my_init(self, warcprocessor=None,
                      # Note that this does not match ".open" files
                      # so we need not worry about heritrix files
                      # in production (as long as we pick them up
                      # when they move to their final filename.
                      oldsuffix='.warc.gz',
                      newsuffix='-ViaTika.warc.gz'):
        if not warcprocessor:
            warcprocessor = WARCTikaProcessor()
        self.warcprocessor = warcprocessor
        self.oldsuffix = oldsuffix
        self.newsuffix = newsuffix
    def process_IN_CREATE(self, event):
        print "IN_CREATE called for "+event.pathname
        # If the new file is a WARC, but not a tikaed one, process it
        # TODO: Check that this is not an 
        if (event.pathname.endswith(self.oldsuffix) and not
                event.pathname.endswith(self.newsuffix)):
            try:
                self.warcprocessor.process(
                    infn=event.pathname,
                    outfn=re.sub(self.oldsuffix+'$',
                                 self.newsuffix,
                                 event.pathname))
            except Exception as e:
                print ("Warning: WARCNotifyHandler failed to process new "+
                       "file "+event.pathname+": "+str(e)+str(e.args)+
                       "\n\tGiving up on it.")
                raise e
            os.remove(event.pathname)
        print "Finished handling", event.pathname
    def process_IN_MOVE_TO(self, event):
        print "IN_MOVE_TO called for "+event.pathname
        # Treat files moved as a creation.
        self.process_IN_CREATE(event)

