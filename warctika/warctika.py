#!/usr/bin/env python
"""Watch a directory for new WARC files, then process them by extracting
non-text content with Apache Tika and re-writing a WARC file with
transformation records in place of the original.

Requirements: TikaJAXRS running on a given port and auto-reloaded.

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
import time
import pyinotify
import re
import requests
import copy
from .warc import WARCFile, WARCRecord


#####
#CLASSES
#####

# Do some ugly monkey-patching to make the warc classes work better. This should
# probably be forked, patched and pushed back upstream.

# class FilePart:
#     def __len__:
#        return _private_len_variable


class WARCTikaProcessor:
    """Processes WARCs by decomposing them, sending the records through
       Apache Tika to produce plain text, then reconstructing a WARC file
       with appropriate Transformation records.

       tikaurl: URL of an instance of Tika's JAX-RS server for processing;
       mimemappings: a list regex/content-type tuples. The regex should
           match the Content-Types you wish to process, with the
           corresponding content-type being the "canonical" type for that
           regex to send to Tika. If the second part of the tuple is "None",
           then the original content type is sent, which is useful for
           matching lengthy and variable content-types such as the
           application/vnd.openxmlformats-officedocument.* types."""
    def __init__(
            self,
            tikaurl='http://localhost:9998/tika',
            mimemappings=[
                # Content-Types taken from a crawl of .gov.uk.
                # It is astonishing what junk some web servers will supply
                # for a Content-Type.
                (r'^application/pdf$',
                    'application/pdf'),
                (r'^application/(x-)?(vnd\.?)?(ms-?)?(excel)|(xls)',
                    'application/vnd.ms-excel'),
                (r'^application/(x-)?(vnd\.?)?(ms-?)?(powerpoint)|(pps)|(ppt)',
                    'application/vnd.ms-powerpoint'),
                (r'^application/(x-)?(vnd\.?)?(ms-?)?(word$)|(doc$)',
                    'application/msword'),
                (r'^application/vnd\.openxmlformats-officedocument',
                    None),
                (r'^((text)|(application))/((rtf)|(richtext))$',
                    'text/rtf'),
                (r'^application/vnd\.oasis\.opendocument',
                    None),
                (r'^acrobat$',
                    'application/pdf')
                ]):
        self._tikaurl = tikaurl
        self._mimemappings = mimemappings
        self._description = (
            "Items collected with content types matching the following "
            "regular expressions have been processed by Apache Tika to "
            "produce plain text formats for storage. These processed items "
            "have been stored as WARC conversion records: ")
        for item in self._mimemappings:
            self._description += item[0]+'; '
        self._description = self._description[:-2]+'.'
        print "Initialised WARCTikaProcessor"

    def process(self, infn, outfn):
        """Process a WARC at a given infn, producing plain text via Tika
        where suitable, and writing a new WARC file to outfn."""
        inwarc = WARCFile(infn, 'rb')
        outwarc = WARCFile(outfn, 'wb')
        print "Processing %s to %s." % (infn, outfn)
        for record in inwarc:
#            print "Processing "+record.type
#        try:
            if record.type == 'warcinfo':
                self.add_description_to_warcinfo(record)
            elif record.type == 'response' or record.type == 'resource':
                if 'WARC-Segment-Number' in record.header:
                    raise Exception("Segmented record. Skipping.")
                record = self.generate_new_record(record)
            # If 'metadata', 'request', 'revisit', 'continuation',
            # 'conversion' or something exotic, we can't do anything more
            # interesting than immediately re-writing it to the new file
            else:
                pass
#        except Exception as e:
#            print ("Warning: WARCTikaProcessor.process() failed on "+
#                   record.header.record_id+": "+str(e.args)+", "+
#                   str(e.message)+"\n\tWriting old record to new WARC.")
#        finally:
            outwarc.write_record(
					WARCRecord(header=record.header,
                    payload=record.payload,
                    defaults=False))
        inwarc.close()
        outwarc.close()

    def add_description_to_warcinfo(self, record):
        """Add a description of our mangling to a warcinfo record's decription
        tag, creating it if necessary"""
        if record.type != 'warcinfo':
            raise Exception("Non-warcinfo record passed to "
                            "add_description_to_warcinfo")

        match = re.search(r'^(description: .*)$', record.payload, re.I | re.M)
        if match:
            record.payload = (record.payload[:match.end(0)-1] +
                              '. ' +
                              self._description +
                              record.payload[match.end(0):])
        else:
            record.payload = ("description: "+self._description+"\n"+
                              record.payload)
#        print record.payload

        # Recalculate the record length
        record.header['Content-Length'] = str(len(record.payload))


    def generate_new_record(self, inrecord):
        """Produce and return a WARC conversion record based on the given
           input WARC record. If conversion is not possible, return the
           input record."""
        # Check if handleable:
        if not inrecord.is_http_response() and not inrecord.type == 'resource':
            return inrecord

        inmimetype = inrecord.get_underlying_mimetype()
        inmimetype = self.make_canonical_mimetype(inmimetype)
        if not inmimetype:
            # Content-Type should not be Tikaised
            return inrecord
        outcontent = self.tikaise(inrecord.get_underlying_content(), inmimetype)
        outheader = self.generate_cv_header(inrecord.header)
        # defaults=true ensures (amongst other things) that the content-length
        # field is regenerated.
#        print outcontent, str(outcontent)
        return WARCRecord(outheader, payload=outcontent, defaults=True)

    def tikaise(self, content, mimetype):
        """Process a file through Apache Tika, reducing to plain text
           if possible. """
        # TODO: Consider carefully whether to send Tika the filename to help
        # guess the MIME type, which can be done by setting the (unofficial)
        # {'File-Name': string} header.
        try:
            resp = requests.put(self._tikaurl, data=content,
                                headers={'Content-Type': mimetype}) 
        except requests.ConnectionError:
            print "Unable to connect to Tika; will wait and retry."
            time.sleep(120) 
        if resp.status_code != 200:
            raise Exception("Bad response code from Tika ("
                            +resp.status_code+")")
        return resp.content

#    def strip_header(self, obj):
#        """Strips the first HTTP/WARC header from an object, returning the
#        rest of the object."""
#        return re.split(u'\n\n', obj, maxsplit=1)[1]

    def make_canonical_mimetype(self, mimetype):
        """Return a canonical mimetype if mimetype matches our list to process,
           else False.
           None is always processable, but Tika will need to guess the type."""
        if mimetype is None:
            # Note: we can make Tika guess the Content-Type without assistance
            # by setting it to the root type 'application/octet-stream'.
            # return 'application/octet-stream'
            # Leave it as it is.
            return False
        for tup in self._mimemappings:
            if re.search(tup[0], mimetype, re.IGNORECASE):
                if tup[1] is None:
                    return mimetype
                return tup[1]
        return False

    def generate_cv_header(self, oldheader):
        """Produce a conversion record header. See WARC spec, p.16
           Note that we do not handle Content-Length or the various
           kinds of digests as these will be automatically produced
           by the WARC record creation process with defaults=True."""
        # Build new header based upon the old header and new content
        d = copy.copy(oldheader)

        # Not valid in conversion records
        d.pop('WARC-Concurrent-To', None)
        # Not valid once we've processed the block
        d.pop('WARC-Block-Digest', None)
        d.pop('WARC-Payload-Digest', None)
        d.pop('Content-Length', None)
        d.pop('WARC-Refers-To', None)
        # If we're throwing away the old record, this is only marginally
        # sensible, but the spec says "should".
        d['WARC-Refers-To'] = d.pop('WARC-Record-ID', None)

        # New type and payload Content-Type.
        d['WARC-Type'] = "conversion"
        d['Content-Type'] = "text/plain"
        #d['WARC-Record-ID'] = "<urn:uuid:%s>" % uuid.uuid1()
        return d

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
            self.warcprocessor = WARCTikaProcessor()
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

