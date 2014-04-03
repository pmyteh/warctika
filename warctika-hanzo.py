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
from collections import defaultdict
# Move to Hanzo warctools library from IA's warc
#import hanzo.warctools as warctools
from hanzo.warctools import WarcRecord
# import WARCRecord, WARCParser, make_conversion
#from .warc import WARCFile, WARCRecord

#####
#CLASSES
#####

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
        # Count of return codes
        self.tikacodes = defaultdict(int)
        print "Initialised WARCTikaProcessor"

    def process(self, infn, outfn):
        """Process a WARC at a given infn, producing plain text via Tika
        where suitable, and writing a new WARC file to outfn."""
        # These are objects of type RecordStream (or a subclass), unlike with
        # the IA library
        inwf = WarcRecord.open_archive(infn, mode='rb')
        outwf = WarcRecord.open_archive(outfn, mode='wb')
        print "Processing %s to %s." % (infn, outfn)
        for record in inwf:
#        try:
            if record.type == WarcRecord.WARCINFO:
                self.add_description_to_warcinfo(record)
            elif (record.type == WarcRecord.RESPONSE
                    or record.type == WarcRecord.RESOURCE):
                if record.get_header('WARC-Segment-Number'):
                    print "Segmented response/resource record. Not processing."
                else:
                    record = self.generate_new_record(record)
            # If 'metadata', 'request', 'revisit', 'continuation',
            # 'conversion' or something exotic, we can't do anything more
            # interesting than immediately re-writing it to the new file

#        except Exception as e:
#            print ("Warning: WARCTikaProcessor.process() failed on "+
#                   record.header.record_id+": "+str(e.args)+", "+
#                   str(e.message)+"\n\tWriting old record to new WARC.")
#        finally:
            newrecord = WARCRecord(headers=record.header,
                    content=record.content,
                    defaults=False)
            outwf.write(newrecord)
        print "****Finished file. Tika status codes:", self.tikacodes.items()
        self.tikacodes = defaultdict(int)
        inwf.close()
        outwf.close()

    def add_description_to_warcinfo(self, record):
        """Add a description of our mangling to a warcinfo record's decription
        tag, creating it if necessary"""
        if record.type != WarcRecord.WARCINFO:
            raise Exception("Non-warcinfo record passed to "
                            "add_description_to_warcinfo")

        match = re.search(r'^(description: .*)$',
                          record.content[1], re.I | re.M)
        if match:
            # It's a bit naughty setting _content directly, but there's
            # no published interface for changing the content of a record
            # once it's been established. This is probably because most
            # WARC users are archivists.
            record._content = (record.content[0],
                               record.content[1][:match.end(0)-1] +
                                   '. ' + self._description +
                                   record.content[1][match.end(0):])
        else:
            record._content = (record.content[0],
                               "description: "+self._description+"\n"+
                                   record.content[1])

        # Recalculate the record length
        record.set_header(WarcRecord.CONTENT_LENGTH,
                          str(len(record.content[1])))


    def generate_new_record(self, inrecord):
        """Produce and return a WARC conversion record based on the given
           input WARC record. If conversion is not possible, return the
           input record."""
        # Check if handleable:
        if (not inrecord.is_http_response() # XXX warcutils fix
                and not inrecord.type == WarcRecord.RESOURCE):
            return inrecord

        inmimetype = inrecord.get_underlying_mimetype() # XXX warcutils fix
        inmimetype = self.make_canonical_mimetype(inmimetype) # XXX warcutils fix (parse using httplibs?)
        if not inmimetype:
            # Content-Type should not be Tikaised
            return inrecord
        try:
            outcontent = self.tikaise((inmimetype, inrecord.get_underlying_content())), # XXX warcutils fix
        except Exception as e:
            print e, "processing", inrecord.url
            return inrecord
        outheader = self.generate_cv_header(inrecord)
        # The Content-length header is regenerated, and the Content-Type
        # header is replaced by content[0].
        return WarcRecord(headers=outheader, content=outcontent)

    def tikaise(self, content):
        """Process a file through Apache Tika, reducing to plain text
           if possible. """
        # TODO: Consider carefully whether to send Tika the filename to help
        # guess the MIME type, which can be done by setting the (unofficial)
        # {'File-Name': string} header.
        try:
            resp = requests.put(self._tikaurl, data=content[1],
                                headers={'Content-Type': content[0]}) 
        except requests.ConnectionError:
            print "Unable to connect to Tika; will wait and retry."
            time.sleep(120)
        self.tikacodes[resp.status_code] += 1
        if resp.status_code != 200:
            raise Exception("Bad response code from Tika ("+
                            str(resp.status_code)+") "+
                            "trying to submit Content-Type "+content[0])
        print "Success from Tika:",mimetype, "Length:", len(resp.content)
        return ('text/plain', resp.content)

#    def strip_header(self, obj):
#        """Strips the first HTTP/WARC header from an object, returning the
#        rest of the object."""
#        return re.split(u'\n\n', obj, maxsplit=1)[1]

    def make_canonical_mimetype(self, mimetype):
        """Return a canonical mimetype if mimetype matches our list to process,
           else False.
           None is always processable, but Tika will need to guess the type."""
#        print "make_canonical_mimetype: received", mimetype
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

    def generate_cv_header(self, oldrecord):
        """Produce a conversion record header. See WARC spec, p.16
           Note that we do not handle Content-Length or the various
           kinds of digests as these will be automatically produced
           by the WARC record creation process with defaults=True."""
        # Build new header based upon the old header and new content
        d = copy.copy(oldrecord.header)

        # Erase various headers. CONCURRENT_TO is not valid in conversion
        # records. The others are not valid once the block is processed.
        # Note that digests are optional and not regenerated.
        removelist = [WarcRecord.CONCURRENT_TO,
                      WarcRecord.BLOCK_DIGEST,
                      WarcRecord.PAYLOAD_DIGEST,
                      WarcRecord.CONTENT_LENGTH,
                      WarcRecord.CONTENT_TYPE]
        d = [(k, v) for (k, v) in d if k not in removelist]
        # If we're throwing away the old record, this is only marginally
        # sensible, but the spec says "should".
        record.set_header(WarcRecord.REFERS_TO,
                          record.get_header(WarcRecord.ID))
        record.set_header(WarcRecord.TYPE, WarcRecord.CONVERSION)
        record.set_header(WarcRecord.ID, record.random_warc_uuid())
        return d

class WARCNonTikaProcessor(WARCTikaProcessor):
    """A dummy class for testing WARC throughput, which does everything
    WARCTikaProcessor does except the actual Tikaisation"""
    def add_description_to_warcinfo(self, record):
        pass
    def generate_new_record(self, inrecord):
        return inrecord
    def tikaise(self, content, mimetype):
        raise NotImplementedError
    def make_canonical_mimetype(self, mimetype):
        raise NotImplementedError
    def generate_cv_header(self, oldrecord):
        raise NotImplementedError

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

