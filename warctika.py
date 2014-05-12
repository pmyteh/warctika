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
import atexit
import traceback
import time
import re
import requests
import fcntl
import copy
from collections import defaultdict
# These can both be installed with 'pip install warctools'. Beware that there
# are several old versions floating around under different names in the index.
from hanzo.warctools import WarcRecord
from hanzo.httptools import RequestMessage, ResponseMessage

#####
#UTILITY FUNCTIONS
#####
def parse_http_response(record):
    """Parses the payload of an HTTP 'response' record, returning code,
    content type and body.

    Adapted from github's internetarchive/warctools hanzo/warcfilter.py,
    commit 1850f328e31e505569126b4739cec62ffa444223. MIT licenced."""
    message = ResponseMessage(RequestMessage())
    remainder = message.feed(record.content[1])
    message.close()
    if remainder or not message.complete():
        if remainder:
            print 'trailing data in http response for', record.url
        if not message.complete():
            print 'truncated http response for', record.url
    header = message.header

    mime_type = [v for k,v in header.headers if k.lower() == b'content-type']
    if mime_type:
        mime_type = mime_type[0].split(b';')[0]
    else:
        mime_type = None

    return header.code, mime_type, message.get_body()


#####
#CLASSES
#####

class WarcTikaException(Exception):
    pass

class WarcTikaNoResultException(WarcTikaException):
    pass

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
                ],
                mintikalen=256):
        self._tikaurl = tikaurl
        self._mintikalen = mintikalen
        self._mimemappings = mimemappings
        self._description = (
            "Items collected with content types matching the following "
            "regular expressions have been processed by Apache Tika to "
            "attempt to produce plain text formats for storage. These "
            "processed items have been stored as WARC conversion records: ")
        for item in self._mimemappings:
            self._description += item[0]+'; '
        self._description = self._description[:-2]+'.'
        # Count of return codes
        self.tikacodes = defaultdict(int)
        self._openfiles = set()
        atexit.register(self._remove_open_files)
        print "Initialised WARCTikaProcessor"

    def process(self, infn, outfn, delete=False):
        """Process a WARC at a given infn, producing plain text via Tika
        where suitable, and writing a new WARC file to outfn."""
        # These are objects of type RecordStream (or a subclass), unlike with
        # the IA library
        inwf = WarcRecord.open_archive(infn, mode='rb')
        outf = open(outfn, 'wb')
        self._openfiles.add(outfn)
#        try:
#            fcntl.lockf(inwf.file_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
#            fcntl.lockf(outf, fcntl.LOCK_EX | fcntl.LOCK_NB)
#            # Get locks on both files
#        except IOError:
#            print ("Unable to get file locks processing", infn, "so will "
#                   "try later")
#            return False
        print "Processing", infn
        for record in inwf:
            try:
                if record.type == WarcRecord.WARCINFO:
                    self.add_description_to_warcinfo(record)
                elif (record.type == WarcRecord.RESPONSE
                      or record.type == WarcRecord.RESOURCE):
                    if record.get_header('WARC-Segment-Number'):
                        raise WarcTikaException("Segmented response/resource "
                                                "record. Not processing.")
                    else:
                        record = self.generate_new_record(record)
                # If 'metadata', 'request', 'revisit', 'continuation',
                # 'conversion' or something exotic, we can't do anything more
                # interesting than immediately re-writing it to the new file

                newrecord = WarcRecord(headers=record.headers,
                        content=record.content)

            except Exception as e:
                print ("Warning: WARCTikaProcessor.process() failed on "+
                       record.url+": "+str(e.message)+
                       "\n\tWriting old record to new WARC.")
                traceback.print_exc()
                newrecord = record
            finally:
                newrecord.write_to(outf, gzip=outfn.endswith('.gz'))
        print "****Finished file. Tika status codes:", self.tikacodes.items()
        self.tikacodes = defaultdict(int)
        inwf.close()
        outf.close()
        self._openfiles.remove(outfn)

        # Check that the file has written correctly - for an excess of caution
        validrc = os.system("warcvalid "+outfn)

        if validrc:
            print "New file", outfn, "appears not to be valid. Deleting it." 
            os.unlink(outfn)
        if delete and not validrc:
            print "Deleting", infn
            os.unlink(infn)
        return True

    def add_description_to_warcinfo(self, record):
        """Add a description of our mangling to a warcinfo record's decription
        tag, creating it if necessary"""
        if record.type != WarcRecord.WARCINFO:
            raise WarcTikaException("Non-warcinfo record passed to "
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
        # We can process resource records and HTTP response records.
        if not ((inrecord.type == WarcRecord.RESPONSE
                    and inrecord.url.startswith('http'))
                or inrecord.type == WarcRecord.RESOURCE):
            print "Can't handle", inrecord.type, inrecord.url
            return inrecord

        if inrecord.type == WarcRecord.RESOURCE:
            inmimetype, inbody = inrecord.content
        else: # inrecord.type == WarcRecord.RESPONSE (HTTP):
            _, inmimetype, inbody = parse_http_response(inrecord)

        mimetype = self.check_mimetype(inmimetype)
        if not mimetype:
            # Content-Type should not be Tikaised
            return inrecord
        try:
            outcontent = self.tikaise((mimetype, inbody), url=inrecord.url)
        except WarcTikaNoResultException:
            # Tika hasn't done the business (image PDF, unparseable source,
            # whatever. Don't report, as these are very common.
            return inrecord
        except Exception as e:
            print e, "processing", inrecord.url
            return inrecord
        outheader = self.generate_cv_header(inrecord)
        # The Content-length header is regenerated, and the Content-Type
        # header is replaced by content[0].
        return WarcRecord(headers=outheader, content=outcontent)

    def tikaise(self, content, url=None):
        """Process a file through Apache Tika, reducing to plain text
           if possible. """
        # TODO: Consider carefully whether to send Tika the filename to help
        # guess the MIME type, which can be done by setting the (unofficial)
        # {'File-Name': string} header.
        resp = requests.put(self._tikaurl, data=content[1],
                                headers={'Content-Type': content[0]}) 
        self.tikacodes[resp.status_code] += 1
        if resp.status_code != 200:
            raise WarcTikaNoResultException("Bad response code from Tika ("+
                            str(resp.status_code)+") "+
                            "trying to submit Content-Type "+content[0])
        if len(resp.content) < self._mintikalen:
            raise WarcTikaNoResultException("Content from Tika only "+
                            str(len(resp.content))+
                            " bytes. Probably image-based PDF. Using original"+
                            " record.")
#       print "Success from Tika:",url, content[0], "Length:",len(resp.content)
        return ('text/plain', resp.content)

#    def strip_header(self, obj):
#        """Strips the first HTTP/WARC header from an object, returning the
#        rest of the object."""
#        return re.split(u'\n\n', obj, maxsplit=1)[1]

    def check_mimetype(self, mimetype):
        """Return a canonical mimetype if mimetype matches our list to process,
           else False.
           None is always processable, but Tika will need to guess the type."""
#        print "check_mimetype: received", mimetype
            # Note: we can make Tika guess the Content-Type without assistance
            # by setting it to the root type 'application/octet-stream'.
            # return 'application/octet-stream'
        if mimetype is not None:
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
        newrecord = copy.copy(oldrecord)

        # Erase various headers. CONCURRENT_TO is not valid in conversion
        # records. The others are not valid once the block is processed.
        # Note that digests are optional and not regenerated.
        removelist = [WarcRecord.CONCURRENT_TO,
                      WarcRecord.BLOCK_DIGEST,
                      WarcRecord.PAYLOAD_DIGEST,
                      WarcRecord.CONTENT_LENGTH,
                      WarcRecord.CONTENT_TYPE]
        newrecord.headers = [(k, v) for (k, v) in newrecord.headers
                             if k not in removelist]
        # If we're throwing away the old record, this is only marginally
        # sensible, but the spec says "should".
        newrecord.set_header(WarcRecord.REFERS_TO,
                             newrecord.get_header(WarcRecord.ID))
        newrecord.set_header(WarcRecord.TYPE, WarcRecord.CONVERSION)
        newrecord.set_header(WarcRecord.ID, newrecord.random_warc_uuid())
        return newrecord.headers

    def _remove_open_files(self):
        """Clean up open files, if they exist"""
        try:
            os.unlink(self._openfiles.pop())
            self._remove_open_files()
        except KeyError:
            return

class WARCNonTikaProcessor(WARCTikaProcessor):
    """A dummy class for testing WARC throughput, which does everything
    WARCTikaProcessor does except the actual Tikaisation"""
    def add_description_to_warcinfo(self, record):
        pass
    def generate_new_record(self, inrecord):
        return inrecord
    def tikaise(self, content, mimetype):
        raise NotImplementedError
    def check_mimetype(self, mimetype):
        raise NotImplementedError
    def generate_cv_header(self, oldrecord):
        raise NotImplementedError
