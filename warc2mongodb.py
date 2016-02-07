#!/usr/bin/env python2
"""Extract text from WARC files, converting HTML to text (and
using Apache Tika to also convert Word/PDF etc. files. Send the text
to a MongoDB database with key=<url> and value=<article text>.

Requirements: MongoDB running on the standard port on localhost
              Apache Tika running as per warctika.py

Copyright 2014-2016 Tom Nicholls

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

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
import traceback
import re
import requests
import html2text
import pymongo
import argparse
from bs4 import UnicodeDammit
from bs4 import BeautifulSoup
from readability.readability import Document as ReadabilityDocument
from warctika import *
import FilterMapper
from collections import defaultdict
# These can both be installed with 'pip install warctools'. Beware that there
# are several old versions floating around under different names in the index.
from hanzo.warctools import WarcRecord
from hanzo.httptools import RequestMessage, ResponseMessage

#####
#UTILITY FUNCTIONS AND CLASSES
#####
def parse_http_response_charset(record):
    """Parses the payload of an HTTP 'response' record, returning code,
    content type, declared character set and body.

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
    charset = None
    if mime_type:
        match = re.search(r'charset=(\S+)', mime_type[0], re.I)
        if match:
            charset = match.group(1).lower()
        mime_type = mime_type[0].split(b';')[0]
    else:
        mime_type = None

    return header.code, mime_type, charset, message.get_body()

class print_counter(object):
    """Print an incrementing counter (by default to sys.stderr)"""
    def __init__(self, stream=sys.stderr):
        self.n = 0
        self.stream = stream
    def reset(self):
        self.n = 0
        self.stream.write('\n')
        self.stream.flush()
    def click(self):
        self.n += 1
        self.stream.write('\r'+str(self.n))
        self.stream.flush()

class ContentFilterBase(object):
    """Select content to discard - discard nil."""
    def discard_content(self, url, code, content):
        return False

class ContentFilterFilterMapper(ContentFilterBase):
    """Select content to discard based upon FilterMapper."""
    def discard_content(self, url, code, content):
        # TODO: Tidy this up, rather than importing from FilterMapper
        if (int(code) < 200 or int(code) > 299
            or FilterMapper.INCIDENTALREGEX.search(content)
            # Images, CSS and other fluff
            or FilterMapper.has_query(url)
            # Parameterised query URLs
            or FilterMapper.on_badlist(url)
            # url to exclude
            or FilterMapper.on_groovylist(url)):
            # url to exclude
            return True
        return False

class ContentFilterUrlSet(ContentFilterBase):
    """Select content to discard based upon a given set of acceptable URLs."""
    def __init__(self, s):
        self.urlset = s
    def discard_content(self, url, code, content):
        return url not in self.urlset


#####
#CLASSES
#####

class WARCMongoDBProcessor(object):
    """Processes WARC files by decomposing them, converting HTML and various
       proprietary document format records to plain text, then storing the
       content in a local MongoDB database.
    """

    def __init__(self,
                 collection,
                 tikaprocessor=WARCTikaProcessor(mintikalen=0),
                 dbclient=pymongo.mongo_client.MongoClient(),
                 contentfilter=ContentFilterBase()):
        self.tikaprocessor = tikaprocessor
        self.db = dbclient.warctext
        self.collection = self.db[collection]
        self.contentfilter = contentfilter
        self.pc = print_counter()
        sys.stderr.write("Initialised "+type(self).__name__+'\n')

    def html_to_text(self, body):
        raise NotImplementedError

    def save_to_db(self, url, text):
        self.collection.save({'_id' : url, 'value' : text})

    def process(self, infn, gzi='auto', delete=False):
        """Process a WARC at a given infn."""
        # These are objects of type RecordStream (or a subclass), unlike with
        # the IA library
        inwf = WarcRecord.open_archive(infn, mode='rb', gzip=gzi)
        print "Processing", infn
        for record in inwf:
            try:
#                print "\nStarting record: "+str(record.url)
                if record.get_header('WARC-Segment-Number'):
                    raise Exception("Segmented response/resource record "
                                    "for "+record.url+". Not processing.")
                # We can process resource records (and conversion records,
                # which we assume are all of resource type (contain a document
                # rather than an HTTP transaction with nested document). This
                # may be unsafe, but conversion records are almost unknown in
                # the wild. The only ones we'll be handling here are those
                # output from WarcTika, which are in that format.
                # TODO: generalise this.
                # We also handle HTTP response records.
                if (record.type == WarcRecord.RESPONSE and
                      record.url.startswith('http')):
#                    print "\nParsing HTTP response"
                    httpcode, mimetype, charset, body = parse_http_response_charset(record)

                elif (record.type == WarcRecord.RESOURCE
                      or record.type == WarcRecord.CONVERSION):
                    mimetype, body = record.content
                    httpcode = 200 # "Success" for stored content
                    charset = None # Not recorded
#                    print "\nResource or conversion"
                    
                # If 'metadata', 'request', 'revisit', 'continuation',
                # or something exotic, we can't do anything interesting
                elif (record.type == WarcRecord.METADATA
                      or record.type == WarcRecord.WARCINFO
                      or record.type == WarcRecord.REQUEST):
#                    print "Metadata, Warcinfo or Request"
                    continue
                else:
                    raise Exception("Can't handle"+str(record.type)+", "+str(record.url))

                # The input data have already been processed through Apache
                # Tika during the fetch process to minimse storage space, but
                # short text output resulted in a retention of the original
                # document to avoid data loss with image-based PDFs.
                # For dealing with text only processing, we muust do our best
                # come what may.
                # So, canonicalise the mimetype using warctika, then
                # if PDFish/Wordish, tikaise without benefit of clergy.
                tikamimetype = self.tikaprocessor.check_mimetype(mimetype)
                if tikamimetype:
#                    print "Processing via Tika"
                    try:
                        mimetype, body = self.tikaprocessor.tikaise((tikamimetype, body))
                    except WarcTikaNoResultException:
                        # Can't be Tika-d - abort
                        continue
#                    mimetype = "text/plain"

                # It's possible that the record is various kinds of junk; if
                # so, don't store it
                if not self.contentfilter.discard_content(record.url, httpcode, mimetype):
                    if 'xml' in mimetype or 'html' in mimetype or mimetype.startswith('text/'):
                        if charset is None:
                            # Try utf-8 as a first guess; the Tika output should
                            # be this at least
                            charset = 'UTF-8'
                        try:
#                            print "Trying to convert to unicode: "+str(charset)
                            # This will fail if charset is None or
                            # if the charset is wrong
                            body = unicode(body, charset)
                        except (TypeError, LookupError, UnicodeDecodeError):
                            # Convert to a unicode string, using bs4 to guess the
                            # encoding, trying the declared encoding if we have one.
                            # This turns out to be computationally expensive, though
                            # necessary for HTML2text and also useful for
                            # standardization in the database
#                            print "Converting to unicode, dammit"
                            try:
                                dammit = UnicodeDammit(body)
                                body = dammit.unicode_markup
#                                if dammit.contains_replacement_characters:
#                                    print record.url, body
                            except Exception as e:
                                sys.stderr.write("UnicodeDammit() failed on body "+str(body)+'\n')
                                raise e

                        # XXX: if HTMLish, send through html2text
                        #      if textish (afterward) then dump to MongoDB
                        #      else print note to stderr and skip
                        if 'xml' in mimetype or 'html' in mimetype:
#                            print "Converting HTML to text"
                            body = self.html_to_text(body)
                            mimetype = "text/plain"
                    if mimetype.startswith('text/'):
#                            print "Saving to MongoDB"
                        # XXX Dump to MongoDB
                        self.save_to_db(record.url, body)
#                            sys.stderr.write("Written "+str(record.url)+"\n")
                    else:
                        # Can't handle it (didn't Tika, or not Tikable)
                        pass
#                        raise Exception("Unprocessable mimetype: "+str(mimetype))
#                print "Done"
            except Exception as e:
                print ("Warning: "+type(self).__name__+".process() failed on "+
                       str(record.url))
                traceback.print_exc()

#            self.pc.click()

        print "****Finished file."
        inwf.close()

        if delete:
            print "Deleting", infn
            os.unlink(infn)
        return True

class WARCMongoDBProcessorBS(WARCMongoDBProcessor):
    """Processes WARC files by decomposing them, converting HTML and various
       proprietary document format records to plain text using BeautifulSoup,
       then storing the content in a local MongoDB database.
    """

    def __init__(self, bsparser='lxml', collection='bs', *args, **kw):
        self.bsparser = bsparser
        super(WARCMongoDBProcessorBS, self).__init__(collection=collection,
                                                     *args, **kw)
#        sys.stderr.write("Initialised WARCMongoDBProcessorBS\n")

    def html_to_text(self, body):
        soup = BeautifulSoup(body, self.bsparser)
        # kill all script and style elements, to prevent dross ending up
        # in the output
        for script in soup(["script", "style"]):
            script.extract()    # rip it out
        return unicode(soup.get_text())

class WARCMongoDBProcessorHTML2Text(WARCMongoDBProcessor):
    """Processes WARC files by decomposing them, converting HTML and various
       proprietary document format records to plain text using HTML2Text,
       then storing the content in a local MongoDB database.
    """

    def __init__(self, collection='html2text', *args, **kw):
        self.htmlprocessor = html2text.HTML2Text()
        self.htmlprocessor.ignore_links = True
        self.htmlprocessor.ignore_images = True
        self.htmlprocessor.ignore_emphasis = True
        self.htmlprocessor.re_unescape = True
        self.htmlprocessor.unicode_snob = True
        self.htmlprocessor.decode_errors = 'strict'
        super(WARCMongoDBProcessorHTML2Text,
              self).__init__(collection=collection, *args, **kw)
#        sys.stderr.write("Initialised WARCMongoDBProcessorHtml2Text\n")

    def html_to_text(self, body):
        return self.htmlprocessor.handle(body)


class WARCMongoDBProcessorReadability(WARCMongoDBProcessorBS):
    """Processes WARC files by decomposing them, converting HTML and various
       proprietary document format records to plain text using
       readability-lxml, then storing the content in a local MongoDB database.
    """

    def __init__(self, collection='readability', *args, **kw):
        super(WARCMongoDBProcessorReadability,
              self).__init__(collection=collection, *args, **kw)
#        sys.stderr.write("Initialised WARCMongoDBProcessorReadability\n")

    def html_to_text(self, body):
        """Transforms HTML to text. Extends WARCMongoDBProcessorBS by using
        readability-lxml to attempt to extract the salient parts of the page
        before processing through BeautifulSoup."""
        return super(WARCMongoDBProcessorReadability, self).html_to_text(ReadabilityDocument(body).summary())



