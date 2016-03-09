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
#import html2text
import pymongo
import argparse
from bs4 import UnicodeDammit
from bs4 import BeautifulSoup
from readability.readability import Document as ReadabilityDocument
#from warctika import *
from collections import defaultdict
from functools import partial
# These can both be installed with 'pip install warctools'. Beware that there
# are several old versions floating around under different names in the index.
from hanzo.warctools import WarcRecord
from hanzo.httptools import RequestMessage, ResponseMessage
import hashlib

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

def tikaise(mimetype, body, url='http://localhost:9998/tika'):
    """Process a file through Apache Tika, reducing to plain text
       if possible.

       :mimetype: an HTTP Content-Type
       :body:     the document body
       :url:      the Tika server's URL (default: http://localhost:9998/tika)
    """
    resp = requests.put(url, data=body, headers={'Content-Type': mimetype}) 
    if resp.status_code != 200:
        raise Exception("Bad response code from Tika ("+
                        str(resp.status_code)+") "+
                        "trying to submit Content-Type "+mimetype)
    return ('text/plain', resp.content)

#    def strip_header(self, obj):
#        """Strips the first HTTP/WARC header from an object, returning the
#        rest of the object."""
#        return re.split(u'\n\n', obj, maxsplit=1)[1]

_mimemappings=[
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
    ]

def check_mimetype(mimetype):
    """Return a canonical mimetype if mimetype matches our list to process,
       else False.
       None is always processable, but Tika will need to guess the type."""
#        print "check_mimetype: received", mimetype
        # Note: we can make Tika guess the Content-Type without assistance
        # by setting it to the root type 'application/octet-stream'.
        # return 'application/octet-stream'
    if mimetype is not None:
        for tup in _mimemappings:
            if re.search(tup[0], mimetype, re.IGNORECASE):
                if tup[1] is None:
                    return mimetype
                return tup[1]
    return False

def md5_hash(s):
    return hashlib.md5(s).digest()

def content_filter_set(s, mode, url, code, content):
    if mode == 'keep':
        return md5_hash(url) not in s
    elif mode == 'drop':
        return md5_hash(url) in s
    else:
        raise Exception("mode must be 'keep' or 'drop'")

def get_content_filter_keepset(s):
    return partial(content_filter_set, s, 'keep')

def get_content_filter_dropset(s):
    return partial(content_filter_set, s, 'drop')

def doc_from_warc(infn, gzip='auto'):
    """Generator to process a WARC at a given infn."""
    # These are objects of type RecordStream (or a subclass), unlike with
    # the IA library
    inwf = WarcRecord.open_archive(infn, mode='rb', gzip=gzip)
    sys.stderr.write("Processing "+str(infn)+"\n")
    for record in inwf:
#                print "\nStarting record: "+str(record.url)
        try:
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
                httpcode, mimetype, charset, body = parse_http_response_charset(record)

            elif (record.type == WarcRecord.RESOURCE
                  or record.type == WarcRecord.CONVERSION):
                mimetype, body = record.content
                httpcode = 200 # "Success" for stored content
                charset = None # Not recorded
                
            # If 'metadata', 'request', 'revisit', 'continuation',
            # or something exotic, we can't do anything interesting
            elif (record.type == WarcRecord.METADATA
                  or record.type == WarcRecord.WARCINFO
                  or record.type == WarcRecord.REQUEST):
                continue
            else:
                sys.stderr.write("Can't handle"+str(record.type)+", "+str(record.url))
            yield (record.url, mimetype, body, httpcode, charset)
        except Exception:
            # General catch to avoid multiprocessing taking down the whole job
            # for one bogus record
            sys.stderr.write("\n\n***** Uncaught exception reading "+record.url
                             +" from file "+infn+":\n")
            traceback.print_exc()
            sys.stderr.write("Continuing.\n\n\n")
    inwf.close()
    

def doc_to_unicode(body, charset):
    if charset is None:
        # Try utf-8 as a first guess; the Tika output should
        # be this at least
        charset = 'UTF-8'
    try:
        # This will fail if charset is None or
        # if the charset is wrong
        body = unicode(body, charset)
    except (TypeError, LookupError, UnicodeDecodeError):
        # Convert to a unicode string, using bs4 to guess the
        # encoding, trying the declared encoding if we have one.
        # This turns out to be computationally expensive, though
        # necessary for HTML2text and also useful for
        # standardization in the database
        dammit = UnicodeDammit(body)
        body = dammit.unicode_markup
#           if dammit.contains_replacement_characters:
#               print record.url, body
    return body


def bs_html_to_better_text(body, bsparser='lxml'):
    soup = BeautifulSoup(body, bsparser)
    # kill all script and style elements, to prevent dross ending up
    # in the output
    for script in soup(["script", "style"]):
        script.extract()    # rip it out
    return unicode(soup.get_text())


def rb_html_to_text(body):
    return bs_html_to_better_text(ReadabilityDocument(body).summary())


def warc_to_text(infn, discardfilter=get_content_filter_dropset({}),
                 html_to_text=bs_html_to_better_text,
                 gzi='auto'):
    """Process a WARC at a given infn to (url, text) tuples."""
    mongoclient = pymongo.mongo_client.MongoClient()
    for (url, mimetype, body, httpcode, charset) in doc_from_warc(infn):
        # The input data have already been processed through Apache
        # Tika during the fetch process to minimse storage space, but
        # short text output resulted in a retention of the original
        # document to avoid data loss with image-based PDFs.
        # For dealing with text only processing, we muust do our best
        # come what may.
        # So, canonicalise the mimetype using warctika, then
        # if PDFish/Wordish, tikaise without benefit of clergy.

        try:
            tikamimetype = check_mimetype(mimetype)
            if tikamimetype:
                try:
                    mimetype, body = tikaise(tikamimetype, body)
                except Exception:
                    # Can't be Tika-d - abort
                    continue

            # It's possible that the record is various kinds of junk; if
            # so, don't store it
            if discardfilter(url, httpcode, mimetype):
                continue

            # If its not vaguely text-y, we don't want to know
            if ('xml' not in mimetype and 'html' not in mimetype
                    and not mimetype.startswith('text/')):
                continue

            try:
                body = doc_to_unicode(body, charset)
            except Exception:
                # Sometimes this just doesn't work. Carry on anyway if possible.
                pass

            # If HTMLish, make it textish
            if 'xml' in mimetype or 'html' in mimetype:
                try:
                    body = html_to_text(body)
                    mimetype = "text/plain"
                except Exception as e:
                    # This is variably successful with random input
                    # if it fails, give up
                    continue

            # If we're here it's (now) textish, so save it
            try:
                # TODO: Abstract collection etc.?
                mongoclient.warctext.bs.save({'url' : url, 'text' : body})
            except Exception:
                sys.stderr.write("Writing to MongoDB failed for "+url+"\n")
                traceback.print_exc()
        except Exception:
            # General catch to avoid multiprocessing taking down the whole job
            # for one bogus record
            sys.stderr.write("\n\n***** Uncaught exception processing "+url+
                             "from "+infn+":\n")
            traceback.print_exc()
            sys.stderr.write("Continuing.\n\n\n")

    sys.stderr.write("****Finished file.\n")


#class WARCMongoDBProcessorHTML2Text(WARCMongoDBProcessor):
#    """Processes WARC files by decomposing them, converting HTML and various
#       proprietary document format records to plain text using HTML2Text,
#       then storing the content in a local MongoDB database.
#    """
#
#    def __init__(self, collection='html2text', *args, **kw):
#        self.htmlprocessor = html2text.HTML2Text()
#        self.htmlprocessor.ignore_links = True
#        self.htmlprocessor.ignore_images = True
#        self.htmlprocessor.ignore_emphasis = True
#        self.htmlprocessor.re_unescape = True
#        self.htmlprocessor.unicode_snob = True
#        self.htmlprocessor.decode_errors = 'strict'
#        super(WARCMongoDBProcessorHTML2Text,
#              self).__init__(collection=collection, *args, **kw)
#
#    def html_to_text(self, body):
#        return self.htmlprocessor.handle(body)


