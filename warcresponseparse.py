"""Utility functions derived from and for use with the hanzo warcutils
library; unhelpfully not packaged as part of that library, only with
the example scripts which accompany it"""

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

