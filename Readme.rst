warctika: Python library for reading and writing warc files, and processing
their contents through Apache Tika
============================================================================

This library is based upon the "warc" library by the Internet Archive
and others.

WARC (Web ARChive) is a file format for storing web crawls.

http://bibnum.bnf.fr/WARC/ 

This library makes it very easy to work with WARC files.::

    import warctika
    f = warctika.open("test.warc")
    for record in f:
        print record['WARC-Target-URI'], record['Content-Length']

Documentation
-------------

The documentation of the original warc library is available at
http://warc.readthedocs.org/.

Note that some changes have been made to the interfaces in warctika, most
notably that WARCRecord.payload should now contain the actual raw contents, and
not a FilePart interface. This may or may not break code which relied on the
old behaviour
	
License
-------

This software is licensed under GPL v2. See LICENSE_ file for details.

.. LICENSE: http://github.com/pmyteh/warctika/blob/master/LICENSE
