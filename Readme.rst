warctika: Python library for processing WARC files through Apache Tika
======================================================================

This library is designed to handle web crawl data fetched using the
Heritrix web crawler (or other tools producing WARC files), extract
the plain text from structured formats and resave the data as WARC
"conversion" records.

The primary use for this tool is to extract text from webcrawl data
sets for use in machine learning and supervised classification work.

WARC (Web ARChive) is a file format for storing web crawls:
http://bibnum.bnf.fr/WARC/ 

This library was originally based upon the "warc" library by the Internet
Archive and others, but now relies upon the hanzo warctools and has no
code in common with the original library.

The hanzo library which this code is dependent upon can be installed
with 'pip install warctools'. Beware that there are several old
versions floating around under different names in the index.
	
License
-------

This software is licensed under GPL v2 or later. See LICENSE_ file for details.
The contents of the warcresponseparse.py file are derived directly from
Hanzo warctools code and can be used under the terms of the MIT license.

.. LICENSE: http://github.com/pmyteh/warctika/blob/master/LICENSE

DOI
---

The formally-released versions of this library have been assigned DOIs for citation purposes.

.. image:: https://zenodo.org/badge/6315/pmyteh/warctika.png
   :target: http://dx.doi.org/10.5281/zenodo.11867
