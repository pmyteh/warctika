#! /usr/bin/env python

from setuptools import setup

setup(
    name="warctika",
    version="0.3.0",
    description="Python library to work with ARC and WARC files",
    long_description=open('Readme.rst').read(),
    license='GPLv2',
    author="Internet Archive and Tom Nicholls",
    author_email="tom.nicholls@oii.ox.ac.uk",
    url="http://github.com/pmyteh/warctika",
    packages=["warctika"],
    platforms=["any"],
    package_data={'': ["LICENSE", "Readme.rst"]},
    include_package_data=True,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v2 (GPLv2)',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
)
