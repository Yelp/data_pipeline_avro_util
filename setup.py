# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from setuptools import find_packages
from setuptools import setup

import yelp_avro

readme = open('README.rst').read()
doclink = """
Documentation
-------------

The full documentation is at http://servicedocs.yelpcorp.com/docs/yelp_avro/"""
history = open('HISTORY.rst').read().replace('.. :changelog:', '')

setup(
    # py2 + setuptools asserts isinstance(name, str) so this needs str()
    name=str('yelp_avro'),
    version=yelp_avro.__version__,
    description="Common functionality build on top of Apache Avro",
    long_description=readme + '\n\n' + doclink + '\n\n' + history,
    author=yelp_avro.__author__,
    author_email=yelp_avro.__email__,
    url='http://servicedocs.yelpcorp.com/docs/yelp_avro/',
    packages=find_packages(exclude=['tests*']),
    install_requires=[
    ],
    zip_safe=False,
    keywords='yelp_avro',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
)
