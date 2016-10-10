# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from setuptools import find_packages
from setuptools import setup

import data_pipeline_avro_util

readme = open('README.rst').read()
doclink = """
Documentation
-------------

The full documentation is at
http://servicedocs.yelpcorp.com/docs/data_pipeline_avro_util/"""
history = open('HISTORY.rst').read().replace('.. :changelog:', '')

setup(
    # py2 + setuptools asserts isinstance(name, str) so this needs str()
    name=str('data_pipeline_avro_util'),
    version=data_pipeline_avro_util.__version__,
    description="Common functionality build on top of Apache Avro",
    long_description=readme + '\n\n' + doclink + '\n\n' + history,
    author=data_pipeline_avro_util.__author__,
    author_email=data_pipeline_avro_util.__email__,
    url='http://servicedocs.yelpcorp.com/docs/data_pipeline_avro_util/',
    packages=find_packages(exclude=['tests*']),
    install_requires=[
        'cached-property>=0.1.5'
    ],
    zip_safe=False,
    keywords='data_pipeline_avro_util',
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
