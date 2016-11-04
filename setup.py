# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

from setuptools import find_packages
from setuptools import setup

import data_pipeline_avro_util

readme = open('README.md').read()
doclink = """
Documentation
-------------

The full documentation is at
TODO (DATAPIPE-2030|abrar): upload servicedocs to public server."""
history = open('HISTORY.rst').read().replace('.. :changelog:', '')

setup(
    # py2 + setuptools asserts isinstance(name, str) so this needs str()
    name=str('data_pipeline_avro_util'),
    version=data_pipeline_avro_util.__version__,
    description="Common functionality build on top of Apache Avro",
    long_description=readme + '\n\n' + doclink + '\n\n' + history,
    author=data_pipeline_avro_util.__author__,
    author_email=data_pipeline_avro_util.__email__,
    url='https://github.com/Yelp/data_pipeline_avro_util',
    packages=find_packages(exclude=['tests*']),
    install_requires=[
        'cached-property>=0.1.5',
        'yelp-avro==1.9.2'
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
