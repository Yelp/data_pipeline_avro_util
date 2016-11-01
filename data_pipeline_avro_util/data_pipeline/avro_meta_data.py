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


class AvroMetaDataKeys(object):
    """Data Pipeline specific valid metadata keys that can be added in the
    Avro Field type"""

    BIT_LEN = 'bitlen'        # length of bit type
    DATE = 'date'             # whether it is a date in ISO 8601 format
    DATETIME = 'datetime'     # whether it is a datetime in ISO 8601 format
    FIXED_POINT = 'fixed_pt'  # fixed-point numeric type
    FIX_LEN = 'fixlen'        # length of char type
    MAX_LEN = 'maxlen'        # length of varchar type
    PRECISION = 'precision'   # precision of numeric type
    PRIMARY_KEY = 'pkey'      # whether it is primary key
    SCALE = 'scale'           # scale of numeric type
    TIME = 'time'             # whether it is a time in ISO 8601 format
    TIMESTAMP = 'timestamp'   # whether it is a timestamp field
    UNSIGNED = 'unsigned'     # whether the int type is unsigned
    YEAR = 'year'             # whether it is a year field
    SORT_KEY = 'sortkey'      # the sort key derived from the redshift schema
    DIST_KEY = 'distkey'      # the dist key derived from the redshift schema
    ENCODE = 'encode'         # the encode derived from the redshift schema
    DISTSTYLE = 'diststyle'   # the diststyle derived from the redshift schema
    SYMBOLS = 'symbols'       # list of strings belonging to an enum type
    FSP = 'fsp'               # decimal precision for date objects
