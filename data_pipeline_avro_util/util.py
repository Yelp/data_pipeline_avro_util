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

import avro.io
import avro.schema


def get_avro_schema_object(schema):
    """ Helper function to simplify dealing with the three ways avro schema may
        be represented:
        - a json string
        - a dictionary (parsed json string)
        - a parsed `avro.schema.Schema` object

        In all cases this returns the `avro.schema.Schema` object form
    """
    if isinstance(schema, avro.schema.Schema):
        return schema
    elif isinstance(schema, basestring):
        return avro.schema.parse(schema)
    else:
        return avro.schema.make_avsc_object(schema)
