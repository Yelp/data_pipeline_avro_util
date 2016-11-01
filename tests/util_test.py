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

import avro

from data_pipeline_avro_util.util import get_avro_schema_object


def test_get_avro_schema_object(avro_schema_json):
    avro_schema_obj = avro.schema.make_avsc_object(avro_schema_json)
    avro_schema = str(avro_schema_obj)
    result1 = get_avro_schema_object(schema=avro_schema_json)
    result2 = get_avro_schema_object(schema=avro_schema)
    result3 = get_avro_schema_object(schema=avro_schema_obj)
    assert result1 == result2
    assert result1 == result3
    assert result2 == result3
