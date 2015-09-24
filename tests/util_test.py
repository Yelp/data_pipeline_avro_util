# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import avro
import simplejson as json

from yelp_avro.util import get_avro_schema_object


def test_get_avro_schema_object(avro_schema_json):
    avro_schema = json.dumps(avro_schema_json)
    avro_schema_obj = avro.schema.parse(avro_schema)
    result1 = get_avro_schema_object(schema=avro_schema_json)
    result2 = get_avro_schema_object(schema=avro_schema)
    result3 = get_avro_schema_object(schema=avro_schema_obj)
    assert result1 == result2
    assert result1 == result3
    assert result2 == result3
