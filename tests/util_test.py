# -*- coding: utf-8 -*-
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
