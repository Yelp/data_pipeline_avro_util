# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest


@pytest.fixture
def avro_schema_json():
    return {
        "type": "record",
        "name": "test_record",
        "fields": [
            {
                "type": ["null", "int"],
                "name": "union_field"
            },
            {
                "type": ["null", "int"],
                "name": "union_field_null",
                "default": None
            },
            {
                "type": ["null", "int"],
                "name": "union_field_101",
                "default": 101
            },
            {
                "type": "boolean",
                "name": "bool_field"
            },
            {
                "type": "boolean",
                "name": "bool_field_F",
                "default": False
            },
            {
                "type": "string",
                "name": "string_field"
            },
            {
                "type": "string",
                "name": "string_field_foo",
                "default": "foo❤"
            },
            {
                "type": "bytes",
                "name": "bytes_field"
            },
            {
                "type": "bytes",
                "name": "bytes_field_bar",
                "default": "bar"
            },
            {
                "type": "int",
                "name": "int_field"
            },
            {
                "type": "int",
                "name": "int_field_1",
                "default": 1
            },
            {
                "type": "long",
                "name": "long_field"
            },
            {
                "type": "long",
                "name": "long_field_42",
                "default": 42
            },
            {
                "type": "float",
                "name": "float_field"
            },
            {
                "type": "float",
                "name": "float_field_p75",
                "default": 0.75
            },
            {
                "type": "double",
                "name": "double_field"
            },
            {
                "type": "double",
                "name": "double_field_pi",
                "default": 3.14
            }
        ]
    }
