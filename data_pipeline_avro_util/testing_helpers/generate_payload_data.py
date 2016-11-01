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

from avro.schema import Field
from avro.schema import PrimitiveSchema
from avro.schema import RecordSchema
from avro.schema import UnionSchema


_avro_primitive_type_to_example_value = {
    'null': None,
    'boolean': True,
    'string': '❤',
    'bytes': b'_',
    'int': 1,
    'long': 2,
    'float': 0.5,  # 0.5 works for a == b comparisons after avro encode/decode
    'double': 2.2,
}


def generate_payload_data(schema, data_spec=None):
    """ Generate a valid payload data dict for a given avro schema, with an
    optional data spec to override defaults.

    Args:
        schema (avro.schema.RecordSchema): An avro schema
        data_spec (dict): {field_name: value} dictionary of values to use
            in the resulting payload data dict

    Returns (dict):
        A valid payload data example
    """
    _data_spec = data_spec or {}
    assert isinstance(schema, RecordSchema)
    data = {}
    for field in schema.fields:
        data[field.name] = _data_spec.get(
            field.name,
            generate_field_value(field)
        )
    return data


def generate_field_value(field):
    """ Generate a value for a given avro schema field. If the field has a
    default value specified, that is used, otherwise the first PrimitiveSchema
    definition is used to generate a default valid value.

    Args:
        field (avro.schema.Field): An avro field

    Returns:
        A value which is valid for the given field.
    """
    assert isinstance(field, Field)

    primitive_type = get_field_primitive_type(field)

    if field.has_default:
        if primitive_type == 'bytes':
            return bytes(field.default)
        else:
            return field.default
    else:
        return _avro_primitive_type_to_example_value[primitive_type]


def get_field_primitive_type(field):
    """ The first PrimitiveSchema definition is used to return the primitive
    type of the field, dealing with single-layer unions

    Args:
        field (avro.schema.Field): An avro field

    Returns (str): the primitve field type
    """
    assert isinstance(field, Field)
    if isinstance(field.type, UnionSchema):
        for schema in field.type.schemas:
            if isinstance(schema, PrimitiveSchema):
                return schema.type
    elif isinstance(field.type, PrimitiveSchema):
        return field.type.type
