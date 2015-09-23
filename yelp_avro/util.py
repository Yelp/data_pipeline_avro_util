# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import avro.io
import avro.schema
import simplejson


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
        return avro.schema.parse(simplejson.dumps(schema))

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


def generate_payload_data(schema, data_spec={}):
    """ Generate a valid payload data dict for a given avro schema, with an
    optional data spec to override defaults.

    Args:
        schema (avro.schema.RecordSchema): An avro schema
        data_spec (dict): {field_name: value} dictionary of values to use
            in the resulting payload data dict

    Returns (dict):
        A valid payload data example
    """
    assert isinstance(schema, avro.schema.RecordSchema)
    data = {}
    for field in schema.fields:
        data[field.name] = data_spec.get(
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
    assert isinstance(field, avro.schema.Field)

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
    assert isinstance(field, avro.schema.Field)
    if isinstance(field.type, avro.schema.UnionSchema):
        for schema in field.type.schemas:
            if isinstance(schema, avro.schema.PrimitiveSchema):
                return schema.type
    elif isinstance(field.type, avro.schema.PrimitiveSchema):
        return field.type.type