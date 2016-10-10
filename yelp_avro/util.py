# -*- coding: utf-8 -*-
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
