# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import cStringIO

import avro.io
from cached_property import cached_property

from data_pipeline_avro_util.util import get_avro_schema_object


class AvroStringWriter(object):
    def __init__(self, schema):
        """ Utility class for encoding Avro.
        Args:
            schema (string|dict|:class:`avro.schema.Schema`): An avro schema
                for encoding.

        Notes:
            The `schema` arg may be given in any of these forms:
                - An avro json string
                - An avro dict representation (parsed json string)
                - An :class:`avro.schema.Schema` object
        """
        self.schema = get_avro_schema_object(schema)

    @cached_property
    def avro_writer(self):
        return avro.io.DatumWriter(
            writers_schema=self.schema
        )

    def encode(self, message_avro_representation):
        """ Encodes a given `message_avro_representation` using `self.schema`.

        Args:
            message_avro_representation (dict): A dictionary which matches the
                schema defined by `self.schema`

        Returns (string):
            An encoded bytes representation.
        """
        # Benchmarking this revealed that recreating stringio and the encoder
        # isn't slower than truncating the stringio object.  This is supported
        # by benchmarks that indicate it's faster to instantiate a new object
        # than truncate an existing one:
        # http://stackoverflow.com/questions/4330812/how-do-i-clear-a-stringio-object
        stringio = cStringIO.StringIO()
        encoder = avro.io.BinaryEncoder(stringio)
        self.avro_writer.write(message_avro_representation, encoder)
        return stringio.getvalue()
