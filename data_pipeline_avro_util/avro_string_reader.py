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

import cStringIO

import avro.io
from cached_property import cached_property

from data_pipeline_avro_util.util import get_avro_schema_object


class AvroStringReader(object):
    def __init__(self, reader_schema, writer_schema):
        """ Utility class for decoding Avro.

        Args:
            reader_schema (string|dict|:class:`avro.schema.Schema`): An avro
                schema for decoding, which represents the object you wish to
                decode into. Must be backwards compatible with `writer_schema`.
            writer_schema (string|dict|:class:`avro.schema.Schema`): An avro
                schema for decoding, which represents the object the data was
                originally encoded with.

        Notes:
            Both the `reader_schema` and `writer_schema` args may be given in
            any of these forms:
                - An avro json string
                - An avro dict representation (parsed json string)
                - An :class:`avro.schema.Schema` object
        """
        self.reader_schema = get_avro_schema_object(reader_schema)
        self.writer_schema = get_avro_schema_object(writer_schema)

    @cached_property
    def avro_reader(self):
        return avro.io.DatumReader(
            readers_schema=self.reader_schema,
            writers_schema=self.writer_schema
        )

    def decode(self, encoded_message):
        """ Decodes a given `encoded_message` which was encoded using the
        same schema as `self.writer_schema` into a representation defined by
        `self.reader_schema`.

        Args:
            encoded_message (string): An encoded object

        Returns (dict):
            The decoded dictionary representation.
        """
        stringio = cStringIO.StringIO(encoded_message)
        decoder = avro.io.BinaryDecoder(stringio)
        return self.avro_reader.read(decoder)
