# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from avro.io import AvroTypeException

from yelp_avro.avro_string_reader import AvroStringReader
from yelp_avro.avro_string_writer import AvroStringWriter
from yelp_avro.testing_helpers.generate_payload_data import \
    _avro_primitive_type_to_example_value
from yelp_avro.testing_helpers.generate_payload_data import \
    generate_payload_data
from yelp_avro.util import get_avro_schema_object


class TestGeneratePayloadData(object):

    @pytest.fixture
    def avro_schema_object(self, avro_schema_json):
        return get_avro_schema_object(avro_schema_json)

    def test_payload_no_fields_filled(self, avro_schema_object):
        expected_data = {
            "union_field": _avro_primitive_type_to_example_value['null'],
            "union_field_null": None,
            "union_field_101": 101,
            "bool_field": _avro_primitive_type_to_example_value['boolean'],
            "bool_field_F": False,
            "string_field": _avro_primitive_type_to_example_value['string'],
            "string_field_foo": 'foo❤',
            "bytes_field": _avro_primitive_type_to_example_value['bytes'],
            "bytes_field_bar": b'bar',
            "int_field": _avro_primitive_type_to_example_value['int'],
            "int_field_1": 1,
            "long_field": _avro_primitive_type_to_example_value['long'],
            "long_field_42": 42,
            "float_field": _avro_primitive_type_to_example_value['float'],
            "float_field_p75": 0.75,
            "double_field": _avro_primitive_type_to_example_value['double'],
            "double_field_pi": 3.14
        }
        data = generate_payload_data(avro_schema_object)
        assert data == expected_data

    def test_payload_all_fields_filled(self, avro_schema_object):
        expected_data = {
            "union_field": 101010,
            "union_field_null": 101010,
            "union_field_101": None,
            "bool_field": False,
            "bool_field_F": True,
            "string_field": 'wow❤wow!',
            "string_field_foo": '❤super!',
            "bytes_field": b'do the robot',
            "bytes_field_bar": b'noooooooo!',
            "int_field": 8,
            "int_field_1": 0,
            "long_field": 10,
            "long_field_42": 999,
            "float_field": 0.1,
            "float_field_p75": 0.75,
            "double_field": 0.3,
            "double_field_pi": 0.4
        }
        data = generate_payload_data(avro_schema_object, expected_data)
        assert data == expected_data

    def test_payload_valid_for_writing_reading(self, avro_schema_object):
        payload_data = generate_payload_data(avro_schema_object)
        writer = AvroStringWriter(schema=avro_schema_object)
        reader = AvroStringReader(
            reader_schema=avro_schema_object,
            writer_schema=avro_schema_object
        )
        encoded_payload = writer.encode(payload_data)
        decoded_payload = reader.decode(encoded_payload)
        assert decoded_payload == payload_data

    def test_rejects_empty_avro_representation_for_writing(
            self,
            avro_schema_object
    ):
        writer = AvroStringWriter(schema=avro_schema_object)
        with pytest.raises(AvroTypeException):
            writer.encode(message_avro_representation=None)

    def test_rejects_empty_encoded_message_for_reading(
            self,
            avro_schema_object
    ):
        reader = AvroStringReader(
            reader_schema=avro_schema_object,
            writer_schema=avro_schema_object
        )
        with pytest.raises(TypeError):
            reader.decode(encoded_message=None)
