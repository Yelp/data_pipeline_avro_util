# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from avro import schema

from yelp_avro.avro_builder import AvroField
from yelp_avro.avro_builder import AvroSchemaBuilder


class TestAvroSchemaBuilder(object):

    @pytest.fixture
    def builder(self):
        return AvroSchemaBuilder()

    @property
    def name(self):
        return 'foo'

    @property
    def namespace(self):
        return 'ns'

    @property
    def aliases(self):
        return ['new_foo']

    @property
    def doc(self):
        return 'sample doc'

    @property
    def metadata(self):
        return {'key1': 'val1', 'key2': 'val2'}

    @property
    def enum_symbols(self):
        return ['a', 'b']

    @property
    def fixed_size(self):
        return 16

    @property
    def another_name(self):
        return 'bar'

    @property
    def invalid_schemas(self):
        undefined_schema_name = 'unknown'
        yield undefined_schema_name

        non_avro_schema = {'foo': 'bar'}
        yield non_avro_schema

        named_schema_without_name = {'name': '', 'type': 'fixed', 'size': 16}
        yield named_schema_without_name

        invalid_schema = {'name': 'foo', 'type': 'enum', 'symbols': ['a', 'a']}
        yield invalid_schema

        none_schema = None
        yield none_schema

        int_schema = 12
        yield int_schema

    @property
    def invalid_names(self):
        missing_name = None
        yield missing_name

        reserved_name = 'int'
        yield reserved_name

        non_string_name = 100
        yield non_string_name

    @property
    def duplicate_name_err(self):
        return '"{0}" is already in use.'

    def test_create_primitive_types(self, builder):
        assert 'null' == builder.create_null()
        assert 'boolean' == builder.create_boolean()
        assert 'int' == builder.create_int()
        assert 'long' == builder.create_long()
        assert 'float' == builder.create_float()
        assert 'double' == builder.create_double()
        assert 'bytes' == builder.create_bytes()
        assert 'string' == builder.create_string()

    def test_create_enum(self, builder):
        actual_json = builder.begin_enum(self.name, self.enum_symbols).end()
        expected_json = {
            'type': 'enum',
            'name': self.name,
            'symbols': self.enum_symbols
        }
        assert actual_json == expected_json

    def test_create_enum_with_optional_attributes(self, builder):
        actual_json = builder.begin_enum(
            self.name,
            self.enum_symbols,
            self.namespace,
            self.aliases,
            self.doc,
            **self.metadata
        ).end()

        expected_json = {
            'type': 'enum',
            'name': self.name,
            'symbols': self.enum_symbols,
            'namespace': self.namespace,
            'aliases': self.aliases,
            'doc': self.doc
        }
        expected_json.update(self.metadata)

        assert actual_json == expected_json

    def test_create_enum_with_invalid_name(self, builder):
        for invalid_name in self.invalid_names:
            builder.clear()
            with pytest.raises(schema.SchemaParseException):
                builder.begin_enum(invalid_name, self.enum_symbols).end()

    def test_create_enum_with_dup_name(self, builder):
        with pytest.raises_regexp(
            schema.SchemaParseException,
            self.duplicate_name_err.format(self.name)
        ):
            builder.begin_record(self.name).add_field(
                self.another_name,
                builder.begin_enum(self.name, self.enum_symbols).end()
            ).end()

    @pytest.fixture(params=[None, '', 'a', ['a', 1], [1, 2, 3], ['a', 'a']])
    def invalid_symbols(self, request):
        return request.param

    def test_create_enum_with_invalid_symbols(self, builder, invalid_symbols):
        with pytest.raises(schema.AvroException):
            builder.begin_enum(self.name, invalid_symbols).end()

    def test_create_fixed_decimal(self, builder):
        precision = 5
        scale = 2
        actual_json = builder.begin_decimal_fixed(
            precision,
            scale,
            self.fixed_size,
            self.name
        ).end()
        expected_json = {
            'type': 'fixed',
            'logicalType': 'decimal',
            'precision': precision,
            'scale': scale,
            'name': self.name,
            'size': self.fixed_size
        }
        assert actual_json == expected_json

    def test_create_negative_precision_fixed_decimal(self, builder):
        with pytest.raises(schema.SchemaParseException):
            builder.begin_decimal_fixed(
                -1,
                2,
                self.fixed_size,
                self.name
            ).end()

    def test_create_negative_scale_fixed_decimal(self, builder):
        with pytest.raises(schema.SchemaParseException):
            builder.begin_decimal_fixed(
                1,
                -2,
                self.fixed_size,
                self.name
            ).end()

    def test_create_invalid_precision_fixed_decimal(self, builder):
        with pytest.raises(schema.SchemaParseException):
            builder.begin_decimal_fixed(
                4,
                6,
                self.fixed_size,
                self.name
            ).end()

    def test_create_str_precision_fixed_decimal(self, builder):
        with pytest.raises(schema.SchemaParseException):
            builder.begin_decimal_fixed(
                '4',
                3,
                self.fixed_size,
                self.name
            ).end()

    def test_create_bytes_decimal(self, builder):
        precision = 5
        scale = 2
        actual_json = builder.begin_decimal_bytes(
            precision,
            scale
        ).end()
        expected_json = {
            'type': 'bytes',
            'logicalType': 'decimal',
            'precision': precision,
            'scale': scale
        }
        assert actual_json == expected_json

    def test_create_negative_precision_bytes_decimal(self, builder):
        with pytest.raises(schema.SchemaParseException):
            builder.begin_decimal_bytes(-1, 3).end()

    def test_create_negative_scale_bytes_decimal(self, builder):
        with pytest.raises(schema.SchemaParseException):
            builder.begin_decimal_bytes(1, -2).end()

    def test_create_invalid_precision_bytes_decimal(self, builder):
        with pytest.raises(schema.SchemaParseException):
            builder.begin_decimal_bytes(1, 3).end()

    def test_create_str_precision_bytes_decimal(self, builder):
        with pytest.raises(schema.SchemaParseException):
            builder.begin_decimal_bytes('4', 3).end()

    def test_create_fixed(self, builder):
        actual_json = builder.begin_fixed(self.name, self.fixed_size).end()
        expected_json = {
            'type': 'fixed',
            'name': self.name,
            'size': self.fixed_size
        }
        assert actual_json == expected_json

    def test_create_fixed_with_optional_attributes(self, builder):
        actual_json = builder.begin_fixed(
            self.name,
            self.fixed_size,
            self.namespace,
            self.aliases,
            **self.metadata
        ).end()

        expected_json = {
            'type': 'fixed',
            'name': self.name,
            'size': self.fixed_size,
            'namespace': self.namespace,
            'aliases': self.aliases,
        }
        expected_json.update(self.metadata)

        assert actual_json == expected_json

    def test_create_fixed_with_invalid_name(self, builder):
        for invalid_name in self.invalid_names:
            builder.clear()
            with pytest.raises(schema.SchemaParseException):
                builder.begin_fixed(invalid_name, self.fixed_size).end()

    def test_create_fixed_with_dup_name(self, builder):
        with pytest.raises_regexp(
            schema.SchemaParseException,
            self.duplicate_name_err.format(self.name)
        ):
            builder.begin_record(self.name).add_field(
                self.another_name,
                builder.begin_fixed(self.name, self.fixed_size).end()
            ).end()

    @pytest.fixture(params=[None, 'ten'])
    def invalid_size(self, request):
        return request.param

    def test_create_fixed_with_invalid_size(self, builder, invalid_size):
        with pytest.raises(schema.AvroException):
            builder.begin_fixed(self.name, invalid_size).end()

    def test_create_array(self, builder):
        actual_json = builder.begin_array(builder.create_int()).end()
        expected_json = {'type': 'array', 'items': 'int'}
        assert actual_json == expected_json

    def test_create_array_with_optional_attributes(self, builder):
        actual_json = builder.begin_array(
            builder.create_int(),
            **self.metadata
        ).end()

        expected_json = {'type': 'array', 'items': 'int'}
        expected_json.update(self.metadata)

        assert actual_json == expected_json

    def test_create_array_with_complex_type(self, builder):
        actual_json = builder.begin_array(
            builder.begin_enum(self.name, self.enum_symbols).end()
        ).end()
        expected_json = {
            'type': 'array',
            'items': {
                'type': 'enum',
                'name': self.name,
                'symbols': self.enum_symbols
            }
        }
        assert actual_json == expected_json

    def test_create_array_with_invalid_items_type(self, builder):
        for invalid_schema in self.invalid_schemas:
            builder.clear()
            with pytest.raises(schema.AvroException):
                builder.begin_array(invalid_schema).end()

    def test_create_map(self, builder):
        actual_json = builder.begin_map(builder.create_string()).end()
        expected_json = {'type': 'map', 'values': 'string'}
        assert actual_json == expected_json

    def test_create_map_with_optional_attributes(self, builder):
        actual_json = builder.begin_map(
            builder.create_string(),
            **self.metadata
        ).end()

        expected_json = {'type': 'map', 'values': 'string'}
        expected_json.update(self.metadata)

        assert actual_json == expected_json

    def test_create_map_with_complex_type(self, builder):
        actual_json = builder.begin_map(
            builder.begin_fixed(self.name, self.fixed_size).end()
        ).end()
        expected_json = {
            'type': 'map',
            'values': {
                'type': 'fixed',
                'name': self.name,
                'size': self.fixed_size
            }
        }
        assert actual_json == expected_json

    def test_create_map_with_invalid_values_type(self, builder):
        for invalid_schema in self.invalid_schemas:
            builder.clear()
            with pytest.raises(schema.AvroException):
                builder.begin_map(invalid_schema).end()

    def test_create_record(self, builder):
        actual_json = builder.begin_record(
            self.name
        ).add_field(
            'bar1',
            builder.create_int()
        ).add_field(
            'bar2',
            builder.begin_map(builder.create_double()).end()
        ).end()

        expected_json = {
            'type': 'record',
            'name': self.name,
            'fields': [
                {'name': 'bar1', 'type': 'int'},
                {'name': 'bar2', 'type': {'type': 'map', 'values': 'double'}}
            ]
        }
        assert actual_json == expected_json

    def test_create_record_with_optional_attributes(self, builder):
        actual_json = builder.begin_record(
            self.name,
            namespace=self.namespace,
            aliases=self.aliases,
            doc=self.doc,
            **self.metadata
        ).add_field(
            self.another_name,
            builder.create_int()
        ).end()

        expected_json = {
            'type': 'record',
            'name': self.name,
            'fields': [{'name': self.another_name, 'type': 'int'}],
            'namespace': self.namespace,
            'aliases': self.aliases,
            'doc': self.doc
        }
        expected_json.update(self.metadata)

        assert actual_json == expected_json

    def test_create_field_with_optional_attributes(self, builder):
        actual_json = builder.begin_record(self.name).add_field(
            self.another_name,
            builder.create_boolean(),
            has_default=True,
            default_value=True,
            sort_order='ascending',
            aliases=self.aliases,
            doc=self.doc,
            **self.metadata
        ).end()

        expected_field = {
            'name': self.another_name,
            'type': 'boolean',
            'default': True,
            'order': 'ascending',
            'aliases': self.aliases,
            'doc': self.doc
        }
        expected_field.update(self.metadata)
        expected_json = {
            'type': 'record',
            'name': self.name,
            'fields': [expected_field]
        }

        assert actual_json == expected_json

    def test_create_record_with_no_field(self, builder):
        actual_json = builder.begin_record(self.name).end()
        expected_json = {'type': 'record', 'name': self.name, 'fields': []}
        assert actual_json == expected_json

    def test_create_record_with_invalid_name(self, builder):
        for invalid_name in self.invalid_names:
            builder.clear()
            with pytest.raises(schema.SchemaParseException):
                builder.begin_record(
                    invalid_name
                ).add_field(
                    self.another_name,
                    builder.create_int()
                ).end()

    def test_create_record_with_dup_name(self, builder):
        with pytest.raises_regexp(
            schema.SchemaParseException,
            self.duplicate_name_err.format(self.name)
        ):
            builder.begin_record(
                self.another_name
            ).add_field(
                'bar1',
                builder.begin_enum(self.name, self.enum_symbols).end()
            ).add_field(
                'bar2',
                builder.begin_record(self.name).end()
            ).end()

    def test_create_record_with_dup_field_name(self, builder):
        with pytest.raises_regexp(
            schema.SchemaParseException,
            "{0} already in use.".format(self.another_name)
        ):
            builder.begin_record(
                self.name
            ).add_field(
                self.another_name,
                builder.create_int()
            ).add_field(
                self.another_name,
                builder.create_string()
            ).end()

    def test_create_field_with_invalid_type(self, builder):
        for invalid_schema in self.invalid_schemas:
            builder.clear()
            with pytest.raises(schema.SchemaParseException):
                builder.begin_record(
                    self.name
                ).add_field(
                    self.another_name,
                    invalid_schema
                ).end()

    def test_create_field_with_invalid_sort_order(self, builder):
        with pytest.raises(schema.SchemaParseException):
            builder.begin_record(
                self.name
            ).add_field(
                self.another_name,
                builder.create_int(),
                sort_order='asc'
            ).end()

    def test_create_union(self, builder):
        actual_json = builder.begin_union(
            builder.create_null(),
            builder.create_string(),
            builder.begin_enum(self.name, self.enum_symbols).end()
        ).end()
        expected_json = [
            'null',
            'string',
            {'type': 'enum', 'name': self.name, 'symbols': self.enum_symbols}
        ]
        assert actual_json == expected_json

    def test_create_union_with_empty_sub_schemas(self, builder):
        actual_json = builder.begin_union().end()
        expected_json = []
        assert actual_json == expected_json

    def test_create_union_with_nested_union_schema(self, builder):
        with pytest.raises(schema.SchemaParseException):
            builder.begin_union(
                builder.begin_union(builder.create_int()).end()
            ).end()

    def test_create_union_with_invalid_schema(self, builder):
        for invalid_schema in self.invalid_schemas:
            builder.clear()
            with pytest.raises(schema.SchemaParseException):
                builder.begin_union(invalid_schema).end()

    def test_create_union_with_dup_primitive_schemas(self, builder):
        with pytest.raises(schema.SchemaParseException):
            builder.begin_union(
                builder.create_int(),
                builder.create_int()
            ).end()

    def test_create_union_with_dup_named_schemas(self, builder):
        with pytest.raises(schema.SchemaParseException):
            builder.begin_union(
                builder.begin_enum(self.name, self.enum_symbols).end(),
                builder.begin_fixed(self.name, self.fixed_size).end()
            ).end()

    def test_create_union_with_dup_complex_schemas(self, builder):
        with pytest.raises(schema.SchemaParseException):
            builder.begin_union(
                builder.begin_map(builder.create_int()).end(),
                builder.begin_map(builder.create_int()).end()
            ).end()

    def test_create_nullable_type(self, builder):
        # non-union schema type
        actual_json = builder.begin_nullable_type(builder.create_int()).end()
        expected_json = ['null', 'int']
        assert actual_json == expected_json

        # union schema type
        actual_json = builder.begin_nullable_type(
            [builder.create_int()]
        ).end()
        expected_json = ['null', 'int']
        assert actual_json == expected_json

    def test_create_nullable_type_with_default_value(self, builder):
        # non-union schema type
        actual_json = builder.begin_nullable_type(
            builder.create_int(),
            default_value=10
        ).end()
        expected_json = ['int', 'null']
        assert actual_json == expected_json

        # union schema type
        actual_json = builder.begin_nullable_type(
            [builder.create_int()],
            default_value=10
        ).end()
        expected_json = ['int', 'null']
        assert actual_json == expected_json

    def test_create_nullable_type_with_null_type(self, builder):
        actual_json = builder.begin_nullable_type(
            builder.create_null()
        ).end()
        expected_json = 'null'
        assert actual_json == expected_json

    def test_create_nullable_type_with_nullable_type(self, builder):
        actual_json = builder.begin_nullable_type(
            builder.begin_union(
                builder.create_null(),
                builder.create_long()
            ).end(),
            default_value=10
        ).end()
        expected_json = ['null', 'long']
        assert actual_json == expected_json

    def test_create_nullable_type_with_invalid_type(self, builder):
        for invalid_schema in self.invalid_schemas:
            builder.clear()
            with pytest.raises(schema.SchemaParseException):
                builder.begin_nullable_type(invalid_schema)

    @property
    def record_schema_json(self):
        return {
            'type': 'record',
            'name': self.name,
            'fields': [self.field_json]
        }

    @property
    def field_json(self):
        return {'name': 'bar', 'type': 'int'}

    def test_create_schema_with_preloaded_json(self, builder):
        actual_json = builder.begin_with_schema_json(
            self.record_schema_json
        ).add_field(
            'new_field',
            typ=builder.create_int()
        ).end()

        expected_json = self.record_schema_json
        expected_json['fields'].append({'name': 'new_field', 'type': 'int'})
        assert actual_json == expected_json

    def test_removed_field(self, builder):
        actual_json = builder.begin_record(
            self.name
        ).add_field(
            'bar1',
            builder.create_int()
        ).add_field(
            'bar2',
            builder.create_int()
        ).remove_field('bar1').end()

        expected_json = {
            'type': 'record',
            'name': self.name,
            'fields': [{'name': 'bar2', 'type': 'int'}]
        }
        assert actual_json == expected_json

    def test_removed_nonexistent_field(self, builder):
        with pytest.raises(ValueError):
            builder.begin_record('foo').add_field('a', 'int').remove_field('b')

    def test_insert_field(self, builder):
        actual_json = builder.begin_with_schema_json(
            self.record_schema_json
        ).insert_field(
            builder.create_field('bar2', builder.create_int()),
            index=0
        ).end()

        expected_json = self.record_schema_json
        expected_json['fields'] = [
            {'name': 'bar2', 'type': 'int'},
            self.field_json
        ]
        assert actual_json == expected_json

    def test_insert_fields(self, builder):
        new_fields = [
            builder.create_field('f1', builder.create_int()),
            builder.create_field('f2', builder.create_int()),
        ]
        actual_json = builder.begin_with_schema_json(
            self.record_schema_json
        ).insert_fields(
            new_fields,
            index=0
        ).end()

        expected_json = self.record_schema_json
        expected_json['fields'] = [
            {'name': 'f1', 'type': 'int'},
            {'name': 'f2', 'type': 'int'},
            self.field_json
        ]
        assert actual_json == expected_json

    def test_get_field(self, builder):
        builder.begin_with_schema_json(self.record_schema_json)
        actual = builder.get_field('bar')
        assert actual == self.field_json

    def test_get_nonexistent_field(self, builder):
        with pytest.raises(ValueError):
            builder.begin_record('foo').add_field('a', 'int').get_field('b')

    def test_get_field_index(self, builder):
        builder.begin_with_schema_json(self.record_schema_json)
        actual = builder.get_field_index('bar')
        assert actual == 0

    def test_get_nonexistent_field_index(self, builder):
        with pytest.raises(ValueError):
            builder.begin_record('foo').add_field('a', 'int')
            builder.get_field_index(field_name='b')

    def test_replace_field_preserve_null_true(self, builder):
        schema_json = {
            'type': 'record',
            'name': self.name,
            'fields': [{'name': 'old', 'type': ['null', 'int']}]
        }
        builder.begin_with_schema_json(schema_json)
        builder.replace_field(
            old_field_name='old',
            new_fields=[
                {'name': 'new_string', 'typ': 'string'},
                {'name': 'new_double_bytes', 'typ': ['double', 'bytes']},
                {'name': 'new_boolean_null', 'typ': ['null', 'boolean']},
            ],
            preserve_null=True
        )
        record = builder.end()
        assert record['fields'] == [
            {'name': 'new_string', 'type': ['null', 'string']},
            {'name': 'new_double_bytes', 'type': ['null', 'double', 'bytes']},
            {'name': 'new_boolean_null', 'type': ['null', 'boolean']},
        ]

    def test_replace_field_preserve_null_false(self, builder):
        schema_json = {
            'type': 'record',
            'name': self.name,
            'fields': [{'name': 'old', 'type': ['null', 'int']}]
        }
        builder.begin_with_schema_json(schema_json)
        builder.replace_field(
            old_field_name='old',
            new_fields=[
                {'name': 'new_string', 'typ': 'string'},
                {'name': 'new_double_bytes', 'typ': ['double', 'bytes']},
                {'name': 'new_boolean_null', 'typ': ['null', 'boolean']},
            ],
            preserve_null=False
        )
        record = builder.end()
        assert record['fields'] == [
            {'name': 'new_string', 'type': 'string'},
            {'name': 'new_double_bytes', 'type': ['double', 'bytes']},
            {'name': 'new_boolean_null', 'type': ['null', 'boolean']},
        ]


class TestAvroField(object):

    def test_instantiate_from_json(self):
        expected_json = {'name': 'bar', 'type': 'int'}

        actual = AvroField(expected_json)
        assert actual.name == 'bar'
        assert actual.field_type == 'int'
        assert not actual.has_default
        assert actual.sort_order is None
        assert actual.aliases is None
        assert actual.doc is None
        assert actual.metadata == {}
        assert actual.field_json == expected_json

    def test_instantiate_from_attributes(self):
        actual = AvroField.from_attributes(
            name='bar',
            typ='int',
            has_default=True,
            default_value=10,
            sort_order='ignore',
            aliases=['rab'],
            doc='bar field',
            **{'key1': 'value1'}
        )
        assert actual.name == 'bar'
        assert actual.field_type == 'int'
        assert actual.has_default
        assert actual.default_value == 10
        assert actual.sort_order == 'ignore'
        assert actual.aliases == ['rab']
        assert actual.doc == 'bar field'
        assert actual.metadata == {'key1': 'value1'}

        expected_json = {
            'name': 'bar',
            'type': 'int',
            'default': 10,
            'order': 'ignore',
            'aliases': ['rab'],
            'doc': 'bar field',
            'key1': 'value1'
        }
        assert actual.field_json == expected_json

    def test_get_metadata(self):
        field = AvroField({'name': 'bar', 'type': 'int', 'key1': 'value1'})
        actual = field.metadata
        assert actual == {'key1': 'value1'}

    def test_clear_metadata(self):
        field = AvroField({'name': 'bar', 'type': 'int', 'key1': 'value1'})
        field.clear_metadata()
        assert field.field_json == {'name': 'bar', 'type': 'int'}
        assert field.metadata == {}
