# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from yelp_avro.data_pipeline.avro_meta_data import AvroMetaDataKeyEnum


class TestAvroMetaDataKeyEnum(object):
    @pytest.fixture(params=[
        (AvroMetaDataKeyEnum.BIT_LEN, 'bitlen'),
        (AvroMetaDataKeyEnum.DATE, 'date'),
        (AvroMetaDataKeyEnum.DATETIME, 'datetime'),
        (AvroMetaDataKeyEnum.FIXED_POINT, 'fixed_pt'),
        (AvroMetaDataKeyEnum.FIX_LEN, 'fixlen'),
        (AvroMetaDataKeyEnum.MAX_LEN, 'maxlen'),
        (AvroMetaDataKeyEnum.PRECISION, 'precision'),
        (AvroMetaDataKeyEnum.PRIMARY_KEY, 'pkey'),
        (AvroMetaDataKeyEnum.SCALE, 'scale'),
        (AvroMetaDataKeyEnum.TIME, 'time'),
        (AvroMetaDataKeyEnum.TIMESTAMP, 'timestamp'),
        (AvroMetaDataKeyEnum.UNSIGNED, 'unsigned'),
        (AvroMetaDataKeyEnum.YEAR, 'year'),
    ])
    def avro_meta_data_key_value_pair(self, request):
        return request.param

    def test_key_is_as_expected(self, avro_meta_data_key_value_pair):
        actual, expected = avro_meta_data_key_value_pair
        assert actual == expected
