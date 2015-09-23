# -*- coding: utf-8 -*-
"""
Tests for `yelp_avro` module.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from yelp_avro import yelp_avro


@pytest.yield_fixture
def test_fixture():
    yield


def test_yelp_avro(test_fixture):
    assert True
