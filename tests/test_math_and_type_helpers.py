import pytest
from ..src.utils.math_and_type_helpers import *


class TestString2Float:
    def test_simple(self):
        a = "1,000.54"
        assert string2float(a) == 1000.54

    def test_dollar_strip(self):
        a = "$1"
        assert string2float(a) == 1

    def test_lower_k_strip(self):
        a = "1K"
        assert string2float(a) == 1000

    def test_upper_k_strip(self):
        a = "1k"
        assert string2float(a) == 1000
