from ...src.utils.math_and_type_helpers import cagr, string2float, pct2float, get_match


class TestCagr:
    def test_simple(self):
        pass


class TestString2Float:
    def test_simple(self):
        a = "1,000.54"
        assert string2float(a) == 1000.54

    def test_dollar_strip(self):
        a = "$1"
        assert string2float(a) == 1

    def test_lower_k_strip(self):
        a = "1k"
        assert string2float(a) == 1000

    def test_upper_k_strip(self):
        a = "1K"
        assert string2float(a) == 1000


class TestPct2Float:
    def test_simple(self):
        a = "1%"
        assert pct2float(a) == 0.01

    def test_one_ten(self):
        a = "110%"
        assert pct2float(a) == 1.1

    def test_no_pct_sign(self):
        a = "69"
        assert pct2float(a) == 0.69

    def test_comma(self):
        a = "1,023.4%"
        assert pct2float(a) == 10.234


class TestGetMatch:
    def test_simple(self):
        pass
