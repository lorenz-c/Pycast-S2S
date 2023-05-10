import sys

import pytest

sys.path.append("..")

from src.helper_modules import decode_processing_months, decode_processing_years


@pytest.mark.parametrize(
    "year_string, decoded_years",
    [
        ("2000", [2000]),
        ("2000,0,2010", range(2000, 2011)),
        ("2000,2002", [2000, 2002]),
        ("2000,2002,2004", [2000, 2002, 2004]),
    ],
)
def test_year_decoding(year_string, decoded_years):
    assert decode_processing_years(year_string) == decoded_years


@pytest.mark.parametrize(
    "month_string, decoded_months",
    [("01", [1]), ("2,0,6", range(2, 7)), ("01,10", [1, 10]), ("4,6,7", [4, 6, 7])],
)
def test_month_decoding(month_string, decoded_months):
    assert decode_processing_months(month_string) == decoded_months
