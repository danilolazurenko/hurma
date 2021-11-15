import pytest

from ..scripts.sparkMain import (
    check_output_format,
    check_input_type,
    InvalidJobParametersException)


def test_proper_output_format():
    with pytest.raises(InvalidJobParametersException):
        check_output_format('txt')


def test_input_type():
    with pytest.raises(InvalidJobParametersException):
        check_output_format('txt')


