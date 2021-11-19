import os
import sys
import unittest

sys.path.append(os.path.abspath('../hurma'))

from hurma.spark_export_jobs.write_csv_to_binary import (
    check_output_format,
    check_input_type,
    InvalidJobParametersException)


class TestSysArgsValidationMethods(unittest.TestCase):

    def test_proper_output_format(self):
        with self.assertRaises(InvalidJobParametersException):
            check_output_format('txt')

    def test_input_type(self):
        with self.assertRaises(InvalidJobParametersException):
            check_output_format('txt')
