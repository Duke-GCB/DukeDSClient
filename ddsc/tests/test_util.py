from unittest import TestCase
from ddsc.core.util import verify_terminal_encoding


class TestVerifyEncoding(TestCase):
    def test_verify_encoding(self):
        bad_values = [
            'ASCII',
            None,
            'US-ASCII',
            '',
        ]
        good_values = [
            'dataUTFdata'
            'UTF-8',
        ]
        for bad_value in bad_values:
            with self.assertRaises(ValueError):
                verify_terminal_encoding(bad_value)
        for good_value in good_values:
            verify_terminal_encoding(good_value)