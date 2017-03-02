from unittest import TestCase

from ddsc.core.util import verify_terminal_encoding


class TestUtil(TestCase):

    def test_verify_terminal_encoding_upper(self):
        verify_terminal_encoding('UTF')

    def test_verify_terminal_encoding_lower(self):
        verify_terminal_encoding('utf')

    def test_verify_terminal_encoding_ascii_raises(self):
        with self.assertRaises(ValueError):
            verify_terminal_encoding('ascii')

    def test_verify_terminal_encoding_empty_raises(self):
        with self.assertRaises(ValueError):
            verify_terminal_encoding('')

    def test_verify_terminal_encoding_none_raises(self):
        with self.assertRaises(ValueError):
            verify_terminal_encoding(None)
