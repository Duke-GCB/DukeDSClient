from unittest import TestCase
from ddsc.versioncheck import get_internal_version, get_pypi_version, check_version, VersionException
from mock.mock import patch
import requests


class TestVerifyEncoding(TestCase):
    def test_check_version_works(self):
        """
        pypi shouldn't be a greater version than the development version
        """
        check_version()

    @patch("ddsc.versioncheck.get_internal_version")
    @patch("ddsc.versioncheck.get_pypi_version")
    def test_check_version_new_version_raises(self, mock_get_pypi_version, mock_get_internal_version):
        """
        When their is a new version at pypi the VersionException should be raised.
        It should contain instructions on how to upgrade DukeDSClient.
        """
        mock_get_pypi_version.return_value = [0, 2, 2]
        mock_get_internal_version.return_value = [0, 1, 2]
        with self.assertRaises(VersionException) as exception_info:
            check_version()
        self.assertIn("pip install --upgrade DukeDSClient", str(exception_info.exception))

    @patch("ddsc.versioncheck.requests")
    def test_check_version_pypi_timeout(self, mock_requests):
        mock_requests.exceptions = requests.exceptions
        mock_requests.get.side_effect = requests.exceptions.Timeout
        with self.assertRaises(VersionException) as exception_info:
            check_version()
        self.assertIn("Timeout", str(exception_info.exception))

    @patch("ddsc.versioncheck.requests")
    def test_check_version_pypi_bad_url(self, mock_requests):
        mock_requests.exceptions = requests.exceptions
        mock_requests.get.side_effect = requests.exceptions.ConnectionError
        with self.assertRaises(VersionException) as exception_info:
            check_version()
        self.assertIn("Failed to connect", str(exception_info.exception))

    def test_internal_version_returns_int_array(self):
        version = get_internal_version()
        self.assertEqual(list, type(version))
        self.assertEqual(int, type(version[0]))

    def test_pypi_version_returns_int_array(self):
        version = get_pypi_version()
        self.assertEqual(list, type(version))
        self.assertEqual(int, type(version[0]))
