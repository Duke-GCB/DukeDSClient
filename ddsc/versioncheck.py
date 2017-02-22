"""
Compares installed DukeDSClient vs what is on PyPI.
"""
from __future__ import print_function
import requests
import pkg_resources

APP_NAME = "DukeDSClient"
HALF_SECOND_TIMEOUT = 0.5
PYPI_URL = 'https://pypi.python.org/pypi/{}/json'.format(APP_NAME)
UNABLE_TO_ACCESS_PYPI = "Unable to check released version:"
UPDATE_VERSION_MESSAGE = """
There is a new version of DukeDSClient. Please upgrade by running:
pip install --upgrade DukeDSClient
"""


def get_pypi_version():
    """
    Returns the version info from pypi for this app.
    """
    try:
        response = requests.get(PYPI_URL, timeout=HALF_SECOND_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        version_str = data["info"]["version"]
        return _parse_version_str(version_str)
    except requests.exceptions.ConnectionError:
        raise VersionException(UNABLE_TO_ACCESS_PYPI + " Failed to connect.")
    except requests.exceptions.Timeout:
        raise VersionException(UNABLE_TO_ACCESS_PYPI + " Timeout")


def get_internal_version():
    """
    Returns internal version info.
    """
    version_str = get_internal_version_str()
    return _parse_version_str(version_str)


def get_internal_version_str():
    """
    Returns internal version string.
    """
    return pkg_resources.get_distribution(APP_NAME).version


def _parse_version_str(version_str):
    return [int(version_part) for version_part in version_str.split(".")]


def check_version():
    """
    Check version and raises VersionException if the installed DukeDSClient is out of date
    or unable to access pypi.
    """
    pypi_version = get_pypi_version()
    internal_version = get_internal_version()

    if pypi_version > internal_version:
        raise VersionException(UPDATE_VERSION_MESSAGE)


class VersionException(Exception):
    def __init__(self, message):
        super(VersionException, self).__init__(message)
