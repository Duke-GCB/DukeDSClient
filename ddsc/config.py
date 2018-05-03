""" Global configuration for the utility based on config files and environment variables."""
import os
import re
import math
import yaml
import multiprocessing
from ddsc.core.util import verify_file_private

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

GLOBAL_CONFIG_FILENAME = '/etc/ddsclient.conf'
LOCAL_CONFIG_FILENAME = '~/.ddsclient'
LOCAL_CONFIG_ENV = 'DDSCLIENT_CONF'
DUKE_DATA_SERVICE_URL = 'https://api.dataservice.duke.edu/api/v1'
D4S2_SERVICE_URL = 'https://datadelivery.genome.duke.edu/api/v1'
MB_TO_BYTES = 1024 * 1024
DDS_DEFAULT_UPLOAD_CHUNKS = 100 * MB_TO_BYTES
AUTH_ENV_KEY_NAME = 'DUKE_DATA_SERVICE_AUTH'
# when uploading skip .DS_Store, our key file, and ._ (resource fork metadata)
FILE_EXCLUDE_REGEX_DEFAULT = '^\.DS_Store$|^\.ddsclient$|^\.\_'
MAX_DEFAULT_WORKERS = 8
GET_PAGE_SIZE_DEFAULT = 100  # fetch 100 items per page


def get_user_config_filename():
    user_config_filename = os.environ.get(LOCAL_CONFIG_ENV)
    if user_config_filename:
        return user_config_filename
    else:
        return LOCAL_CONFIG_FILENAME


def create_config(allow_insecure_config_file=False):
    """
    Create config based on /etc/ddsclient.conf and ~/.ddsclient.conf($DDSCLIENT_CONF)
    :param allow_insecure_config_file: bool: when true we will not check ~/.ddsclient permissions.
    :return: Config with the configuration to use for DDSClient.
    """
    config = Config()
    config.add_properties(GLOBAL_CONFIG_FILENAME)
    user_config_filename = get_user_config_filename()
    if user_config_filename == LOCAL_CONFIG_FILENAME and not allow_insecure_config_file:
        verify_file_private(user_config_filename)
    config.add_properties(user_config_filename)
    return config


def default_num_workers():
    """
    Return the number of workers to use as default if not specified by a config file.
    Returns the number of CPUs or MAX_DEFAULT_WORKERS (whichever is less).
    """
    return min(multiprocessing.cpu_count(), MAX_DEFAULT_WORKERS)


class Config(object):
    """
    Global configuration object based on config files an environment variables.
    """
    URL = 'url'                                        # specifies the dataservice host we are connecting too
    USER_KEY = 'user_key'                              # user key: /api/v1/current_user/api_key
    AGENT_KEY = 'agent_key'                            # software_agent key: /api/v1/software_agents/{id}/api_key
    AUTH = 'auth'                                      # Holds actual auth token for connecting to the dataservice
    UPLOAD_BYTES_PER_CHUNK = 'upload_bytes_per_chunk'  # bytes per chunk we will upload
    UPLOAD_WORKERS = 'upload_workers'                  # how many worker processes used for uploading
    DOWNLOAD_WORKERS = 'download_workers'              # how many worker processes used for downloading
    DEBUG_MODE = 'debug'                               # show stack traces
    D4S2_URL = 'd4s2_url'                              # url for use with the D4S2 (share/deliver service)
    FILE_EXCLUDE_REGEX = 'file_exclude_regex'          # allows customization of which filenames will be uploaded
    GET_PAGE_SIZE = 'get_page_size'                    # page size used for GET pagination requests

    def __init__(self):
        self.values = {}

    def add_properties(self, filename):
        """
        Add properties to config based on filename replacing previous values.
        :param filename: str path to YAML file to pull top level properties from
        """
        filename = os.path.expanduser(filename)
        if os.path.exists(filename):
            with open(filename, 'r') as yaml_file:
                self.update_properties(yaml.load(yaml_file))

    def update_properties(self, new_values):
        """
        Add items in new_values to the internal list replacing existing values.
        :param new_values: dict properties to set
        """
        self.values = dict(self.values, **new_values)

    @property
    def url(self):
        """
        Specifies the dataservice host we are connecting too.
        :return: str url to a dataservice host
        """
        return self.values.get(Config.URL, DUKE_DATA_SERVICE_URL)

    def get_portal_url_base(self):
        """
        Determine root url of the data service from the url specified.
        :return: str root url of the data service (eg: https://dataservice.duke.edu)
        """
        api_url = urlparse(self.url).hostname
        portal_url = re.sub('^api\.', '', api_url)
        portal_url = re.sub(r'api', '', portal_url)
        return portal_url

    @property
    def user_key(self):
        """
        Contains user key user created from /api/v1/current_user/api_key used to create a login token.
        :return: str user key that can be used to create an auth token
        """
        return self.values.get(Config.USER_KEY, None)

    @property
    def agent_key(self):
        """
        Contains user agent key created from /api/v1/software_agents/{id}/api_key used to create a login token.
        :return: str agent key that can be used to create an auth token
        """
        return self.values.get(Config.AGENT_KEY, None)

    @property
    def auth(self):
        """
        Contains the auth token for use with connecting to the dataservice.
        :return:
        """
        return self.values.get(Config.AUTH, os.environ.get(AUTH_ENV_KEY_NAME, None))

    @property
    def upload_bytes_per_chunk(self):
        """
        Return the bytes per chunk to be sent to external store.
        :return: int bytes per upload chunk
        """
        value = self.values.get(Config.UPLOAD_BYTES_PER_CHUNK, DDS_DEFAULT_UPLOAD_CHUNKS)
        return Config.parse_bytes_str(value)

    @property
    def upload_workers(self):
        """
        Return the number of parallel works to use when uploading a file.
        :return: int number of workers. Specify None or 1 to disable parallel uploading
        """
        return self.values.get(Config.UPLOAD_WORKERS, default_num_workers())

    @property
    def download_workers(self):
        """
        Return the number of parallel works to use when downloading a file.
        :return: int number of workers. Specify None or 1 to disable parallel downloading
        """
        # Profiling download on different servers showed half the number of CPUs to be optimum for speed.
        default_workers = int(math.ceil(default_num_workers() / 2))
        return self.values.get(Config.DOWNLOAD_WORKERS, default_workers)

    @property
    def debug_mode(self):
        """
        Return true if we should show stack traces on error.
        :return: boolean True if debugging is enabled
        """
        return self.values.get(Config.DEBUG_MODE, False)

    @property
    def d4s2_url(self):
        """
        Returns url for D4S2 service or '' if not setup.
        :return: str url
        """
        return self.values.get(Config.D4S2_URL, D4S2_SERVICE_URL)

    @staticmethod
    def parse_bytes_str(value):
        """
        Given a value return the integer number of bytes it represents.
        Trailing "MB" causes the value multiplied by 1024*1024
        :param value:
        :return: int number of bytes represented by value.
        """
        if type(value) == str:
            if "MB" in value:
                return int(value.replace("MB", "")) * MB_TO_BYTES
            else:
                return int(value)
        else:
            return value

    @property
    def file_exclude_regex(self):
        """
        Returns regex that should be used to filter out filenames.
        :return: str: regex that when matches we should exclude a file from uploading.
        """
        return self.values.get(Config.FILE_EXCLUDE_REGEX, FILE_EXCLUDE_REGEX_DEFAULT)

    @property
    def page_size(self):
        """
        Returns the page size used to fetch paginated lists from DukeDS.
        For DukeDS APIs that fail related to timeouts lowering this value can help.
        :return:
        """
        return self.values.get(Config.GET_PAGE_SIZE, GET_PAGE_SIZE_DEFAULT)
