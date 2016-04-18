""" Global configuration for the utility based on config files and environment variables."""
import os
import re
import yaml
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

GLOBAL_CONFIG_FILENAME = '/etc/ddsclient.conf'
LOCAL_CONFIG_FILENAME = '~/.ddsclient'
LOCAL_CONFIG_ENV = 'DDSCLIENT_CONF'
DUKE_DATA_SERVICE_URL = 'https://api.dataservice.duke.edu/api/v1'
HANDOVER_SERVICE_URL = 'https://itlab-1.gcb.duke.edu/api/v1'
DDS_DEFAULT_UPLOAD_CHUNKS = 100 * 1024 * 1024
AUTH_ENV_KEY_NAME = 'DUKE_DATA_SERVICE_AUTH'


def create_config():
    """
    Create config based on /etc/ddsclient.conf and ~/.ddsclient.conf($DDSCLIENT_CONF)
    :return: Config with the configuration to use for DDSClient.
    """
    config = Config()
    config.add_properties(GLOBAL_CONFIG_FILENAME)
    config.add_properties(os.environ.get(LOCAL_CONFIG_ENV, LOCAL_CONFIG_FILENAME))
    return config


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
    DEBUG_MODE = 'debug'                               # show stack traces
    HANDOVER_URL = 'handover_url'                      # url for use with the handover service

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
        if type(value) == str and "MB" in value:
            value = int(value.replace("MB","")) * 1024 * 1024
        return value

    @property
    def upload_workers(self):
        """
        Return the number of parallel works to use when uploading a file.
        :return: int number of workers. Specify None or 1 to disable parallel uploading
        """
        return self.values.get(Config.UPLOAD_WORKERS, None)

    @property
    def debug_mode(self):
        """
        Return true if we should show stack traces on error.
        :return: boolean True if debugging is enabled
        """
        return self.values.get(Config.DEBUG_MODE, False)

    @property
    def handover_url(self):
        """
        Returns url for handover service or '' if not setup.
        :return: str url
        """
        return self.values.get(Config.HANDOVER_URL, HANDOVER_SERVICE_URL)
