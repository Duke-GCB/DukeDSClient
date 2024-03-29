from unittest import TestCase
import math
import ddsc.config
from ddsc.exceptions import DDSUserException
import multiprocessing
from mock.mock import patch, mock_open


class TestConfig(TestCase):
    def test_empty_config(self):
        config = ddsc.config.Config()
        self.assertEqual(config.url, ddsc.config.DUKE_DATA_SERVICE_URL)
        self.assertEqual(config.user_key, None)
        self.assertEqual(config.agent_key, None)
        self.assertEqual(config.auth, None)
        self.assertEqual(config.upload_bytes_per_chunk, ddsc.config.DDS_DEFAULT_UPLOAD_CHUNKS)
        self.assertEqual(config.upload_workers, min(multiprocessing.cpu_count(), ddsc.config.MAX_DEFAULT_WORKERS))
        self.assertEqual(config.storage_provider_id, None)
        self.assertEqual(config.azure_delivery_url, ddsc.config.AZ_DELIVERY_URL)

    def test_global_then_local(self):
        config = ddsc.config.Config()
        global_config = {
            'url': 'dataservice1.com',
            'user_key': 'abc',
            'agent_key': '123',
            'auth': 'secret',
            'upload_bytes_per_chunk': 1293892,
        }
        local_config = {
            'url': 'dataservice2.com',
            'user_key': 'cde',
            'agent_key': '456',
            'upload_workers': 45,
            'download_workers': 44,
            'file_download_retries': 2
        }

        config.update_properties(global_config)
        self.assertEqual(config.url, 'dataservice1.com')
        self.assertEqual(config.user_key, 'abc')
        self.assertEqual(config.agent_key, '123')
        self.assertEqual(config.auth, 'secret')
        self.assertEqual(config.upload_bytes_per_chunk, 1293892)
        num_upload_workers = min(multiprocessing.cpu_count(), ddsc.config.MAX_DEFAULT_WORKERS)
        self.assertEqual(config.upload_workers, num_upload_workers)
        self.assertEqual(config.download_workers, int(math.ceil(num_upload_workers)))
        self.assertEqual(config.file_download_retries, ddsc.config.DEFAULT_FILE_DOWNLOAD_RETRIES)

        config.update_properties(local_config)
        self.assertEqual(config.url, 'dataservice2.com')
        self.assertEqual(config.user_key, 'cde')
        self.assertEqual(config.agent_key, '456')
        self.assertEqual(config.auth, 'secret')
        self.assertEqual(config.upload_bytes_per_chunk, 1293892)
        self.assertEqual(config.upload_workers, 45)
        self.assertEqual(config.download_workers, 44)
        self.assertEqual(config.file_download_retries, 2)

    def test_MB_chunk_convert(self):
        config = ddsc.config.Config()
        some_config = {
            'upload_bytes_per_chunk': '10MB',
            'download_bytes_per_chunk': '20MB',
        }
        config.update_properties(some_config)
        self.assertEqual(config.upload_bytes_per_chunk, 10485760)
        some_config = {
            'upload_bytes_per_chunk': '20MB',
        }
        config.update_properties(some_config)
        self.assertEqual(config.upload_bytes_per_chunk, 20971520)

    def test_get_portal_url_base(self):
        config = ddsc.config.Config()
        config1 = {
            'url': 'https://api.dataservice1.com/api/v1',
        }
        config.update_properties(config1)
        self.assertEqual(config.get_portal_url_base(), 'dataservice1.com')
        config2 = {
            'url': 'https://apiuatest.dataservice1.com/api/v1',
        }
        config.update_properties(config2)
        self.assertEqual(config.get_portal_url_base(), 'uatest.dataservice1.com')

        config3 = {
            'url': 'https://apidev.dataservice1.com/api/v1',
        }
        config.update_properties(config3)
        self.assertEqual(config.get_portal_url_base(), 'dev.dataservice1.com')

    def test_parse_bytes_str(self):
        value_and_expected = {
            (1, 1),
            (2, 2),
            ("1", 1),
            ("2", 2),
            ("1MB", 1024 * 1024),
            ("1 MB", 1024 * 1024),
            ("3MB", 3 * 1024 * 1024),
            ("100MB", 100 * 1024 * 1024),
        }
        for value, exp in value_and_expected:
            self.assertEqual(exp, ddsc.config.Config.parse_bytes_str(value))

    def test_default_num_workers(self):
        orig_max_default_workers = ddsc.config.MAX_DEFAULT_WORKERS
        ddsc.config.MAX_DEFAULT_WORKERS = 5000
        self.assertEqual(multiprocessing.cpu_count(), ddsc.config.default_num_workers())
        ddsc.config.MAX_DEFAULT_WORKERS = 1
        self.assertEqual(1, ddsc.config.default_num_workers())
        ddsc.config.MAX_DEFAULT_WORKERS = orig_max_default_workers

    @patch('ddsc.config.os')
    @patch('ddsc.config.verify_file_private')
    def test_create_config_no_env_set(self, mock_verify_file_private, mock_os):
        mock_os.path.expanduser.return_value = '/never/gonna/happen.file'
        mock_os.path.exists.return_value = False
        mock_os.environ.get.return_value = None
        ddsc.config.create_config()
        mock_verify_file_private.assert_called()

    @patch('ddsc.config.os')
    @patch('ddsc.config.verify_file_private')
    def test_create_config_with_env_set(self, mock_verify_file_private, mock_os):
        mock_os.path.expanduser.return_value = '/never/gonna/happen.file'
        mock_os.path.exists.return_value = False
        mock_os.environ.get.return_value = "/shared/ddsclient.config"
        ddsc.config.create_config()
        mock_verify_file_private.assert_not_called()

    @patch('ddsc.config.os')
    def test_get_user_config_filename(self, mock_os):
        # Pretend there is no LOCAL_CONFIG_ENV set
        mock_os.environ.get.return_value = None
        self.assertEqual(ddsc.config.LOCAL_CONFIG_FILENAME, ddsc.config.get_user_config_filename())
        mock_os.environ.get.assert_called_with(ddsc.config.LOCAL_CONFIG_ENV)
        mock_os.environ.get.reset_mock()
        # Pretend LOCAL_CONFIG_ENV is set to /tmp/special.ddsclient.conf
        mock_os.environ.get.return_value = '/tmp/special.ddsclient.conf'
        self.assertEqual('/tmp/special.ddsclient.conf', ddsc.config.get_user_config_filename())
        mock_os.environ.get.assert_called_with(ddsc.config.LOCAL_CONFIG_ENV)

    @patch('ddsc.config.os')
    def test_get_user_config_get_page_size(self, mock_os):
        config = ddsc.config.Config()
        self.assertEqual(config.page_size, 100)
        some_config = {
            'get_page_size': 200,
        }
        config.update_properties(some_config)
        self.assertEqual(config.page_size, 200)

    @patch('ddsc.config.os')
    def test_storage_provider_id(self, mock_os):
        config = ddsc.config.Config()
        self.assertEqual(config.storage_provider_id, None)
        some_config = {
            'storage_provider_id': '123456',
        }
        config.update_properties(some_config)
        self.assertEqual(config.storage_provider_id, '123456')

    @patch('ddsc.config.os')
    def test_add_properties_empty_file(self, mock_os):
        mock_os.path.expanduser.return_value = '/home/user/.ddsclient'
        mock_os.path.exists.return_value = True
        config = ddsc.config.Config()
        with self.assertRaises(DDSUserException) as raised_exception:
            with patch('builtins.open', mock_open(read_data='')):
                config.add_properties('~/.ddsclient')
        self.assertEqual(str(raised_exception.exception), 'Error: Empty config file /home/user/.ddsclient')

    @patch('ddsc.config.os')
    def test_azure_properties(self, mock_os):
        config = ddsc.config.Config()
        some_config = {
            'azure_storage_account': '678',
            'azure_container_name': '890',
            'azure_delivery_url': 'someurl',
            'delivery_token': 'secret'
        }
        config.update_properties(some_config)
        self.assertEqual(config.azure_storage_account, '678')
        self.assertEqual(config.azure_container_name, '890')
        self.assertEqual(config.azure_delivery_url, 'someurl')
        self.assertEqual(config.delivery_token, 'secret')
