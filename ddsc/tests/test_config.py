from unittest import TestCase
import math
import ddsc.config
import multiprocessing


class TestConfig(TestCase):
    def test_empty_config(self):
        config = ddsc.config.Config()
        self.assertEqual(config.url, ddsc.config.DUKE_DATA_SERVICE_URL)
        self.assertEqual(config.user_key, None)
        self.assertEqual(config.agent_key, None)
        self.assertEqual(config.auth, None)
        self.assertEqual(config.upload_bytes_per_chunk, ddsc.config.DDS_DEFAULT_UPLOAD_CHUNKS)
        self.assertEqual(config.upload_workers, min(multiprocessing.cpu_count(), ddsc.config.MAX_DEFAULT_WORKERS))

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
        }

        config.update_properties(global_config)
        self.assertEqual(config.url, 'dataservice1.com')
        self.assertEqual(config.user_key, 'abc')
        self.assertEqual(config.agent_key, '123')
        self.assertEqual(config.auth, 'secret')
        self.assertEqual(config.upload_bytes_per_chunk, 1293892)
        num_upload_workers = min(multiprocessing.cpu_count(), ddsc.config.MAX_DEFAULT_WORKERS)
        self.assertEqual(config.upload_workers, num_upload_workers)
        self.assertEqual(config.download_workers, int(math.ceil(num_upload_workers / 2)))

        config.update_properties(local_config)
        self.assertEqual(config.url, 'dataservice2.com')
        self.assertEqual(config.user_key, 'cde')
        self.assertEqual(config.agent_key, '456')
        self.assertEqual(config.auth, 'secret')
        self.assertEqual(config.upload_bytes_per_chunk, 1293892)
        self.assertEqual(config.upload_workers, 45)
        self.assertEqual(config.download_workers, 44)

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
