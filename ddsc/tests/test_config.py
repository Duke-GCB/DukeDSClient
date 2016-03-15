from unittest import TestCase
import ddsc.config


class TestConfig(TestCase):
    def test_empty_config(self):
        config = ddsc.config.Config()
        self.assertEqual(config.url, ddsc.config.DDS_DEFAULT_URL)
        self.assertEqual(config.user_key, None)
        self.assertEqual(config.agent_key, None)
        self.assertEqual(config.auth, None)
        self.assertEqual(config.upload_bytes_per_chunk, ddsc.config.DDS_DEFAULT_UPLOAD_CHUNKS)
        self.assertEqual(config.upload_workers, None)

    def test_global_then_local(self):
        config = ddsc.config.Config()
        global_config = {
            'url':'dataservice1.com',
            'user_key': 'abc',
            'agent_key': '123',
            'auth': 'secret',
            'upload_bytes_per_chunk': 1293892
        }
        local_config = {
            'url':'dataservice2.com',
            'user_key': 'cde',
            'agent_key': '456',
            'upload_workers': 45,
        }

        config.update_properties(global_config)
        self.assertEqual(config.url, 'dataservice1.com')
        self.assertEqual(config.user_key, 'abc')
        self.assertEqual(config.agent_key, '123')
        self.assertEqual(config.auth, 'secret')
        self.assertEqual(config.upload_bytes_per_chunk, 1293892)
        self.assertEqual(config.upload_workers, None)

        config.update_properties(local_config)
        self.assertEqual(config.url, 'dataservice2.com')
        self.assertEqual(config.user_key, 'cde')
        self.assertEqual(config.agent_key, '456')
        self.assertEqual(config.auth, 'secret')
        self.assertEqual(config.upload_bytes_per_chunk, 1293892)
        self.assertEqual(config.upload_workers, 45)

    def test_MB_chunk_convert(self):
        config = ddsc.config.Config()
        some_config = {
            'upload_bytes_per_chunk': '10MB'
        }
        config.update_properties(some_config)
        self.assertEqual(config.upload_bytes_per_chunk, 10485760)
        some_config = {
            'upload_bytes_per_chunk': '20MB'
        }
        config.update_properties(some_config)
        self.assertEqual(config.upload_bytes_per_chunk, 20971520)