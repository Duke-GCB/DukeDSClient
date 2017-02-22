from unittest import TestCase
import pickle
from ddsc.core.projectuploader import UploadSettings, UploadContext


class FakeDataServiceApi(object):
    def __init__(self):
        self.auth = FakeDataServiceAuth()


class FakeDataServiceAuth(object):
    def get_auth_data(self):
        return ()


class TestUploadContext(TestCase):
    def test_can_pickle(self):
        """Make sure we can pickle context since it must be passed to another process."""
        settings = UploadSettings(None, FakeDataServiceApi(), None, None)
        params = ('one', 'two', 'three')
        context = UploadContext(settings, params)
        pickle.dumps(context)
