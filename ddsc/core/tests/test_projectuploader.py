from unittest import TestCase
import pickle
from ddsc.core.projectuploader import UploadSettings, UploadContext, ProjectUploadDryRun
from ddsc.core.util import KindType
from mock import MagicMock


class FakeDataServiceApi(object):
    def __init__(self):
        self.auth = FakeDataServiceAuth()


class FakeDataServiceAuth(object):
    def get_auth_data(self):
        return ()


class TestUploadContext(TestCase):
    def test_can_pickle(self):
        """Make sure we can pickle context since it must be passed to another process."""
        settings = UploadSettings(None, FakeDataServiceApi(), None, None, None)
        params = ('one', 'two', 'three')
        context = UploadContext(settings, params)
        pickle.dumps(context)


class TestProjectUploadDryRun(TestCase):
    def test_single_empty_non_existant_directory(self):
        local_file = MagicMock(kind=KindType.folder_str, children=[], path='joe', remote_id=None)
        local_project = MagicMock(kind=KindType.project_str, children=[local_file])
        upload_dry_run = ProjectUploadDryRun()
        upload_dry_run.run(local_project)
        self.assertEqual(['joe'], upload_dry_run.upload_items)

    def test_single_empty_existing_directory(self):
        local_file = MagicMock(kind=KindType.folder_str, children=[], path='joe', remote_id='abc')
        local_project = MagicMock(kind=KindType.project_str, children=[local_file])
        upload_dry_run = ProjectUploadDryRun()
        upload_dry_run.run(local_project)
        self.assertEqual([], upload_dry_run.upload_items)

    def test_some_files(self):
        local_file1 = MagicMock(kind=KindType.file_str, path='joe.txt', need_to_send=True)
        local_file2 = MagicMock(kind=KindType.file_str, path='data.txt', need_to_send=False)
        local_file3 = MagicMock(kind=KindType.file_str, path='results.txt', need_to_send=True)
        local_project = MagicMock(kind=KindType.project_str, children=[local_file1, local_file2, local_file3])
        upload_dry_run = ProjectUploadDryRun()
        upload_dry_run.run(local_project)
        self.assertEqual(['joe.txt', 'results.txt'], upload_dry_run.upload_items)

    def test_nested_directories(self):
        local_file1 = MagicMock(kind=KindType.file_str, path='/data/2017/08/flyresults/joe.txt', need_to_send=True)
        local_file2 = MagicMock(kind=KindType.file_str, path='/data/2017/08/flyresults/data.txt', need_to_send=False)
        local_file3 = MagicMock(kind=KindType.file_str, path='/data/2017/08/flyresults/results.txt', need_to_send=True)
        grandchild_folder = MagicMock(kind=KindType.folder_str,
                                      path="/data/2017/08/flyresults",
                                      children=[local_file1, local_file2, local_file3],
                                      remote_id=None)
        child_folder = MagicMock(kind=KindType.folder_str,
                                 path="/data/2017/08",
                                 children=[grandchild_folder],
                                 remote_id=None)
        parent_folder = MagicMock(kind=KindType.folder_str,
                                  path="/data/2017",
                                  children=[child_folder],
                                  remote_id=None)
        local_project = MagicMock(kind=KindType.project_str, children=[parent_folder])
        upload_dry_run = ProjectUploadDryRun()
        upload_dry_run.run(local_project)
        expected_results = [
            '/data/2017',
            '/data/2017/08',
            '/data/2017/08/flyresults',
            '/data/2017/08/flyresults/joe.txt',
            '/data/2017/08/flyresults/results.txt',
        ]
        self.assertEqual(expected_results, upload_dry_run.upload_items)

    def test_nested_directories_skip_parents(self):
        local_file1 = MagicMock(kind=KindType.file_str, path='/data/2017/08/flyresults/joe.txt', need_to_send=True)
        local_file2 = MagicMock(kind=KindType.file_str, path='/data/2017/08/flyresults/data.txt', need_to_send=False)
        local_file3 = MagicMock(kind=KindType.file_str, path='/data/2017/08/flyresults/results.txt', need_to_send=True)
        grandchild_folder = MagicMock(kind=KindType.folder_str,
                                      path="/data/2017/08/flyresults",
                                      children=[local_file1, local_file2, local_file3],
                                      remote_id=None)
        child_folder = MagicMock(kind=KindType.folder_str,
                                 path="/data/2017/08",
                                 children=[grandchild_folder],
                                 remote_id='355')
        parent_folder = MagicMock(kind=KindType.folder_str,
                                  path="/data/2017",
                                  children=[child_folder],
                                  remote_id='123')
        local_project = MagicMock(kind=KindType.project_str, children=[parent_folder])
        upload_dry_run = ProjectUploadDryRun()
        upload_dry_run.run(local_project)
        expected_results = [
            '/data/2017/08/flyresults',
            '/data/2017/08/flyresults/joe.txt',
            '/data/2017/08/flyresults/results.txt',
        ]
        self.assertEqual(expected_results, upload_dry_run.upload_items)
