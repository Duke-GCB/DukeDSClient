from __future__ import absolute_import
from unittest import TestCase
from ddsc.core.download import ProjectDownload
from mock import MagicMock, Mock, patch


class TestProjectDownload(TestCase):
    @patch('ddsc.core.download.FileDownloader')
    @patch('ddsc.core.download.ProjectDownload.check_file_size')
    @patch('ddsc.core.download.os')
    @patch('ddsc.core.download.PathData')
    def test_visit_file_download_pre_processor_off(self, mock_path_data, mock_os, mock_file_downloader,
                                                   mock_check_file_size):
        project_download = ProjectDownload(remote_store=MagicMock(),
                                           project=Mock(name='test'),
                                           dest_directory='/tmp/fakedir',
                                           path_filter=MagicMock())
        project_download.watcher = Mock()
        project_download.visit_file(Mock(), None)

    @patch('ddsc.core.download.FileDownloader')
    @patch('ddsc.core.download.ProjectDownload.check_file_size')
    @patch('ddsc.core.download.os')
    @patch('ddsc.core.download.PathData')
    def test_visit_file_download_pre_processor_on(self, mock_path_data, mock_os, mock_file_downloader,
                                                  mock_check_file_size):
        pre_processor_run = MagicMock()
        pre_processor = Mock(run=pre_processor_run)
        project_download = ProjectDownload(remote_store=MagicMock(),
                                           project=Mock(name='test'),
                                           dest_directory='/tmp/fakedir',
                                           path_filter=MagicMock(),
                                           file_download_pre_processor=pre_processor)
        project_download.watcher = Mock()
        fake_file = MagicMock()
        project_download.visit_file(fake_file, None)
        pre_processor_run.assert_called()
        args, kwargs = pre_processor_run.call_args
        # args[0] is data_service
        self.assertEqual(fake_file, args[1])

    @patch('ddsc.core.download.os')
    @patch('ddsc.core.download.PathData')
    def test_file_exists_with_same_hash(self, mock_path_data, mock_os):
        item = Mock(hash_alg='md5', file_hash='f@ncyh@shvalue')
        path = '/tmp/somepath/data.txt'

        # Case where file doesn't exist
        mock_os.path.exists.return_value = False
        self.assertEqual(False, ProjectDownload.file_exists_with_same_hash(item, path))
        mock_path_data.reset_mock()

        # Case where file exists with same file hash
        mock_os.path.exists.return_value = True
        mock_path_data.return_value.get_hash.return_value.matches.return_value = True
        self.assertEqual(True, ProjectDownload.file_exists_with_same_hash(item, path))
        mock_path_data.return_value.get_hash.return_value.matches.assert_called_with('md5', 'f@ncyh@shvalue')
        mock_path_data.reset_mock()

        # Case where file exists with different file hash
        mock_os.path.exists.return_value = True
        mock_path_data.return_value.get_hash.return_value.matches.return_value = False
        self.assertEqual(False, ProjectDownload.file_exists_with_same_hash(item, path))
        mock_path_data.return_value.get_hash.return_value.matches.assert_called_with('md5', 'f@ncyh@shvalue')
        mock_path_data.reset_mock()
