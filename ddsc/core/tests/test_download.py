from __future__ import absolute_import
from unittest import TestCase
from ddsc.core.download import ProjectDownload
from mock import MagicMock, Mock, patch


class TestProjectDownload(TestCase):
    @patch('ddsc.core.download.FileDownloader')
    @patch('ddsc.core.download.ProjectDownload.check_file_size')
    def test_visit_file_download_pre_processor_off(self, mock_file_downloader, mock_check_file_size):
        project_download = ProjectDownload(remote_store=MagicMock(),
                                           project_name='test',
                                           dest_directory='/tmp/fakedir',
                                           path_filter=MagicMock())
        project_download.visit_file(Mock(), None)

    @patch('ddsc.core.download.FileDownloader')
    @patch('ddsc.core.download.ProjectDownload.check_file_size')
    def test_visit_file_download_pre_processor_on(self, mock_file_downloader, mock_check_file_size):
        pre_processor_run = MagicMock()
        pre_processor = Mock(run=pre_processor_run)
        project_download = ProjectDownload(remote_store=MagicMock(),
                                           project_name='test',
                                           dest_directory='/tmp/fakedir',
                                           path_filter=MagicMock(),
                                           file_download_pre_processor=pre_processor)
        fake_file = MagicMock()
        project_download.visit_file(fake_file, None)
        pre_processor_run.assert_called()
        args, kwargs = pre_processor_run.call_args
        # args[0] is data_service
        self.assertEqual(fake_file, args[1])
