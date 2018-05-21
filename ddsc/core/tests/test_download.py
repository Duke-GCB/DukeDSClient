from __future__ import absolute_import
from unittest import TestCase
from ddsc.core.download import ProjectDownload, RetryChunkDownloader, DownloadInconsistentError, \
    PartialChunkDownloadError, TooLargeChunkDownloadError
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


class TestRetryChunkDownloader(TestCase):
    def test_run_handles_exception(self):
        mock_project_file = Mock()
        mock_context = Mock()
        downloader = RetryChunkDownloader(project_file=mock_project_file, local_path=None, seek_amt=None,
                                          bytes_to_read=None, download_context=mock_context)
        downloader.retry_download_loop = Mock()
        downloader.retry_download_loop.side_effect = ValueError("Oops")
        downloader.run()

        self.assertTrue(downloader.retry_download_loop.called)
        self.assertTrue(mock_context.send_error_message.called)
        args, kwargs = mock_context.send_error_message.call_args
        self.assertIn('ValueError: Oops', args[0])

    @patch('ddsc.core.download.RemoteFileUrl')
    def test_retry_download_loop(self, mock_remote_file_url):
        mock_project_file = Mock()
        mock_context = Mock()
        downloader = RetryChunkDownloader(project_file=mock_project_file, local_path=None, seek_amt=None,
                                          bytes_to_read=None, download_context=mock_context)

        downloader.get_url_and_headers_for_range = Mock()
        downloader.get_url_and_headers_for_range.return_value = 'someurl', ['headers']
        downloader.download_chunk = Mock()

        downloader.retry_download_loop()

        self.assertTrue(downloader.get_url_and_headers_for_range.called)
        downloader.download_chunk.assert_called_with('someurl', ['headers'])
        self.assertFalse(downloader.remote_store.get_project_file.called)

    @patch('ddsc.core.download.RemoteFileUrl')
    def test_retry_download_loop_retries_then_works(self, mock_remote_file_url):
        mock_project_file = Mock()
        mock_context = Mock()
        downloader = RetryChunkDownloader(project_file=mock_project_file, local_path=None, seek_amt=None,
                                          bytes_to_read=None, download_context=mock_context)

        downloader.get_url_and_headers_for_range = Mock()
        downloader.get_url_and_headers_for_range.side_effect = [
            DownloadInconsistentError(),
            ('someurl', ['headers'])
        ]
        downloader.download_chunk = Mock()

        downloader.retry_download_loop()

        self.assertEqual(downloader.get_url_and_headers_for_range.call_count, 2)
        downloader.remote_store.get_project_file.assert_called_with(mock_project_file.id)

    @patch('ddsc.core.download.RemoteFileUrl')
    def test_retry_download_loop_raises_when_out_of_retries(self, mock_remote_file_url):
        mock_project_file = Mock()
        mock_context = Mock()
        downloader = RetryChunkDownloader(project_file=mock_project_file, local_path=None, seek_amt=None,
                                          bytes_to_read=None, download_context=mock_context)

        downloader.get_url_and_headers_for_range = Mock()
        downloader.get_url_and_headers_for_range.side_effect = [
            DownloadInconsistentError(),
        ]
        downloader.retry_times = downloader.max_retry_times

        with self.assertRaises(DownloadInconsistentError):
            downloader.retry_download_loop()

    def test_get_url_and_headers_for_range(self):
        mock_context = Mock()
        mock_file_download = Mock(host='somehost', url='someurl')
        mock_file_download.http_headers = {'secret': '222'}
        downloader = RetryChunkDownloader(project_file=None, local_path=None, seek_amt=None,
                                          bytes_to_read=None, download_context=mock_context)

        downloader.get_range_headers = Mock()
        downloader.get_range_headers.return_value = {'Range': 'bytes=100-200'}

        url, headers = downloader.get_url_and_headers_for_range(mock_file_download)

        self.assertEqual(url, 'somehost/someurl')
        self.assertEqual(headers, {'Range': 'bytes=100-200', 'secret': '222'})

    def test_get_range_headers(self):
        mock_context = Mock()
        downloader = RetryChunkDownloader(project_file=None, local_path=None, seek_amt=None,
                                          bytes_to_read=None, download_context=mock_context)
        downloader.seek_amt = 200
        downloader.bytes_to_read = 100

        self.assertEqual(downloader.get_range_headers(), {'Range': 'bytes=200-299'})

    @patch('ddsc.core.download.requests')
    def test_download_chunk_inconsistent(self, mock_requests):
        mock_context = Mock()
        downloader = RetryChunkDownloader(project_file=None, local_path=None, seek_amt=None,
                                          bytes_to_read=None, download_context=mock_context)

        mock_requests.get.return_value = Mock(status_code=401, text='Bad Download')

        with self.assertRaises(DownloadInconsistentError) as raised_exception:
            downloader.download_chunk('someurl', {})
            self.assertEqual(str(raised_exception), 'Bad Download')

    @patch('ddsc.core.download.requests')
    def test_download_chunk_works(self, mock_requests):
        mock_context = Mock()
        downloader = RetryChunkDownloader(project_file=None, local_path=None, seek_amt=None,
                                          bytes_to_read=None, download_context=mock_context)

        mock_requests.get.return_value = Mock(status_code=200)
        downloader._write_response_to_file = Mock()
        downloader._verify_download_complete = Mock()

        downloader.download_chunk('someurl', {})

        self.assertTrue(mock_requests.get.return_value.raise_for_status.called)
        self.assertTrue(downloader._write_response_to_file.called)
        self.assertTrue(downloader._verify_download_complete.called)

    def test_on_bytes_read(self):
        mock_context = Mock()
        downloader = RetryChunkDownloader(project_file=None, local_path=None, seek_amt=None,
                                          bytes_to_read=None, download_context=mock_context)

        downloader.actual_bytes_read = 10
        downloader.bytes_to_read = 100

        downloader._on_bytes_read(num_bytes_read=90)

        mock_context.send_processed_message.assert_called_with(90)

    def test_on_bytes_read_too_much(self):
        mock_context = Mock()
        downloader = RetryChunkDownloader(project_file=None, local_path=None, seek_amt=None,
                                          bytes_to_read=None, download_context=mock_context)

        downloader.actual_bytes_read = 10
        downloader.bytes_to_read = 100

        with self.assertRaises(TooLargeChunkDownloadError):
            downloader._on_bytes_read(num_bytes_read=100)

    def test_verify_download_complete_ok(self):
        mock_context = Mock()
        downloader = RetryChunkDownloader(project_file=None, local_path=None, seek_amt=None,
                                          bytes_to_read=None, download_context=mock_context)
        downloader.actual_bytes_read = 100
        downloader.bytes_to_read = 100

        downloader._verify_download_complete()

    def test_verify_download_complete_too_large(self):
        mock_context = Mock()
        downloader = RetryChunkDownloader(project_file=None, local_path=None, seek_amt=None,
                                          bytes_to_read=None, download_context=mock_context)
        downloader.actual_bytes_read = 200
        downloader.bytes_to_read = 100

        with self.assertRaises(TooLargeChunkDownloadError):
            downloader._verify_download_complete()

    def test_verify_download_complete_partial(self):
        mock_context = Mock()
        downloader = RetryChunkDownloader(project_file=None, local_path=None, seek_amt=None,
                                          bytes_to_read=None, download_context=mock_context)
        downloader.actual_bytes_read = 10
        downloader.bytes_to_read = 100

        with self.assertRaises(PartialChunkDownloadError):
            downloader._verify_download_complete()
