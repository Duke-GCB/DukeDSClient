from __future__ import absolute_import
from unittest import TestCase
import os
from ddsc.core.download import ProjectDownload, RetryChunkDownloader, DownloadInconsistentError, \
    PartialChunkDownloadError, TooLargeChunkDownloadError, DownloadSettings, DownloadContext, \
    download_file_part_run, DownloadFilePartCommand, FileUrlDownloader, MIN_DOWNLOAD_CHUNK_SIZE
from mock import Mock, patch, mock_open


class TestProjectDownload(TestCase):
    def setUp(self):
        self.mock_file1 = Mock(path="somepath/data1.txt", size=100)
        self.mock_file2 = Mock(path="somepath/data2.txt", size=452)
        self.mock_remote_store = Mock()
        self.mock_remote_store.get_project_files.return_value = [
            self.mock_file1,
            self.mock_file2,
        ]
        self.mock_project = Mock()
        self.mock_path_filter = Mock()
        self.mock_path_filter.include_path.return_value = True

    @patch('ddsc.core.download.FileUrlDownloader')
    @patch('ddsc.core.download.ProgressPrinter')
    @patch('ddsc.core.download.DownloadSettings')
    @patch('ddsc.core.download.os')
    @patch('ddsc.core.download.PathData')
    def test_run(self, mock_path_data, mock_os, mock_download_settings, mock_progress_printer, mock_file_url_downloader):
        project_download = ProjectDownload(self.mock_remote_store, self.mock_project, '/tmp/dest',
                                           self.mock_path_filter)

        project_download.try_create_dir = Mock()
        project_download.check_warnings = Mock()
        project_download.check_warnings.return_value = 'Things went wrong'

        project_download.run()

        # Makes root directory and updates progress
        project_download.try_create_dir.assert_called_with('/tmp/dest')
        mock_progress_printer.assert_called_with(100 + 452, msg_verb='downloading')
        self.assertTrue(mock_progress_printer.return_value.finished.called)
        mock_progress_printer.return_value.show_warning.assert_called_with('Things went wrong')

        # Downloads files
        mock_file_url_downloader.assert_called_with(mock_download_settings.return_value,
                                                    [self.mock_file1, self.mock_file2],
                                                    mock_progress_printer.return_value)
        self.assertTrue(mock_file_url_downloader.return_value.make_local_directories.called)
        self.assertTrue(mock_file_url_downloader.return_value.make_big_empty_files.called)
        self.assertTrue(mock_file_url_downloader.return_value.download_files.called)

    def test_check_warnings(self):
        project_download = ProjectDownload(self.mock_remote_store, self.mock_project, '/tmp/dest',
                                           self.mock_path_filter)
        project_download.path_filter = Mock()
        project_download.path_filter.get_unused_paths.return_value = []
        self.assertEqual(project_download.check_warnings(), None)
        project_download.path_filter.get_unused_paths.return_value = ["tmp/data.txt"]
        self.assertEqual(project_download.check_warnings().strip(), 'WARNING: Path(s) not found: tmp/data.txt.')

    @patch('ddsc.core.download.os')
    @patch('ddsc.core.download.PathData')
    def test_include_project_file(self, mock_path_data, mock_os):
        project_download = ProjectDownload(self.mock_remote_store, self.mock_project, '/tmp/dest',
                                           self.mock_path_filter)

        project_download.path_filter = Mock()

        # File excluded by path filter
        project_download.path_filter.include_path.return_value = False
        self.assertEqual(False, project_download.include_project_file(Mock(path='/tmp/data.txt')))

        # Local file doesn't exist
        project_download.path_filter.include_path.return_value = True
        mock_os.path.exists.return_value = False
        self.assertEqual(True, project_download.include_project_file(Mock(path='/tmp/data.txt')))

        # Local file has same hash as remote file
        project_download.path_filter.include_path.return_value = True
        mock_os.path.exists.return_value = True
        mock_project_file = Mock(path='/tmp/data.txt')
        mock_project_file.get_hash.return_value = 'abcd'
        mock_path_data.return_value.get_hash.return_value = Mock(value='abcd')
        self.assertEqual(False, project_download.include_project_file(mock_project_file))

        # Local file has different hash than remote file
        mock_project_file = Mock(path='/tmp/data.txt')
        mock_project_file.get_hash.return_value = 'abcd'
        mock_path_data.return_value.get_hash.return_value = Mock(value='abcd')
        self.assertEqual(False, project_download.include_project_file(mock_project_file))


class TestDownloadSettings(TestCase):
    def test_get_data_service_auth_data(self):
        mock_remote_store = Mock()
        mock_remote_store.data_service.auth.get_auth_data.return_value = 'auth data'

        settings = DownloadSettings(remote_store=mock_remote_store, dest_directory='/tmp/data', watcher=Mock())

        self.assertEqual(settings.get_data_service_auth_data(), 'auth data')


class TestDownloadContext(TestCase):
    def setUp(self):
        self.mock_config = Mock()
        self.mock_settings = Mock(config=self.mock_config)
        self.mock_message_queue = Mock()

    @patch('ddsc.core.download.DataServiceAuth')
    @patch('ddsc.core.download.DataServiceApi')
    def test_create_data_service(self, mock_data_service_api, mock_data_service_auth):
        context = DownloadContext(self.mock_settings, None, self.mock_message_queue, task_id='123')
        data_service = context.create_data_service()

        self.assertEqual(mock_data_service_api.return_value, data_service)
        mock_data_service_auth.assert_called_with(self.mock_config)
        mock_data_service_api.assert_called_with(mock_data_service_auth.return_value, self.mock_config.url)

    @patch('ddsc.core.download.DataServiceAuth')
    @patch('ddsc.core.download.DataServiceApi')
    @patch('ddsc.core.download.RemoteStore')
    def test_create_remote_store(self, mock_remote_store, mock_data_service_api, mock_data_service_auth):
        context = DownloadContext(self.mock_settings, None, self.mock_message_queue, task_id='123')
        remote_store = context.create_remote_store()
        self.assertEqual(remote_store, mock_remote_store.return_value)
        mock_remote_store.assert_called_with(self.mock_config, mock_data_service_api.return_value)

    def test_send_message(self):
        context = DownloadContext(self.mock_settings, None, self.mock_message_queue, task_id='123')
        context.send_message('somedata')
        self.mock_message_queue.put.assert_called_with(('123', 'somedata'))

    def test_send_processed_message(self):
        context = DownloadContext(self.mock_settings, None, self.mock_message_queue, task_id='123')
        context.send_processed_message(100)
        self.mock_message_queue.put.assert_called_with(('123', ('processed', 100)))

    def test_send_error_message(self):
        context = DownloadContext(self.mock_settings, None, self.mock_message_queue, task_id='123')
        context.send_error_message('oops')
        self.mock_message_queue.put.assert_called_with(('123', ('error', 'oops')))


class TestFileUrlDownloader(TestCase):
    def setUp(self):
        self.mock_settings = Mock(dest_directory='/tmp/data2')
        self.mock_file1 = Mock(path="data/file1.txt", size=200)
        self.mock_file1.get_remote_parent_path.return_value = 'data'
        self.mock_file1.get_local_path.return_value = '/tmp/data2/data/file1.txt'
        self.mock_file_urls = [
            self.mock_file1
        ]
        self.mock_watcher = Mock()

    @patch('ddsc.core.download.TaskRunner')
    @patch('ddsc.core.download.TaskExecutor')
    @patch('ddsc.core.download.os')
    def test_make_local_directories(self, mock_os, mock_task_executor, mock_task_runner):
        mock_os.path.exists.return_value = True
        mock_os.path = os.path
        downloader = FileUrlDownloader(self.mock_settings, self.mock_file_urls, self.mock_watcher)
        downloader.make_local_directories()
        mock_os.makedirs.assert_called_with('/tmp/data2/data')

    @patch('ddsc.core.download.TaskRunner')
    @patch('ddsc.core.download.TaskExecutor')
    @patch('ddsc.core.download.os')
    def test_make_big_empty_files(self, mock_os, mock_task_executor, mock_task_runner):
        mock_os.path.exists.return_value = True
        mock_os.path = os.path
        downloader = FileUrlDownloader(self.mock_settings, self.mock_file_urls, self.mock_watcher)
        fake_open = mock_open()
        with patch('ddsc.core.download.open', fake_open, create=True):
            downloader.make_big_empty_files()
        fake_open.assert_called_with('/tmp/data2/data/file1.txt', 'wb')
        fake_open.return_value.seek.assert_called_with(199)
        fake_open.return_value.write.assert_called_with(b'\0')

    @patch('ddsc.core.download.TaskRunner')
    @patch('ddsc.core.download.TaskExecutor')
    def test_download_files(self, mock_task_executor, mock_task_runner):
        downloader = FileUrlDownloader(self.mock_settings, self.mock_file_urls, self.mock_watcher)
        downloader.split_file_urls_by_size = Mock()
        mock_small_file = Mock(size=100)
        mock_small_files = [mock_small_file]
        mock_large_file = Mock(size=200)
        mock_large_files = [mock_large_file]
        downloader.split_file_urls_by_size.return_value = [mock_large_files, mock_small_files]
        downloader.make_ranges = Mock()
        downloader.make_ranges.return_value = [
            (0, 90),
            (91, 200),
        ]
        downloader.download_files()

        # Download small file in one command, large file in two commands).
        add_calls = mock_task_runner.return_value.add.call_args_list
        self.assertEqual(3, len(add_calls))
        command = add_calls[0][1]['command']
        self.assertEqual(command.bytes_to_read, 100)
        self.assertEqual(command.seek_amt, 0)
        command = add_calls[1][1]['command']
        self.assertEqual(command.bytes_to_read, 91)
        self.assertEqual(command.seek_amt, 0)
        command = add_calls[2][1]['command']
        self.assertEqual(command.bytes_to_read, 110)
        self.assertEqual(command.seek_amt, 91)

        self.assertTrue(mock_task_runner.return_value.run.called)

    def test_make_ranges(self):
        # Only one worker because file size is too small
        self.assert_make_ranges(
            workers=2,
            file_size=100,
            expected=[
                (0, 99),
            ]
        )

        # Big enough file should split into two
        self.assert_make_ranges(
            workers=2,
            file_size=100 * 1000 * 1000,
            expected=[
                (0, 49999999),
                (50000000, 99999999)
            ]
        )

        # Big enough file should split into three
        self.assert_make_ranges(
            workers=3,
            file_size=100 * 1000 * 1000,
            expected=[
                (0, 33333333),
                (33333334, 66666667),
                (66666668, 99999999),
            ]
        )

        # Uneven split
        self.assert_make_ranges(
            workers=3,
            file_size=100 * 1000 * 1000 - 1,
            expected=[
                (0, 33333332),
                (33333333, 66666665),
                (66666666, 99999998),
            ]
        )

    def assert_make_ranges(self, workers, file_size, expected):
        self.mock_settings.config.download_workers = workers
        downloader = FileUrlDownloader(self.mock_settings, self.mock_file_urls, self.mock_watcher)
        self.assertEqual(expected, downloader.make_ranges(Mock(size=file_size)))

    def test_determine_bytes_per_chunk(self):
        downloader = FileUrlDownloader(self.mock_settings, self.mock_file_urls, self.mock_watcher)
        downloader.settings.config.download_workers = 2
        size = 10
        self.assertEqual(downloader.determine_bytes_per_chunk(size), MIN_DOWNLOAD_CHUNK_SIZE)
        size = MIN_DOWNLOAD_CHUNK_SIZE * 4
        self.assertEqual(downloader.determine_bytes_per_chunk(size), MIN_DOWNLOAD_CHUNK_SIZE * 2)
        size = MIN_DOWNLOAD_CHUNK_SIZE * 5
        self.assertEqual(downloader.determine_bytes_per_chunk(size), MIN_DOWNLOAD_CHUNK_SIZE * 2.5)

    def test_split_file_urls_by_size(self):
        downloader = FileUrlDownloader(self.mock_settings, self.mock_file_urls, self.mock_watcher)
        downloader.file_urls = [
            Mock(size=90),
            Mock(size=100),
            Mock(size=99),
            Mock(size=200),
            Mock(size=400),
        ]
        large_items, small_items = downloader.split_file_urls_by_size(100)
        self.assertEqual(set([90, 99]), set([item.size for item in small_items]))
        self.assertEqual(set([200, 100, 400]), set([item.size for item in large_items]))


class TestDownloadFilePartCommand(TestCase):
    @patch('ddsc.core.download.DownloadContext')
    def test_create_context(self, mock_download_context):
        mock_settings = Mock(dest_directory='/tmp/dest')
        mock_file_url = Mock(json_data={})
        command = DownloadFilePartCommand(mock_settings, mock_file_url, 100, 200)
        mock_message_queue = Mock()
        context = command.create_context(mock_message_queue, 123)
        self.assertEqual(context, mock_download_context.return_value)
        mock_download_context.assert_called_with(mock_settings, ('/tmp/dest', {}, 100, 200), mock_message_queue, 123)

    @patch('ddsc.core.download.DownloadContext')
    def test_on_message_processed(self, mock_download_context):
        mock_settings = Mock(dest_directory='/tmp/dest')
        mock_file_url = Mock(json_data={})
        command = DownloadFilePartCommand(mock_settings, mock_file_url, 100, 200)
        command.on_message(('processed', 2000))
        mock_settings.watcher.transferring_item.assert_called_with(mock_file_url, 2000)

    @patch('ddsc.core.download.DownloadContext')
    def test_on_message_error(self, mock_download_context):
        mock_settings = Mock(dest_directory='/tmp/dest')
        mock_file_url = Mock(json_data={})
        command = DownloadFilePartCommand(mock_settings, mock_file_url, 100, 200)
        with self.assertRaises(ValueError) as raised_error:
            command.on_message(('error', 'Oops'))
            self.assertEqual(str(raised_error.exception), 'Oops')


class TestDownloadFilePartRun(TestCase):
    @patch('ddsc.core.download.RetryChunkDownloader')
    @patch('ddsc.core.download.ProjectFile')
    def test_download_file_part_run(self, mock_project_file, mock_retry_chunk_downloader):
        mock_context = Mock(params=('/tmp/dest', {}, 0, 100))

        download_file_part_run(mock_context)

        mock_retry_chunk_downloader.assert_called_with(mock_project_file.return_value,
                                                       mock_project_file.return_value.get_local_path.return_value,
                                                       0, 100, mock_context)


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
