from __future__ import absolute_import
from unittest import TestCase
import os
from ddsc.core.download import ProjectDownload, RetryChunkDownloader, DownloadInconsistentError, \
    PartialChunkDownloadError, TooLargeChunkDownloadError, DownloadSettings, DownloadContext, \
    download_file_part_run, DownloadFilePartCommand, FileDownloader, ProjectFile, FileHash, FileToDownload, \
    FileHashStatus
from mock import Mock, patch, mock_open, call, ANY


class TestProjectDownload(TestCase):
    def setUp(self):
        self.mock_file1 = Mock(path="somepath/data1.txt", size=100, json_data={
            "id": "1",
            "name": "data1.txt",
            "size": 100,
            "file_url": "someurl",
            "hashes": [{"algorithm": "md5", "value": "abc"}],
            "ancestors": [],
        })
        self.mock_file2 = Mock(path="somepath/data2.txt", size=452, json_data={
            "id": "2",
            "name": "data2.txt",
            "size": 452,
            "file_url": "someurl",
            "hashes": [{"algorithm": "md5", "value": "abc"}],
            "ancestors": [],
        })
        self.mock_remote_store = Mock()
        self.mock_remote_store.get_project_files.return_value = [
            self.mock_file1,
            self.mock_file2,
        ]
        self.mock_project = Mock()
        self.mock_path_filter = Mock()
        self.mock_path_filter.include_path.return_value = True
        self.mock_file_json_data = {
            "id": "abc",
            "name": "data.txt",
            "size": 100,
            "file_url": "someurl",
            "hashes": [],
            "ancestors": [],
        }

    @patch('ddsc.core.download.print')
    @patch('ddsc.core.download.HashUtil')
    @patch('ddsc.core.download.FileDownloader')
    @patch('ddsc.core.download.ProgressPrinter')
    @patch('ddsc.core.download.DownloadSettings')
    @patch('ddsc.core.download.os')
    def test_run(self, mock_os, mock_download_settings, mock_progress_printer, mock_file_downloader,
                 mock_hash_util, mock_print):
        project_download = ProjectDownload(self.mock_remote_store, self.mock_project, '/tmp/dest',
                                           self.mock_path_filter)
        project_download.try_create_dir = Mock()
        project_download.check_warnings = Mock()
        project_download.check_warnings.return_value = 'Things went wrong'
        # remote hashes for files we are downloading
        self.mock_file1.hashes = [{'algorithm': 'md5', 'value': '111'}]
        self.mock_file1.json_data["hashes"] = [{'algorithm': 'md5', 'value': '111'}]
        self.mock_file2.hashes = [{'algorithm': 'md5', 'value': '111'}]
        self.mock_file2.json_data["hashes"] = [{'algorithm': 'md5', 'value': '222'}]
        mock_hash_util.return_value.hash.hexdigest.side_effect = [
            '333',  # local file1 hash doesn't match before upload
            '444',  # local file2 hash doesn't match before upload
            '111',  # file1 hash matches after download
            '222',  # file1 hash matches after download
        ]

        project_download.run()

        # Makes root directory and updates progress
        project_download.try_create_dir.assert_called_with('/tmp/dest')
        mock_progress_printer.assert_called_with(100 + 452, msg_verb='downloading')
        self.assertTrue(mock_progress_printer.return_value.finished.called)
        mock_progress_printer.return_value.show_warning.assert_called_with('Things went wrong')

        # Downloads files
        mock_file_downloader.assert_called_with(mock_download_settings.return_value, [ANY, ANY])
        args, kwargs = mock_file_downloader.call_args

        self.assertTrue(mock_file_downloader.return_value.run.called)

        mock_print.assert_has_calls([
            call('Downloading 2 files.'),
            call('Verifying contents of 2 downloaded files using file hashes.'),
            call('/tmp/dest/data1.txt 111 md5 OK'),
            call('/tmp/dest/data2.txt 222 md5 OK'),
            call('All downloaded files have been verified successfully.')
        ])

    @patch('ddsc.core.download.print')
    def test_run_no_files(self, mock_print):
        project_download = ProjectDownload(self.mock_remote_store, self.mock_project, '/tmp/dest',
                                           self.mock_path_filter)
        project_download.try_create_dir = Mock()
        project_download.check_warnings = Mock()
        project_download.check_warnings.return_value = ''
        project_download.get_files_to_download = Mock()
        project_download.get_files_to_download.return_value = []

        project_download.run()

        project_download.try_create_dir.assert_not_called()
        mock_print.assert_has_calls([
            call('All content is already downloaded.')
        ])

    @patch('ddsc.core.download.print')
    @patch('ddsc.core.download.HashUtil')
    @patch('ddsc.core.download.FileDownloader')
    @patch('ddsc.core.download.ProgressPrinter')
    @patch('ddsc.core.download.DownloadSettings')
    @patch('ddsc.core.download.os')
    def test_run_mismatched_hash(self, mock_os, mock_download_settings, mock_progress_printer,
                                 mock_file_downloader, mock_hash_util, mock_print):
        project_download = ProjectDownload(self.mock_remote_store, self.mock_project, '/tmp/dest',
                                           self.mock_path_filter)

        project_download.try_create_dir = Mock()
        project_download.check_warnings = Mock()
        project_download.check_warnings.return_value = 'Things went wrong'
        self.mock_file1.hashes = [{'value': '111', 'algorithm': 'md5'}]
        self.mock_file1.json_data["hashes"] = [{'algorithm': 'md5', 'value': '111'}]
        self.mock_remote_store.get_project_files.return_value = [self.mock_file1]

        mock_hash_util.return_value.add_file.return_value.hexdigest.side_effect = [
            ('md5', '222'),  # local file doesn't match remote hash
            ('md5', '333'),  # after download has is wrong
        ]

        with self.assertRaises(ValueError) as raised_exception:
            project_download.run()

        # Makes root directory and updates progress
        project_download.try_create_dir.assert_called_with('/tmp/dest')
        mock_progress_printer.assert_called_with(100, msg_verb='downloading')
        self.assertTrue(mock_progress_printer.return_value.finished.called)
        mock_progress_printer.return_value.show_warning.assert_called_with('Things went wrong')

        # Downloads files
        mock_file_downloader.assert_called_with(mock_download_settings.return_value, [ANY])
        self.assertTrue(mock_file_downloader.return_value.run.called)

        mock_print.assert_has_calls([
            call('Downloading 1 files.'),
            call('Verifying contents of 1 downloaded files using file hashes.'),
            call('/tmp/dest/data1.txt 111 md5 FAILED'),
        ])

        self.assertEqual(str(raised_exception.exception),
                         "ERROR: Downloaded file(s) do not match the expected hashes.")

    def test_check_warnings(self):
        project_download = ProjectDownload(self.mock_remote_store, self.mock_project, '/tmp/dest',
                                           self.mock_path_filter)
        project_download.path_filter = Mock()
        project_download.path_filter.get_unused_paths.return_value = []
        self.assertEqual(project_download.check_warnings(), None)
        project_download.path_filter.get_unused_paths.return_value = ["tmp/data.txt"]
        self.assertEqual(project_download.check_warnings().strip(), 'WARNING: Path(s) not found: tmp/data.txt.')

    @patch('ddsc.core.download.FileToDownload')
    def test_get_files_to_download(self, mock_file_to_download):
        project_download = ProjectDownload(self.mock_remote_store, self.mock_project, '/tmp/dest',
                                           self.mock_path_filter)
        project_download.include_project_file = Mock()
        project_download.include_project_file.return_value = True
        files = project_download.get_files_to_download()
        self.assertEqual(2, len(files))
        self.assertEqual(files[0], mock_file_to_download.return_value)
        self.assertEqual(files[1], mock_file_to_download.return_value)
        mock_file_to_download.assert_has_calls([
            call(self.mock_file1.json_data, self.mock_file1.get_local_path.return_value),
            call(self.mock_file2.json_data, self.mock_file2.get_local_path.return_value),
        ])

    @patch('ddsc.core.download.os')
    @patch('ddsc.core.download.HashUtil')
    def test_include_project_file(self, mock_hash_util, mock_os):
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
        mock_project_file.hashes = [{'value': 'abcd', 'algorithm': 'md5'}]
        mock_hash_util.return_value.hash.hexdigest.return_value = 'abcd'
        self.assertEqual(False, project_download.include_project_file(mock_project_file))

        # Local file has different hash than remote file
        mock_project_file = Mock(path='/tmp/data.txt')
        mock_project_file.hashes = [{'value': 'abcd', 'algorithm': 'md5'}]
        mock_hash_util.return_value.hash.hexdigest.return_value = 'cdef'
        self.assertEqual(True, project_download.include_project_file(mock_project_file))

    @patch('ddsc.core.download.os')
    def test_run_preprocessor(self, mock_os):
        mock_preprocessor = Mock()
        project_download = ProjectDownload(self.mock_remote_store, self.mock_project, '/tmp/dest',
                                           self.mock_path_filter, file_download_pre_processor=mock_preprocessor)
        mock_file1 = Mock(path='file1.txt')
        mock_file2 = Mock(path='file2.txt')
        project_download.run_preprocessor([mock_file1, mock_file2])
        mock_preprocessor.run.assert_has_calls([
            call(self.mock_remote_store.data_service, mock_file1),
            call(self.mock_remote_store.data_service, mock_file2)
        ])

    @patch('ddsc.core.download.HashUtil')
    @patch('ddsc.core.download.print')
    def test_check_downloaded_files_when_matching_hash(self, mock_print, mock_hash_util):
        project_file = ProjectFile(self.mock_file_json_data)
        project_file.hashes = [{"algorithm": "md5", "value": "abc"}]
        mock_hash_util.return_value.hash.hexdigest.return_value = 'abc'

        downloader = ProjectDownload(None, None, dest_directory='/tmp/data2/', path_filter=None)
        downloader.check_downloaded_files([project_file])

    @patch('ddsc.core.download.HashUtil')
    @patch('ddsc.core.download.print')
    def test_check_downloaded_files_value_mismatched_hash(self, mock_print, mock_hash_util):
        project_file = ProjectFile(self.mock_file_json_data)
        project_file.hashes = [{"algorithm": "md5", "value": "abc"}]
        mock_hash_util.return_value.hash.hexdigest.return_value = 'efgh'

        downloader = ProjectDownload(None, None, dest_directory='/tmp/data2', path_filter=None)
        with self.assertRaises(ValueError) as raised_exception:
            downloader.check_downloaded_files([project_file])

        mock_print.assert_called_with("/tmp/data2/data.txt abc md5 FAILED")
        exception_str = str(raised_exception.exception)
        self.assertEqual(exception_str, "ERROR: Downloaded file(s) do not match the expected hashes.")

    @patch('ddsc.core.download.HashUtil')
    @patch('ddsc.core.download.print')
    def test_check_downloaded_files_when_no_supported_hashes(self, mock_print, mock_hash_util):
        project_file = ProjectFile(self.mock_file_json_data)
        project_file.hashes = [{"algorithm": "sha1", "value": "abc"}]
        mock_hash_util.return_value.hash.hexdigest.return_value = 'abc'

        downloader = ProjectDownload(None, None, dest_directory='/tmp/data2/', path_filter=None)
        with self.assertRaises(ValueError) as raised_exception:
            downloader.check_downloaded_files([project_file])
        exception_str = str(raised_exception.exception)
        self.assertEqual(exception_str, 'Unable to validate: No supported hashes found for file /tmp/data2/data.txt')

    @patch('ddsc.core.download.HashUtil')
    @patch('ddsc.core.download.print')
    def test_check_downloaded_files_when_conflicted_hashes(self, mock_print, mock_hash_util):
        project_file = ProjectFile(self.mock_file_json_data)
        project_file.hashes = [{"algorithm": "md5", "value": "abc"}, {"algorithm": "md5", "value": "def"}]
        mock_hash_util.return_value.hash.hexdigest.return_value = 'abc'

        downloader = ProjectDownload(None, None, dest_directory='/tmp/data2/', path_filter=None)
        downloader.check_downloaded_files([project_file])
        mock_print.assert_has_calls([
            call('All downloaded files have at least one valid hash.'),
            call("\nWARNING: Some downloaded files also have invalid hashes.\n")
        ])


class TestDownloadSettings(TestCase):
    def test_get_data_service_auth_data(self):
        mock_data_service = Mock()
        mock_data_service.auth.get_auth_data.return_value = 'auth data'

        settings = DownloadSettings(data_service=mock_data_service, config=Mock(), watcher=Mock())

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


class TestFileToDownload(TestCase):
    def test_constructor(self):
        project_file_json_data = {
            'id': '123',
            'name': 'somefile',
            'size': 100,
            'file_url': 'someurl',
            'hashes': [],
            'ancestors': [],
        }
        file_to_download = FileToDownload(json_data=project_file_json_data, local_path="/tmp/data.txt")
        self.assertEqual(file_to_download.name, "somefile")
        self.assertEqual(file_to_download.local_path, "/tmp/data.txt")


class TestFileDownloader(TestCase):
    def setUp(self):
        self.mock_config = Mock(download_bytes_per_chunk=1000)
        self.mock_settings = Mock(dest_directory='/tmp/data2', config=self.mock_config)
        self.mock_file1 = Mock(path="data/file1.txt", size=200, local_path='/tmp/data2/data/file1.txt')
        self.mock_file1.get_remote_parent_path.return_value = 'data'
        self.mock_file1.get_local_path.return_value = '/tmp/data2/data/file1.txt'
        self.mock_files_to_download = [
            self.mock_file1
        ]
        self.mock_watcher = Mock()

    def test_run(self):
        downloader = FileDownloader(self.mock_settings, self.mock_files_to_download)
        downloader.make_local_directories = Mock()
        downloader.make_big_empty_files = Mock()
        downloader.download_files = Mock()

        downloader.run()

        downloader.make_local_directories.assert_called_with()
        downloader.make_big_empty_files.assert_called_with()
        downloader.download_files.assert_called_with()

    @patch('ddsc.core.download.TaskRunner')
    @patch('ddsc.core.download.TaskExecutor')
    @patch('ddsc.core.download.os')
    def test_make_local_directories(self, mock_os, mock_task_executor, mock_task_runner):
        mock_os.path.exists.return_value = True
        mock_os.path = os.path
        downloader = FileDownloader(self.mock_settings, self.mock_files_to_download)
        downloader.make_local_directories()
        mock_os.makedirs.assert_called_with('/tmp/data2/data')

    @patch('ddsc.core.download.TaskRunner')
    @patch('ddsc.core.download.TaskExecutor')
    @patch('ddsc.core.download.os')
    def test_make_big_empty_files(self, mock_os, mock_task_executor, mock_task_runner):
        mock_os.path.exists.return_value = True
        mock_os.path = os.path
        downloader = FileDownloader(self.mock_settings, self.mock_files_to_download)
        fake_open = mock_open()
        with patch('ddsc.core.download.open', fake_open, create=True):
            downloader.make_big_empty_files()
        fake_open.assert_called_with('/tmp/data2/data/file1.txt', 'wb')
        fake_open.return_value.seek.assert_called_with(199)
        fake_open.return_value.write.assert_called_with(b'\0')

    @patch('ddsc.core.download.TaskRunner')
    @patch('ddsc.core.download.TaskExecutor')
    def test_download_files(self, mock_task_executor, mock_task_runner):
        downloader = FileDownloader(self.mock_settings, self.mock_files_to_download)
        downloader.group_files_by_size = Mock()
        mock_small_file = Mock(size=100)
        mock_small_files = [mock_small_file]
        mock_large_file1 = Mock(size=200)
        mock_large_file2 = Mock(size=200)
        mock_large_files = [mock_large_file1, mock_large_file2]
        downloader.group_files_by_size.return_value = [mock_large_files, mock_small_files]
        downloader.make_ranges = Mock()
        downloader.make_ranges.return_value = [
            (0, 90),
            (91, 200)
        ]
        downloader.download_files()

        # Download small file in one command, large file in two commands).
        add_calls = mock_task_runner.return_value.add.call_args_list
        self.assertEqual(5, len(add_calls))
        command = add_calls[0][1]['command']
        self.assertEqual(command.bytes_to_read, 100)
        self.assertEqual(command.seek_amt, 0)
        command = add_calls[1][1]['command']
        self.assertEqual(command.bytes_to_read, 91)
        self.assertEqual(command.seek_amt, 0)
        command = add_calls[2][1]['command']
        self.assertEqual(command.bytes_to_read, 110)
        self.assertEqual(command.seek_amt, 91)
        command = add_calls[3][1]['command']
        self.assertEqual(command.bytes_to_read, 91)
        self.assertEqual(command.seek_amt, 0)
        command = add_calls[4][1]['command']
        self.assertEqual(command.bytes_to_read, 110)
        self.assertEqual(command.seek_amt, 91)

        self.assertEqual(mock_task_runner.return_value.run.call_count, 2)

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
        downloader = FileDownloader(self.mock_settings, self.mock_files_to_download)
        self.assertEqual(expected, downloader.make_ranges(Mock(size=file_size)))

    def test_determine_bytes_per_chunk(self):
        downloader = FileDownloader(self.mock_settings, self.mock_files_to_download)
        downloader.settings.config.download_workers = 2
        size = 10
        bytes_per_chunk = self.mock_config.download_bytes_per_chunk
        self.assertEqual(downloader.determine_bytes_per_chunk(size), bytes_per_chunk)
        size = bytes_per_chunk * 4
        self.assertEqual(downloader.determine_bytes_per_chunk(size), bytes_per_chunk * 2)
        size = bytes_per_chunk * 5
        self.assertEqual(downloader.determine_bytes_per_chunk(size), bytes_per_chunk * 2.5)

    def test_group_files_by_size(self):
        downloader = FileDownloader(self.mock_settings, self.mock_files_to_download)
        downloader.files_to_download = [
            Mock(size=90),
            Mock(size=100),
            Mock(size=99),
            Mock(size=200),
            Mock(size=400),
        ]
        large_items, small_items = downloader.group_files_by_size(100)
        self.assertEqual(set([90, 99]), set([item.size for item in small_items]))
        self.assertEqual(set([200, 100, 400]), set([item.size for item in large_items]))


class TestDownloadFilePartCommand(TestCase):
    @patch('ddsc.core.download.DownloadContext')
    def test_create_context(self, mock_download_context):
        mock_settings = Mock()
        mock_file_url = Mock(json_data={})
        command = DownloadFilePartCommand(mock_settings, mock_file_url, 100, 200, '/tmp/dest/data.txt')
        mock_message_queue = Mock()
        context = command.create_context(mock_message_queue, 123)
        self.assertEqual(context, mock_download_context.return_value)
        mock_download_context.assert_called_with(mock_settings, ({}, 100, 200, '/tmp/dest/data.txt'), mock_message_queue, 123)

    @patch('ddsc.core.download.DownloadContext')
    def test_on_message_processed(self, mock_download_context):
        mock_settings = Mock()
        mock_file_url = Mock(json_data={})
        command = DownloadFilePartCommand(mock_settings, mock_file_url, '/tmp/dest/data.txt', 100, 200)
        command.on_message(('processed', 2000))
        mock_settings.watcher.transferring_item.assert_called_with(mock_file_url, 2000)

    @patch('ddsc.core.download.DownloadContext')
    def test_on_message_error(self, mock_download_context):
        mock_settings = Mock()
        mock_file_url = Mock(json_data={})
        command = DownloadFilePartCommand(mock_settings, mock_file_url, '/tmp/dest/data.txt', 100, 200)
        with self.assertRaises(ValueError) as raised_error:
            command.on_message(('error', 'Oops'))
            self.assertEqual(str(raised_error.exception), 'Oops')


class TestDownloadFilePartRun(TestCase):
    @patch('ddsc.core.download.RetryChunkDownloader')
    @patch('ddsc.core.download.ProjectFile')
    def test_download_file_part_run(self, mock_project_file, mock_retry_chunk_downloader):
        mock_context = Mock(params=({}, 0, 100, '/tmp/dest/data.txt', ))

        download_file_part_run(mock_context)

        mock_retry_chunk_downloader.assert_called_with(mock_project_file.return_value,
                                                       '/tmp/dest/data.txt',
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
        mock_remote_file_url.assert_called_with(mock_project_file.file_url)
        mock_context.create_remote_store.return_value.get_file_url.assert_not_called()

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
        downloader.remote_store.get_file_url.assert_called_with(mock_project_file.id)

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

    @patch('ddsc.core.download.RemoteFileUrl')
    def test_retry_download_loop_handles_no_file_url(self, mock_remote_file_url):
        mock_project_file = Mock()
        mock_context = Mock()
        mock_project_file.file_url = None
        downloader = RetryChunkDownloader(project_file=mock_project_file, local_path=None, seek_amt=None,
                                          bytes_to_read=None, download_context=mock_context)

        downloader.get_url_and_headers_for_range = Mock()
        downloader.get_url_and_headers_for_range.return_value = 'someurl', ['headers']
        downloader.download_chunk = Mock()

        downloader.retry_download_loop()

        self.assertTrue(downloader.get_url_and_headers_for_range.called)
        downloader.download_chunk.assert_called_with('someurl', ['headers'])
        self.assertFalse(downloader.remote_store.get_project_file.called)
        mock_remote_file_url.assert_not_called()
        mock_context.create_remote_store.return_value.get_file_url.assert_called_with(mock_project_file.id)

    def test_get_url_and_headers_for_range_with_no_slashes(self):
        mock_context = Mock()
        mock_file_download = Mock(host='somehost', url='someurl')
        mock_file_download.http_headers = {}
        downloader = RetryChunkDownloader(project_file=None, local_path=None, seek_amt=None,
                                          bytes_to_read=None, download_context=mock_context)

        downloader.get_range_headers = Mock()
        downloader.get_range_headers.return_value = {'Range': 'bytes=100-200'}

        url, headers = downloader.get_url_and_headers_for_range(mock_file_download)

        self.assertEqual(url, 'somehost/someurl')

    def test_get_url_and_headers_for_range(self):
        mock_context = Mock()
        mock_file_download = Mock(host='somehost', url='/someurl')
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
    def test_download_chunk_inconsistent_swift(self, mock_requests):
        mock_context = Mock()
        downloader = RetryChunkDownloader(project_file=None, local_path=None, seek_amt=None,
                                          bytes_to_read=None, download_context=mock_context)

        mock_requests.get.return_value = Mock(status_code=401, text='Bad Swift Download')

        with self.assertRaises(DownloadInconsistentError) as raised_exception:
            downloader.download_chunk('someurl', {})
            self.assertEqual(str(raised_exception), 'Bad Swift Download')

    @patch('ddsc.core.download.requests')
    def test_download_chunk_inconsistent_s3(self, mock_requests):
        mock_context = Mock()
        downloader = RetryChunkDownloader(project_file=None, local_path=None, seek_amt=None,
                                          bytes_to_read=None, download_context=mock_context)

        mock_requests.get.return_value = Mock(status_code=403, text='Bad S3 Download')

        with self.assertRaises(DownloadInconsistentError) as raised_exception:
            downloader.download_chunk('someurl', {})
            self.assertEqual(str(raised_exception), 'Bad S3 Download')

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

    @patch('ddsc.core.download.RemoteFileUrl')
    @patch('ddsc.core.download.requests')
    def test_retry_download_loop_too_few_actual_bytes_read(self, mock_requests, mock_remote_file_url):
        mock_requests.exceptions.ConnectionError = ValueError
        mock_project_file = Mock()
        mock_context = Mock()
        downloader = RetryChunkDownloader(project_file=mock_project_file, local_path=None, seek_amt=0,
                                          bytes_to_read=100, download_context=mock_context)
        downloader.max_retry_times = 2
        downloader.get_url_and_headers_for_range = Mock()
        downloader.get_url_and_headers_for_range.return_value = ('someurl', ['headers'])
        mock_requests.get.return_value.iter_content.return_value = ['X' * 10]
        fake_open = mock_open()
        with patch('ddsc.core.download.open', fake_open, create=True):
            with self.assertRaises(PartialChunkDownloadError):
                downloader.retry_download_loop()
        self.assertEqual(downloader.actual_bytes_read, 10)

    @patch('ddsc.core.download.RemoteFileUrl')
    @patch('ddsc.core.download.requests')
    def test_retry_download_loop_too_few_actual_bytes_read_then_works(self, mock_requests, mock_remote_file_url):
        mock_requests.exceptions.ConnectionError = ValueError
        mock_project_file = Mock()
        mock_context = Mock()
        downloader = RetryChunkDownloader(project_file=mock_project_file, local_path=None, seek_amt=0,
                                          bytes_to_read=100, download_context=mock_context)
        downloader.max_retry_times = 2
        downloader.get_url_and_headers_for_range = Mock()
        downloader.get_url_and_headers_for_range.return_value = ('someurl', ['headers'])
        mock_requests.get.return_value.iter_content.side_effect = [
            ['X' * 10],  # only 10 bytes the first time
            ['X' * 100],  # all 100 bytes the second time
        ]
        fake_open = mock_open()
        with patch('ddsc.core.download.open', fake_open, create=True):
            downloader.retry_download_loop()
        self.assertEqual(downloader.actual_bytes_read, 100)


class TestFileHash(TestCase):
    def test_is_valid__no_supported_hash_found(self):
        file_hash = FileHash(algorithm='sha1', expected_hash_value='abc', file_path='/tmp/fakepath.dat')
        with self.assertRaises(ValueError) as raised_exception:
            file_hash.is_valid()
        self.assertEqual(str(raised_exception.exception), 'Unsupported algorithm sha1.')

    @patch('ddsc.core.download.HashUtil')
    def test_is_valid__hash_matches(self, mock_hash_util):
        mock_hash_util.return_value.hash.hexdigest.return_value = 'abc'
        file_hash = FileHash(algorithm='md5', expected_hash_value='abc', file_path='/tmp/fakepath.dat')
        self.assertEqual(file_hash.is_valid(), True)

    @patch('ddsc.core.download.HashUtil')
    def test_is_valid__hash_mismatch(self, mock_hash_util):
        mock_hash_util.return_value.hash.hexdigest.return_value = 'abc'
        file_hash = FileHash(algorithm='md5', expected_hash_value='def', file_path='/tmp/fakepath.dat')
        self.assertEqual(file_hash.is_valid(), False)

    def test_get_supported_file_hashes(self):
        dds_hashes = [
            {"algorithm": "sha1", "value": "abc"},
            {"algorithm": "md5", "value": "def"},
            {"algorithm": "md5", "value": "hij"},
        ]
        file_hashes = FileHash.get_supported_file_hashes(dds_hashes, '/tmp/data.txt')
        self.assertEqual(len(file_hashes), 2)
        self.assertEqual(file_hashes[0].expected_hash_value, 'def')
        self.assertEqual(file_hashes[1].expected_hash_value, 'hij')

    def test_separate_valid_and_failed_hashes(self):
        valid_hash = Mock()
        valid_hash.is_valid.return_value = True
        failed_hash = Mock()
        failed_hash.is_valid.return_value = False

        valid_file_hashes, failed_file_hashes = FileHash.separate_valid_and_failed_hashes([failed_hash, valid_hash])

        self.assertEqual(len(valid_file_hashes), 1)
        self.assertEqual(valid_file_hashes[0], valid_hash)
        self.assertEqual(len(failed_file_hashes), 1)
        self.assertEqual(failed_file_hashes[0], failed_hash)


class TestFileHashStatus(TestCase):
    def setUp(self):
        self.file_hash = FileHash(algorithm='md5', expected_hash_value='abc', file_path='/tmp/data.txt')
        self.file_hash2 = FileHash(algorithm='md5', expected_hash_value='def', file_path='/tmp/data.txt')

    def test_has_a_valid_hash(self):
        file_hash_status = FileHashStatus(self.file_hash, status=FileHashStatus.STATUS_OK)
        self.assertEqual(file_hash_status.has_a_valid_hash(), True)
        file_hash_status.status = FileHashStatus.STATUS_CONFLICTED
        self.assertEqual(file_hash_status.has_a_valid_hash(), True)
        file_hash_status.status = FileHashStatus.STATUS_FAILED
        self.assertEqual(file_hash_status.has_a_valid_hash(), False)

    def test_get_status_line(self):
        file_hash_status = FileHashStatus(self.file_hash, status=FileHashStatus.STATUS_OK)
        self.assertEqual(file_hash_status.get_status_line(), '/tmp/data.txt abc md5 OK')
        file_hash_status.status = FileHashStatus.STATUS_CONFLICTED
        self.assertEqual(file_hash_status.get_status_line(), '/tmp/data.txt abc md5 CONFLICTED')
        file_hash_status.status = FileHashStatus.STATUS_FAILED
        self.assertEqual(file_hash_status.get_status_line(), '/tmp/data.txt abc md5 FAILED')

    def test_raise_for_status(self):
        file_hash_status = FileHashStatus(self.file_hash, status=FileHashStatus.STATUS_OK)
        file_hash_status.raise_for_status()

        file_hash_status.status = FileHashStatus.STATUS_CONFLICTED
        file_hash_status.raise_for_status()

        file_hash_status.status = FileHashStatus.STATUS_FAILED
        with self.assertRaises(ValueError) as raised_exception:
            file_hash_status.raise_for_status()
        self.assertEqual(str(raised_exception.exception), 'Hash validation error: /tmp/data.txt abc md5 FAILED')

    @patch('ddsc.core.download.FileHash')
    def test_determine_for_hashes__no_hashes_found(self, mock_file_hash):
        mock_file_hash.separate_valid_and_failed_hashes.return_value = [], []
        dds_hashes = []
        with self.assertRaises(ValueError) as raised_exception:
            FileHashStatus.determine_for_hashes(dds_hashes, file_path='/tmp/fakepath.dat')
        self.assertEqual(str(raised_exception.exception),
                         'Unable to validate: No supported hashes found for file /tmp/fakepath.dat')
        mock_file_hash.get_supported_file_hashes.assert_called_with(dds_hashes, '/tmp/fakepath.dat')
        mock_file_hash.separate_valid_and_failed_hashes.assert_called_with(
            mock_file_hash.get_supported_file_hashes.return_value
        )

    @patch('ddsc.core.download.FileHash')
    def test_determine_for_hashes__only_valid_hashes(self, mock_file_hash):
        mock_file_hash.separate_valid_and_failed_hashes.return_value = [self.file_hash, self.file_hash2], []
        dds_hashes = [
            {"algorithm": "md5", "value": "abc"},
            {"algorithm": "md5", "value": "def"}
        ]
        file_hash_status = FileHashStatus.determine_for_hashes(dds_hashes, file_path='/tmp/fakepath.dat')
        self.assertEqual(file_hash_status.status, FileHashStatus.STATUS_OK)
        self.assertEqual(file_hash_status.file_hash, self.file_hash)

    @patch('ddsc.core.download.FileHash')
    def test_determine_for_hashes__only_failed_hashes(self, mock_file_hash):
        mock_file_hash.separate_valid_and_failed_hashes.return_value = [], [self.file_hash, self.file_hash2]
        dds_hashes = [
            {"algorithm": "md5", "value": "abc"},
            {"algorithm": "md5", "value": "def"}
        ]
        file_hash_status = FileHashStatus.determine_for_hashes(dds_hashes, file_path='/tmp/fakepath.dat')
        self.assertEqual(file_hash_status.status, FileHashStatus.STATUS_FAILED)
        self.assertEqual(file_hash_status.file_hash, self.file_hash)

    @patch('ddsc.core.download.FileHash')
    def test_determine_for_hashes__both_valid_and_failed_hashes(self, mock_file_hash):
        mock_file_hash.separate_valid_and_failed_hashes.return_value = [self.file_hash], [self.file_hash2]
        dds_hashes = [
            {"algorithm": "md5", "value": "abc"},
            {"algorithm": "md5", "value": "def"}
        ]
        file_hash_status = FileHashStatus.determine_for_hashes(dds_hashes, file_path='/tmp/fakepath.dat')
        self.assertEqual(file_hash_status.status, FileHashStatus.STATUS_CONFLICTED)
        self.assertEqual(file_hash_status.file_hash, self.file_hash)
