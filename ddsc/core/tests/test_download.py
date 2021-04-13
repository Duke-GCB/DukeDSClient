from unittest import TestCase
from ddsc.core.download import FileHash, FileHashStatus, FileDownloadState, ProjectFileDownloader, DDS_TOTAL_HEADER, \
    download_file, MISMATCHED_FILE_HASH_WARNING, URLExpiredException, download_url_to_path, S3_EXPIRED_STATUS_CODE, \
    compute_download_result
from ddsc.core.pathfilter import PathFilter
import queue
import requests
from mock.mock import patch, Mock, call, mock_open, ANY


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
        file_hash_status.status = FileHashStatus.STATUS_WARNING
        self.assertEqual(file_hash_status.has_a_valid_hash(), True)
        file_hash_status.status = FileHashStatus.STATUS_FAILED
        self.assertEqual(file_hash_status.has_a_valid_hash(), False)

    def test_get_status_line(self):
        file_hash_status = FileHashStatus(self.file_hash, status=FileHashStatus.STATUS_OK)
        self.assertEqual(file_hash_status.get_status_line(), '/tmp/data.txt abc md5 OK')
        file_hash_status.status = FileHashStatus.STATUS_WARNING
        self.assertEqual(file_hash_status.get_status_line(), '/tmp/data.txt abc md5 WARNING')
        file_hash_status.status = FileHashStatus.STATUS_FAILED
        self.assertEqual(file_hash_status.get_status_line(), '/tmp/data.txt abc md5 FAILED')

    def test_raise_for_status(self):
        file_hash_status = FileHashStatus(self.file_hash, status=FileHashStatus.STATUS_OK)
        file_hash_status.raise_for_status()

        file_hash_status.status = FileHashStatus.STATUS_WARNING
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
        self.assertEqual(file_hash_status.status, FileHashStatus.STATUS_WARNING)
        self.assertEqual(file_hash_status.file_hash, self.file_hash)


class TestFileDownloadState(TestCase):
    def setUp(self):
        self.project_file = Mock(size=4000, hashes=[{}], file_url={
            'host': 'somehost',
            'url': '/data/file.txt',
        })
        self.project_file.id = '123'
        self.output_path = '/tmp/dest.txt'
        self.config = Mock(download_bytes_per_chunk=20 * 1024 * 1024, file_download_retries=2)
        self.file_download_state = FileDownloadState(self.project_file, self.output_path, self.config)

    def test_constructor(self):
        self.assertEqual(self.file_download_state.file_id, '123')
        self.assertEqual(self.file_download_state.size, 4000)
        self.assertEqual(self.file_download_state.hashes, [{}])
        self.assertEqual(self.file_download_state.output_path, '/tmp/dest.txt')
        self.assertEqual(self.file_download_state.url, 'somehost/data/file.txt')
        self.assertEqual(self.file_download_state.retries, 2)
        self.assertEqual(self.file_download_state.download_bytes_per_chunk, 20 * 1024 * 1024)

    @patch('ddsc.core.download.FileHashStatus')
    def test_calculate_file_hash_status(self, mock_file_hash_status):
        file_hash_status = self.file_download_state.calculate_file_hash_status()

        self.assertEqual(file_hash_status, mock_file_hash_status.determine_for_hashes.return_value)
        mock_file_hash_status.determine_for_hashes(self.project_file.hashes, '/tmp/dest.txt')

    def test_is_ok_state(self):
        self.assertEqual(self.file_download_state.is_ok_state(), False)

        self.file_download_state.mark_good(None)
        self.assertEqual(self.file_download_state.is_ok_state(), True)

        self.file_download_state.mark_already_complete(None)
        self.assertEqual(self.file_download_state.is_ok_state(), True)

        self.file_download_state.mark_error(None)
        self.assertEqual(self.file_download_state.is_ok_state(), False)

        self.file_download_state.mark_expired_url(None)
        self.assertEqual(self.file_download_state.is_ok_state(), False)

        self.file_download_state.mark_error(None)
        self.assertEqual(self.file_download_state.is_ok_state(), False)

    def test_raise_for_status(self):
        ret = self.file_download_state.mark_error('generic error')
        self.assertEqual(ret, self.file_download_state)
        with self.assertRaises(ValueError) as raised_exception:
            self.file_download_state.raise_for_status()
        self.assertEqual(str(raised_exception.exception), 'generic error')

        good_status = Mock()
        ret = self.file_download_state.mark_good(status=good_status)
        self.assertEqual(ret, self.file_download_state)
        self.file_download_state.raise_for_status()


class TestProjectFileDownloader(TestCase):
    def setUp(self):
        self.config = Mock(download_workers=4)
        self.dest_directory = '/tmp/outdir'
        self.project = Mock()

    @patch('ddsc.core.download.multiprocessing')
    @patch('ddsc.core.download.os')
    @patch('ddsc.core.download.print')
    @patch('ddsc.core.download.FileDownloadState')
    @patch('ddsc.core.download.time')
    def test_run(self, mock_time, mock_file_download_state, mock_print, mock_os, mock_multiprocessing):
        mock_project_file = Mock(file_url={'host': 'somehost', 'url': '/api/file1.txt'})
        self.project.get_project_files_generator.return_value = [
            (mock_project_file, {DDS_TOTAL_HEADER: 1})
        ]
        mock_pool = mock_multiprocessing.Pool.return_value
        mock_apply_async = mock_pool.apply_async
        mock_apply_async.return_value.get.return_value.status.get_status_line.return_value = 'Hash Status Line'

        project_file_downloader = ProjectFileDownloader(self.config, self.dest_directory, self.project,
                                                        path_filter=None, num_workers=1)
        project_file_downloader.show_progress_bar = Mock()
        project_file_downloader.run()

        mock_queue = mock_multiprocessing.Manager.return_value.Queue.return_value
        mock_pool.apply_async.assert_called_with(
            download_file, (mock_file_download_state.return_value, mock_queue)
        )
        output_path = mock_project_file.get_local_path.return_value
        mock_file_download_state.assert_called_with(mock_project_file, output_path, self.config)
        mock_print.assert_has_calls([
            call('\nVerifying contents of 1 downloaded files using file hashes.'),
            call('Hash Status Line'),
            call('All downloaded files have been verified successfully.')
        ])

    @patch('ddsc.core.download.multiprocessing')
    def test_download_files(self, mock_multiprocessing):
        project_file_downloader = ProjectFileDownloader(self.config, self.dest_directory, self.project,
                                                        path_filter=None, num_workers=1)
        project_file_downloader.show_progress_bar = Mock()
        project_file_downloader._get_project_files = Mock()
        project_file_downloader._get_project_files.return_value = [
            'project_file1_obj', 'project_file2_obj'
        ]
        project_file_downloader._download_file = Mock()
        project_file_downloader._work_queue_is_full = Mock()
        project_file_downloader._work_queue_is_full.side_effect = [
            True,
            False,
            False
        ]
        project_file_downloader._wait_for_and_retry_failed_downloads = Mock()
        project_file_downloader._work_queue_is_not_empty = Mock()
        project_file_downloader._work_queue_is_not_empty.side_effect = [
            True,
            False
        ]

        manager = Mock()
        manager.attach_mock(project_file_downloader._download_file, 'download_file')
        manager.attach_mock(project_file_downloader._work_queue_is_full, 'work_queue_is_full')
        manager.attach_mock(project_file_downloader._wait_for_and_retry_failed_downloads,
                            'wait_for_and_retry_failed_downloads')
        manager.attach_mock(project_file_downloader._work_queue_is_not_empty, 'work_queue_is_not_empty')

        project_file_downloader._download_files()

        # Expected behavior is to download file 1, wait for that to finish, download file 2, then wait for one more time
        manager.assert_has_calls([
            call.download_file(ANY, 'project_file1_obj'),
            call.work_queue_is_full(),
            call.wait_for_and_retry_failed_downloads(ANY),
            call.work_queue_is_full(),
            call.download_file(ANY, 'project_file2_obj'),
            call.work_queue_is_full(),
            call.work_queue_is_not_empty(),
            call.wait_for_and_retry_failed_downloads(ANY),
            call.work_queue_is_not_empty()
        ])

    @patch('ddsc.core.download.multiprocessing')
    @patch('ddsc.core.download.print')
    def test_show_downloaded_files_status_all_good(self, mock_print, mock_multiprocessing):
        project_file_downloader = ProjectFileDownloader(self.config, self.dest_directory, self.project,
                                                        path_filter=None, num_workers=1)
        file_hash1 = Mock(file_path='/data/file1.txt', expected_hash_value='abcd', algorithm='md5')
        project_file_downloader.download_status_list = [
            FileHashStatus(file_hash1, FileHashStatus.STATUS_OK)
        ]
        project_file_downloader._show_downloaded_files_status()

        mock_print.assert_has_calls([
            call('\nVerifying contents of None downloaded files using file hashes.'),
            call('/data/file1.txt abcd md5 OK'),
            call('All downloaded files have been verified successfully.')
        ])

    @patch('ddsc.core.download.multiprocessing')
    @patch('ddsc.core.download.print')
    def test_show_downloaded_files_status_all_good_warning(self, mock_print, mock_multiprocessing):
        project_file_downloader = ProjectFileDownloader(self.config, self.dest_directory, self.project,
                                                        path_filter=None, num_workers=1)
        file_hash1 = Mock(file_path='/data/file1.txt', expected_hash_value='abcd', algorithm='md5')
        project_file_downloader.download_status_list = [
            FileHashStatus(file_hash1, FileHashStatus.STATUS_WARNING)
        ]
        project_file_downloader._show_downloaded_files_status()

        mock_print.assert_has_calls([
            call('\nVerifying contents of None downloaded files using file hashes.'),
            call('/data/file1.txt abcd md5 WARNING'),
            call('All downloaded files have been verified successfully.'),
            call(MISMATCHED_FILE_HASH_WARNING.format(1)),
        ])

    @patch('ddsc.core.download.multiprocessing')
    @patch('ddsc.core.download.print')
    def test_show_downloaded_files_status_error(self, mock_print, mock_multiprocessing):
        project_file_downloader = ProjectFileDownloader(self.config, self.dest_directory, self.project,
                                                        path_filter=None, num_workers=1)
        file_hash1 = Mock(file_path='/data/file1.txt', expected_hash_value='abcd', algorithm='md5')
        project_file_downloader.download_status_list = [
            FileHashStatus(file_hash1, FileHashStatus.STATUS_FAILED)
        ]
        with self.assertRaises(ValueError) as raised_exception:
            project_file_downloader._show_downloaded_files_status()
        self.assertEqual(str(raised_exception.exception), 'ERROR: Downloaded file(s) do not match the expected hashes.')

        mock_print.assert_has_calls([
            call('\nVerifying contents of None downloaded files using file hashes.'),
            call('/data/file1.txt abcd md5 FAILED'),
        ])

    @patch('ddsc.core.download.multiprocessing')
    def test_get_project_files_no_filter(self, mock_multiprocessing):
        project_file_downloader = ProjectFileDownloader(self.config, self.dest_directory, self.project,
                                                        path_filter=None, num_workers=1)
        project_file_downloader.show_progress_bar = Mock()
        self.project.get_project_files_generator = Mock()
        self.project.get_project_files_generator.return_value = [
            ('file1', {DDS_TOTAL_HEADER: 2}),
            ('file2', {DDS_TOTAL_HEADER: 2}),
        ]

        self.assertEqual(project_file_downloader.files_to_download, None)
        result = list(project_file_downloader._get_project_files())
        self.assertEqual(result, ['file1', 'file2'])
        self.assertEqual(project_file_downloader.files_to_download, 2)
        project_file_downloader.show_progress_bar.assert_called_with()

    @patch('ddsc.core.download.multiprocessing')
    @patch('ddsc.core.download.print')
    def test_get_project_files_with_filter(self, mock_print, mock_multiprocessing):
        project_file_downloader = ProjectFileDownloader(self.config, self.dest_directory, self.project,
                                                        path_filter=PathFilter(include_paths=None,
                                                                               exclude_paths=['/data/file1']),
                                                        num_workers=1)
        project_file_downloader.show_progress_bar = Mock()
        self.project.get_project_files_generator = Mock()
        mock_file1 = Mock(path='/data/file1')
        mock_file2 = Mock(path='/data/file2')
        self.project.get_project_files_generator.return_value = [
            (mock_file1, {DDS_TOTAL_HEADER: 2}),
            (mock_file2, {DDS_TOTAL_HEADER: 2}),
        ]
        self.assertEqual(project_file_downloader.files_to_download, None)
        result = list(project_file_downloader._get_project_files())
        self.assertEqual(result, [mock_file2])
        self.assertEqual(project_file_downloader.files_to_download, 1)
        project_file_downloader.show_progress_bar.assert_called_with()
        mock_print.assert_not_called()

    @patch('ddsc.core.download.multiprocessing')
    @patch('ddsc.core.download.print')
    def test_get_project_files_with_filter_warnings(self, mock_print, mock_multiprocessing):
        project_file_downloader = ProjectFileDownloader(self.config, self.dest_directory, self.project,
                                                        path_filter=PathFilter(include_paths=None,
                                                                               exclude_paths=['/data/fileX']),
                                                        num_workers=1)
        project_file_downloader.show_progress_bar = Mock()
        self.project.get_project_files_generator = Mock()
        mock_file1 = Mock(path='/data/file1')
        mock_file2 = Mock(path='/data/file2')
        self.project.get_project_files_generator.return_value = [
            (mock_file1, {DDS_TOTAL_HEADER: 2}),
            (mock_file2, {DDS_TOTAL_HEADER: 2}),
        ]
        self.assertEqual(project_file_downloader.files_to_download, None)
        result = list(project_file_downloader._get_project_files())
        self.assertEqual(result, [mock_file1, mock_file2])
        self.assertEqual(project_file_downloader.files_to_download, 2)
        project_file_downloader.show_progress_bar.assert_called_with()
        mock_print.assert_called_with('WARNING: Path(s) not found: /data/fileX.')

    @patch('ddsc.core.download.multiprocessing')
    @patch('ddsc.core.download.os')
    @patch('ddsc.core.download.FileDownloadState')
    def test_download_file(self, mock_file_download_state, mock_os, mock_multiprocessing):
        mock_os.path.exists.return_value = False
        mock_pool = Mock()
        mock_project_file = Mock(file_url={'host': 'somehost', 'url': 'someurl'})
        mock_project_file.get_local_path.return_value = '/tmp/data.out'
        project_file_downloader = ProjectFileDownloader(self.config, self.dest_directory, self.project,
                                                        path_filter=None, num_workers=1)
        project_file_downloader._download_file(mock_pool, mock_project_file)
        mock_os.path.dirname.assert_called_with("/tmp/data.out")
        mock_os.path.exists.assert_called_with(mock_os.path.dirname.return_value)
        mock_os.makedirs.assert_called_with(mock_os.path.dirname.return_value)
        mock_file_download_state.assert_called_with(mock_project_file, '/tmp/data.out', self.config)
        mock_pool.apply_async.assert_called_with(download_file, (mock_file_download_state.return_value,
                                                                 project_file_downloader.message_queue))

    @patch('ddsc.core.download.multiprocessing')
    def test_work_queue_is_full(self, mock_multiprocessing):
        downloader = ProjectFileDownloader(self.config, self.dest_directory, self.project, path_filter=None,
                                           num_workers=1)
        downloader.num_workers = 3
        self.assertEqual(False, downloader._work_queue_is_full())
        downloader.async_download_results = [1, 2]
        self.assertEqual(False, downloader._work_queue_is_full())
        downloader.async_download_results = [1, 2, 3]
        self.assertEqual(True, downloader._work_queue_is_full())
        downloader.async_download_results = [1, 2, 3, 4]
        self.assertEqual(True, downloader._work_queue_is_full())

    @patch('ddsc.core.download.multiprocessing')
    def test_work_queue_is_not_empty(self, mock_multiprocessing):
        downloader = ProjectFileDownloader(self.config, self.dest_directory, self.project, path_filter=None,
                                           num_workers=1)
        self.assertEqual(False, downloader._work_queue_is_not_empty())
        downloader.async_download_results = [1, 2, 3]
        self.assertEqual(True, downloader._work_queue_is_not_empty())

    @patch('ddsc.core.download.multiprocessing')
    @patch('ddsc.core.download.time')
    def test_wait_for_and_retry_failed_downloads_no_results_ready(self, mock_time, mock_multiprocessing):
        mock_pool = Mock()
        downloader = ProjectFileDownloader(self.config, self.dest_directory, self.project, path_filter=None,
                                           num_workers=1)
        downloader._pop_ready_download_results = Mock()
        downloader._pop_ready_download_results.return_value = []
        downloader._try_process_message_queue = Mock()
        downloader._wait_for_and_retry_failed_downloads(mock_pool)
        mock_time.sleep.assert_called_with(0)

    @patch('ddsc.core.download.multiprocessing')
    @patch('ddsc.core.download.time')
    def test_wait_for_and_retry_failed_downloads_results_ready(self, mock_time, mock_multiprocessing):
        mock_pool = Mock()
        downloader = ProjectFileDownloader(self.config, self.dest_directory, self.project, path_filter=None,
                                           num_workers=1)
        downloader._pop_ready_download_results = Mock()
        downloader._pop_ready_download_results.return_value = ['item1']
        downloader._process_download_results = Mock()
        downloader._try_process_message_queue = Mock()
        downloader._wait_for_and_retry_failed_downloads(mock_pool)
        downloader._process_download_results.assert_called_with(mock_pool, ['item1'])
        downloader._try_process_message_queue.assert_not_called()
        mock_time.sleep.assert_not_called()

    @patch('ddsc.core.download.multiprocessing')
    def test_try_process_message_queue_empty(self, mock_multiprocessing):
        downloader = ProjectFileDownloader(self.config, self.dest_directory, self.project, path_filter=None,
                                           num_workers=1)
        downloader.message_queue.get_nowait.side_effect = queue.Empty()
        downloader.show_progress_bar = Mock()
        downloader._try_process_message_queue()
        downloader.show_progress_bar.assert_not_called()

    @patch('ddsc.core.download.multiprocessing')
    def test_try_process_message_queue_has_data(self, mock_multiprocessing):
        downloader = ProjectFileDownloader(self.config, self.dest_directory, self.project, path_filter=None,
                                           num_workers=1)
        downloader.message_queue.get_nowait.return_value = ('123', 100, 200, FileDownloadState.DOWNLOADING)
        downloader.show_progress_bar = Mock()
        downloader._try_process_message_queue()
        self.assertEqual(downloader.file_download_statuses, {'123': (100, 200, FileDownloadState.DOWNLOADING)})
        downloader.show_progress_bar.assert_called_with()

    @patch('ddsc.core.download.multiprocessing')
    @patch('ddsc.core.download.sys')
    @patch('ddsc.core.download.time')
    def test_show_progress_bar(self, mock_time, mock_sys, mock_multiprocessing):
        mock_time.time.return_value = 100
        downloader = ProjectFileDownloader(self.config, self.dest_directory, self.project, path_filter=None,
                                           num_workers=1)
        downloader.files_to_download = 20
        downloader.start_time = 0
        downloader.get_download_progress = Mock()
        downloader.get_download_progress.return_value = (10, 1000)
        downloader.show_progress_bar()
        mock_sys.stdout.write.assert_called_with('\r| downloaded 1 KB @ 10 B/s          (10 of 20 files complete)')
        mock_sys.stdout.flush.assert_called_with()

    @patch('ddsc.core.download.multiprocessing')
    def test_make_spinner_char(self, mock_multiprocessing):
        downloader = ProjectFileDownloader(self.config, self.dest_directory, self.project, path_filter=None,
                                           num_workers=1)
        downloader.start_time = 0
        self.assertEqual(downloader.make_spinner_char(current_time=0), '|')
        self.assertEqual(downloader.make_spinner_char(current_time=1), '/')
        self.assertEqual(downloader.make_spinner_char(current_time=2), '-')
        self.assertEqual(downloader.make_spinner_char(current_time=3), '\\')
        self.assertEqual(downloader.make_spinner_char(current_time=4), '|')
        self.assertEqual(downloader.make_spinner_char(current_time=5), '/')

    @patch('ddsc.core.download.multiprocessing')
    def test_make_download_speed(self, mock_multiprocessing):
        downloader = ProjectFileDownloader(self.config, self.dest_directory, self.project, path_filter=None,
                                           num_workers=1)
        downloader.start_time = 0
        test_values = [
            # current_time, total_bytes_downloaded, expected
            (0, 0, ''),  # must have elapsed time and bytes downloaded to display speed
            (1, 0, ''),
            (0, 1, ''),
            (59, 1000 * 60, '@ 1 KB/s'),
            (59, 60, '@ 1 B/s'),
            (59 + 60, 2 * 1000 * 1000 * 60, '@ 1 MB/s'),
        ]
        for current_time, total_bytes_downloaded, expected in test_values:
            result = downloader.make_download_speed(current_time, total_bytes_downloaded)
            self.assertEqual(result, expected)

    @patch('ddsc.core.download.multiprocessing')
    def test_get_download_progress(self, mock_multiprocessing):
        downloader = ProjectFileDownloader(self.config, self.dest_directory, self.project, path_filter=None,
                                           num_workers=1)
        downloader.files_to_download = 10
        downloader.file_download_statuses = {
            '123': (10, 100, FileDownloadState.DOWNLOADING),
            '124': (200, 200, FileDownloadState.ALREADY_COMPLETE),
            '125': (200, 200, FileDownloadState.GOOD),
            '126': (200, 200, FileDownloadState.GOOD),
        }
        files_downloaded, total_bytes_downloaded = downloader.get_download_progress()
        self.assertEqual(files_downloaded, 3)
        self.assertEqual(total_bytes_downloaded, 410)

    @patch('ddsc.core.download.multiprocessing')
    def test_pop_ready_download_results(self, mock_multiprocessing):
        downloader = ProjectFileDownloader(self.config, self.dest_directory, self.project, path_filter=None,
                                           num_workers=1)
        result1 = Mock()
        result1.ready.return_value = True
        result1.get.return_value = "123"
        result2 = Mock()
        result2.ready.return_value = False
        result2.get.return_value = "456"
        result3 = Mock()
        result3.ready.return_value = True
        result3.get.return_value = "789"
        downloader.async_download_results = [result1, result2, result3]

        items = downloader._pop_ready_download_results()
        self.assertEqual(items, ['123', '789'])
        self.assertEqual(downloader.async_download_results, [result2])

    @patch('ddsc.core.download.multiprocessing')
    def test_process_download_results_ok_state(self, mock_multiprocessing):
        pool = Mock()
        result1 = Mock(output_path='/tmp/data.txt', msg='Download failed')
        result1.is_ok_state.return_value = True
        result1.file_id = '123'
        result1.size = 4000
        result1.status = 'mystatus'
        result1.state = FileDownloadState.GOOD
        download_results = [
            result1
        ]
        downloader = ProjectFileDownloader(self.config, self.dest_directory, self.project, path_filter=None,
                                           num_workers=1)
        downloader.show_progress_bar = Mock()
        downloader._process_download_results(pool, download_results)
        self.assertEqual(downloader.file_download_statuses, {'123': (4000, 4000, 'good')})
        self.assertEqual(downloader.download_status_list, ['mystatus'])
        downloader.show_progress_bar.assert_called_with()

    @patch('ddsc.core.download.multiprocessing')
    def test_process_download_results_has_retry(self, mock_multiprocessing):
        pool = Mock()
        result1 = Mock(output_path='/tmp/data.txt', msg='Download failed')
        result1.is_ok_state.return_value = False
        result1.retries = 1
        download_results = [
            result1
        ]
        downloader = ProjectFileDownloader(self.config, self.dest_directory, self.project, path_filter=None,
                                           num_workers=1)
        downloader.show_progress_bar = Mock()
        downloader.dds_connection.get_file_download.return_value = Mock(host='somehost', url='/api/v1/datafile.txt')
        downloader._process_download_results(pool, download_results)
        self.assertEqual(result1.url, 'somehost/api/v1/datafile.txt')
        pool.apply_async.assert_called_with(download_file, (result1, downloader.message_queue))
        downloader.show_progress_bar.assert_called_with()

    @patch('ddsc.core.download.multiprocessing')
    def test_process_download_results_out_of_retries(self, mock_multiprocessing):
        pool = Mock()
        result1 = Mock(output_path='/tmp/data.txt', msg='Download failed')
        result1.is_ok_state.return_value = False
        result1.retries = 0
        download_results = [
            result1
        ]
        downloader = ProjectFileDownloader(self.config, self.dest_directory, self.project, path_filter=None,
                                           num_workers=1)
        downloader.show_progress_bar = Mock()
        with self.assertRaises(ValueError) as raised_exception:
            downloader._process_download_results(pool, download_results)
        self.assertEqual(str(raised_exception.exception), 'Error downloading /tmp/data.txt\nDownload failed')
        downloader.show_progress_bar.assert_not_called()


class TestDownloadFunctions(TestCase):
    @patch('ddsc.core.download.os')
    def test_download_file_already_good(self, mock_os):
        mock_os.path.exists.return_value = True
        file_download_state = Mock()
        file_download_state.calculate_file_hash_status.return_value.has_a_valid_hash.return_value = True
        result = download_file(file_download_state)
        self.assertEqual(result, file_download_state.mark_already_complete.return_value)
        file_download_state.mark_already_complete.assert_called_with(
            file_download_state.calculate_file_hash_status.return_value
        )

    @patch('ddsc.core.download.os')
    @patch('ddsc.core.download.download_url_to_path')
    @patch('ddsc.core.download.compute_download_result')
    def test_download_file_saves_file(self, mock_compute_download_result, mock_download_url_to_path, mock_os):
        mock_os.path.exists.return_value = False
        file_download_state = Mock()
        result = download_file(file_download_state)
        self.assertEqual(result, mock_compute_download_result.return_value)
        mock_compute_download_result.assert_called_with(file_download_state, mock_download_url_to_path.return_value)
        mock_download_url_to_path.assert_called_with(file_download_state, None)

    @patch('ddsc.core.download.os')
    @patch('ddsc.core.download.download_url_to_path')
    @patch('ddsc.core.download.compute_download_result')
    def test_download_file_expired(self, mock_compute_download_result, mock_download_url_to_path, mock_os):
        mock_os.path.exists.return_value = False
        file_download_state = Mock(url="somehost/api/v1/data1.txt")
        mock_download_url_to_path.side_effect = URLExpiredException()
        result = download_file(file_download_state)
        self.assertEqual(result, file_download_state.mark_expired_url.return_value)
        file_download_state.mark_expired_url.assert_called_with("Expired URL: somehost/api/v1/data1.txt")

    @patch('ddsc.core.download.os')
    @patch('ddsc.core.download.download_url_to_path')
    @patch('ddsc.core.download.compute_download_result')
    def test_download_file_error(self, mock_compute_download_result, mock_download_url_to_path, mock_os):
        mock_os.path.exists.return_value = False
        file_download_state = Mock(url="somehost/api/v1/data1.txt")
        mock_download_url_to_path.side_effect = ValueError("SomeError")
        result = download_file(file_download_state)
        self.assertEqual(result, file_download_state.mark_error.return_value)
        file_download_state.mark_error.assert_called_with(msg='SomeError')

    @patch('ddsc.core.download.requests')
    def test_download_url_to_path_works(self, mock_requests):
        file_download_state = Mock(
            url='someurl',
            output_path='/tmp/outfile.dat',
            download_bytes_per_chunk=10,
            file_id='123abc',
            size=100,
            state=FileDownloadState.DOWNLOADING
        )
        message_queue = Mock()
        fake_open = mock_open()
        mock_response = mock_requests.get.return_value
        mock_response.iter_content.return_value = ['1234567890'] * 10  # 100 bytes of data
        with patch('ddsc.core.download.open', fake_open, create=True):
            written_size = download_url_to_path(file_download_state, message_queue)
        fake_open.assert_called_with('/tmp/outfile.dat', 'wb')
        mock_response.iter_content.assert_called_with(chunk_size=10)
        mock_requests.get.assert_called_with('someurl', stream=True)
        message_queue.put.assert_has_calls([
            call(('123abc', 10, 100, 'downloading')),
            call(('123abc', 20, 100, 'downloading')),
            call(('123abc', 30, 100, 'downloading')),
            call(('123abc', 40, 100, 'downloading')),
            call(('123abc', 50, 100, 'downloading')),
            call(('123abc', 60, 100, 'downloading')),
            call(('123abc', 70, 100, 'downloading')),
            call(('123abc', 80, 100, 'downloading')),
            call(('123abc', 90, 100, 'downloading')),
            call(('123abc', 100, 100, 'downloading'))
        ])
        self.assertEqual(written_size, 100)

    @patch('ddsc.core.download.requests')
    def test_download_url_to_path_expired(self, mock_requests):
        file_download_state = Mock(
            url='someurl',
            output_path='/tmp/outfile.dat',
            download_bytes_per_chunk=10,
            file_id='123abc',
            size=100
        )
        message_queue = Mock()
        mock_requests.get.return_value = Mock(status_code=S3_EXPIRED_STATUS_CODE)
        mock_requests.get.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError()
        with self.assertRaises(URLExpiredException):
            download_url_to_path(file_download_state, message_queue)

    @patch('ddsc.core.download.requests')
    def test_download_url_to_path_other_error(self, mock_requests):
        file_download_state = Mock(
            url='someurl',
            output_path='/tmp/outfile.dat',
            download_bytes_per_chunk=10,
            file_id='123abc',
            size=100
        )
        message_queue = Mock()
        mock_requests.get.return_value.raise_for_status.side_effect = ValueError()
        with self.assertRaises(ValueError):
            download_url_to_path(file_download_state, message_queue)

    @patch('ddsc.core.download.requests')
    def test_compute_download_result_good(self, mock_requests):
        file_download_state = Mock(size=100)
        file_download_state.calculate_file_hash_status.return_value.has_a_valid_hash.return_value = True
        result = compute_download_result(file_download_state, written_size=100)
        self.assertEqual(result, file_download_state.mark_good.return_value)

    @patch('ddsc.core.download.requests')
    def test_compute_download_result_bad_hash_error(self, mock_requests):
        file_download_state = Mock(size=100)
        file_hash = file_download_state.calculate_file_hash_status.return_value
        file_hash.has_a_valid_hash.return_value = False
        result = compute_download_result(file_download_state, written_size=100)
        self.assertEqual(result, file_download_state.mark_error.return_value)
        file_download_state.mark_error.assert_called_with(msg=file_hash.get_status_line.return_value)

    @patch('ddsc.core.download.requests')
    def test_compute_download_result_wrong_size_error(self, mock_requests):
        file_download_state = Mock(size=100)
        result = compute_download_result(file_download_state, written_size=80)
        self.assertEqual(result, file_download_state.mark_error.return_value)
        file_download_state.mark_error.assert_called_with(
            msg="Downloaded file was wrong size. Expected: 100 Actual: 80")
