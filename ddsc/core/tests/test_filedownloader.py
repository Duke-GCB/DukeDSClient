from unittest import TestCase
import ddsc.core.filedownloader
from ddsc.core.filedownloader import FileDownloader, download_async, ChunkDownloader, \
    TooLargeChunkDownloadError, PartialChunkDownloadError, get_file_chunk_url_and_headers, GetFileUrl
from requests.exceptions import ConnectionError
from mock import patch, MagicMock, Mock, call


class FakeConfig(object):
    def __init__(self, download_workers):
        self.download_workers = download_workers


class FakeFile(object):
    def __init__(self, size):
        self.size = size
        self.id = '123'


class FakeWatcher(object):
    def __init__(self):
        self.amt = 0

    def transferring_item(self, remote_file, increment_amt):
        self.amt += increment_amt


class TestDownloader(FileDownloader):
    def __init__(self, config, remote_file, path, watcher):
        super(TestDownloader, self).__init__(config, remote_file, path, watcher)

    def make_big_empty_file(self):
        pass


sample_url_parts = {
    'host': 'myhost',
    'url': 'stuff/',
    'http_headers': {},
}


class TestFileDownloader(TestCase):
    def test_make_chunk_processor_with_none(self):
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
        config = FakeConfig(workers)
        downloader = FileDownloader(config, FakeFile(file_size), None, None)
        self.assertEqual(expected, downloader.make_ranges())

    def test_chunk_that_fails(self):
        file_size = 83833112
        config = FakeConfig(3)
        ddsc.core.filedownloader.download_async = self.chunk_download_fails
        downloader = TestDownloader(config, FakeFile(file_size), None, None)
        try:
            downloader.run()
        except ValueError as err:
            self.assertEqual("oops", str(err))

    def chunk_download_fails(self, config, remote_file_id, range_headers, path, seek_amt, bytes_to_read,
                             progress_queue):
        progress_queue.error("oops")

    def test_download_whole_chunk(self):
        file_size = 83833112
        config = FakeConfig(3)
        watcher = FakeWatcher()
        ddsc.core.filedownloader.download_async = self.chunk_download_one_piece
        downloader = TestDownloader(config, FakeFile(file_size), None, watcher)
        downloader.run()
        self.assertEqual(file_size, watcher.amt)

    def chunk_download_one_piece(self, config, remote_file_id, range_headers, path, seek_amt, bytes_to_read,
                                 progress_queue):
        start, end = range_headers['Range'].replace("bytes=", "").split('-')
        total = (int(end) - int(start) + 1)
        progress_queue.processed(total)

    def test_download_chunk_in_two_parts(self):
        file_size = 83833112
        config = FakeConfig(3)
        watcher = FakeWatcher()
        ddsc.core.filedownloader.download_async = self.chunk_download_two_parts
        downloader = TestDownloader(config, FakeFile(file_size), None, watcher)
        downloader.run()
        self.assertEqual(file_size, watcher.amt)

    def chunk_download_two_parts(self, config, remote_file_id, range_headers, path, seek_amt, bytes_to_read,
                                 progress_queue):
        start, end = range_headers['Range'].replace("bytes=", "").split('-')
        total = (int(end) - int(start) + 1)
        first = int(total / 2)
        rest = total - first
        progress_queue.processed(first)
        progress_queue.processed(rest)


class TestDownloadFunctions(TestCase):
    @patch('ddsc.core.filedownloader.ChunkDownloader')
    @patch('ddsc.core.filedownloader.RemoteStore')
    def test_download_async_too_large_error(self, mock_remote_store, mock_chunk_downloader):
        # If we get too much data we should quit immediately sending a message to the progress_queue error
        mock_chunk_downloader.return_value.run.side_effect = TooLargeChunkDownloadError(100, 10, '/tmp/data.dat')
        progress_queue = MagicMock()
        download_async(config=MagicMock(), remote_file_id=123, range_headers={}, path=None, seek_amt=0,
                       bytes_to_read=10, progress_queue=progress_queue)
        self.assertEqual(1, mock_chunk_downloader.call_count, "on too big we should only try downloading once")
        expected = 'Received too many bytes downloading part of a file. Actual: 100 Expected: 10 File:/tmp/data.dat'
        progress_queue.error.assert_called_with(expected)

    @patch('ddsc.core.filedownloader.ChunkDownloader')
    @patch('ddsc.core.filedownloader.time.sleep')
    @patch('ddsc.core.filedownloader.RemoteStore')
    def test_download_async_partial_twice(self, mock_remote_store, mock_sleep, mock_chunk_downloader):
        mock_chunk_downloader.return_value.run.side_effect = [
            PartialChunkDownloadError(2, 10, '/tmp/data.dat'),
            PartialChunkDownloadError(2, 10, '/tmp/data.dat'),
            None
        ]
        progress_queue = MagicMock()
        download_async(config=MagicMock(), remote_file_id=123, range_headers={}, path=None, seek_amt=0,
                       bytes_to_read=10, progress_queue=progress_queue)
        self.assertEqual(3, mock_chunk_downloader.call_count, 'we should retry downloading multiple times')
        self.assertEqual(0, progress_queue.error.call_count, 'there should have been no errors')
        self.assertEqual(2, mock_sleep.call_count, 'we should have called sleep')
        self.assertEqual(2, mock_chunk_downloader().revert_progress.call_count)

    @patch('ddsc.core.filedownloader.ChunkDownloader')
    @patch('ddsc.core.filedownloader.time.sleep')
    @patch('ddsc.core.filedownloader.RemoteStore')
    def test_download_async_connection_error_twice(self, mock_remote_store, mock_sleep, mock_chunk_downloader):
        mock_chunk_downloader.return_value.run.side_effect = [
            ConnectionError(),
            ConnectionError(),
            None
        ]
        progress_queue = MagicMock()
        download_async(config=MagicMock(), remote_file_id=123, range_headers={}, path=None, seek_amt=0,
                       bytes_to_read=10, progress_queue=progress_queue)
        self.assertEqual(3, mock_chunk_downloader.call_count, 'we should retry downloading multiple times')
        self.assertEqual(0, progress_queue.error.call_count, 'there should have been no errors')
        self.assertEqual(2, mock_sleep.call_count, 'we should have called sleep')
        self.assertEqual(2, mock_chunk_downloader().revert_progress.call_count)

    @patch('ddsc.core.filedownloader.ChunkDownloader')
    @patch('ddsc.core.filedownloader.time.sleep')
    @patch('ddsc.core.filedownloader.RemoteStore')
    def test_download_async_partial_too_many_times(self, mock_remote_store, mock_sleep, mock_chunk_downloader):
        mock_chunk_downloader.return_value.run.side_effect = [
            PartialChunkDownloadError(2, 10, '/tmp/data.dat'),
            PartialChunkDownloadError(2, 10, '/tmp/data.dat'),
            PartialChunkDownloadError(2, 10, '/tmp/data.dat'),
            PartialChunkDownloadError(2, 10, '/tmp/data.dat'),
            PartialChunkDownloadError(2, 10, '/tmp/data.dat'),
            PartialChunkDownloadError(2, 10, '/tmp/data.dat'),
        ]
        progress_queue = MagicMock()
        download_async(config=MagicMock(), remote_file_id=123, range_headers={}, path=None, seek_amt=0,
                       bytes_to_read=10, progress_queue=progress_queue)
        self.assertEqual(6, mock_chunk_downloader.call_count, 'we should retry downloading multiple times')
        self.assertEqual(5, mock_sleep.call_count, 'we should have called sleep four times')
        self.assertEqual(1, progress_queue.error.call_count)
        expected = 'Received too few bytes downloading part of a file. Actual: 2 Expected: 10 File:/tmp/data.dat'
        progress_queue.error.assert_called_with(expected)
        self.assertEqual(5, mock_chunk_downloader().revert_progress.call_count)

    @patch('ddsc.core.filedownloader.retry_until_resource_is_consistent')
    def test_get_file_chunk_url_and_headers(self, mock_retry_func):
        range_headers = {
            "Range": "bytes=1-456"
        }
        mock_retry_func.return_value = {
            "http_headers": {
                'key': '123'
            },
            "host": "somewhere.com",
            "url": "/get_file=?123"
        }
        url, headers = get_file_chunk_url_and_headers(remote_store=MagicMock(),
                                                      remote_file_id='123',
                                                      range_headers=range_headers,
                                                      progress_queue=MagicMock())
        self.assertEqual('somewhere.com/get_file=?123', url)
        self.assertEqual({'Range': 'bytes=1-456', 'key': '123'}, headers)


class ChunkDownloaderTest(TestCase):
    @patch('ddsc.core.filedownloader.requests')
    @patch("ddsc.core.filedownloader.open")
    def test_reading_correct_amount_of_data(self, mock_open, mock_requests):
        progress_queue = MagicMock()
        get_response = MagicMock()
        get_response.iter_content.return_value = [
            '12345',
            '67890'
        ]
        mock_requests.get.return_value = get_response
        chunk_downloader = ChunkDownloader(url=None,
                                           http_headers=None,
                                           path=None,
                                           seek_amt=10,
                                           bytes_to_read=10,
                                           progress_queue=progress_queue)
        chunk_downloader.run()
        mock_open.return_value.__enter__.return_value.write.assert_has_calls([
            call('12345'),
            call('67890'),
        ])

    @patch('ddsc.core.filedownloader.requests')
    @patch("ddsc.core.filedownloader.open")
    def test_reading_too_much_data(self, mock_open, mock_requests):
        progress_queue = MagicMock()
        get_response = MagicMock()
        get_response.iter_content.return_value = [
            '12345678901',
        ]
        mock_requests.get.return_value = get_response
        chunk_downloader = ChunkDownloader(url=None,
                                           http_headers=None,
                                           path=None,
                                           seek_amt=10,
                                           bytes_to_read=10,
                                           progress_queue=progress_queue)
        with self.assertRaises(TooLargeChunkDownloadError):
            chunk_downloader.run()

    @patch('ddsc.core.filedownloader.requests')
    @patch("ddsc.core.filedownloader.open")
    def test_reading_too_few_data(self, mock_open, mock_requests):
        progress_queue = MagicMock()
        get_response = MagicMock()
        get_response.iter_content.return_value = [
            '123456789',
        ]
        mock_requests.get.return_value = get_response
        chunk_downloader = ChunkDownloader(url=None,
                                           http_headers=None,
                                           path=None,
                                           seek_amt=10,
                                           bytes_to_read=10,
                                           progress_queue=progress_queue)
        with self.assertRaises(PartialChunkDownloadError):
            chunk_downloader.run()

    def test_revert_progress(self):
        progress_queue = MagicMock()
        chunk_downloader = ChunkDownloader(url=None,
                                           http_headers=None,
                                           path=None,
                                           seek_amt=10,
                                           bytes_to_read=10,
                                           progress_queue=progress_queue)
        chunk_downloader.actual_bytes_read = 101
        chunk_downloader.revert_progress()
        progress_queue.processed.assert_called_with(-101)


class TestGetFileUrl(TestCase):
    def test_stuff(self):
        mock_data_service = MagicMock()
        mock_response = Mock()
        mock_response.json.return_value = {
            'url': '/files/1'
        }
        mock_data_service.get_file_url.return_value = mock_response
        get_file_url = GetFileUrl(mock_data_service, remote_file_id='123')
        url_info = get_file_url.run()
        mock_data_service.get_file_url.assert_called_with('123')
        self.assertEqual({'url': '/files/1'}, url_info)
