from unittest import TestCase
import ddsc.core.filedownloader
from ddsc.core.filedownloader import FileDownloader


class FakeConfig(object):
    def __init__(self, download_workers):
        self.download_workers = download_workers


class FakeFile(object):
    def __init__(self, size):
        self.size = size


class FakeWatcher(object):
    def __init__(self):
        self.amt = 0

    def transferring_item(self, remote_file, increment_amt):
        self.amt += increment_amt


class TestDownloader(FileDownloader):
    def __init__(self, config, remote_file, url_parts, path, watcher):
        super(TestDownloader, self).__init__(config, remote_file, url_parts, path, watcher)

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
        downloader = FileDownloader(config, FakeFile(file_size), None, None, None)
        self.assertEqual(expected, downloader.make_ranges())

    def test_chunk_that_fails(self):
        file_size = 83833112
        config = FakeConfig(3)
        ddsc.core.filedownloader.download_async = self.chunk_download_fails
        downloader = TestDownloader(config, FakeFile(file_size), sample_url_parts, None, None)
        try:
            downloader.run()
        except ValueError as err:
            self.assertEqual("oops", str(err))

    def chunk_download_fails(self, url, headers, path, seek_amt, bytes_to_read, progress_queue):
        progress_queue.error("oops")

    def test_download_whole_chunk(self):
        file_size = 83833112
        config = FakeConfig(3)
        watcher = FakeWatcher()
        ddsc.core.filedownloader.download_async = self.chunk_download_one_piece
        downloader = TestDownloader(config, FakeFile(file_size), sample_url_parts, None, watcher)
        downloader.run()
        self.assertEqual(file_size, watcher.amt)

    def chunk_download_one_piece(self, url, headers, path, seek_amt, bytes_to_read, progress_queue):
        start, end = headers['Range'].replace("bytes=", "").split('-')
        total = (int(end) - int(start) + 1)
        progress_queue.processed(total)

    def test_download_chunk_in_two_parts(self):
        file_size = 83833112
        config = FakeConfig(3)
        watcher = FakeWatcher()
        ddsc.core.filedownloader.download_async = self.chunk_download_two_parts
        downloader = TestDownloader(config, FakeFile(file_size), sample_url_parts, None, watcher)
        downloader.run()
        self.assertEqual(file_size, watcher.amt)

    def chunk_download_two_parts(self, url, headers, path, seek_amt, bytes_to_read, progress_queue):
        start, end = headers['Range'].replace("bytes=", "").split('-')
        total = (int(end) - int(start) + 1)
        first = int(total / 2)
        rest = total - first
        progress_queue.processed(first)
        progress_queue.processed(rest)
