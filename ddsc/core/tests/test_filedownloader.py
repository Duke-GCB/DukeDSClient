from unittest import TestCase
from ddsc.core.filedownloader import FileDownloader


class FakeConfig(object):
    def __init__(self, download_workers):
        self.download_workers = download_workers


class TestFileDownloader(TestCase):
    def test_make_chunk_processor_with_none(self):
        # Only one worker because file size is too small
        self.assert_make_ranges(
            workers=2,
            file_size=100,
            expected=[
                '0-99',
            ]
        )

        # Big enough file should split into two
        self.assert_make_ranges(
            workers=2,
            file_size=100 * 1000 * 1000,
            expected=[
                '0-49999999',
                '50000000-99999999'
            ]
        )


        # Big enough file should split into three
        self.assert_make_ranges(
            workers=3,
            file_size=100 * 1000 * 1000,
            expected=[
                '0-33333333',
                '33333334-66666667',
                '66666668-99999999',
            ]
        )

        # Uneven split
        self.assert_make_ranges(
            workers=3,
            file_size=100 * 1000 * 1000 - 1,
            expected=[
                '0-33333332',
                '33333333-66666665',
                '66666666-99999998',
            ]
        )

    def assert_make_ranges(self, workers, file_size, expected):
        config = FakeConfig(workers)
        downloader = FileDownloader(config, None, None, file_size, None)
        self.assertEqual(expected, downloader.make_ranges())