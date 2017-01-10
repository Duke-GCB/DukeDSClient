from unittest import TestCase
from ddsc.core.fileuploader import ParallelChunkProcessor


class FakeConfig(object):
    def __init__(self, upload_workers, upload_bytes_per_chunk):
        self.upload_workers = upload_workers
        self.upload_bytes_per_chunk = upload_bytes_per_chunk


class FakeLocalFile(object):
    def __init__(self, path, mimetype):
        self.path = path
        self.mimetype = mimetype


class TestParallelChunkProcessor(TestCase):
    def test_determine_num_chunks(self):
        values = [
            # chunk_size, file_size, expected
            (100, 300, 3),
            (100, 101, 2),
            (100, 199, 2),
            (5, 7, 2),
            (100, 900000, 9000),
            (125, 123, 1),
            (122, 123, 2),
            (100, 0, 1)
        ]
        for chunk_size, file_size, expected in values:
            num_chunks = ParallelChunkProcessor.determine_num_chunks(chunk_size, file_size)
            self.assertEqual(expected, num_chunks)

    def test_make_work_parcels(self):
        values = [
            # upload_workers, num_chunks, expected
            (4, 4, [(0, 1), (1, 1), (2, 1), (3, 1)]),
            (4, 19, [(0, 5), (5, 5), (10, 5), (15, 4)]),
            (5, 31, [(0, 7), (7, 7), (14, 7), (21, 7), (28, 3)]),
            (5, 4, [(0, 1), (1, 1), (2, 1), (3, 1)]),
            (1, 4, [(0, 4)]),
        ]
        for upload_workers, num_chunks, expected in values:
            result = ParallelChunkProcessor.make_work_parcels(upload_workers, num_chunks)
            self.assertEqual(expected, result)
