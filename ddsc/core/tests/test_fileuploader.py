from unittest import TestCase

from ddsc.core.fileuploader import FileUploader, ParallelChunkProcessor, ChunkGrouper

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
            (5, 31, [(0, 7), (7, 7), (14, 7), (21, 7), (28,3)]),
            (5, 4, [(0, 1), (1, 1), (2, 1), (3, 1)]),
            (1, 4, [(0, 4)]),
        ]
        for upload_workers, num_chunks, expected in values:
            result = ParallelChunkProcessor.make_work_parcels(upload_workers, num_chunks, set())
            self.assertEqual(expected, result)


class TestChunkGrouper(TestCase):
    def test_make_index_groups(self):
        values = [
            (1, 1, set(),       [[0]]),                 # single chunk file
            (4, 1, set(),       [[0], [1], [2], [3]]),  # 4 chunk file with batch size 1
            (4, 2, set(),       [[0, 1], [2, 3]]),      # 4 chunk file with batch size 2
            (4, 3, set(),       [[0, 1, 2], [3]]),      # 4 chunk file with batch size 3
            (5, 3, set(),       [[0, 1, 2], [3, 4]]),   # 5 chunk file with batch size 3
            (4, 1, set([1]),    [[0], [2], [3]]),       # 4 chunk file with batch size 1 skipping chunk index 1
            (4, 2, set([1]),    [[0], [2, 3]]),         # 4 chunk file with batch size 2 skipping chunk index 1
            (4, 2, set([0]),    [[1, 2], [3]]),         # 4 chunk file with batch size 2 skipping chunk index 0
            (4, 2, set([0, 2]), [[1], [3]]),            # 4 chunk file with batch size 2 skipping chunk index 0,2
            (4, 2, set([1, 2]), [[0], [3]]),            # 4 chunk file with batch size 2 skipping chunk index 1,2
        ]
        for num_chunks, batch_size, exclude_set, expected in values:
            grouper = ChunkGrouper(num_chunks=num_chunks, batch_size=batch_size, exclude_set=exclude_set)
            result = grouper._make_index_groups()
            self.assertEqual(expected, result)

    def test_range_tuple_from_index_group(self):
        with self.assertRaises(ValueError):
            ChunkGrouper.range_tuple_from_index_group([])
        self.assertEqual((0, 1), ChunkGrouper.range_tuple_from_index_group([0]))
        self.assertEqual((0, 2), ChunkGrouper.range_tuple_from_index_group([0, 1]))
        self.assertEqual((0, 3), ChunkGrouper.range_tuple_from_index_group([0, 1, 2]))
        self.assertEqual((5, 3), ChunkGrouper.range_tuple_from_index_group([5, 6, 7]))

    def test_make_range_tuples(self):
        values = [
            (1, 1, set(),       [(0, 1)]),                    # single chunk file
            (4, 1, set(), [(0, 1), (1, 1), (2, 1), (3, 1)]),  # 4 chunks with batch size 1
            (4, 2, set(), [(0, 2), (2, 2)]),                  # 4 chunks with batch size 2
            (4, 3, set(), [(0, 3), (3, 1)]),                  # 4 chunks with batch size 3
            (5, 3, set(), [(0, 3), (3, 2)]),                  # 5 chunks with batch size 3
            (4, 1, set([1]), [(0, 1), (2, 1), (3, 1)]),       # 4 chunks with batch size 1 skipping chunk index 1
            (4, 2, set([1]), [(0, 1), (2, 2)]),               # 4 chunks with batch size 2 skipping chunk index 1
            (4, 2, set([0]), [(1, 2), (3, 1)]),               # 4 chunks with batch size 2 skipping chunk index 0
            (4, 2, set([0, 2]), [(1, 1), (3, 1)]),            # 4 chunks with batch size 2 skipping chunk index 0,2
            (4, 2, set([1, 2]), [(0, 1), (3, 1)]),            # 4 chunks with batch size 2 skipping chunk index 1,2
        ]
        for num_chunks, batch_size, exclude_set, expected in values:
            grouper = ChunkGrouper(num_chunks=num_chunks, batch_size=batch_size, exclude_set=exclude_set)
            result = grouper.make_range_tuples()
            self.assertEqual(expected, result)