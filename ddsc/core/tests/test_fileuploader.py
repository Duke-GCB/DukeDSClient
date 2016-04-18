from unittest import TestCase

from ddsc.core.fileuploader import FileUploader, SerialChunkProcessor, ParallelChunkProcessor

class FakeConfig(object):
    def __init__(self, upload_workers, upload_bytes_per_chunk):
        self.upload_workers = upload_workers
        self.upload_bytes_per_chunk = upload_bytes_per_chunk

class FakeLocalFile(object):
    def __init__(self, path, mimetype):
        self.path = path
        self.mimetype = mimetype

class TestFileUploader(TestCase):
    def setUp(self):
        self.config = FakeConfig(None, 10)
        self.local_file = FakeLocalFile('/tmp/ok.txt', 'txt')

    def test_make_chunk_processor_with_none(self):
        self.config.upload_workers = None
        file_uploader = FileUploader(self.config, None, self.local_file, None)
        processor = file_uploader._make_chunk_processor()
        self.assertEqual(type(processor), SerialChunkProcessor)

    def test_make_chunk_processor_with_one(self):
        self.config.upload_workers = 1
        file_uploader = FileUploader(self.config, None, self.local_file, None)
        processor = file_uploader._make_chunk_processor()
        self.assertEqual(type(processor), SerialChunkProcessor)

    def test_make_chunk_processor_with_two(self):
        self.config.upload_workers = 2
        file_uploader = FileUploader(self.config, None, self.local_file, None)
        processor = file_uploader._make_chunk_processor()
        self.assertEqual(type(processor), ParallelChunkProcessor)