from unittest import TestCase
from ddsc.core.fileuploader import ParallelChunkProcessor, upload_async, FileUploadOperations
import requests
from mock import MagicMock, Mock, patch


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


class TestUploadAsync(TestCase):
    @patch('ddsc.core.fileuploader.ChunkSender')
    def test_upload_async_sends_exception_to_progress_queue(self, mock_chunk_sender):
        data_service_auth_data = MagicMock()
        config = MagicMock()
        upload_id = 123
        filename = 'somefile.txt'
        index = 0
        num_chunks_to_send = 10
        progress_queue = MagicMock()
        mock_chunk_sender().send.side_effect = ValueError("Something Failed!")
        upload_async(data_service_auth_data, config, upload_id, filename, index, num_chunks_to_send, progress_queue)
        progress_queue.error.assert_called()
        params = progress_queue.error.call_args
        positional_args = params[0]
        self.assertIn('Something Failed!', positional_args[0])


class TestFileUploadOperations(TestCase):
    def test_send_file_external_works_first_time(self):
        data_service = MagicMock()
        data_service.send_external.side_effect = [Mock(status_code=201)]
        fop = FileUploadOperations(data_service)
        url_json = {
            'http_verb': 'PUT',
            'host': 'something.com',
            'url': '/putdata',
            'http_headers': [],
        }
        fop.send_file_external(url_json, chunk='DATADATADATA')
        self.assertEqual(1, data_service.send_external.call_count)

    def test_send_file_external_retry_put(self):
        data_service = MagicMock()
        data_service.send_external.side_effect = [requests.exceptions.ConnectionError, Mock(status_code=201)]
        fop = FileUploadOperations(data_service)
        url_json = {
            'http_verb': 'PUT',
            'host': 'something.com',
            'url': '/putdata',
            'http_headers': [],
        }
        fop.send_file_external(url_json, chunk='DATADATADATA')
        self.assertEqual(2, data_service.send_external.call_count)

    @patch('ddsc.core.fileuploader.time')
    def test_send_file_external_retry_put_fail_after_5_times(self, mock_time):
        data_service = MagicMock()
        connection_err = requests.exceptions.ConnectionError
        data_service.send_external.side_effect = [connection_err, connection_err, connection_err, connection_err,
                                                  connection_err, connection_err]
        fop = FileUploadOperations(data_service)
        url_json = {
            'http_verb': 'PUT',
            'host': 'something.com',
            'url': '/putdata',
            'http_headers': [],
        }
        with self.assertRaises(requests.exceptions.ConnectionError):
            fop.send_file_external(url_json, chunk='DATADATADATA')
        self.assertEqual(5, data_service.send_external.call_count)
        self.assertEqual(4, data_service.recreate_requests_session.call_count)

    @patch('ddsc.core.fileuploader.time')
    def test_send_file_external_succeeds_3rd_time(self, mock_time):
        data_service = MagicMock()
        connection_err = requests.exceptions.ConnectionError
        data_service.send_external.side_effect = [connection_err, connection_err, Mock(status_code=201)]
        fop = FileUploadOperations(data_service)
        url_json = {
            'http_verb': 'PUT',
            'host': 'something.com',
            'url': '/putdata',
            'http_headers': [],
        }
        fop.send_file_external(url_json, chunk='DATADATADATA')
        self.assertEqual(3, data_service.send_external.call_count)
        self.assertEqual(2, data_service.recreate_requests_session.call_count)

    def test_send_file_external_no_retry_post(self):
        data_service = MagicMock()
        data_service.send_external.side_effect = [requests.exceptions.ConnectionError]
        fop = FileUploadOperations(data_service)
        url_json = {
            'http_verb': 'POST',
            'host': 'something.com',
            'url': '/putdata',
            'http_headers': [],
        }
        with self.assertRaises(requests.exceptions.ConnectionError):
            fop.send_file_external(url_json, chunk='DATADATADATA')
        self.assertEqual(1, data_service.send_external.call_count)

    def test_finish_upload(self):
        data_service = MagicMock()
        fop = FileUploadOperations(data_service)
        fop.finish_upload(upload_id="123",
                          hash_data=MagicMock(),
                          parent_data=MagicMock(),
                          remote_file_id="456")
        data_service.complete_upload.assert_called()
