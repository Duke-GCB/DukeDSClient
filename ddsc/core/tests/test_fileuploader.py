from unittest import TestCase
from ddsc.core.fileuploader import ParallelChunkProcessor, upload_async, FileUploadOperations, \
    RetrySettings, ForbiddenSendExternalException, ChunkSender
from ddsc.core.ddsapi import DSResourceNotConsistentError, DataServiceError
import requests
from mock import MagicMock, Mock, patch, call


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
        fop = FileUploadOperations(data_service, MagicMock())
        fop._show_retry_warning = Mock()
        url_json = {
            'http_verb': 'PUT',
            'host': 'something.com',
            'url': '/putdata',
            'http_headers': [],
        }
        fop.send_file_external(url_json, chunk='DATADATADATA')
        self.assertEqual(1, data_service.send_external.call_count)
        self.assertEqual(0, fop._show_retry_warning.call_count)

    def test_send_file_external_retry_put(self):
        data_service = MagicMock()
        data_service.send_external.side_effect = [requests.exceptions.ConnectionError, Mock(status_code=201)]
        fop = FileUploadOperations(data_service, MagicMock())
        url_json = {
            'http_verb': 'PUT',
            'host': 'something.com',
            'url': '/putdata',
            'http_headers': [],
        }
        fop.send_file_external(url_json, chunk='DATADATADATA')
        self.assertEqual(2, data_service.send_external.call_count)

    def test_send_file_external_403_exception(self):
        data_service = MagicMock()
        data_service.send_external.side_effect = [Mock(status_code=403)]
        fop = FileUploadOperations(data_service, MagicMock())
        url_json = {
            'http_verb': 'PUT',
            'host': 'something.com',
            'url': '/putdata',
            'http_headers': [],
        }
        with self.assertRaises(ForbiddenSendExternalException) as raised_exception:
            fop.send_file_external(url_json, chunk='DATADATADATA')
        self.assertEqual(str(raised_exception.exception),
                         'Failed to send file to external store. Error:403 something.com/putdata')

    def test_send_file_external_retry_put_fail_after_4_retries(self):
        data_service = MagicMock()
        connection_err = requests.exceptions.ConnectionError
        data_service.send_external.side_effect = [connection_err, connection_err, connection_err, connection_err,
                                                  connection_err, connection_err]
        fop = FileUploadOperations(data_service, MagicMock())
        url_json = {
            'http_verb': 'PUT',
            'host': 'something.com',
            'url': '/putdata',
            'http_headers': [],
        }
        fop._show_retry_warning = Mock()
        with self.assertRaises(requests.exceptions.ConnectionError):
            fop.send_file_external(url_json, chunk='DATADATADATA')
        self.assertEqual(4, data_service.send_external.call_count)
        self.assertEqual(4, data_service.recreate_requests_session.call_count)
        self.assertEqual(1, fop._show_retry_warning.call_count)

    def test_send_file_external_succeeds_3rd_time(self):
        data_service = MagicMock()
        connection_err = requests.exceptions.ConnectionError
        data_service.send_external.side_effect = [connection_err, connection_err, Mock(status_code=201)]
        fop = FileUploadOperations(data_service, MagicMock())
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
        fop = FileUploadOperations(data_service, MagicMock())
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
        fop = FileUploadOperations(data_service, MagicMock())
        fop.finish_upload(upload_id="123",
                          hash_data=MagicMock(),
                          parent_data=MagicMock(),
                          remote_file_id="456")
        data_service.complete_upload.assert_called()

    @patch('ddsc.core.ddsapi.time.sleep')
    def test_create_upload_with_one_pause(self, mock_sleep):
        data_service = MagicMock()
        response = Mock()
        response.json.return_value = {'id': '123'}
        data_service.create_upload.side_effect = [
            DSResourceNotConsistentError(MagicMock(), MagicMock(), MagicMock()),
            response
        ]
        fop = FileUploadOperations(data_service, MagicMock())
        path_data = MagicMock()
        path_data.name.return_value = '/tmp/data.dat'
        upload_id = fop.create_upload(project_id='12', path_data=path_data, hash_data=MagicMock())
        self.assertEqual(upload_id, '123')
        mock_sleep.assert_called_with(RetrySettings.RESOURCE_NOT_CONSISTENT_RETRY_SECONDS)

    @patch('ddsc.core.ddsapi.time.sleep')
    def test_create_upload_with_two_pauses(self, mock_sleep):
        data_service = MagicMock()
        response = Mock()
        response.json.return_value = {'id': '124'}
        data_service.create_upload.side_effect = [
            DSResourceNotConsistentError(MagicMock(), MagicMock(), MagicMock()),
            DSResourceNotConsistentError(MagicMock(), MagicMock(), MagicMock()),
            response
        ]
        fop = FileUploadOperations(data_service, MagicMock())
        path_data = MagicMock()
        path_data.name.return_value = '/tmp/data.dat'
        upload_id = fop.create_upload(project_id='12', path_data=path_data, hash_data=MagicMock())
        self.assertEqual(upload_id, '124')
        mock_sleep.assert_has_calls([
            call(RetrySettings.RESOURCE_NOT_CONSISTENT_RETRY_SECONDS),
            call(RetrySettings.RESOURCE_NOT_CONSISTENT_RETRY_SECONDS)])

    @patch('ddsc.core.ddsapi.time.sleep')
    def test_create_upload_with_one_pause_then_failure(self, mock_sleep):
        data_service = MagicMock()
        response = Mock()
        response.json.return_value = {'id': '123'}
        data_service.create_upload.side_effect = [
            DSResourceNotConsistentError(MagicMock(), MagicMock(), MagicMock()),
            DataServiceError(MagicMock(), MagicMock(), MagicMock())
        ]
        path_data = MagicMock()
        path_data.name.return_value = '/tmp/data.dat'
        fop = FileUploadOperations(data_service, MagicMock())
        with self.assertRaises(DataServiceError):
            fop.create_upload(project_id='12', path_data=path_data, hash_data=MagicMock())

    def test_create_upload_default_remote_filename(self):
        data_service = MagicMock()
        response = Mock()
        response.json.return_value = {'id': '123'}
        data_service.create_upload.side_effect = [
            response
        ]
        fop = FileUploadOperations(data_service, MagicMock())
        path_data = MagicMock()
        path_data.name.return_value = 'data.dat'
        upload_id = fop.create_upload(project_id='12', path_data=path_data, hash_data=MagicMock())
        self.assertEqual(upload_id, '123')
        args, kwargs = data_service.create_upload.call_args
        self.assertEqual(args[0], '12')
        self.assertEqual(args[1], 'data.dat')

    def test_create_upload_remote_filename(self):
        data_service = MagicMock()
        response = Mock()
        response.json.return_value = {'id': '123'}
        data_service.create_upload.side_effect = [
            response
        ]
        fop = FileUploadOperations(data_service, MagicMock())
        path_data = MagicMock()
        path_data.name.return_value = 'data.dat'
        upload_id = fop.create_upload(project_id='12', path_data=path_data, hash_data=MagicMock(),
                                      remote_filename='other.dat')
        self.assertEqual(upload_id, '123')
        args, kwargs = data_service.create_upload.call_args
        self.assertEqual(args[0], '12')
        self.assertEqual(args[1], 'other.dat')
        self.assertEqual(kwargs['chunked'], True)

    @patch('ddsc.core.fileuploader.HashData')
    def test_create_file_chunk_url_uses_one_based_indexing(self, hash_data):
        data_service = MagicMock()
        mock_response = Mock()
        mock_response.json.return_value = 'result'
        data_service.create_upload_url.return_value = mock_response
        response = Mock()
        response.json.return_value = {'id': '123'}
        data_service.create_upload.side_effect = [
            response
        ]
        hash_data.create_from_chunk.return_value = Mock(value='h@sh', alg='md5')

        fop = FileUploadOperations(data_service, MagicMock())
        resp = fop.create_file_chunk_url(upload_id='someId', chunk_num=0, chunk='data')

        self.assertEqual(resp, 'result')
        data_service.create_upload_url.assert_called_with('someId',
                                                          1,       # one based_index
                                                          4,       # chunk_len
                                                          'h@sh',  # hash_value
                                                          'md5')   # hash_alg

    def test_create_upload_and_chunk_url(self):
        data_service = MagicMock()
        response = Mock()
        response.json.return_value = {'id': '123', 'signed_url': {
            "http_verb": "PUT",
            "host": "duke_data_service_prod.s3.amazonaws.com",
        }}
        data_service.create_upload.side_effect = [
            response
        ]
        fop = FileUploadOperations(data_service, MagicMock())
        path_data = MagicMock()
        path_data.name.return_value = 'data.dat'
        upload_id, upload_chunk_url = fop.create_upload_and_chunk_url(
            project_id='12', path_data=path_data, hash_data=MagicMock(), remote_filename='other.dat')
        self.assertEqual(upload_id, '123')
        args, kwargs = data_service.create_upload.call_args
        self.assertEqual(args[0], '12')
        self.assertEqual(args[1], 'other.dat')
        self.assertEqual(kwargs['chunked'], False)
        self.assertEqual(upload_chunk_url, {
            "http_verb": "PUT",
            "host": "duke_data_service_prod.s3.amazonaws.com",
        })


class TestChunkSender(TestCase):
    @patch('ddsc.core.fileuploader.FileUploadOperations')
    def test__send_chunk(self, mock_file_upload_operations):
        chunk_sender = ChunkSender(
            data_service=Mock(), upload_id='abc123', filename='data.txt',
            chunk_size=100, index=0, num_chunks_to_send=1, progress_queue=Mock()
        )
        chunk_sender._send_chunk(chunk='abc', chunk_num=1)

        mock_operations = mock_file_upload_operations.return_value
        mock_operations.create_file_chunk_url.assert_called_with('abc123', 1, 'abc')
        mock_operations.send_file_external.assert_called_with(
            mock_operations.create_file_chunk_url.return_value, 'abc'
        )
        self.assertEqual(mock_operations.create_file_chunk_url.call_count, 1)
        self.assertEqual(mock_operations.send_file_external.call_count, 1)

    @patch('ddsc.core.fileuploader.FileUploadOperations')
    def test__send_chunk_with_one_retry(self, mock_file_upload_operations):
        mock_operations = mock_file_upload_operations.return_value
        chunk_sender = ChunkSender(
            data_service=Mock(), upload_id='abc123', filename='data.txt',
            chunk_size=100, index=0, num_chunks_to_send=1, progress_queue=Mock()
        )
        mock_operations.send_file_external.side_effect = [
            ForbiddenSendExternalException("Forbidden"),  # raise exception
            None  # then return a value
        ]
        chunk_sender._send_chunk(chunk='abc', chunk_num=1)

        self.assertEqual(mock_operations.create_file_chunk_url.call_count, 2)
        self.assertEqual(mock_operations.send_file_external.call_count, 2)

    @patch('ddsc.core.fileuploader.FileUploadOperations')
    def test__send_chunk_with_only_forbidden(self, mock_file_upload_operations):
        mock_operations = mock_file_upload_operations.return_value
        chunk_sender = ChunkSender(
            data_service=Mock(), upload_id='abc123', filename='data.txt',
            chunk_size=100, index=0, num_chunks_to_send=1, progress_queue=Mock()
        )
        mock_operations.send_file_external.side_effect = ForbiddenSendExternalException('Forbidden')
        with self.assertRaises(ForbiddenSendExternalException) as raised_exception:
            chunk_sender._send_chunk(chunk='abc', chunk_num=1)

        self.assertEqual(str(raised_exception.exception), 'Forbidden')
