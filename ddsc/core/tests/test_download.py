from unittest import TestCase
from ddsc.core.download import FileHash, FileHashStatus, FileDownloadState, ProjectFileDownloader
from mock.mock import patch, Mock


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
    def test_stuff(self):
        # FileDownloadState
        self.assertEqual(1, 2)


class TestProjectFileDownloader(TestCase):
    def test_stuff(self):
        # ProjectFileDownloader
        self.assertEqual(1, 2)
