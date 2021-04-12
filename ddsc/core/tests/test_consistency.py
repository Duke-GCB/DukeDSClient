from unittest import TestCase
from ddsc.core.consistency import UploadDetails, ProjectChecker, DSResourceNotConsistentError
from mock import Mock


class TestUploadDetails(TestCase):
    def test_inconsistent_status(self):
        mock_dds_file = Mock()
        mock_dds_file.name = 'file1.dat'
        mock_dds_file.id = '123'
        mock_status = Mock(is_consistent=False, initiated_on='2021-01-01', error_on=None)
        mock_dds_file.get_upload.return_value.status = mock_status
        upload_details = UploadDetails(mock_dds_file)
        self.assertEqual(upload_details.inconsistent(), True)
        self.assertEqual(upload_details.had_error(), False)
        self.assertEqual(upload_details.is_bad(), True)
        self.assertEqual(upload_details.name(), 'file1.dat')
        self.assertEqual(upload_details.status_str(), 'Inconsistent')
        self.assertEqual(upload_details.file_id(), '123')
        self.assertEqual(upload_details.message(), 'started upload at 2021-01-01')

    def test_error_status(self):
        mock_dds_file = Mock()
        mock_dds_file.name = 'file1.dat'
        mock_dds_file.id = '123'
        mock_status = Mock(is_consistent=True, initiated_on='2021-01-01', error_on='2021-01-02', error_message='bad data')
        mock_dds_file.get_upload.return_value.status = mock_status
        upload_details = UploadDetails(mock_dds_file)
        self.assertEqual(upload_details.inconsistent(), False)
        self.assertEqual(upload_details.had_error(), True)
        self.assertEqual(upload_details.is_bad(), True)
        self.assertEqual(upload_details.name(), 'file1.dat')
        self.assertEqual(upload_details.status_str(), 'Error')
        self.assertEqual(upload_details.file_id(), '123')
        self.assertEqual(upload_details.message(), 'bad data')

    def test_error_ok(self):
        mock_dds_file = Mock()
        mock_dds_file.name = 'file1.dat'
        mock_dds_file.id = '123'
        mock_status = Mock(is_consistent=True, initiated_on='2021-01-01', error_on=None, error_message=None)
        mock_dds_file.get_upload.return_value.status = mock_status
        upload_details = UploadDetails(mock_dds_file)
        self.assertEqual(upload_details.inconsistent(), False)
        self.assertEqual(upload_details.had_error(), False)
        self.assertEqual(upload_details.is_bad(), False)
        self.assertEqual(upload_details.name(), 'file1.dat')
        self.assertEqual(upload_details.status_str(), 'Ok')
        self.assertEqual(upload_details.file_id(), '123')
        self.assertEqual(upload_details.message(), '')


class TestProjectChecker(TestCase):
    def setUp(self):
        self.config = Mock()
        self.project = Mock()
        self.checker = ProjectChecker(self.config, self.project)

    def test_files_are_ok__good(self):
        self.project.get_project_files_generator.return_value = []
        self.assertEqual(self.checker.files_are_ok(), True)

    def test_files_are_ok__error(self):
        self.project.get_project_files_generator.side_effect = DSResourceNotConsistentError(Mock(), Mock(), Mock())
        dds_file = Mock()
        dds_file.name = "file1.txt"
        dds_file.id = "123"
        dds_file.get_upload.return_value.status.is_consistent = False
        dds_file.get_upload.return_value.status.error_on = None
        dds_file.get_upload.return_value.status.initiated_on = '2021-01-01'
        self.project.get_path_to_files.return_value.items.return_value = [
            (None, dds_file)
        ]
        self.assertEqual(self.checker.files_are_ok(), False)
        headers, data = self.checker.get_bad_uploads_table_data()
        self.assertEqual(headers, ['File', 'Status', 'Message', 'File ID'])
        self.assertEqual(data, [['file1.txt', 'Inconsistent', 'started upload at 2021-01-01', '123']])
