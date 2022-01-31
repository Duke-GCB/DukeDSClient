from unittest import TestCase
from ddsc.core.consistency import UploadDetails, ProjectChecker, DSResourceNotConsistentError
from ddsc.core.ddsapi import DataServiceError
from mock import Mock, patch, call


class TestUploadDetails(TestCase):
    def test_inconsistent_status(self):
        mock_dds_file = Mock()
        mock_dds_file.name = 'file1.dat'
        mock_dds_file.id = '123'
        mock_status = Mock(is_consistent=False, initiated_on='2021-01-01', error_on=None)
        mock_dds_file.get_upload.return_value.status = mock_status
        upload_details = UploadDetails(mock_dds_file, '/data/file1.dat')
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
        upload_details = UploadDetails(mock_dds_file, '/data/file1.dat')
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
        upload_details = UploadDetails(mock_dds_file, '/data/file1.dat')
        self.assertEqual(upload_details.inconsistent(), False)
        self.assertEqual(upload_details.had_error(), False)
        self.assertEqual(upload_details.is_bad(), False)
        self.assertEqual(upload_details.name(), 'file1.dat')
        self.assertEqual(upload_details.status_str(), 'Ok')
        self.assertEqual(upload_details.file_id(), '123')
        self.assertEqual(upload_details.message(), '')
        self.assertEqual(upload_details.remote_path, '/data/file1.dat')


class TestProjectChecker(TestCase):
    def setUp(self):
        self.config = Mock()
        self.project = Mock()
        self.project.name = "Mouse"
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
            ("/data/bad/file1.txt", dds_file)
        ]
        self.assertEqual(self.checker.files_are_ok(), False)
        headers, data = self.checker.get_bad_uploads_table_data()
        self.assertEqual(headers, ['File', 'Status', 'Message', 'FileID', 'RemotePath'])
        self.assertEqual(data, [['file1.txt', 'Inconsistent', 'started upload at 2021-01-01', '123',
                                 '/data/bad/file1.txt']])

    def test_files_are_ok__DDS_400(self):
        self.project.get_project_files_generator.side_effect = DataServiceError(Mock(status_code=400), Mock(), Mock())
        dds_file = Mock()
        dds_file.name = "file1.txt"
        dds_file.id = "123"
        dds_file.get_upload.return_value.status.is_consistent = False
        dds_file.get_upload.return_value.status.error_on = None
        dds_file.get_upload.return_value.status.initiated_on = '2021-01-01'
        self.project.get_path_to_files.return_value.items.return_value = [
            ("/data/bad/file1.txt", dds_file)
        ]
        self.assertEqual(self.checker.files_are_ok(), False)
        headers, data = self.checker.get_bad_uploads_table_data()
        self.assertEqual(headers, ['File', 'Status', 'Message', 'FileID', 'RemotePath'])
        self.assertEqual(data, [['file1.txt', 'Inconsistent', 'started upload at 2021-01-01', '123',
                                 '/data/bad/file1.txt']])

    @patch('ddsc.core.consistency.print')
    @patch('ddsc.core.consistency.UploadDetails')
    def test_print_bad_uploads_table(self, mock_upload_details, mock_print):
        mock_upload_details.return_value.is_bad.return_value = True
        mock_upload_details.return_value.name.return_value = 'file1.txt'
        mock_upload_details.return_value.status_str.return_value = 'BAD'
        mock_upload_details.return_value.message.return_value = ' file is bad'
        mock_upload_details.return_value.file_id.return_value = '123'
        mock_upload_details.return_value.remote_path = '/data/file1.txt'
        self.project.get_path_to_files.return_value.items.return_value = [
            ('/data/file1.txt', Mock())
        ]
        self.checker.print_bad_uploads_table()
        mock_print.assert_has_calls([
            call("ERROR: Project Mouse is not in a consistent state.\n"),
            call("Please wait while file uploads are checked.\nThis process can take quite a while."),
            call('File       Status    Message        FileID  RemotePath\n'
                 '---------  --------  -----------  --------  ---------------\n'
                 'file1.txt  BAD       file is bad       123  /data/file1.txt'),
            call('\nNOTE: Inconsistent files should resolve in a few minutes after starting.'),
            call('\nAn inconsistent file can be deleted by running:\n ddsclient delete -p <ProjectName> '
                 '--path <RemotePath>'),
            call()
        ])

    @patch('ddsc.core.consistency.print')
    @patch('ddsc.core.consistency.time')
    def test_wait_for_consistency(self, mock_time, mock_print):
        self.project.get_project_files_generator.side_effect = [
            DSResourceNotConsistentError(Mock(), Mock(), Mock()),
            []
        ]
        self.checker.wait_for_consistency(wait_sec=10)
        mock_print.assert_has_calls([
            call('Checking files for project Mouse.'),
            call('Project not consistent yet. Waiting.'),
            call('Checking files for project Mouse.'),
            call('Project Mouse is consistent.')
        ])
        mock_time.sleep.assert_called_with(10)
