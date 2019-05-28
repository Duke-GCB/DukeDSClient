from unittest import TestCase
from ddsc import DukeDS, ItemNotFound, DuplicateNameError
from mock import patch, Mock, ANY


class TestDukeDS(TestCase):
    def setUp(self):
        self.bat_dna_project = Mock()
        self.bat_dna_project.name = 'bat_dna'
        self.mouse_rna_project = Mock()
        self.mouse_rna_project.name = 'mouse_rna'

    @patch('ddsc.sdk.dukeds.Client')
    def test_list_projects(self, mock_client):
        mock_client.return_value.get_projects.return_value = [
            self.bat_dna_project,
            self.mouse_rna_project
        ]

        projects = DukeDS.list_projects()

        self.assertEqual(projects, ['bat_dna', 'mouse_rna'])

    @patch('ddsc.sdk.dukeds.Client')
    def test_create_project(self, mock_client):
        DukeDS.create_project('mouse', 'Mouse research project')

        mock_client.return_value.create_project.assert_called_with('mouse', 'Mouse research project')

    @patch('ddsc.sdk.dukeds.Client')
    def test_create_project_raises_for_duplicate_project_name(self, mock_client):
        mock_existing_project = Mock()
        mock_existing_project.name = 'mouse'
        mock_client.return_value.get_projects.return_value = [mock_existing_project]

        with self.assertRaises(DuplicateNameError):
            DukeDS.create_project('mouse', 'Mouse research project')

    @patch('ddsc.sdk.dukeds.Client')
    def test_delete_project_not_found(self, mock_client):
        mock_client.return_value.get_projects.return_value = [
            self.bat_dna_project
        ]
        with self.assertRaises(ItemNotFound):
            DukeDS.delete_project('mouse_rna')

    @patch('ddsc.sdk.dukeds.Client')
    def test_delete_project(self, mock_client):
        mock_client.return_value.get_projects.return_value = [
            self.bat_dna_project,
            self.mouse_rna_project
        ]

        DukeDS.delete_project('mouse_rna')

        self.bat_dna_project.delete.assert_not_called()
        self.mouse_rna_project.delete.assert_called()

    @patch('ddsc.sdk.dukeds.Client')
    @patch('ddsc.sdk.dukeds.PathToFiles')
    def test_list_files(self, mock_path_to_files, mock_client):
        mock_client.return_value.get_projects.return_value = [
            self.bat_dna_project,
            self.mouse_rna_project
        ]
        mock_path_to_files.return_value.paths = [
            'README.txt',
            'data/file1.dat',
            'data/file2.dat'
        ]

        filenames = DukeDS.list_files('mouse_rna')

        self.assertEqual(filenames, [
            'README.txt',
            'data/file1.dat',
            'data/file2.dat'
        ])

    @patch('ddsc.sdk.dukeds.Client')
    def test_download_file(self, mock_client):
        self.mouse_rna_project.get_children.return_value = []
        mock_client.return_value.get_projects.return_value = [
            self.mouse_rna_project
        ]

        DukeDS.download_file('mouse_rna', 'data/file1.dat')

    @patch('ddsc.sdk.dukeds.Client')
    def test_upload_file(self, mock_client):
        self.mouse_rna_project.get_children.return_value = []
        mock_client.return_value.get_projects.return_value = [
            self.mouse_rna_project
        ]

        DukeDS.upload_file('mouse_rna', '/tmp/file1.dat', 'file1.dat')

        self.mouse_rna_project.upload_file.assert_called_with('/tmp/file1.dat', remote_filename='file1.dat')

    @patch('ddsc.sdk.dukeds.Client')
    def test_upload_file_creating_project_and_parent_dir(self, mock_client):
        mock_client.return_value.get_projects.return_value = [
        ]
        mock_project = Mock()
        mock_project.get_children.return_value = []
        mock_folder = Mock()
        mock_folder.get_children.return_value = []
        mock_project.create_folder.return_value = mock_folder
        mock_client.return_value.create_project.return_value = mock_project

        DukeDS.upload_file('mouse_rna', '/tmp/file1.dat', 'data/file1.dat')

        mock_client.return_value.create_project.assert_called_with('mouse_rna', 'mouse_rna')
        mock_project.create_folder.assert_called_with('data')
        mock_folder.upload_file.assert_called_with('/tmp/file1.dat', remote_filename='file1.dat')

    @patch('ddsc.sdk.dukeds.Client')
    def test_upload_file_makes_new_version(self, mock_client):
        self.mouse_rna_project.get_children.return_value = []
        mock_client.return_value.get_projects.return_value = [
            self.mouse_rna_project
        ]
        mock_file1 = Mock()
        mock_file1.name = 'file1.dat'
        self.mouse_rna_project.get_children.return_value = [
            mock_file1
        ]

        DukeDS.upload_file('mouse_rna', '/tmp/file1.dat', 'file1.dat')
        self.mouse_rna_project.upload_file.assert_not_called()
        mock_file1.upload_new_version.assert_called_with('/tmp/file1.dat')

    @patch('ddsc.sdk.dukeds.Client')
    def test_upload_file_default_dest_is_local_basename(self, mock_client):
        self.mouse_rna_project.get_children.return_value = []
        mock_client.return_value.get_projects.return_value = [
            self.mouse_rna_project
        ]

        DukeDS.upload_file('mouse_rna', '/tmp/file1.dat')

        self.mouse_rna_project.upload_file.assert_called_with('/tmp/file1.dat', remote_filename='file1.dat')

    @patch('ddsc.sdk.dukeds.Client')
    def test_delete_file(self, mock_client):
        mock_client.return_value.get_projects.return_value = [
            self.bat_dna_project,
            self.mouse_rna_project
        ]
        mock_file1 = Mock()
        self.mouse_rna_project.get_child_for_path.return_value = mock_file1

        DukeDS.delete_file('mouse_rna', 'data/file1.dat')

        self.mouse_rna_project.get_child_for_path.assert_called_with('data/file1.dat')
        mock_file1.delete.assert_called()

    @patch('ddsc.sdk.dukeds.Client')
    @patch('ddsc.sdk.dukeds.DDSUserUtil')
    def test_can_deliver_to_user_with_email(self, mock_dds_user_util, mock_client):
        result = DukeDS.can_deliver_to_user_with_email(email_address='fakeuser@duke.edu')
        mock_dds_user_util.assert_called_with(mock_client.return_value.dds_connection.data_service,
                                              logging_func=ANY)
        dds_user_util = mock_dds_user_util.return_value
        dds_user_util.valid_dds_user_or_affiliate_exists_for_email.assert_called_with("fakeuser@duke.edu")
        self.assertEqual(result, dds_user_util.valid_dds_user_or_affiliate_exists_for_email.return_value)

    @patch('ddsc.sdk.dukeds.Client')
    @patch('ddsc.sdk.dukeds.DDSUserUtil')
    def test_can_deliver_to_user_with_email_with_logging_func(self, mock_dds_user_util, mock_client):
        mock_log_func = Mock()
        result = DukeDS.can_deliver_to_user_with_email(email_address='fakeuser@duke.edu', logging_func=mock_log_func)
        mock_dds_user_util.assert_called_with(mock_client.return_value.dds_connection.data_service,
                                              logging_func=mock_log_func)
        dds_user_util = mock_dds_user_util.return_value
        dds_user_util.valid_dds_user_or_affiliate_exists_for_email.assert_called_with("fakeuser@duke.edu")
        self.assertEqual(result, dds_user_util.valid_dds_user_or_affiliate_exists_for_email.return_value)
