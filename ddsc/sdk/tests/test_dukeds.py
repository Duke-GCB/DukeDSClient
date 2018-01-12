from unittest import TestCase
from ddsc import DukeDS, ItemNotFound, DuplicateNameError
from mock import patch, Mock


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
        mock_path_to_files.return_value.paths.keys.return_value = [
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

        DukeDS.upload_file('/tmp/file1.dat', 'mouse_rna', 'file1.dat')

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

        DukeDS.upload_file('/tmp/file1.dat', 'mouse_rna', 'data/file1.dat')

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

        DukeDS.upload_file('/tmp/file1.dat', 'mouse_rna', 'file1.dat')
        self.mouse_rna_project.upload_file.assert_not_called()
        mock_file1.upload_new_version.assert_called_with('/tmp/file1.dat')

    @patch('ddsc.sdk.dukeds.Client')
    def test_upload_file_default_dest_is_local_basename(self, mock_client):
        self.mouse_rna_project.get_children.return_value = []
        mock_client.return_value.get_projects.return_value = [
            self.mouse_rna_project
        ]

        DukeDS.upload_file('/tmp/file1.dat', 'mouse_rna')

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
