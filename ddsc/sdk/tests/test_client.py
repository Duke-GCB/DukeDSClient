from unittest import TestCase
from ddsc.sdk.client import Client, DDSConnection, BaseResponseItem, Project, Folder, File, FileDownload, FileUpload, \
    ChildFinder, PathToFiles, ItemNotFound
from ddsc.core.util import KindType
from mock import patch, Mock


class TestClient(TestCase):
    @patch('ddsc.sdk.client.create_config')
    @patch('ddsc.sdk.client.DDSConnection')
    def test_get_projects(self, mock_dss_connection, mock_create_config):
        some_project = Mock()
        other_project = Mock()
        mock_dss_connection.return_value.get_projects.return_value = [
            some_project,
            other_project
        ]

        client = Client()
        projects = client.get_projects()

        self.assertEqual(projects, [some_project, other_project])

    @patch('ddsc.sdk.client.create_config')
    @patch('ddsc.sdk.client.DDSConnection')
    def test_get_project_by_id(self, mock_dss_connection, mock_create_config):
        some_project = Mock()
        mock_dss_connection.return_value.get_project_by_id.return_value = some_project

        client = Client()
        project = client.get_project_by_id('123')

        self.assertEqual(project, some_project)
        mock_dss_connection.return_value.get_project_by_id.assert_called_with('123')

    @patch('ddsc.sdk.client.create_config')
    @patch('ddsc.sdk.client.DDSConnection')
    def test_create_project(self, mock_dss_connection, mock_create_config):
        some_project = Mock()
        mock_dss_connection.return_value.create_project.return_value = some_project

        client = Client()
        project = client.create_project('mouse', 'mouse data')

        self.assertEqual(project, some_project)
        mock_dss_connection.return_value.create_project.assert_called_with('mouse', 'mouse data')

    @patch('ddsc.sdk.client.create_config')
    @patch('ddsc.sdk.client.DDSConnection')
    def test_get_folder_by_id(self, mock_dss_connection, mock_create_config):
        some_folder = Mock()
        mock_dss_connection.return_value.get_folder_by_id.return_value = some_folder

        client = Client()
        folder = client.get_folder_by_id('456')

        self.assertEqual(folder, some_folder)
        mock_dss_connection.return_value.get_folder_by_id.assert_called_with('456')

    @patch('ddsc.sdk.client.create_config')
    @patch('ddsc.sdk.client.DDSConnection')
    def test_get_file_by_id(self, mock_dss_connection, mock_create_config):
        some_file = Mock()
        mock_dss_connection.return_value.get_file_by_id.return_value = some_file

        client = Client()
        folder = client.get_file_by_id('456')

        self.assertEqual(folder, some_file)
        mock_dss_connection.return_value.get_file_by_id.assert_called_with('456')


class TestDDSConnection(TestCase):
    @patch('ddsc.sdk.client.DataServiceApi')
    @patch('ddsc.sdk.client.DataServiceAuth')
    def test_get_projects(self, mock_data_service_auth, mock_data_service_api):
        response = Mock()
        response.json.return_value = {
            "results": [
                {
                    'id': '345',
                    'name': 'my project'
                }
            ]
        }
        mock_data_service_api.return_value.get_projects.return_value = response

        dds_connection = DDSConnection(Mock())
        projects = dds_connection.get_projects()

        self.assertEqual(len(projects), 1)
        self.assertEqual(projects[0].id, '345')
        self.assertEqual(projects[0].name, 'my project')

    @patch('ddsc.sdk.client.DataServiceApi')
    @patch('ddsc.sdk.client.DataServiceAuth')
    def test_get_project_by_id(self, mock_data_service_auth, mock_data_service_api):
        response = Mock()
        response.json.return_value = {
            'id': '456',
            'name': 'other project'
        }
        mock_data_service_api.return_value.get_project_by_id.return_value = response

        dds_connection = DDSConnection(Mock())
        project = dds_connection.get_project_by_id('456')

        mock_data_service_api.return_value.get_project_by_id.assert_called_with('456')
        self.assertEqual(project.id, '456')
        self.assertEqual(project.name, 'other project')

    @patch('ddsc.sdk.client.DataServiceApi')
    @patch('ddsc.sdk.client.DataServiceAuth')
    def test_create_project(self, mock_data_service_auth, mock_data_service_api):
        response = Mock()
        response.json.return_value = {
            'id': '456',
            'name': 'mouse'
        }
        mock_data_service_api.return_value.create_project.return_value = response

        dds_connection = DDSConnection(Mock())
        project = dds_connection.create_project('mouse', 'mouse project')

        mock_data_service_api.return_value.create_project.assert_called_with('mouse', 'mouse project')
        self.assertEqual(project.id, '456')
        self.assertEqual(project.name, 'mouse')

    @patch('ddsc.sdk.client.DataServiceApi')
    @patch('ddsc.sdk.client.DataServiceAuth')
    def test_delete_project(self, mock_data_service_auth, mock_data_service_api):
        dds_connection = DDSConnection(Mock())
        dds_connection.delete_project('678')

        mock_data_service_api.return_value.delete_project.assert_called_with('678')

    @patch('ddsc.sdk.client.DataServiceApi')
    @patch('ddsc.sdk.client.DataServiceAuth')
    def test_create_folder(self, mock_data_service_auth, mock_data_service_api):
        response = Mock()
        response.json.return_value = {
            'id': '456',
            'name': 'data',
            'project': {
                'id': '123'
            }
        }
        mock_data_service_api.return_value.create_folder.return_value = response

        dds_connection = DDSConnection(Mock())
        folder = dds_connection.create_folder('data', parent_kind_str=KindType.project_str, parent_uuid='123')

        mock_data_service_api.return_value.create_folder.assert_called_with('data', KindType.project_str, '123')
        self.assertEqual(folder.id, '456')
        self.assertEqual(folder.name, 'data')
        self.assertEqual(folder.project_id, '123')

    @patch('ddsc.sdk.client.DataServiceApi')
    @patch('ddsc.sdk.client.DataServiceAuth')
    def test_delete_folder(self, mock_data_service_auth, mock_data_service_api):
        dds_connection = DDSConnection(Mock())
        dds_connection.delete_folder('789')

        mock_data_service_api.return_value.delete_folder.assert_called_with('789')

    @patch('ddsc.sdk.client.DataServiceApi')
    @patch('ddsc.sdk.client.DataServiceAuth')
    def test_get_project_children(self, mock_data_service_auth, mock_data_service_api):
        response = Mock()
        response.json.return_value = {
            'results': [
                {
                    'id': '456',
                    'name': 'mouse',
                    'kind': KindType.folder_str,
                    'project': {
                        'id': '123'
                    }
                }
            ]
        }
        mock_data_service_api.return_value.get_project_children.return_value = response

        dds_connection = DDSConnection(Mock())
        children = dds_connection.get_project_children('123')

        mock_data_service_api.return_value.get_project_children.assert_called_with('123', None)
        self.assertEqual(len(children), 1)
        self.assertEqual(children[0].id, '456')
        self.assertEqual(children[0].name, 'mouse')
        self.assertEqual(children[0].project_id, '123')

    @patch('ddsc.sdk.client.DataServiceApi')
    @patch('ddsc.sdk.client.DataServiceAuth')
    def test_get_folder_children(self, mock_data_service_auth, mock_data_service_api):
        response = Mock()
        response.json.return_value = {
            'results': [
                {
                    'id': '456',
                    'name': 'mouse',
                    'kind': KindType.folder_str,
                    'project': {
                        'id': '123'
                    }
                }
            ]
        }
        mock_data_service_api.return_value.get_folder_children.return_value = response

        dds_connection = DDSConnection(Mock())
        children = dds_connection.get_folder_children('123')

        mock_data_service_api.return_value.get_folder_children.assert_called_with('123', None)
        self.assertEqual(len(children), 1)
        self.assertEqual(children[0].id, '456')
        self.assertEqual(children[0].name, 'mouse')
        self.assertEqual(children[0].project_id, '123')

    @patch('ddsc.sdk.client.DataServiceApi')
    @patch('ddsc.sdk.client.DataServiceAuth')
    def test_get_file_download(self, mock_data_service_auth, mock_data_service_api):
        response = Mock()
        response.json.return_value = {
            'id': '456',
        }
        mock_data_service_api.return_value.get_file_url.return_value = response

        dds_connection = DDSConnection(Mock())
        file_download = dds_connection.get_file_download('123')

        mock_data_service_api.return_value.get_file_url.assert_called_with('123')
        self.assertEqual(file_download.id, '456')

    @patch('ddsc.sdk.client.DataServiceApi')
    @patch('ddsc.sdk.client.DataServiceAuth')
    @patch('ddsc.sdk.client.PathData')
    @patch('ddsc.sdk.client.FileUploadOperations')
    @patch('ddsc.sdk.client.ParallelChunkProcessor')
    def test_upload_file(self, mock_parallel_chunk_processor, mock_file_upload_operations, mock_path_data,
                         mock_data_service_auth, mock_data_service_api):
        response = {
            'id': '456',
            'name': 'data.dat',
            'project': {
                'id': '123'
            }
        }
        mock_file_upload_operations.return_value.finish_upload.return_value = response

        dds_connection = DDSConnection(Mock())
        file_info = dds_connection.upload_file(
            local_path='/tmp/data.dat',
            project_id='123',
            parent_data=Mock()
        )

        self.assertEqual(file_info.id, '456')
        self.assertEqual(file_info.name, 'data.dat')
        self.assertEqual(file_info.project_id, '123')
        mock_file_upload_operations.return_value.create_upload.assert_called()
        mock_parallel_chunk_processor.return_value.run.assert_called()
        mock_file_upload_operations.return_value.finish_upload.assert_called()

    @patch('ddsc.sdk.client.DataServiceApi')
    @patch('ddsc.sdk.client.DataServiceAuth')
    def test_get_folder_by_id(self, mock_data_service_auth, mock_data_service_api):
        response = Mock()
        response.json.return_value = {
            'id': '456',
            'name': 'myfolder',
            'project': {
                'id': '123'
            }
        }
        mock_data_service_api.return_value.get_folder.return_value = response

        dds_connection = DDSConnection(Mock())
        folder = dds_connection.get_folder_by_id('456')

        mock_data_service_api.return_value.get_folder.assert_called_with('456')
        self.assertEqual(folder.id, '456')
        self.assertEqual(folder.name, 'myfolder')
        self.assertEqual(folder.project_id, '123')

    @patch('ddsc.sdk.client.DataServiceApi')
    @patch('ddsc.sdk.client.DataServiceAuth')
    def test_get_file_by_id(self, mock_data_service_auth, mock_data_service_api):
        response = Mock()
        response.json.return_value = {
            'id': '456',
            'name': 'myfile.txt',
            'project': {
                'id': '123'
            }
        }
        mock_data_service_api.return_value.get_file.return_value = response

        dds_connection = DDSConnection(Mock())
        my_file = dds_connection.get_file_by_id('456')

        mock_data_service_api.return_value.get_file.assert_called_with('456')
        self.assertEqual(my_file.id, '456')
        self.assertEqual(my_file.name, 'myfile.txt')
        self.assertEqual(my_file.project_id, '123')

    @patch('ddsc.sdk.client.DataServiceApi')
    @patch('ddsc.sdk.client.DataServiceAuth')
    def test_delete_file(self, mock_data_service_auth, mock_data_service_api):
        dds_connection = DDSConnection(Mock())
        dds_connection.delete_file('456')

        mock_data_service_api.return_value.delete_file.assert_called_with('456')


class TestBaseResponseItem(TestCase):
    def test_get_attr(self):
        item = BaseResponseItem(Mock(), {
            'id': '123',
            'name': 'data.dat'
        })
        self.assertEqual(item.id, '123')
        self.assertEqual(item.name, 'data.dat')


class TestProject(TestCase):
    def setUp(self):
        self.project_dict = {'id': '123', 'kind': KindType.project_str}

    def test_get_children(self):
        mock_dds_connection = Mock()
        response_children = [
            Mock()
        ]
        mock_dds_connection.get_project_children.return_value = response_children

        project = Project(mock_dds_connection, self.project_dict)
        children = project.get_children()

        mock_dds_connection.get_project_children.assert_called_with('123')
        self.assertEqual(children, response_children)

    @patch('ddsc.sdk.client.ChildFinder')
    def test_get_child_for_path(self, mock_child_finder):
        mock_dds_connection = Mock()
        mock_child = Mock()
        mock_child_finder.return_value.get_child.return_value = mock_child

        project = Project(mock_dds_connection, self.project_dict)
        child = project.get_child_for_path('data/file1.dat')

        mock_child_finder.assert_called_with('data/file1.dat', project)
        self.assertEqual(child, mock_child)

    def test_create_folder(self):
        mock_dds_connection = Mock()
        mock_folder = Mock()
        mock_dds_connection.create_folder.return_value = mock_folder

        project = Project(mock_dds_connection, self.project_dict)
        my_folder = project.create_folder('results')

        mock_dds_connection.create_folder.assert_called_with('results', 'dds-project', '123')
        self.assertEqual(my_folder, mock_folder)

    @patch('ddsc.sdk.client.ParentData')
    def test_upload_file(self, mock_parent_data):
        mock_dds_connection = Mock()
        mock_file = Mock()
        mock_dds_connection.upload_file.return_value = mock_file

        project = Project(mock_dds_connection, self.project_dict)
        my_file = project.upload_file('data.txt')

        mock_dds_connection.upload_file.assert_called_with('data.txt', project_id='123', remote_filename=None,
                                                           parent_data=mock_parent_data.return_value)
        mock_parent_data.assert_called_with('dds-project', '123')
        self.assertEqual(my_file, mock_file)

    def test_delete(self):
        mock_dds_connection = Mock()

        project = Project(mock_dds_connection, self.project_dict)
        project.delete()

        mock_dds_connection.delete_project.assert_called_with('123')


class TestFolder(TestCase):
    def setUp(self):
        self.folder_dict = {
            'id': '456',
            'kind': KindType.folder_str,
            'project': {'id': '123'}
        }

    def test_get_children(self):
        mock_dds_connection = Mock()
        response_children = [
            Mock()
        ]
        mock_dds_connection.get_folder_children.return_value = response_children

        folder = Folder(mock_dds_connection, self.folder_dict)
        children = folder.get_children()

        mock_dds_connection.get_folder_children.assert_called_with('456')
        self.assertEqual(children, response_children)

    def test_create_folder(self):
        mock_dds_connection = Mock()
        mock_folder = Mock()
        mock_dds_connection.create_folder.return_value = mock_folder

        folder = Folder(mock_dds_connection, self.folder_dict)
        sub_folder = folder.create_folder('results')

        mock_dds_connection.create_folder.assert_called_with('results', 'dds-folder', '456')
        self.assertEqual(sub_folder, mock_folder)

    @patch('ddsc.sdk.client.ParentData')
    def test_upload_file(self, mock_parent_data):
        mock_dds_connection = Mock()
        mock_file = Mock()
        mock_dds_connection.upload_file.return_value = mock_file

        folder = Folder(mock_dds_connection, self.folder_dict)
        my_file = folder.upload_file('data.txt')

        mock_dds_connection.upload_file.assert_called_with('data.txt', project_id='123', remote_filename=None,
                                                           parent_data=mock_parent_data.return_value)
        mock_parent_data.assert_called_with('dds-folder', '456')
        self.assertEqual(my_file, mock_file)


class TestFile(TestCase):
    def setUp(self):
        self.file_dict = {
            'id': '456',
            'kind': KindType.file_str,
            'project': {'id': '123'},
            'parent': {
                'id': '123',
                'kind': KindType.project_str
            }
        }

    def test_download_to_path(self):
        mock_dds_connection = Mock()

        file = File(mock_dds_connection, self.file_dict)
        file.download_to_path('/tmp/data.dat')

        mock_dds_connection.get_file_download.assert_called_with('456')
        mock_dds_connection.get_file_download.return_value.save_to_path('/tmp/data.dat')

    def test_delete(self):
        mock_dds_connection = Mock()

        file = File(mock_dds_connection, self.file_dict)
        file.delete()

        mock_dds_connection.delete_file.assert_called_with('456')

    @patch('ddsc.sdk.client.ParentData')
    def test_upload_new_version(self, mock_parent_data):
        mock_dds_connection = Mock()

        file = File(mock_dds_connection, self.file_dict)
        file.upload_new_version('/tmp/data2.dat')

        mock_dds_connection.upload_file.assert_called_with('/tmp/data2.dat', project_id='123',
                                                           parent_data=mock_parent_data.return_value,
                                                           existing_file_id='456')


class TestFileDownload(TestCase):
    @patch('ddsc.sdk.client.open')
    def test_save_to_path(self, mock_open):
        mock_dds_connection = Mock()
        response = Mock()
        response.iter_content.return_value = ['my', 'data']
        mock_dds_connection.data_service.receive_external.return_value = response

        file_download = FileDownload(mock_dds_connection, {
            'http_verb': 'GET',
            'host': 'somehost',
            'url': 'v1/get_file',
            'http_headers': ''
        })
        file_download.save_to_path('/tmp/data.out')

        mock_dds_connection.data_service.receive_external.assert_called_with('GET', 'somehost', 'v1/get_file', '')


class TestFileUpload(TestCase):
    def test_run_create_folder_new_file(self):
        mock_project = Mock()
        mock_project.get_children.return_value = []
        mock_folder = Mock()
        mock_folder.get_children.return_value = []
        mock_project.create_folder.return_value = mock_folder

        file_upload = FileUpload(mock_project, remote_path='results/data.txt', local_path='/tmp/data.txt')
        file_upload.run()

        mock_project.create_folder.assert_called_with('results')
        mock_folder.upload_file.assert_called_with('/tmp/data.txt', remote_filename='data.txt')

    def test_run_existing_file_no_parent_folder(self):
        mock_project = Mock()
        mock_file = Mock()
        mock_file.name = 'data.txt'
        mock_project.get_children.return_value = [mock_file]

        file_upload = FileUpload(mock_project, remote_path='data.txt', local_path='/tmp/data.txt')
        file_upload.run()

        mock_file.upload_new_version.assert_called_with('/tmp/data.txt')


class TestChildFinder(TestCase):
    def test_direct_children(self):
        mock_project = Mock()
        mock_folder = Mock()
        mock_file = Mock()
        mock_file.name = 'data.txt'
        mock_project.get_children.return_value = [
            mock_folder,
            mock_file
        ]
        child_finder = ChildFinder('data.txt', mock_project)
        found_child = child_finder.get_child()
        self.assertEqual(found_child, mock_file)

    def test_grand_children(self):
        mock_project = Mock()
        mock_folder = Mock()
        mock_file = Mock()
        mock_folder.name = 'results'
        mock_folder.get_children.return_value = [
            mock_file
        ]
        mock_file.name = 'data.txt'
        mock_project.get_children.return_value = [
            mock_folder,
        ]
        child_finder = ChildFinder('results/data.txt', mock_project)
        found_child = child_finder.get_child()
        self.assertEqual(found_child, mock_file)

    def test_child_not_found(self):
        mock_project = Mock()
        mock_folder = Mock()
        mock_project.get_children.return_value = [
            mock_folder,
        ]
        child_finder = ChildFinder('data.txt', mock_project)
        with self.assertRaises(ItemNotFound):
            child_finder.get_child()


class TestPathToFiles(TestCase):
    def test_path_creation(self):
        path_to_files = PathToFiles()
        mock_project = Mock(kind=KindType.project_str)
        mock_project.name = 'myproject'
        mock_folder = Mock(kind=KindType.folder_str)
        mock_folder.name = 'myfolder'
        mock_file1 = Mock(kind=KindType.file_str)
        mock_file1.name = 'myfile1'
        mock_file2 = Mock(kind=KindType.file_str)
        mock_file2.name = 'myfile2'
        mock_project.get_children.return_value = [
            mock_folder
        ]
        mock_folder.get_children.return_value = [
            mock_file1,
            mock_file2
        ]

        path_to_files.add_paths_for_children_of_node(mock_project)

        self.assertEqual({
            'myfolder/myfile1': mock_file1,
            'myfolder/myfile2': mock_file2
        }, path_to_files.paths)
