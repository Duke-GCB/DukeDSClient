from unittest import TestCase
from ddsc.sdk.client import Client, DDSConnection
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
