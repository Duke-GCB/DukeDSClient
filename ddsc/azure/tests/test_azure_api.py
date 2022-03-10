from unittest import TestCase
from mock import patch, Mock, call
import os
from ddsc.azure.api import strip_top_directory, make_acl, remove_prefix, AzureAuthRole, Users, \
    AzureUserNotFoundException, Bucket, AzureProject, AzureFile, AzureProjectSummary, AzureApi, ResourceNotFoundError


class TestApiFunctions(TestCase):
    def test_strip_top_directory(self):
        self.assertEqual(strip_top_directory("data/subdir/one.txt"), "subdir/one.txt")
        self.assertEqual(strip_top_directory("subdir/one.txt"), "one.txt")

    def test_make_acl(self):
        self.assertEqual(make_acl("12345678"), "user:12345678,default:user:12345678")
        self.assertEqual(make_acl("12345678", permissions="rwx"), "user:12345678:rwx,default:user:12345678:rwx")
        self.assertEqual(make_acl("12345678", permissions="rwx", apply_default=False), "user:12345678:rwx")

    def test_remove_prefix(self):
        self.assertEqual(remove_prefix("SomeText", "Some"), "Text")
        self.assertEqual(remove_prefix("ZomeText", "Some"), "ZomeText")


class TestAzureAuthRole(TestCase):
    def test_get_acl(self):
        auth_role = AzureAuthRole(id="project_downloader", permissions="rwx", description="Desc")
        self.assertEqual(auth_role.get_acl(user_id="123"), "user:123:rwx,default:user:123:rwx")


class TestUsers(TestCase):
    def setUp(self):
        self.credential = Mock()

    @patch('ddsc.azure.api.GraphClient')
    def test_get_current_user_netid(self, mock_graph_client):
        mock_response = Mock()
        mock_response.json.return_value = {"userPrincipalName": "user1@duke.edu"}
        mock_graph_client.return_value.get.return_value = mock_response
        users = Users(self.credential)
        self.assertEqual(users.get_current_user_netid(), "user1")

    @patch('ddsc.azure.api.GraphClient')
    def test_get_user_for_netid(self, mock_graph_client):
        mock_response = Mock()
        mock_response.json.return_value = {"userPrincipalName": "user1@duke.edu"}
        mock_graph_client.return_value.get.return_value = mock_response
        users = Users(self.credential)
        self.assertEqual(users.get_user_for_netid("user1"), {"userPrincipalName": "user1@duke.edu"})

    @patch('ddsc.azure.api.GraphClient')
    def test_get_user_for_netid_not_found(self, mock_graph_client):
        mock_response = Mock(status_code=404)
        mock_graph_client.return_value.get.return_value = mock_response
        users = Users(self.credential)
        with self.assertRaises(AzureUserNotFoundException):
            users.get_user_for_netid("user2")

    @patch('ddsc.azure.api.GraphClient')
    def test_get_id_and_name(self, mock_graph_client):
        mock_response = Mock()
        mock_response.json.return_value = {"id": "123", "displayName": "Bob"}
        mock_graph_client.return_value.get.return_value = mock_response
        users = Users(self.credential)
        user_id, display_name = users.get_id_and_name("user1")
        self.assertEqual(user_id, "123")
        self.assertEqual(display_name, "Bob")


class TestBucket(TestCase):
    def setUp(self):
        self.bucket = Bucket("Credentials", "subscription1", "resourceGroup2", "storageAccount3", "container4")
        self.bucket.storage_mgmt_client = Mock()
        self.bucket.service = Mock(account_name="storageAccount3")
        self.bucket.file_system = Mock(file_system_name="container4")
        self.bucket.storage_mgmt_client.storage_accounts.list_keys.return_value = Mock(keys=[Mock(value="SomeKey")])
        self.bucket.azcopy = Mock()

    def test_get_paths(self):
        result = self.bucket.get_paths("user1/mouse", recursive=True)
        self.assertEqual(result, self.bucket.file_system.get_paths.return_value)
        self.bucket.file_system.get_paths.assert_called_with("user1/mouse", recursive=True)

    def test_get_file_paths(self):
        self.bucket.file_system.get_paths.return_value = [
            {"is_directory": True, "id": "111"},
            {"is_directory": False, "id": "222"},
        ]
        result = self.bucket.get_file_paths("user1/mouse")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], "222")

    def test_get_directory_properties(self):
        result = self.bucket.get_directory_properties("user1/mouse")
        dc = self.bucket.file_system.get_directory_client.return_value
        self.assertEqual(result, dc.get_directory_properties.return_value)

    def test_move_directory(self):
        self.bucket.move_directory("user1/mouse", "user1/mouse2")
        dc = self.bucket.file_system.get_directory_client.return_value
        dc.rename_directory.assert_called_with(new_name='container4/user1/mouse2')

    def test_get_file_properties(self):
        result = self.bucket.get_file_properties("user1/mouse/file1.txt")
        fc = self.bucket.file_system.get_file_client.return_value
        self.assertEqual(result, fc.get_file_properties.return_value)

    def test_get_storage_account_key1(self):
        result = self.bucket.get_storage_account_key1()
        self.assertEqual(result, "SomeKey")

    @patch('ddsc.azure.api.generate_file_system_sas')
    def test_get_sas_url(self, mock_generate_file_system_sas):
        mock_generate_file_system_sas.return_value = "sas=Key"
        url = self.bucket.get_sas_url("user1/mouse/file1.txt")
        self.assertEqual(url, "https://storageAccount3.blob.core.windows.net/container4/user1/mouse/file1.txt?sas=Key")

    def test_get_url(self):
        url = self.bucket.get_url("user1/mouse/file1.txt")
        self.assertEqual(url, "https://storageAccount3.blob.core.windows.net/container4/user1/mouse/file1.txt")

    def test_update_access_control_recursive(self):
        self.bucket.update_access_control_recursive(path="user1/mouse/file1.txt", acl="someacl")
        dc = self.bucket.file_system.get_directory_client.return_value
        dc.update_access_control_recursive.assert_called_with(acl="someacl")

    def test_remove_access_control_recursive(self):
        self.bucket.remove_access_control_recursive(path="user1/mouse/file1.txt", acl="someacl")
        dc = self.bucket.file_system.get_directory_client.return_value
        dc.remove_access_control_recursive.assert_called_with(acl="someacl")

    @patch('ddsc.azure.api.os')
    def test_upload_paths(self, mock_os):
        mock_os.path.isfile.side_effect = [False, True, True, True]
        mock_os.path.basename = os.path.basename
        paths = ["data", "other/filea.txt", "other/fileb.txt", "other2/filec.txt"]
        self.bucket.upload_paths("user1/mouse", paths=paths, dry_run=False)
        self.bucket.azcopy.upload_directory.assert_called_with(
            source='data',
            destination='https://storageAccount3.blob.core.windows.net/container4/user1/mouse/data',
            dry_run=False)
        self.bucket.azcopy.upload_files.assert_has_calls([
            call(source_parent_dir='other', source_filenames=['filea.txt', 'fileb.txt'], dry_run=False,
                 destination='https://storageAccount3.blob.core.windows.net/container4/user1/mouse'),
            call(source_parent_dir='other2', source_filenames=['filec.txt'], dry_run=False,
                 destination='https://storageAccount3.blob.core.windows.net/container4/user1/mouse'),
        ])

    def test_download_paths(self):
        self.bucket.download_paths("user1/mouse", [], [], destination="/tmp/mouse", dry_run=False)
        self.bucket.azcopy.download_directory.assert_called_with(
            source='https://storageAccount3.blob.core.windows.net/container4/user1/mouse',
            destination='/tmp/mouse', dry_run=False, exclude_paths=[], include_paths=[])

    def test_move_path(self):
        self.bucket.move_path(project_path="user1/mouse", source_remote_path="data1.txt",
                              target_remote_path="data2.txt")
        self.bucket.file_system.get_file_client.assert_called_with('user1/mouse/data1.txt')
        fc = self.bucket.file_system.get_file_client.return_value
        fc.rename_file.assert_called_with('container4/user1/mouse/data2.txt')

    def test_delete_path(self):
        self.bucket.delete_path(path="user1/mouse")
        self.bucket.file_system.get_directory_client.assert_called_with('user1/mouse')
        fc = self.bucket.file_system.get_directory_client.return_value
        fc.delete_directory.assert_called_with()


class TestAzureProject(TestCase):
    def setUp(self):
        self.api = Mock()
        self.api.get_url.return_value = "someurl"
        self.path_dict = {"name": "user1/mouse/data"}
        self.project = AzureProject(self.api, path_dict=self.path_dict)

    def test_get_url(self):
        self.assertEqual(self.project.get_url(), "someurl")
        self.api.get_url.assert_called_with("user1/mouse/data")

    def test_get_size_str(self):
        self.assertEqual(self.project.get_size_str(), self.api.get_size_str.return_value)
        self.assertEqual(self.api.get_size_str.call_args[1]["project"], self.project)

    def test_get_file_paths(self):
        self.assertEqual(self.project.get_file_paths(), self.api.get_file_paths.return_value)
        self.api.get_file_paths.assert_called_with('user1/mouse/data')


class TestAzureFile(TestCase):
    def setUp(self):
        self.path_dict = {
            "name": "user1/mouse/data"
        }
        self.api = Mock()
        self.azure_file = AzureFile(api=self.api, path_dict=self.path_dict)

    def test_constructor(self):
        self.assertEqual(self.azure_file.name, "user1/mouse/data")
        self.assertEqual(self.azure_file.project_path, "data")

    def test_get_properties(self):
        result = self.azure_file.get_properties()
        self.assertEqual(result, self.api.get_file_properties.return_value)
        self.api.get_file_properties.assert_called_with("user1/mouse/data")

    def test_get_md5(self):
        mock_md5 = Mock()
        mock_md5.hex.return_value = "SomeHash"
        self.api.get_file_properties.return_value = {
            "content_settings": {
                "content_md5": mock_md5
            }
        }
        self.assertEqual(self.azure_file.get_md5(), "SomeHash")


class TestAzureProjectSummary(TestCase):
    def test_str(self):
        summary = AzureProjectSummary()
        self.assertEqual(str(summary), "0 folders, 0 files (0 B)")
        summary.apply_path_dict({
            "is_directory": True,
            "name": "user1/mouse/mydir"
        })
        summary.apply_path_dict({
            "is_directory": False,
            "name": "user1/mouse/mydir/file1.dat",
            "content_length": 1024
        })
        self.assertEqual(str(summary), "1 top level folder, 0 subfolders, 1 file (1 KB)")
        summary.apply_path_dict({
            "is_directory": True,
            "name": "user1/mouse/mydir/subdir"
        })
        self.assertEqual(str(summary), "1 top level folder, 1 subfolder, 1 file (1 KB)")


class TestAzureApi(TestCase):
    @patch('ddsc.azure.api.Users')
    @patch('ddsc.azure.api.Bucket')
    def setUp(self, mock_bucket, mock_users):
        self.config = Mock()
        self.credential = Mock()
        self.api = AzureApi(config=self.config, credential=self.credential, subscription_id='sub2',
                            resource_group='rg3', storage_account='sa4', container_name='cn5')
        self.api.current_user_netid = 'user1'
        self.bucket = self.api.bucket
        self.users = self.api.users
        self.users.get_id_and_name.return_value = ('123123', "Bob")
        self.bucket.get_paths.return_value = [
            {"name": "user1/mouse", "is_directory": True},
            {"name": "user1/somestrayfile.txt", "is_directory": False},
        ]
        self.bucket.get_directory_properties.return_value = {
            "name": "user1/mouse",
            "is_directory": True
        }

    def test_list_projects(self):
        result = self.api.list_projects()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, 'mouse')

    def test_get_project_by_name(self):
        project = self.api.get_project_by_name(name="mouse")
        self.assertEqual(project.name, "mouse")
        self.bucket.get_directory_properties.assert_called_with('user1/mouse')
        self.bucket.get_directory_properties.side_effect = ResourceNotFoundError()
        self.assertEqual(self.api.get_project_by_name(name="mouse"), None)

    def test_get_url(self):
        self.bucket.get_url.return_value = "someurl/user1/mouse/file1.txt"
        self.assertEqual(self.api.get_url("user1/mouse/file1.txt"), "someurl/user1/mouse/file1.txt")

    def test_get_container_url(self):
        self.assertEqual(self.api.get_container_url(), self.bucket.get_url.return_value.rstrip.return_value)

    def test_get_auth_roles(self):
        self.assertEqual(len(self.api.get_auth_roles()), 5)

    def test_get_auth_role_by_id(self):
        self.assertEqual(self.api.get_auth_role_by_id("project_admin").permissions, "rwx")
        self.assertEqual(self.api.get_auth_role_by_id("other"), None)

    def test_get_file_paths(self):
        self.bucket.get_paths.return_value = [
            {"name": "user1/mouse/file1.txt", "is_directory": False},
        ]
        result = self.api.get_file_paths("user1/mouse")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, "user1/mouse/file1.txt")

    def test_get_file_properties(self):
        self.assertEqual(self.api.get_file_properties("user1/mouse/file1.txt"),
                         self.bucket.get_file_properties.return_value)

    def test_get_sas_url(self):
        self.assertEqual(self.api.get_sas_url(), self.bucket.get_sas_url.return_value)
        self.bucket.get_sas_url.assert_called_with(path='user1')

    @patch('ddsc.azure.api.print')
    def test_add_user_to_project(self, mock_print):
        self.api.add_user_to_project(Mock(path="user1/mouse"), netid="user2", auth_role="file_uploader")
        self.bucket.update_access_control_recursive.assert_called_with(
            path='user1/mouse',
            acl='user:123123:rwx,default:user:123123:rwx')
        mock_print.assert_called_with('Gave user Bob file_uploader permissions for project user1/mouse.')

    @patch('ddsc.azure.api.print')
    def test_remove_user_from_project(self, mock_print):
        self.api.remove_user_from_project(Mock(path="user1/mouse"), netid="user2")
        self.bucket.remove_access_control_recursive.assert_called_with(
            path='user1/mouse',
            acl='user:123123,default:user:123123')
        mock_print.assert_called_with('Removed permissions from user Bob for project user1/mouse.')

    @patch('ddsc.azure.api.print')
    def test_upload_paths(self, mock_print):
        self.api.upload_paths(project_name="mouse", paths="/tmp/data.txt", dry_run=False)
        self.bucket.upload_paths.assert_called_with('user1/mouse/', '/tmp/data.txt', False)

    @patch('ddsc.azure.api.print')
    def test_download_paths(self, mock_print):
        self.api.download_paths(project_name="mouse", include_paths=None, exclude_paths=None, destination="/tmp/mouse",
                                dry_run=False)
        self.bucket.download_paths.assert_called_with('user1/mouse/', None, None, '/tmp/mouse', dry_run=False)

    @patch('ddsc.azure.api.print')
    def test_move_path(self, mock_print):
        self.api.move_path(project=Mock(path="user1/mouse"),
                           source_remote_path="data/file1.txt",
                           target_remote_path="data/file2.txt")
        self.bucket.move_path.assert_called_with('user1/mouse', 'data/file1.txt', 'data/file2.txt')

    @patch('ddsc.azure.api.print')
    def test_delete_remote_path(self, mock_print):
        self.api.delete_remote_path(project=Mock(path="user1/mouse"), remote_path='data/one.txt')
        self.bucket.delete_path.assert_called_with(path='user1/mouse/data/one.txt')

    @patch('ddsc.azure.api.AzureProjectSummary')
    def test_get_size_str(self, mock_summary):
        self.bucket.get_paths.return_value = [
            {"name": "user1/mouse", "is_directory": True},
            {"name": "user1/somestrayfile.txt", "is_directory": False},
        ]
        result = self.api.get_size_str(project=Mock(path="user1/mouse"))
        self.assertEqual(mock_summary.return_value.apply_path_dict.call_count, 2)
        self.assertEqual(result, str(mock_summary.return_value))

    @patch('ddsc.azure.api.DataDelivery')
    def test_deliver(self, mock_data_delivery):
        self.api.deliver(project=Mock(path="user1/mouse"), netid="user2", resend=True,
                         user_message="Hi", share_usernames=["user3"])
        mock_data_delivery.return_value.deliver.assert_called_with(
            'user1/mouse', resend=True, share_user_ids=['user3'], to_netid='user2', user_message='Hi'
        )
