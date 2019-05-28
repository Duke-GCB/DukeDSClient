import json
from unittest import TestCase
from ddsc.core.util import KindType
from mock import MagicMock, Mock
from mock.mock import patch
from ddsc.core.remotestore import RemoteProject, RemoteFolder, RemoteFile, RemoteUser
from ddsc.core.remotestore import RemoteStore
from ddsc.core.remotestore import RemoteAuthRole
from ddsc.core.remotestore import RemoteProjectChildren
from ddsc.core.remotestore import RemoteAuthProvider
from ddsc.core.remotestore import ProjectNameOrId
from ddsc.core.remotestore import ProjectFile, RemoteFileUrl


class TestProjectFolderFile(TestCase):
    def test_project_list_item(self):
        projects_sample_json = """
            {
              "results": [
                {
                  "kind": "dds-project",
                  "id": "bc6d2ac6-4a52-4421-b6ef-89b96731e843",
                  "name": "test4",
                  "description": "test4desc",
                  "is_deleted": false,
                  "audit": {
                    "created_on": "2016-02-19T16:33:47.589Z",
                    "created_by": {
                      "id": "5dd78297-1604-457c-87c1-e3a792be16b9",
                      "username": "jpb67",
                      "full_name": "John Bradley"
                    },
                    "last_updated_on": "2016-02-19T16:33:47.764Z",
                    "last_updated_by": {
                      "id": "5dd78297-1604-457c-87c1-e3a792be16b9",
                      "username": "jpb67",
                      "full_name": "John Bradley"
                    },
                    "deleted_on": null,
                    "deleted_by": null
                  }
                }]
            }"""
        blob = json.loads(projects_sample_json)
        project_json = blob['results'][0]
        project = RemoteProject(project_json)
        self.assertEqual('dds-project', project.kind)
        self.assertEqual('bc6d2ac6-4a52-4421-b6ef-89b96731e843', project.id)
        self.assertEqual('test4', project.name)
        self.assertEqual('test4desc', project.description)
        self.assertEqual(False, project.is_deleted)
        self.assertEqual('', project.remote_path)

    def test_folder_item(self):
        projects_id_children_sample_json = """
        {
          "results": [
            {
              "kind": "dds-folder",
              "id": "cf99a8f1-aebd-4640-8854-f34d03b7511e",
              "parent": {
                "kind": "dds-project",
                "id": "bc6d2ac6-4a52-4421-b6ef-89b96731e843"
              },
              "name": "thistest",
              "is_deleted": false,
              "audit": {
                "created_on": "2016-02-19T16:38:39.910Z",
                "created_by": {
                  "id": "5dd78297-1604-457c-87c1-e3a792be16b9",
                  "username": "jpb67",
                  "full_name": "John Bradley"
                },
                "last_updated_on": null,
                "last_updated_by": null,
                "deleted_on": null,
                "deleted_by": null
              },
              "project": {
                "id": "bc6d2ac6-4a52-4421-b6ef-89b96731e843"
              },
              "ancestors": [
                {
                  "kind": "dds-project",
                  "id": "bc6d2ac6-4a52-4421-b6ef-89b96731e843",
                  "name": "test4"
                }
              ]
            }
          ]
        }"""
        blob = json.loads(projects_id_children_sample_json)
        folder_json = blob['results'][0]
        folder = RemoteFolder(folder_json, 'tmp')
        self.assertEqual('dds-folder', folder.kind)
        self.assertEqual('cf99a8f1-aebd-4640-8854-f34d03b7511e', folder.id)
        self.assertEqual('thistest', folder.name)
        self.assertEqual(False, folder.is_deleted)
        self.assertEqual('tmp/thistest', folder.remote_path)

    def test_file_item_new_version(self):
        folders_id_children_sample_json = """
        {
          "results": [
            {
              "kind": "dds-file",
              "id": "3a14eac9-90f4-4667-9999-1625dd6c3d9a",
              "parent": {
                "kind": "dds-folder",
                "id": "cf99a8f1-aebd-4640-8854-f34d03b7511e"
              },
              "name": "bigWigToWig",
              "audit": {
                "created_on": "2016-02-19T16:38:56.229Z",
                "created_by": {
                  "id": "5dd78297-1604-457c-87c1-e3a792be16b9",
                  "username": "jpb67",
                  "full_name": "John Bradley"
                },
                "last_updated_on": null,
                "last_updated_by": null,
                "deleted_on": null,
                "deleted_by": null
              },
              "is_deleted": false,
              "current_version": {
                  "id": "1234",
                  "upload": {
                    "id": "fbdaf8bd-949f-427e-9601-18e8a71b30db",
                    "size": 1874572,
                    "hash": null,
                    "storage_provider": {
                      "id": "90b31bfd-dabe-4d4b-a040-0ac448ede0ad",
                      "name": "duke_swift",
                      "description": "Duke OIT Swift Service"
                    }
                  }
              },
              "project": {
                "id": "bc6d2ac6-4a52-4421-b6ef-89b96731e843"
              },
              "ancestors": [
                {
                  "kind": "dds-project",
                  "id": "bc6d2ac6-4a52-4421-b6ef-89b96731e843",
                  "name": "test4"
                },
                {
                  "kind": "dds-folder",
                  "id": "cf99a8f1-aebd-4640-8854-f34d03b7511e",
                  "name": "thistest"
                }
              ]
            }
          ]
        }
        """
        blob = json.loads(folders_id_children_sample_json)
        file_json = blob['results'][0]
        file = RemoteFile(file_json, '')
        self.assertEqual('dds-file', file.kind)
        self.assertEqual('3a14eac9-90f4-4667-9999-1625dd6c3d9a', file.id)
        self.assertEqual('bigWigToWig', file.name)
        self.assertEqual(False, file.is_deleted)
        self.assertEqual(1874572, file.size)
        self.assertEqual('bigWigToWig', file.remote_path)

    def test_file_item(self):
        folders_id_children_sample_json = """
        {
          "results": [
            {
              "kind": "dds-file",
              "id": "3a14eac9-90f4-4667-9999-1625dd6c3d9a",
              "parent": {
                "kind": "dds-folder",
                "id": "cf99a8f1-aebd-4640-8854-f34d03b7511e"
              },
              "current_version": {
                 "id": "1235",
                 "upload": {
                   "id": "fbdaf8bd-949f-427e-9601-18e8a71b30db",
                   "size": 1874572,
                   "hash": null,
                   "storage_provider": {
                     "id": "90b31bfd-dabe-4d4b-a040-0ac448ede0ad",
                     "name": "duke_swift",
                     "description": "Duke OIT Swift Service"
                    }
                 }
              },
              "name": "bigWigToWig",
              "audit": {
                "created_on": "2016-02-19T16:38:56.229Z",
                "created_by": {
                  "id": "5dd78297-1604-457c-87c1-e3a792be16b9",
                  "username": "jpb67",
                  "full_name": "John Bradley"
                },
                "last_updated_on": null,
                "last_updated_by": null,
                "deleted_on": null,
                "deleted_by": null
              },
              "is_deleted": false,
              "project": {
                "id": "bc6d2ac6-4a52-4421-b6ef-89b96731e843"
              },
              "ancestors": [
                {
                  "kind": "dds-project",
                  "id": "bc6d2ac6-4a52-4421-b6ef-89b96731e843",
                  "name": "test4"
                },
                {
                  "kind": "dds-folder",
                  "id": "cf99a8f1-aebd-4640-8854-f34d03b7511e",
                  "name": "thistest"
                }
              ]
            }
          ]
        }
        """
        blob = json.loads(folders_id_children_sample_json)
        file_json = blob['results'][0]
        file = RemoteFile(file_json, 'test')
        self.assertEqual('dds-file', file.kind)
        self.assertEqual('3a14eac9-90f4-4667-9999-1625dd6c3d9a', file.id)
        self.assertEqual('bigWigToWig', file.name)
        self.assertEqual(False, file.is_deleted)
        self.assertEqual(1874572, file.size)
        self.assertEqual('test/bigWigToWig', file.remote_path)


class TestRemoteUser(TestCase):
    def test_parse_user(self):
        users_json_str = """{
            "results": [
            {
              "id": "12789123897123978",
              "username": "js123",
              "full_name": "John Smith",
              "email": "john.smith@duke.edu",
              "first_name" : "John",
              "last_name" : "Smith"
            }
            ]
        }
        """
        blob = json.loads(users_json_str)
        project_json = blob['results'][0]
        user = RemoteUser(project_json)
        self.assertEqual('12789123897123978', user.id)
        self.assertEqual('js123', user.username)
        self.assertEqual('John Smith', user.full_name)
        self.assertEqual('John', user.first_name)
        self.assertEqual('Smith', user.last_name)
        self.assertEqual('id:12789123897123978 username:js123 full_name:John Smith', str(user))


class TestRemoteAuthRole(TestCase):
    def test_parse_auth_role(self):
        ROLE_DATA = {
            "id": "project_admin",
            "name": "Project Admin",
            "description": "Can update project details, delete project...",
            "permissions": [
                {
                    "id": "view_project"
                },
                {
                    "id": "update_project"
                },
                {
                    "id": "delete_project"
                },
                {
                    "id": "manage_project_permissions"
                },
                {
                    "id": "download_file"
                },
                {
                    "id": "create_file"
                },
                {
                    "id": "update_file"
                },
                {
                    "id": "delete_file"
                }
            ],
            "contexts": [
                "project"
            ],
            "is_deprecated": False
        }
        auth_role = RemoteAuthRole(ROLE_DATA)
        self.assertEqual("project_admin", auth_role.id)
        self.assertEqual("Project Admin", auth_role.name)
        self.assertEqual("Can update project details, delete project...", auth_role.description)
        self.assertEqual(False, auth_role.is_deprecated)

    def test_deprecated_system_role(self):
        ROLE_DATA = {
            "id": "system_admin",
            "name": "System Admin",
            "description": "Can administrate the system",
            "permissions": [
                {
                    "id": "system_admin"
                }
            ],
            "contexts": [
                "system"
            ],
            "is_deprecated": True
        }
        auth_role = RemoteAuthRole(ROLE_DATA)
        self.assertEqual("system_admin", auth_role.id)
        self.assertEqual("System Admin", auth_role.name)
        self.assertEqual("Can administrate the system", auth_role.description)
        self.assertEqual(True, auth_role.is_deprecated)


class TestRemoteStore(TestCase):
    def test_auth_roles_system(self):
        JSON_DATA = {
            "results": [
                {
                    "id": "system_admin",
                    "name": "System Admin",
                    "description": "Can administrate the system",
                    "permissions": [
                        {
                            "id": "system_admin"
                        }
                    ],
                    "contexts": [
                        "system"
                    ],
                    "is_deprecated": False
                },
                {
                    "id": "helper_admin",
                    "name": "Helper Admin",
                    "description": "Can administrate the system also",
                    "permissions": [
                        {
                            "id": "helper_admin"
                        }
                    ],
                    "contexts": [
                        "system"
                    ],
                    "is_deprecated": True
                }
            ]
        }
        expected_str = "id:system_admin name:System Admin description:Can administrate the system"
        auth_roles = RemoteStore.get_active_auth_roles_from_json(JSON_DATA)
        self.assertEqual(1, len(auth_roles))
        self.assertEqual(expected_str, str(auth_roles[0]))

    def test_auth_roles_project(self):
        JSON_DATA = {
            "results": [
                {
                    "id": "project_admin",
                    "name": "Project Admin",
                    "description": "Can update project details, delete project, manage project level permissions and perform all file operations",
                    "permissions": [
                        {
                            "id": "view_project"
                        },
                        {
                            "id": "update_project"
                        },
                        {
                            "id": "delete_project"
                        },
                        {
                            "id": "manage_project_permissions"
                        },
                        {
                            "id": "download_file"
                        },
                        {
                            "id": "create_file"
                        },
                        {
                            "id": "update_file"
                        },
                        {
                            "id": "delete_file"
                        }
                    ],
                    "contexts": [
                        "project"
                    ],
                    "is_deprecated": False
                },
                {
                    "id": "project_viewer",
                    "name": "Project Viewer",
                    "description": "Can only view project and file meta-data",
                    "permissions": [
                        {
                            "id": "view_project"
                        }
                    ],
                    "contexts": [
                        "project"
                    ],
                    "is_deprecated": False
                }
            ]
        }
        auth_roles = RemoteStore.get_active_auth_roles_from_json(JSON_DATA)
        self.assertEqual(2, len(auth_roles))
        ids = set([auth_role.id for auth_role in auth_roles])
        expected_ids = set(["project_admin", "project_viewer"])
        self.assertEqual(expected_ids, ids)

    @patch("ddsc.core.remotestore.DataServiceApi")
    def test_get_projects_with_auth_role(self, mock_data_service_api):
        projects_resp = Mock()
        projects_resp.json.return_value = {
            'results': [
                {
                    'id': '123'
                },
                {
                    'id': '456'
                }
            ]
        }
        mock_data_service_api.return_value.get_projects.return_value = projects_resp
        permission_resp = Mock()
        permission_resp.json.side_effect = [
            {
                'auth_role': {
                    'id': 'project_admin'
                }
            }, {
                'auth_role': {
                    'id': 'file_downloader'
                }
            }
        ]
        mock_data_service_api.return_value.get_user_project_permission.return_value = permission_resp
        remote_store = RemoteStore(config=MagicMock())
        result = remote_store.get_projects_with_auth_role(auth_role='project_admin')
        mock_data_service_api.return_value.get_projects.assert_called()
        self.assertEqual(1, len(result))
        self.assertEqual('123', result[0]['id'])

    @patch("ddsc.core.remotestore.DataServiceApi")
    def test_fetch_remote_project_exclude_response_fields(self, mock_data_service_api):
        exclude_response_fields = ['audit', 'ancestors', 'project']
        projects_resp = Mock()
        projects_resp.json.return_value = {
            'results': [
                {
                    'id': '123',
                    'kind': 'dds-project',
                    'name': 'Project1',
                    'description': '',
                    'is_deleted': False,
                },
            ]
        }
        mock_data_service_api.return_value.get_projects.return_value = projects_resp
        remote_store = RemoteStore(config=MagicMock())
        project_name_or_id = ProjectNameOrId.create_from_project_id('123')
        remote_store.fetch_remote_project(project_name_or_id, must_exist=True, include_children=True)
        mock_data_service_api.return_value.get_project_children.assert_called_with('123', '', exclude_response_fields)

    @patch("ddsc.core.remotestore.DataServiceApi")
    def test_get_project_files(self, mock_data_service_api):
        file_dict = {
            'id': '123',
            'name': 'somefile',
            'size': 100,
            'file_url': 'someurl',
            'hashes': [],
            'ancestors': [],
        }
        files_resp = Mock()
        files_resp.json.return_value = {
            'results': [
                file_dict,
            ]
        }
        mock_data_service_api.return_value.get_project_files.return_value = files_resp

        remote_store = RemoteStore(config=MagicMock())
        project_files = remote_store.get_project_files(Mock())

        self.assertEqual(len(project_files), 1)
        self.assertEqual(project_files[0].id, '123')
        self.assertEqual(project_files[0].name, 'somefile')
        self.assertEqual(project_files[0].size, 100)

    @patch("ddsc.core.remotestore.DataServiceApi")
    def test_constructor(self, mock_data_service_api):
        mock_data_serivce = Mock()
        mock_config = Mock()

        remote_store = RemoteStore(config=mock_config, data_service=mock_data_serivce)
        self.assertFalse(mock_data_service_api.called)
        self.assertEqual(remote_store.data_service, mock_data_serivce)

        remote_store = RemoteStore(config=mock_config)
        self.assertTrue(mock_data_service_api.called)

    @patch("ddsc.core.remotestore.DataServiceApi")
    @patch("ddsc.core.remotestore.RemoteUser")
    def test_fetch_users_no_filter(self, mock_remote_user, mock_data_service_api):
        user_dict = {
            'id': '123',
        }
        users_resp = Mock()
        users_resp.json.return_value = {
            'results': [
                user_dict,
            ]
        }
        mock_data_service_api.return_value.get_users.return_value = users_resp

        remote_store = RemoteStore(config=MagicMock())
        users = remote_store.fetch_users()

        mock_data_service_api.return_value.get_users.assert_called_with(email=None, username=None)
        self.assertEqual(len(users), 1)
        self.assertEqual(users[0], mock_remote_user.return_value)
        mock_remote_user.assert_called_with(user_dict)

    @patch("ddsc.core.remotestore.DataServiceApi")
    @patch("ddsc.core.remotestore.RemoteUser")
    def test_fetch_users_with_filter(self, mock_remote_user, mock_data_service_api):
        user_dict = {
            'id': '123',
        }
        users_resp = Mock()
        users_resp.json.return_value = {
            'results': [
                user_dict,
            ]
        }
        mock_data_service_api.return_value.get_users.return_value = users_resp

        remote_store = RemoteStore(config=MagicMock())
        users = remote_store.fetch_users(email='joe@joe.com', username='joe')

        mock_data_service_api.return_value.get_users.assert_called_with(email='joe@joe.com', username='joe')
        self.assertEqual(len(users), 1)
        self.assertEqual(users[0], mock_remote_user.return_value)
        mock_remote_user.assert_called_with(user_dict)

    def test_lookup_or_register_user_by_email_or_username_with_username(self):
        remote_store = RemoteStore(config=MagicMock())
        remote_store.get_or_register_user_by_username = Mock()
        remote_store.get_or_register_user_by_email = Mock()
        result = remote_store.lookup_or_register_user_by_email_or_username(email=None, username="myuser")
        self.assertEqual(result, remote_store.get_or_register_user_by_username.return_value)
        remote_store.get_or_register_user_by_username.assert_called_with("myuser")
        remote_store.get_or_register_user_by_email.assert_not_called()

    def test_lookup_or_register_user_by_email_or_username_with_email(self):
        remote_store = RemoteStore(config=MagicMock())
        remote_store.get_or_register_user_by_username = Mock()
        remote_store.get_or_register_user_by_email = Mock()
        result = remote_store.lookup_or_register_user_by_email_or_username(email="fakemail@duke.edu", username=None)
        self.assertEqual(result, remote_store.get_or_register_user_by_email.return_value)
        remote_store.get_or_register_user_by_username.assert_not_called()
        remote_store.get_or_register_user_by_email.assert_called_with("fakemail@duke.edu")

    @patch('ddsc.core.remotestore.UserUtil', autospec=True)
    @patch('ddsc.core.remotestore.RemoteUser')
    def test_get_or_register_user_by_username_finds_user(self, mock_remote_user, mock_user_util):
        mock_user_util.return_value.find_dds_user_by_username.return_value = {"id": "123"}

        remote_store = RemoteStore(config=MagicMock())
        result = remote_store.get_or_register_user_by_username("user123")
        self.assertEqual(result, mock_remote_user.return_value)
        mock_remote_user.assert_called_with({"id": "123"})
        mock_util = mock_user_util.return_value
        mock_util.find_dds_user_by_username.assert_called_with("user123")
        mock_util.register_dds_user_by_username.assert_not_called()

    @patch('ddsc.core.remotestore.UserUtil', autospec=True)
    @patch('ddsc.core.remotestore.RemoteUser')
    def test_get_or_register_user_by_username_registers_user(self, mock_remote_user, mock_user_util):
        mock_user_util.return_value.find_dds_user_by_username.return_value = None
        mock_user_util.return_value.register_dds_user_by_username.return_value = {"id": "456"}

        remote_store = RemoteStore(config=MagicMock())
        result = remote_store.get_or_register_user_by_username("user123")
        self.assertEqual(result, mock_remote_user.return_value)
        mock_remote_user.assert_called_with({"id": "456"})
        mock_util = mock_user_util.return_value
        mock_util.find_dds_user_by_username.assert_called_with("user123")
        mock_util.register_dds_user_by_username.assert_called_with("user123")

    @patch('ddsc.core.remotestore.UserUtil', autospec=True)
    @patch('ddsc.core.remotestore.RemoteUser')
    def test_get_or_register_user_by_email_finds_user(self, mock_remote_user, mock_user_util):
        mock_user_util.return_value.find_dds_user_by_email.return_value = {"id": "123"}

        remote_store = RemoteStore(config=MagicMock())
        result = remote_store.get_or_register_user_by_email("user@user.user")
        self.assertEqual(result, mock_remote_user.return_value)
        mock_remote_user.assert_called_with({"id": "123"})
        mock_util = mock_user_util.return_value
        mock_util.find_dds_user_by_email.assert_called_with("user@user.user")
        mock_util.register_dds_user_by_username.assert_not_called()

    @patch('ddsc.core.remotestore.UserUtil', autospec=True)
    @patch('ddsc.core.remotestore.RemoteUser')
    def test_get_or_register_user_by_email_registers_user(self, mock_remote_user, mock_user_util):
        mock_user_util.return_value.find_dds_user_by_email.return_value = None
        mock_user_util.return_value.register_dds_user_with_email.return_value = {"id": "456"}

        remote_store = RemoteStore(config=MagicMock())
        result = remote_store.get_or_register_user_by_email("user@user.user")
        self.assertEqual(result, mock_remote_user.return_value)
        mock_remote_user.assert_called_with({"id": "456"})
        mock_util = mock_user_util.return_value
        mock_util.find_dds_user_by_email.assert_called_with("user@user.user")
        mock_util.register_dds_user_with_email.assert_called_with("user@user.user")


class TestRemoteProjectChildren(TestCase):
    def test_simple_case(self):
        project_id = '7aa64c07-6427-44e0-ba38-0959454f77d7'
        folder_id = '29f59d7f-c32e-4305-8148-d0c5800dd06d'
        file_id = '85506b5f-a118-4d7a-8af7-da97b02fc6f2'
        sample_data = [
            {'kind': 'dds-file',
             'parent': {'kind': 'dds-folder', 'id': folder_id},
             'current_version': {
                 'id': '1235',
                 'upload': {'size': 10, 'hash': {'algorithm': 'md5', 'value': '3664d6f3812dbb0d80302ef990b96b51'}},
             },
             'is_deleted': False,
             'name': 'data.txt',
             'id': file_id},
            {'kind': 'dds-folder',
             'parent': {'kind': 'dds-project', 'id': project_id},
             'is_deleted': False,
             'name': 'folder1',
             'id': folder_id}]
        remote_children = RemoteProjectChildren(project_id, sample_data)

        children = remote_children._get_children_for_parent(project_id)
        self.assert_field_values(children, field_name='id', expected_values=[folder_id])

        children = remote_children._get_children_for_parent(folder_id)
        self.assert_field_values(children, field_name='id', expected_values=[file_id])

        children = remote_children._get_children_for_parent(file_id)
        self.assert_field_values(children, field_name='id', expected_values=[])

        tree = remote_children.get_tree()
        self.assertEqual(1, len(tree))
        self.assertEqual(folder_id, tree[0].id)
        self.assertEqual(1, len(tree[0].children))
        self.assertEqual(file_id, tree[0].children[0].id)
        self.assertEqual('3664d6f3812dbb0d80302ef990b96b51', tree[0].children[0].file_hash)

    def assert_field_values(self, items, field_name, expected_values):
        values = [item[field_name] for item in items]
        self.assertEqual(values, expected_values)

    def test_top_level_files(self):
        project_id = 'c5da4e5e-0906-41a0-8b8f-20d99863bbaa'
        file1_id = '35735a28-2d2c-4a11-9440-674bd345d866'
        file2_id = 'eb3514e1-72d7-41ee-bd34-bf059f6f5947'
        file3_id = 'cbb6e497-ed59-4456-98dc-f1aeee01b626'
        sample_data = [{'id': file1_id,
                        'kind': 'dds-file',
                        'name': 'three',
                        'is_deleted': False,
                        'current_version': {
                            'id': '1235',
                            'upload':
                                {'id': '6002c346-d952-4140-8255-a28978e9a1af',
                                 'size': 10,
                                 'hash':
                                     {'algorithm': 'md5',
                                      'value': 'b9dea26997ca089d9f20e372c50565e8'}},
                        },
                        'parent': {'id': project_id, 'kind': 'dds-project'}},
                       {'id': file2_id,
                        'kind': 'dds-file',
                        'name': 'two',
                        'is_deleted': False,
                        'current_version': {
                            'id': '1236',
                            'upload':
                                {'id': 'e710c232-14b2-4cdc-be29-9c9b4c811834',
                                 'size': 10,
                                 'hash':
                                     {'algorithm': 'md5', 'value': '99003d4d61ca0f5367e5d88a24db7812'}},
                        },
                        'parent': {'id': project_id, 'kind': 'dds-project'}},
                       {'id': file3_id,
                        'kind': 'dds-file',
                        'name': 'one',
                        'is_deleted': False,
                        'current_version': {
                            'id': '1236',
                            'upload':
                                {'id': 'ecd507c4-8f04-404d-acef-0ced912e4cdf',
                                 'size': 10,
                                 'hash': None},
                        },
                        'parent': {'id': project_id, 'kind': 'dds-project'}}]

        remote_children = RemoteProjectChildren(project_id, sample_data)
        tree = remote_children.get_tree()
        self.assertEqual(3, len(tree))
        self.assertEqual(file1_id, tree[0].id)
        self.assertEqual('b9dea26997ca089d9f20e372c50565e8', tree[0].file_hash)
        self.assertEqual(file2_id, tree[1].id)
        self.assertEqual('99003d4d61ca0f5367e5d88a24db7812', tree[1].file_hash)
        self.assertEqual(file3_id, tree[2].id)
        self.assertEqual(None, tree[2].file_hash)


class TestReadRemoteHash(TestCase):
    def test_old_way(self):
        """
        Upload contains single "hash" property which contains "value" and "algorithm".
        """
        upload = {
            "hash": {
                "value": "aabbcc",
                "algorithm": "md5"
            }
        }
        hash_info = RemoteFile.get_hash_from_upload(upload)
        self.assertEqual(hash_info["value"], "aabbcc")
        self.assertEqual(hash_info["algorithm"], "md5")

    def test_new_way_one_item(self):
        """
        Upload contains "hashes" array which contains a single element with properties "value" and "algorithm".
        """
        upload = {
            "hashes": [{
                "value": "aabbcc",
                "algorithm": "md5"
            }]
        }
        hash_info = RemoteFile.get_hash_from_upload(upload)
        self.assertEqual(hash_info["value"], "aabbcc")
        self.assertEqual(hash_info["algorithm"], "md5")

    def test_new_way_two_item(self):
        """
        Upload contains "hashes" array which contains a single element with properties "value" and "algorithm".
        """
        upload = {
            "hashes": [
                {
                    "value": "cheese",
                    "algorithm": "cheese"
                }, {
                    "value": "aabbcc",
                    "algorithm": "md5"
                }]
        }
        hash_info = RemoteFile.get_hash_from_upload(upload)
        self.assertEqual(hash_info["value"], "aabbcc")
        self.assertEqual(hash_info["algorithm"], "md5")


class TestRemoteAuthProvider(TestCase):
    def setUp(self):
        self.provider_data1 = {
            "id": "aca35ba3-a44a-47c2-8b3b-afe43a88360d",
            "service_id": "cfde039d-f550-47e7-833c-9ebc4e257847",
            "name": "Duke Authentication Service",
            "is_deprecated": False,
            "is_default": True,
            "login_initiation_url": "https://someurl"
        }

    def test_constructor(self):
        auth_provider = RemoteAuthProvider(self.provider_data1)
        self.assertEqual(auth_provider.id, "aca35ba3-a44a-47c2-8b3b-afe43a88360d")
        self.assertEqual(auth_provider.service_id, "cfde039d-f550-47e7-833c-9ebc4e257847")
        self.assertEqual(auth_provider.name, "Duke Authentication Service")
        self.assertEqual(auth_provider.is_deprecated, False)
        self.assertEqual(auth_provider.is_default, True)
        self.assertEqual(auth_provider.login_initiation_url, "https://someurl")

    @patch("ddsc.core.remotestore.DataServiceApi")
    def test_get_auth_providers(self, mock_data_service_api):
        response = MagicMock()
        response.json.return_value = {
            "results": [self.provider_data1]
        }
        mock_data_service_api().get_auth_providers.return_value = response
        remote_store = RemoteStore(MagicMock())
        providers = remote_store.get_auth_providers()
        self.assertEqual(len(providers), 1)
        self.assertEqual(providers[0].name, "Duke Authentication Service")


class TestProjectNameOrId(TestCase):
    def test_constructors(self):
        item = ProjectNameOrId.create_from_project_id('123')
        self.assertEqual(item.is_name, False)
        self.assertEqual(item.value, '123')
        self.assertEqual('123', item.get_id_or_raise())
        with self.assertRaises(ValueError):
            item.get_name_or_raise()

        item = ProjectNameOrId.create_from_name('mouse')
        self.assertEqual(item.is_name, True)
        self.assertEqual(item.value, 'mouse')
        self.assertEqual('mouse', item.get_name_or_raise())
        with self.assertRaises(ValueError):
            item.get_id_or_raise()

        mock_project = Mock()
        mock_project.id = '456'
        item = ProjectNameOrId.create_from_remote_project(mock_project)
        self.assertEqual(item.is_name, False)
        self.assertEqual(item.value, '456')
        self.assertEqual('456', item.get_id_or_raise())
        with self.assertRaises(ValueError):
            item.get_name_or_raise()

    def test_description(self):
        item = ProjectNameOrId(value='mouse', is_name=True)
        self.assertEqual('name mouse', item.description())
        item = ProjectNameOrId(value='123', is_name=False)
        self.assertEqual('id 123', item.description())

    def test_contained_in_dict(self):
        item = ProjectNameOrId(value='mouse', is_name=True)
        self.assertEqual(True, item.contained_in_dict({'name': 'mouse'}))
        self.assertEqual(False, item.contained_in_dict({'name': 'mouse2'}))
        item = ProjectNameOrId(value='576', is_name=False)
        self.assertEqual(False, item.contained_in_dict({'id': '123'}))
        self.assertEqual(True, item.contained_in_dict({'id': '576'}))


class TestProjectFile(TestCase):
    def setUp(self):
        self.project_file_dict = {
            'id': '123',
            'name': 'somefile',
            'size': 100,
            'file_url': 'someurl',
            'hashes': [],
            'ancestors': [
                {
                    'name': 'data',
                    'kind': KindType.folder_str
                },
                {
                    'name': 'docs',
                    'kind': KindType.folder_str
                },
            ],
        }

    def test_get_remote_parent_path(self):
        project_file = ProjectFile(self.project_file_dict)
        self.assertEqual('data/docs', project_file.get_remote_parent_path())

    def test_path(self):
        project_file = ProjectFile(self.project_file_dict)
        self.assertEqual('data/docs/somefile', project_file.path)

    def test_get_local_path(self):
        project_file = ProjectFile(self.project_file_dict)
        self.assertEqual('/tmp/data/docs/somefile', project_file.get_local_path('/tmp/'))

    def test_create_for_dds_file_dict(self):
        file_dict = {
            "id": "123",
            "name": "file1.txt",
            "current_version": {
                "upload": {
                    "size": 100,
                    "hashes": [
                        {"algorithm": "md5", "value": "abc"}
                    ]
                }
            },
            "ancestors": [
                {"name": "parentdir"}
            ]
        }
        project_file = ProjectFile.create_for_dds_file_dict(file_dict)
        self.assertEqual(project_file.id, "123")
        self.assertEqual(project_file.name, "file1.txt")
        self.assertEqual(project_file.size, 100)
        self.assertEqual(project_file.file_url, None)
        self.assertEqual(project_file.hashes, [{"algorithm": "md5", "value": "abc"}])
        self.assertEqual(project_file.ancestors, [{"name": "parentdir"}])
        self.assertEqual(project_file.json_data, {
            'ancestors': [{'name': 'parentdir'}],
            'file_url': None,
            'hashes': [{'algorithm': 'md5', 'value': 'abc'}],
            'id': '123',
            'name': 'file1.txt',
            'size': 100
        })


class TestRemoteFileUrl(TestCase):
    def test_constructor(self):
        remote_file_url = RemoteFileUrl({
            'http_verb': 'GET',
            'host': 'somehost',
            'url': 'someurl',
            'http_headers': []
        })
        self.assertEqual(remote_file_url.http_verb, 'GET')
        self.assertEqual(remote_file_url.host, 'somehost')
        self.assertEqual(remote_file_url.url, 'someurl')
        self.assertEqual(remote_file_url.http_headers, [])
