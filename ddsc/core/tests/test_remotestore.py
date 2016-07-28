import json
from unittest import TestCase

from ddsc.core.remotestore import RemoteProject, RemoteFolder, RemoteFile, RemoteUser
from ddsc.core.remotestore import RemoteStore
from ddsc.core.remotestore import RemoteAuthRole


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
              "upload": {
                "id": "fbdaf8bd-949f-427e-9601-18e8a71b30db",
                "size": 1874572,
                "hash": null,
                "storage_provider": {
                  "id": "90b31bfd-dabe-4d4b-a040-0ac448ede0ad",
                  "name": "duke_swift",
                  "description": "Duke OIT Swift Service"
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
              "email": "john.smith@duke.edu"
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
        expected_str = "joe"
        auth_roles = RemoteStore.get_active_auth_roles_from_json(JSON_DATA)
        self.assertEqual(2, len(auth_roles))
        ids = set([auth_role.id for auth_role in  auth_roles])
        expected_ids = set(["project_admin","project_viewer"])
        self.assertEqual(expected_ids, ids)