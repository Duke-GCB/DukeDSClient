from unittest import TestCase
import json
from ddsc.remotestore import RemoteProject, RemoteFolder, RemoteFile, RemoteUser


class TestProjectFolderFile(TestCase):
    def test_project_list_item(self):
        projects_sample_json = """
            {
              "results": [
                {
                  "kind": "dds-projectNOPE",
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
        self.assertEquals('dds-project', project.kind)
        self.assertEquals('bc6d2ac6-4a52-4421-b6ef-89b96731e843', project.id)
        self.assertEquals('test4', project.name)
        self.assertEquals('test4desc', project.description)
        self.assertEquals(False, project.is_deleted)

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
        folder = RemoteFolder(folder_json)
        self.assertEquals('dds-folder', folder.kind)
        self.assertEquals('cf99a8f1-aebd-4640-8854-f34d03b7511e', folder.id)
        self.assertEquals('thistest', folder.name)
        self.assertEquals(False, folder.is_deleted)

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
        file = RemoteFile(file_json)
        self.assertEquals('dds-file', file.kind)
        self.assertEquals('3a14eac9-90f4-4667-9999-1625dd6c3d9a', file.id)
        self.assertEquals('bigWigToWig', file.name)
        self.assertEquals(False, file.is_deleted)
        self.assertEquals(1874572, file.size)


class TestRemoteUser(TestCase):
    def test_parse_user(self):
        users_json_str = """{
            "results": [
            {
              "id": "12789123897123978",
              "username": "js123",
              "full_name": "John Smith"
            }
            ]
        }
        """
        blob = json.loads(users_json_str)
        project_json = blob['results'][0]
        user = RemoteUser(project_json)
        self.assertEquals('12789123897123978', user.id)
        self.assertEquals('js123', user.username)
        self.assertEquals('John Smith', user.full_name)
        self.assertEquals('id:12789123897123978 username:js123 full_name:John Smith', str(user))

