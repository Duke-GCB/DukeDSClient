from ddsc.config import Config
from ddsc.core.ddsapi import DataServiceApi, DataServiceAuth
from ddsc.core.fileuploader import FileUploadOperations, ParentData
from ddsc.core.localstore import PathData
import os


def create_config(agent_key, user_key, url):
    config_properties = {
        Config.AGENT_KEY: agent_key,
        Config.USER_KEY: user_key,
    }
    if url:
        config_properties[Config.URL] = url
    config = Config()
    config.update_properties(config_properties)
    return config


def response_to_object_list(connection, response, constructor, filter_func=None):
    items = response.json()['results']
    if filter_func:
        items = [item for item in items if filter_func(item)]
    return [constructor(connection, details) for details in items]


def response_to_object(connection, response, constructor):
    return constructor(connection, response.json())


def create_file_or_folder(connection, details):
    kind = details['kind']
    if kind == 'dds-folder':
        return Folder(connection, details)
    if kind == 'dds-file':
        return File(connection, details)
    raise ValueError("Received invalid kind {}".format(kind))


def is_file(item):
    return item['kind'] == 'dds-file'


class Connection(object):
    def __init__(self, agent_key, user_key, url):
        self.config = create_config(agent_key, user_key, url)
        self.data_service = DataServiceApi(DataServiceAuth(self.config), self.config.url)


class DukeDSClient(object):
    def __init__(self, agent_key, user_key, url=None):
        self._connection = Connection(agent_key, user_key, url)

    def projects_list(self):
        response = self._connection.data_service.get_projects()
        return response_to_object_list(self._connection, response, Project)

    def projects_get(self, project_id):
        response = self._connection.data_service.get_project_by_id(project_id)
        return response_to_object(self._connection, response, Project)

    def projects_create(self, name, description):
        response = self._connection.data_service.create_project(name, description)
        return response_to_object(self._connection, response, Project)


class Project(object):
    def __init__(self, connection, details):
        self._connection = connection
        self.id = details['id']
        self.name = details['name']
        self.description = details['description']
        self.details = details

    def children_list(self):
        response = self._connection.data_service.get_project_children(self.id, name_contains=None)
        return response_to_object_list(self._connection, response, create_file_or_folder)

    def files_list(self):
        response = self._connection.data_service.get_project_children(self.id, name_contains='')
        return response_to_object_list(self._connection, response, create_file_or_folder, is_file)

    def delete(self):
        self._connection.data_service.delete_project(self.id)

    def download_to_path(self, path):
        os.mkdir(path)
        for child in self.children_list():
            child.download_to_path('{}/{}'.format(path, child.name))

    def folders_create(self, name):
        self._connection.data_service.create_folder(name, self.details['kind'], self.id)

    def files_upload(self, name, path):

        #self._connection.data_service.create_upload()
        #project_id, filename, content_type, size,
        #              hash_value, hash_alg
        pass
#    def upload_from_path(self, path):
#        self.upload_from_path(path, ParentData(details['kind']))


class Folder(object):
    def __init__(self, connection, details):
        self._connection = connection
        self.id = details['id']
        self.name = details['name']
        self.details = details

    def children_list(self):
        response = self._connection.data_service.get_folder_children(self.id, name_contains=None)
        return response_to_object_list(self._connection, response, create_file_or_folder)

    def delete(self):
        self._connection.data_service.delete_folder(self.id)

    def download_to_path(self, path):
        os.mkdir(path)
        for child in self.children_list():
            child.download_to_path('{}/{}'.format(path, child.name))

    def folders_create(self, name):
        self._connection.data_service.create_folder(name, self.details['kind'], self.id)


class File(object):
    def __init__(self, connection, details):
        self._connection = connection
        self.id = details['id']
        self.name = details['name']
        self.details = details

    def delete(self):
        self._connection.data_service.delete_file(self.id)

    def download_content(self):
        url_details = self._connection.data_service.get_file_url(self.id).json()
        return self._connection.data_service.receive_external(
            url_details['http_verb'],
            url_details['host'],
            url_details['url'],
            url_details['http_headers'])

    def download_to_path(self, path):
        response = self.download_content()
        with open(path, 'wb') as f:
            for chunk in response.iter_content():
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)

    @staticmethod
    def upload_to_parent(path, parent_data, data_service, upload_context, remote_id=None):
        chunk_num = 1
        path_data = PathData(path)
        chunk = path_data.read_whole_file()
        hash_data = path_data.get_hash()
        upload_operations = FileUploadOperations(data_service, upload_context)
        upload_id = upload_operations.create_upload(upload_context.project_id, path_data, hash_data)
        url_info = upload_operations.create_file_chunk_url(upload_id, chunk_num, chunk)
        upload_operations.send_file_external(url_info, chunk)
        return upload_operations.finish_upload(upload_id, hash_data, parent_data, remote_id)
