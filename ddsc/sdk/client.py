# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import os
from ddsc.core.ddsapi import DataServiceAuth, DataServiceApi
from ddsc.config import DUKE_DATA_SERVICE_URL, Config, create_config
from ddsc.core.remotestore import PROJECT_LIST_EXCLUDE_RESPONSE_FIELDS, DOWNLOAD_FILE_CHUNK_SIZE
from ddsc.core.fileuploader import FileUploadOperations, ParallelChunkProcessor, ParentData
from ddsc.core.localstore import PathData
from ddsc.core.util import KindType
from future.utils import python_2_unicode_compatible


class Client(object):
    def __init__(self, config=create_config()):
        self.duke_ds = DukeDS(config)

    def get_projects(self):
        """
        Get list of all projects user has access to.
        :return: [Project]: list of projects
        """
        return self.duke_ds.get_projects()

    def get_project_by_id(self, project_id):
        """
        Retrieve a single project.
        :param project_id:
        :return:
        """
        return self.duke_ds.get_project_by_id(project_id)

    def create_project(self, name, description):
        return self.duke_ds.create_project(name, description)


class DukeDS(object):
    def __init__(self, config):
        self.config = config
        self.data_service = DataServiceApi(DataServiceAuth(config), config.url)

    def _create_array_response(self, resp, array_item_constructor):
        items = resp.json()['results']
        return [array_item_constructor(self, data_dict) for data_dict in items]

    def _create_item_response(self, resp, item_constructor):
        data_dict = resp.json()
        return item_constructor(self, data_dict)

    def get_projects(self):
        return self._create_array_response(
            self.data_service.get_projects(),
            Project)

    def get_project_by_id(self, project_id):
        return self._create_item_response(
            self.data_service.get_project_by_id(project_id),
            Project)

    def create_project(self, name, description):
        return self._create_item_response(
            self.data_service.create_project(name, description),
            Project)

    def delete_project(self, project_id):
        self.data_service.delete_project(project_id)

    def create_folder(self, folder_name, parent_kind_str, parent_uuid):
        return self._create_item_response(
            self.data_service.create_folder(folder_name, parent_kind_str, parent_uuid),
            Folder
        )

    def delete_folder(self, folder_id):
        self.data_service.delete_folder(folder_id)

    def get_project_children(self, project_id, name_contains=''):
        return self._create_array_response(
            self.data_service.get_project_children(
                project_id, name_contains
            ),
            DukeDS._folder_or_file_constructor
        )

    def get_folder_children(self, folder_id, name_contains=''):
        return self._create_array_response(
            self.data_service.get_folder_children(
                folder_id, name_contains
            ),
            DukeDS._folder_or_file_constructor
        )

    def get_file_download(self, file_id):
        return self._create_item_response(
            self.data_service.get_file_url(file_id),
            FileDownload
        )

    def upload_file(self, local_path, project_id, parent_data, existing_file_id=None):
        path_data = PathData(local_path)
        hash_data = path_data.get_hash()
        file_upload_operations = FileUploadOperations(self.data_service, None)
        upload_id = file_upload_operations.create_upload(project_id, path_data, hash_data)
        context = UploadContext(self.config, self.data_service, upload_id, path_data)
        ParallelChunkProcessor(context).run()
        remote_file_data = file_upload_operations.finish_upload(upload_id, hash_data, parent_data, existing_file_id)
        return File(self, remote_file_data)

    @staticmethod
    def _folder_or_file_constructor(duke_ds, data_dict):
        kind = data_dict['kind']
        if kind == KindType.folder_str:
            return Folder(duke_ds, data_dict)
        elif data_dict['kind'] == KindType.file_str:
            return File(duke_ds, data_dict)


class BaseResponseItem(object):
    def __init__(self, duke_ds, data_dict):
        self.duke_ds = duke_ds
        self._data_dict = dict(data_dict)

    def __getattr__(self, key):
        try:
            return self._data_dict[key]
        except KeyError:
            raise AttributeError


@python_2_unicode_compatible
class Project(BaseResponseItem):
    def __init__(self, duke_ds, data):
        super(Project, self).__init__(duke_ds, data)

    def get_children(self):
        return self.duke_ds.get_project_children(self.id)

    def get_child_for_path(self, path):
        child_finder = ChildFinder(path, self)
        return child_finder.get_child()

    def create_folder(self, folder_name):
        return self.duke_ds.create_folder(folder_name, KindType.project_str, self.id)

    def upload_file(self, local_path):
        parent_data = ParentData(self.kind, self.id)
        return self.duke_ds.upload_file(local_path, project_id=self.id, parent_data=parent_data)

    def delete(self):
        self.duke_ds.delete_project(self.id)

    def __str__(self):
        return u'{} id:{} name:{}'.format(self.__class__.__name__, self.id, self.name)


class Folder(BaseResponseItem):
    def __init__(self, duke_ds, data):
        super(Folder, self).__init__(duke_ds, data)
        self.project_id = self.project['id']

    def get_children(self):
        return self.duke_ds.get_folder_children(self.id)

    def create_folder(self, folder_name):
        return self.duke_ds.create_folder(folder_name, KindType.folder_str, self.id)

    def upload_file(self, local_path):
        parent_data = ParentData(self.kind, self.id)
        return self.duke_ds.upload_file(local_path, project_id=self.project_id, parent_data=parent_data)

    def delete(self):
        self.duke_ds.delete_folder(self.id)


class File(BaseResponseItem):
    def __init__(self, duke_ds, data):
        super(File, self).__init__(duke_ds, data)
        self.project_id = self.project['id']

    def download_to_path(self, file_path):
        file_download = self.duke_ds.get_file_download(self.id)
        file_download.save_to_path(file_path)

    def delete(self):
        self.duke_ds.delete_file(self.id)

    def upload_new_version(self, file_path):
        parent_data = ParentData(self.parent['kind'], self.parent['id'])
        return self.duke_ds.upload_file(file_path, project_id=self.project_id, parent_data=parent_data,
                                        existing_file_id=self.id)


class FileDownload(BaseResponseItem):
    def __init__(self, duke_ds, data):
        super(FileDownload, self).__init__(duke_ds, data)

    def _get_download_response(self):
        return self.duke_ds.data_service.receive_external(self.http_verb, self.host, self.url, self.http_headers)

    def save_to_path(self, file_path, chunk_size=DOWNLOAD_FILE_CHUNK_SIZE):
        response = self._get_download_response()
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)


class FileUpload(BaseResponseItem):
    def __init__(self, duke_ds, data):
        super(FileUpload, self).__init__(duke_ds, data)
        print(data)


class ChildFinder(object):
    def __init__(self, remote_path, node):
        self.remote_path = remote_path
        self.node = node

    def get_child(self):
        path_parts = self.remote_path.split(os.sep)
        return self._get_child_recurse(path_parts, self.node)

    def _get_child_recurse(self, path_parts, node):
        if not path_parts:
            return node
        head, tail = path_parts[0], path_parts[1:]
        for child in node.get_children():
            if child.name == head:
                return self._get_child_recurse(tail, child)
        raise ItemNotFound("No item at path {}".format(self.remote_path))


class UploadContext(object):
    def __init__(self, config, data_service, upload_id, path_data):
        self.config = config
        self.data_service = data_service
        self.upload_id = upload_id
        self.watcher = self
        self.local_file = UploadFileInfo(path_data)

    def transferring_item(self, item, increment_amt):
        pass

    def start_waiting(self):
        pass

    def done_waiting(self):
        pass


class UploadFileInfo(object):
    def __init__(self, path_data):
        self.size = path_data.size()
        self.path = path_data.path
        self.kind = KindType.file_str


class ItemNotFound(Exception):
    pass
