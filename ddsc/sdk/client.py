import os
from ddsc.core.ddsapi import DataServiceAuth, DataServiceApi
from ddsc.config import create_config
from ddsc.core.remotestore import DOWNLOAD_FILE_CHUNK_SIZE
from ddsc.core.fileuploader import FileUploadOperations, ParallelChunkProcessor, ParentData
from ddsc.core.localstore import PathData
from ddsc.core.util import KindType
from future.utils import python_2_unicode_compatible


class Client(object):
    """
    Client that connects to the DukeDS base on ~/.ddsclient configuration.
    This configuration can be customized by passing in a ddsc.config.Config object
    """
    def __init__(self, config=create_config()):
        """
        :param config: ddsc.config.Config: settings used to connect to DukeDS
        """
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
        :return: Project
        """
        return self.duke_ds.get_project_by_id(project_id)

    def create_project(self, name, description):
        """
        Create a new project with the specified name and description
        :param name: str: name of the project
        :param description: str: description of the project
        :return: Project
        """
        return self.duke_ds.create_project(name, description)

    def get_folder_by_id(self, folder_id):
        """
        Return details about a folder with the specified uuid
        :param folder_id: str: uuid of the folder to fetch
        :return: Folder
        """
        return self.duke_ds.get_folder_by_id(folder_id)

    def get_file_by_id(self, file_id):
        """
        Return details about a file with the specified uuid
        :param file_id: str: uuid of the file to fetch
        :return: File
        """
        return self.duke_ds.get_file_by_id(file_id)


class DukeDS(object):
    """
    Contains methods for accessing various DukeDS API functionality
    """
    def __init__(self, config):
        """
        :param config: ddsc.config.Config: settings used to connect to DukeDS
        """
        self.config = config
        self.data_service = DataServiceApi(DataServiceAuth(config), config.url)

    def _create_array_response(self, resp, array_item_constructor):
        items = resp.json()['results']
        return [array_item_constructor(self, data_dict) for data_dict in items]

    def _create_item_response(self, resp, item_constructor):
        data_dict = resp.json()
        return item_constructor(self, data_dict)

    def get_projects(self):
        """
        Get details for all projects you have access to in DukeDS
        :return: [Project]: list of projects
        """
        return self._create_array_response(
            self.data_service.get_projects(),
            Project)

    def get_project_by_id(self, project_id):
        """
        Get details about project with the specified uuid
        :param project_id: str: uuid of the project to fetch
        :return: Project
        """
        return self._create_item_response(
            self.data_service.get_project_by_id(project_id),
            Project)

    def create_project(self, name, description):
        """
        Create a new project with the specified name and description
        :param name: str: name of the project to create
        :param description: str: description of the project to create
        :return: Project
        """
        return self._create_item_response(
            self.data_service.create_project(name, description),
            Project)

    def delete_project(self, project_id):
        """
        Delete the project with the specified uuid
        :param project_id: str: uuid of the project to delete
        """
        self.data_service.delete_project(project_id)

    def create_folder(self, folder_name, parent_kind_str, parent_uuid):
        """
        Create a folder under a particular parent
        :param folder_name: str: name of the folder to create
        :param parent_kind_str: str: kind of the parent of this folder
        :param parent_uuid: str: uuid of the parent of this folder (project or another folder)
        :return: Folder: folder metadata
        """
        return self._create_item_response(
            self.data_service.create_folder(folder_name, parent_kind_str, parent_uuid),
            Folder
        )

    def delete_folder(self, folder_id):
        """
        Delete the folder with the specified uuid
        :param folder_id: str: uuid of the folder to delete
        """
        self.data_service.delete_folder(folder_id)

    def get_project_children(self, project_id, name_contains=''):
        """
        Get direct files and folders of a project.
        :param project_id: str: uuid of the project to list contents
        :param name_contains: str: filter children based on a pattern
        :return: [File|Folder]: list of Files/Folders contained by the project
        """
        return self._create_array_response(
            self.data_service.get_project_children(
                project_id, name_contains
            ),
            DukeDS._folder_or_file_constructor
        )

    def get_folder_children(self, folder_id, name_contains=''):
        """
        Get direct files and folders of a folder.
        :param folder_id: str: uuid of the folder
        :param name_contains: str: filter children based on a pattern
        :return: File|Folder
        """
        return self._create_array_response(
            self.data_service.get_folder_children(
                folder_id, name_contains
            ),
            DukeDS._folder_or_file_constructor
        )

    def get_file_download(self, file_id):
        """
        Get a file download object that contains temporary url settings needed to download the contents of a file.
        :param file_id: str: uuid of the file
        :return: FileDownload
        """
        return self._create_item_response(
            self.data_service.get_file_url(file_id),
            FileDownload
        )

    def upload_file(self, local_path, project_id, parent_data, existing_file_id=None):
        """
        Upload a file under a specific location in DukeDS possibly replacing an existing file.
        :param local_path: str: path to a local file to upload
        :param project_id: str: uuid of the project to add this file to
        :param parent_data: ParentData: info about the parent of this file
        :param existing_file_id: str: uuid of file to create a new version of (or None to create a new file)
        :return: File
        """
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
        """
        Create a File or Folder based on the kind value in data_dict
        :param duke_ds: DukeDS
        :param data_dict: dict: payload received from DukeDS API
        :return: File|Folder
        """
        kind = data_dict['kind']
        if kind == KindType.folder_str:
            return Folder(duke_ds, data_dict)
        elif data_dict['kind'] == KindType.file_str:
            return File(duke_ds, data_dict)

    def get_folder_by_id(self, folder_id):
        """
        Get folder details for a folder id.
        :param folder_id: str: uuid of the folder
        :return: Folder
        """
        return self._create_item_response(
            self.data_service.get_folder(folder_id),
            Folder
        )

    def get_file_by_id(self, file_id):
        """
        Get folder details for a file id.
        :param file_id: str: uuid of the file
        :return: File
        """
        return self._create_item_response(
            self.data_service.get_file(file_id),
            File
        )


class BaseResponseItem(object):
    """
    Base class for responses from DukeDS API converts dict into properties for subclasses.
    """
    def __init__(self, duke_ds, data_dict):
        """
        :param duke_ds: DukeDS
        :param data_dict: dict: dictionary response from DukeDS API
        """
        self.duke_ds = duke_ds
        self._data_dict = dict(data_dict)

    def __getattr__(self, key):
        """
        Return property from the dictionary passed to the constructor.
        """
        try:
            return self._data_dict[key]
        except KeyError:
            msg = "'{}' object has no attribute '{}'".format(self.__class__.__name__, key)
            raise AttributeError(msg)


@python_2_unicode_compatible
class Project(BaseResponseItem):
    """
    Contains project details based on DukeDS API response
    """
    def __init__(self, duke_ds, data):
        """
        :param duke_ds: DukeDS
        :param data: dict: dictionary response from DukeDS API in project format
        """
        super(Project, self).__init__(duke_ds, data)

    def get_children(self):
        """
        Fetch the direct children of this project.
        :return: [File|Folder]
        """
        return self.duke_ds.get_project_children(self.id)

    def get_child_for_path(self, path):
        """
        Based on a remote path get a single remote child.
        :param path: str: path within a project specifying a file or folder to download
        :return: File|Folder
        """
        child_finder = ChildFinder(path, self)
        return child_finder.get_child()

    def create_folder(self, folder_name):
        """
        Create a new folder as a top level child of this project.
        :param folder_name: str: name of the folder to create
        :return: Folder
        """
        return self.duke_ds.create_folder(folder_name, KindType.project_str, self.id)

    def upload_file(self, local_path):
        """
        Upload a new file based on a file on the file system as a top level child of this project.
        :param local_path: str: path to a file to upload
        :return: File
        """
        parent_data = ParentData(self.kind, self.id)
        return self.duke_ds.upload_file(local_path, project_id=self.id, parent_data=parent_data)

    def delete(self):
        """
        Delete this project and it's children.
        """
        self.duke_ds.delete_project(self.id)

    def __str__(self):
        return u'{} id:{} name:{}'.format(self.__class__.__name__, self.id, self.name)


@python_2_unicode_compatible
class Folder(BaseResponseItem):
    """
    Contains folder details based on DukeDS API response
    """
    def __init__(self, duke_ds, data):
        """
        :param duke_ds: DukeDS
        :param data: dict: dictionary response from DukeDS API in folder format
        """
        super(Folder, self).__init__(duke_ds, data)
        self.project_id = self.project['id']

    def get_children(self):
        """
        Fetch the direct children of this folder.
        :return: [File|Folder]
        """
        return self.duke_ds.get_folder_children(self.id)

    def create_folder(self, folder_name):
        """
        Create a new folder as a top level child of this folder.
        :param folder_name: str: name of the folder to create
        :return: Folder
        """
        return self.duke_ds.create_folder(folder_name, KindType.folder_str, self.id)

    def upload_file(self, local_path):
        """
        Upload a new file based on a file on the file system as a top level child of this folder.
        :param local_path: str: path to a file to upload
        :return: File
        """
        parent_data = ParentData(self.kind, self.id)
        return self.duke_ds.upload_file(local_path, project_id=self.project_id, parent_data=parent_data)

    def delete(self):
        """
        Delete this folder and it's children.
        """
        self.duke_ds.delete_folder(self.id)

    def __str__(self):
        return u'{} id:{} name:{}'.format(self.__class__.__name__, self.id, self.name)


@python_2_unicode_compatible
class File(BaseResponseItem):
    """
    Contains folder details based on DukeDS API response
    """
    def __init__(self, duke_ds, data):
        """
        :param duke_ds: DukeDS
        :param data: dict: dictionary response from DukeDS API in folder format
        """
        super(File, self).__init__(duke_ds, data)
        self.project_id = self.project['id']

    def download_to_path(self, file_path):
        """
        Download the contents of this file to a local file path
        :param file_path: str: local filesystem path to write this file contents into
        """
        file_download = self.duke_ds.get_file_download(self.id)
        file_download.save_to_path(file_path)

    def delete(self):
        """
        Delete this file and it's children.
        """
        self.duke_ds.delete_file(self.id)

    def upload_new_version(self, file_path):
        """
        Upload a new version of this file.
        :param file_path: str: local filesystem path to write this file contents into
        :return: File
        """
        parent_data = ParentData(self.parent['kind'], self.parent['id'])
        return self.duke_ds.upload_file(file_path, project_id=self.project_id, parent_data=parent_data,
                                        existing_file_id=self.id)

    def __str__(self):
        return u'{} id:{} name:{}'.format(self.__class__.__name__, self.id, self.name)


class FileDownload(BaseResponseItem):
    """
    Contains file download url details based on DukeDS API response
    """
    def __init__(self, duke_ds, data):
        """
        :param duke_ds: DukeDS
        :param data: dict: dictionary response from DukeDS API in file download url format
        """
        super(FileDownload, self).__init__(duke_ds, data)

    def _get_download_response(self):
        return self.duke_ds.data_service.receive_external(self.http_verb, self.host, self.url, self.http_headers)

    def save_to_path(self, file_path, chunk_size=DOWNLOAD_FILE_CHUNK_SIZE):
        """
        Save the contents of the remote file to a local path.
        :param file_path: str: file path
        :param chunk_size: chunk size used to write local file
        """
        response = self._get_download_response()
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)


class ChildFinder(object):
    """
    Recursively looks for a child based on a path
    """
    def __init__(self, remote_path, node):
        """
        :param remote_path: path under a project in DukeDS
        :param node: Project|Folder to find children under
        """
        self.remote_path = remote_path
        self.node = node

    def get_child(self):
        """
        Find file or folder at the remote_path
        :return: File|Folder
        """
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
    """
    Contains settings and monitoring methods used while uploading a file.
    """
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
    """
    Settings about a file being uploaded
    """
    def __init__(self, path_data):
        """
        :param path_data: PathData
        """
        self.size = path_data.size()
        self.path = path_data.path
        self.kind = KindType.file_str


class ItemNotFound(Exception):
    pass
