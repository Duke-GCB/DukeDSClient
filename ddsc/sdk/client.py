import os
from collections import OrderedDict
from ddsc.core.ddsapi import DataServiceAuth, DataServiceApi
from ddsc.config import create_config
from ddsc.core.remotestore import DOWNLOAD_FILE_CHUNK_SIZE, RemoteFile, ProjectFile, RemotePath
from ddsc.core.fileuploader import FileUploadOperations, ParallelChunkProcessor, ParentData
from ddsc.core.localstore import PathData
from ddsc.core.download import FileDownloadState, download_file
from ddsc.core.util import KindType, REMOTE_PATH_SEP, humanize_bytes, plural_fmt
from ddsc.core.moveutil import MoveUtil
from ddsc.exceptions import DDSUserException
from future.utils import python_2_unicode_compatible
from ddsc.core.consistency import ProjectChecker


class Client(object):
    """
    Client that connects to the DDSConnection base on ~/.ddsclient configuration.
    This configuration can be customized by passing in a ddsc.config.Config object
    """
    def __init__(self, config=None, create_data_service_auth=DataServiceAuth):
        """
        :param config: ddsc.config.Config: settings used to connect to DDSConnection
        :param create_data_service_auth: func(config): function used to create a DataServiceAuth object
        """
        if not config:
            config = create_config()
        self.dds_connection = DDSConnection(config, create_data_service_auth)

    def get_projects(self):
        """
        Get list of all projects user has access to.
        :return: [Project]: list of projects
        """
        return self.dds_connection.get_projects()

    def get_project_by_id(self, project_id):
        """
        Retrieve a single project.
        :param project_id:
        :return: Project
        """
        return self.dds_connection.get_project_by_id(project_id)

    def get_project_by_name(self, project_name):
        """
        Retrieve a single project.
        :param project_name: name of the project to get.
        :return: Project
        """
        projects = [project for project in self.get_projects() if project.name == project_name]
        if not projects:
            raise ItemNotFound("No project named {} found.".format(project_name))
        if len(projects) != 1:
            raise DDSUserException("Multiple projects found with name {}.".format(project_name))
        return projects[0]

    def create_project(self, name, description):
        """
        Create a new project with the specified name and description
        :param name: str: name of the project
        :param description: str: description of the project
        :return: Project
        """
        return self.dds_connection.create_project(name, description)

    def get_folder_by_id(self, folder_id):
        """
        Return details about a folder with the specified uuid
        :param folder_id: str: uuid of the folder to fetch
        :return: Folder
        """
        return self.dds_connection.get_folder_by_id(folder_id)

    def get_file_by_id(self, file_id):
        """
        Return details about a file with the specified uuid
        :param file_id: str: uuid of the file to fetch
        :return: File
        """
        return self.dds_connection.get_file_by_id(file_id)

    def close(self):
        self.dds_connection.close()


class DDSConnection(object):
    """
    Contains methods for accessing various DDSConnection API functionality
    """
    def __init__(self, config, create_data_service_auth=DataServiceAuth):
        """
        :param config: ddsc.config.Config: settings used to connect to DDSConnection
        :param create_data_service_auth: func(config): function used to create a DataServiceAuth object
        """
        self.config = config
        self.data_service = DataServiceApi(create_data_service_auth(config), config.url)

    def close(self):
        self.data_service.close()

    def _create_array_response(self, resp, array_item_constructor):
        items = resp.json()['results']
        return [array_item_constructor(self, data_dict) for data_dict in items]

    def _create_item_response(self, resp, item_constructor):
        data_dict = resp.json()
        return item_constructor(self, data_dict)

    def get_projects(self):
        """
        Get details for all projects you have access to in DDSConnection
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

    def get_project_children(self, project_id, name_contains=None):
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
            DDSConnection._folder_or_file_constructor
        )

    def get_folder_children(self, folder_id, name_contains=None):
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
            DDSConnection._folder_or_file_constructor
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

    def upload_file(self, local_path, project_id, parent_data, existing_file_id=None, remote_filename=None):
        """
        Upload a file under a specific location in DDSConnection possibly replacing an existing file.
        :param local_path: str: path to a local file to upload
        :param project_id: str: uuid of the project to add this file to
        :param parent_data: ParentData: info about the parent of this file
        :param existing_file_id: str: uuid of file to create a new version of (or None to create a new file)
        :param remote_filename: str: name to use for our remote file (defaults to local_path basename otherwise)
        :return: File
        """
        path_data = PathData(local_path)
        hash_data = path_data.get_hash()
        file_upload_operations = FileUploadOperations(self.data_service, None)
        upload_id = file_upload_operations.create_upload(project_id, path_data, hash_data,
                                                         remote_filename=remote_filename,
                                                         storage_provider_id=self.config.storage_provider_id)
        context = UploadContext(self.config, self.data_service, upload_id, path_data)
        ParallelChunkProcessor(context).run()
        remote_file_data = file_upload_operations.finish_upload(upload_id, hash_data, parent_data, existing_file_id)
        return File(self, remote_file_data)

    @staticmethod
    def _folder_or_file_constructor(dds_connection, data_dict):
        """
        Create a File or Folder based on the kind value in data_dict
        :param dds_connection: DDSConnection
        :param data_dict: dict: payload received from DDSConnection API
        :return: File|Folder
        """
        kind = data_dict['kind']
        if kind == KindType.folder_str:
            return Folder(dds_connection, data_dict)
        elif data_dict['kind'] == KindType.file_str:
            return File(dds_connection, data_dict)

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

    def delete_file(self, file_id):
        self.data_service.delete_file(file_id)

    def rename_folder(self, folder_id, name):
        return self._create_item_response(
            self.data_service.rename_folder(folder_id, name),
            Folder
        )

    def move_folder(self, folder_id, parent_kind_str, parent_uuid):
        return self._create_item_response(
            self.data_service.move_folder(folder_id, parent_kind_str, parent_uuid),
            Folder
        )

    def rename_file(self, file_id, name):
        return self._create_item_response(
            self.data_service.rename_file(file_id, name),
            File
        )

    def move_file(self, file_id, parent_kind_str, parent_uuid):
        return self._create_item_response(
            self.data_service.move_file(file_id, parent_kind_str, parent_uuid),
            File
        )

    def get_project_files_generator(self, project_id, page_size):
        for project_file_dict, header_metadata in self.data_service.get_project_files_generator(project_id, page_size):
            yield ProjectFile(project_file_dict), header_metadata

    def get_file_url_dict(self, file_id):
        return self.data_service.get_file_url(file_id).json()

    def get_upload(self, upload_id):
        return self._create_item_response(
            self.data_service.get_upload(
                upload_id
            ),
            Upload
        )


class BaseResponseItem(object):
    """
    Base class for responses from DDSConnection API converts dict into properties for subclasses.
    """
    def __init__(self, dds_connection, data_dict):
        """
        :param dds_connection: DDSConnection
        :param data_dict: dict: dictionary response from DDSConnection API
        """
        self.dds_connection = dds_connection
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
    Contains project details based on DDSConnection API response
    """
    def __init__(self, dds_connection, data):
        """
        :param dds_connection: DDSConnection
        :param data: dict: dictionary response from DDSConnection API in project format
        """
        super(Project, self).__init__(dds_connection, data)

    def get_children(self):
        """
        Fetch the direct children of this project.
        :return: [File|Folder]
        """
        return self.dds_connection.get_project_children(self.id)

    def get_child_for_path(self, path):
        """
        Based on a remote path get a single remote child. When not found raises ItemNotFound.
        :param path: str: path within a project specifying a file or folder to download
        :return: File|Folder
        """
        return ChildFinder.get_child_for_path(self, path)

    def try_get_item_for_path(self, path):
        """
        Based on a remote path get a single remote child. When not found returns None.
        :param path: str: path within a project specifying a file or folder to download
        :return: File|Folder|Project|None
        """
        return ChildFinder.try_get_item_for_path(self, path)

    def create_folder(self, folder_name):
        """
        Create a new folder as a top level child of this project.
        :param folder_name: str: name of the folder to create
        :return: Folder
        """
        return self.dds_connection.create_folder(folder_name, KindType.project_str, self.id)

    def upload_file(self, local_path, remote_filename=None):
        """
        Upload a new file based on a file on the file system as a top level child of this project.
        :param local_path: str: path to a file to upload
        :param remote_filename: str: name to use for our remote file (defaults to local_path basename otherwise)
        :return: File
        """
        parent_data = ParentData(self.kind, self.id)
        return self.dds_connection.upload_file(local_path, project_id=self.id, parent_data=parent_data,
                                               remote_filename=remote_filename)

    def move_file_or_folder(self, source_remote_path, target_remote_path):
        """
        Move a file or folder specified by source_remote_path to target_remote_path.
        This operation is loosely modeled after linux 'mv' command.
        :param source_remote_path: str: remote path specifying the file/folder to be moved
        :param target_remote_path: str: remote path specifying where to move the file/folder to
        :return File|Folder: moved item with updated data
        """
        move_util = MoveUtil(self, source_remote_path, target_remote_path)
        return move_util.run()

    def delete(self):
        """
        Delete this project and it's children.
        """
        self.dds_connection.delete_project(self.id)

    def get_summary(self):
        return ProjectSummary(self)

    def portal_url(self):
        return self.dds_connection.data_service.portal_url(self.id)

    def get_project_files_generator(self, page_size):
        return self.dds_connection.get_project_files_generator(self.id, page_size)

    def get_path_to_files(self):
        path_to_nodes = PathToFiles()
        path_to_nodes.add_paths_for_children_of_node(self)
        return path_to_nodes.paths

    def is_consistent(self):
        """
        Check that a project is in a consistent state. When large projects are uploaded the DukeDS service can take
        a while finish background processing. Before deleting uploaded files you should check that the project is in
        a consistent state. If there is a problem with the project a message will be printed.
        :return: bool: True if the project is consistent.
        """
        checker = ProjectChecker(self.dds_connection.config, self)
        if checker.files_are_ok():
            return True
        else:
            print("Project {} is not in a consistent state!\n".format(self.name))
            checker.print_bad_uploads_table()
            return False

    def wait_for_consistency(self, wait_sec=5):
        """
        Wait for the project to become consistent.
        :param wait_sec: Seconds to wait before giving up.
        """
        checker = ProjectChecker(self.dds_connection.config, self)
        checker.wait_for_consistency(wait_sec=5)

    def __str__(self):
        return u'{} id:{} name:{}'.format(self.__class__.__name__, self.id, self.name)


class ProjectSummary(object):
    def __init__(self, project):
        self.total_size = 0
        self.file_count = 0
        self.folder_count = 0
        self.root_folder_count = 0
        self._recurse_children(project)
        self.sub_folder_count = self.folder_count - self.root_folder_count

    def _recurse_children(self, node):
        parent_is_project = node.kind == KindType.project_str
        for child in node.get_children():
            if child.kind == KindType.file_str:
                self._process_file(child)
            elif child.kind == KindType.folder_str:
                self._process_folder(parent_is_project)
                self._recurse_children(child)

    def _process_file(self, node):
        self.file_count += 1
        self.total_size += node.current_size()

    def _process_folder(self, parent_is_project):
        self.folder_count += 1
        if parent_is_project:
            self.root_folder_count += 1

    def __str__(self):
        parts = []
        if self.folder_count:
            parts.append(plural_fmt("top level folder", self.root_folder_count))
            parts.append(plural_fmt("subfolder", self.sub_folder_count))
        else:
            parts.append(plural_fmt("folder", self.folder_count))
        files_str = plural_fmt("file", self.file_count)
        files_str += " ({})".format(humanize_bytes(self.total_size))
        parts.append(files_str)
        return ", ".join(parts)


@python_2_unicode_compatible
class Folder(BaseResponseItem):
    """
    Contains folder details based on DDSConnection API response
    """
    def __init__(self, dds_connection, data):
        """
        :param dds_connection: DDSConnection
        :param data: dict: dictionary response from DDSConnection API in folder format
        """
        super(Folder, self).__init__(dds_connection, data)
        self.project_id = self.project['id']

    def get_children(self):
        """
        Fetch the direct children of this folder.
        :return: [File|Folder]
        """
        return self.dds_connection.get_folder_children(self.id)

    def create_folder(self, folder_name):
        """
        Create a new folder as a top level child of this folder.
        :param folder_name: str: name of the folder to create
        :return: Folder
        """
        return self.dds_connection.create_folder(folder_name, KindType.folder_str, self.id)

    def get_child_for_path(self, path):
        """
        Based on a remote path get a single remote child. When not found raises ItemNotFound.
        :param path: str: path within a project specifying a file or folder to download
        :return: File|Folder
        """
        return ChildFinder.get_child_for_path(self, path)

    def try_get_item_for_path(self, path):
        """
        Based on a remote path get a single remote child. When not found returns None.
        :param path: str: path within a project specifying a file or folder to download
        :return: File|Folder|Project|None
        """
        return ChildFinder.try_get_item_for_path(self, path)

    def upload_file(self, local_path, remote_filename=None):
        """
        Upload a new file based on a file on the file system as a top level child of this folder.
        :param local_path: str: path to a file to upload
        :param remote_filename: str: name to use for our remote file (defaults to local_path basename otherwise)
        :return: File
        """
        parent_data = ParentData(self.kind, self.id)
        return self.dds_connection.upload_file(local_path, project_id=self.project_id, parent_data=parent_data,
                                               remote_filename=remote_filename)

    def delete(self):
        """
        Delete this folder and it's children.
        """
        self.dds_connection.delete_folder(self.id)

    def rename(self, name):
        return self.dds_connection.rename_folder(self.id, name)

    def change_parent(self, parent):
        return self.dds_connection.move_folder(self.id, parent.kind, parent.id)

    def __str__(self):
        return u'{} id:{} name:{}'.format(self.__class__.__name__, self.id, self.name)


@python_2_unicode_compatible
class File(BaseResponseItem):
    """
    Contains folder details based on DDSConnection API response
    """
    def __init__(self, dds_connection, data):
        """
        :param dds_connection: DDSConnection
        :param data: dict: dictionary response from DDSConnection API in folder format
        """
        super(File, self).__init__(dds_connection, data)
        self.project_id = self.project['id']

    def download_to_path(self, file_path):
        """
        Download the contents of this file to a local file path
        :param file_path: str: local filesystem path to write this file contents into, if none it will default to self.name
        """
        path = file_path
        if not path:
            path = self.name
        project_file_dict = dict(self._data_dict)
        project_file_dict['file_url'] = self.dds_connection.get_file_url_dict(self.id)
        project_file = ProjectFile.create_for_dds_file_dict(project_file_dict)
        download_file_state = download_file(FileDownloadState(project_file, path, self.dds_connection.config))
        download_file_state.raise_for_status()

    def delete(self):
        """
        Delete this file and it's children.
        """
        self.dds_connection.delete_file(self.id)

    def upload_new_version(self, file_path):
        """
        Upload a new version of this file.
        :param file_path: str: local filesystem path to write this file contents into
        :return: File
        """
        parent_data = ParentData(self.parent['kind'], self.parent['id'])
        return self.dds_connection.upload_file(file_path, project_id=self.project_id, parent_data=parent_data,
                                               existing_file_id=self.id)

    def get_hash(self):
        return RemoteFile.get_hash_from_upload(self.current_version["upload"])

    def rename(self, name):
        return self.dds_connection.rename_file(self.id, name)

    def change_parent(self, parent):
        return self.dds_connection.move_file(self.id, parent.kind, parent.id)

    def current_size(self):
        return self.current_version['upload']['size']

    def get_upload(self):
        return self.dds_connection.get_upload(self.current_version["upload"]["id"])

    def __str__(self):
        return u'{} id:{} name:{}'.format(self.__class__.__name__, self.id, self.name)


@python_2_unicode_compatible
class Upload(BaseResponseItem):
    """
    Contains upload details based on DDSConnection API response
    """
    def __init__(self, dds_connection, data):
        """
        :param dds_connection: DDSConnection
        :param data: dict: dictionary response from DDSConnection API in upload format
        """
        super(Upload, self).__init__(dds_connection, data)
        self.status = UploadStatus(data["status"])

    def __str__(self):
        return u'{} id:{} name:{}'.format(self.__class__.__name__, self.id, self.name)


class UploadStatus(object):
    def __init__(self, data):
        self.initiated_on = data["initiated_on"]
        self.completed_on = data["completed_on"]
        self.is_consistent = data["is_consistent"]
        self.purged_on = data["purged_on"]
        self.error_on = data["error_on"]
        self.error_message = data["error_message"]


class FileDownload(BaseResponseItem):
    """
    Contains file download url details based on DDSConnection API response
    """
    def __init__(self, dds_connection, data):
        """
        :param dds_connection: DDSConnection
        :param data: dict: dictionary response from DDSConnection API in file download url format
        """
        super(FileDownload, self).__init__(dds_connection, data)

    def _get_download_response(self):
        return self.dds_connection.data_service.receive_external(self.http_verb, self.host, self.url, self.http_headers)

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


class FileUpload(object):
    def __init__(self, project, remote_path, local_path):
        self.project = project
        self.remote_path = remote_path
        if not self.remote_path:
            self.remote_path = os.path.basename(local_path)
        self.local_path = local_path

    def run(self):
        parts = RemotePath.split(self.remote_path)
        if len(parts) == 1:
            self._upload_to_parent(self.project)
        else:
            folder_names = parts[:-1]
            parent = self.project
            for folder_name in folder_names:
                folder = self._try_get_child(parent, folder_name)
                if not folder:
                    folder = parent.create_folder(folder_name)
                parent = folder
            self._upload_to_parent(parent)

    def _upload_to_parent(self, parent):
        remote_filename = os.path.basename(self.remote_path)
        child = self._try_get_child(parent, remote_filename)
        if child:
            child.upload_new_version(self.local_path)
        else:
            parent.upload_file(self.local_path, remote_filename=remote_filename)

    @staticmethod
    def _try_get_child(parent, child_name):
        for child in parent.get_children():
            if child.name == child_name:
                return child
        return None


class ChildFinder(object):
    """
    Recursively looks for a child based on a path
    """
    def __init__(self, node, remote_path):
        """
        :param node: Project|Folder to find children under
        :param remote_path: path under a project in DDSConnection
        """
        self.node = node
        self.remote_path = remote_path

    def get_child(self):
        """
        Find file or folder at the remote_path
        :return: File|Folder
        """
        path_parts = RemotePath.split(self.remote_path)
        return self._get_child_recurse(path_parts, self.node)

    def _get_child_recurse(self, path_parts, node):
        if not path_parts:
            return node
        head, tail = path_parts[0], path_parts[1:]
        for child in node.get_children():
            if child.name == head:
                return self._get_child_recurse(tail, child)
        raise ItemNotFound("No item at path {}".format(self.remote_path))

    @staticmethod
    def get_child_for_path(node, path):
        """
        Based on a remote path get a single remote child. When not found raises ItemNotFound.
        :param path: str: path within a project specifying a file or folder to download
        :return: File|Folder
        """
        child_finder = ChildFinder(node, path)
        return child_finder.get_child()

    @staticmethod
    def try_get_item_for_path(node, path):
        """
        Based on a remote path get a single remote child. When not found returns None.
        :param path: str: path within a project specifying a file or folder to download
        :return: File|Folder|Project|None
        """
        try:
            if path == REMOTE_PATH_SEP:
                return node
            return ChildFinder.get_child_for_path(node, path)
        except ItemNotFound:
            return None


class PathToFiles(object):
    def __init__(self):
        self.paths = OrderedDict()

    def add_paths_for_children_of_node(self, node):
        self._child_recurse(node, REMOTE_PATH_SEP)

    def _child_recurse(self, node, parent_path):
        for child in node.get_children():
            path = self._make_path(parent_path, child)
            if child.kind == KindType.file_str:
                self.paths[path] = child
            else:
                self._child_recurse(child, path)

    @staticmethod
    def _make_path(parent_path, child):
        if parent_path:
            return os.path.join(parent_path, child.name)
        else:
            return child.name


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

    def transferring_item(self, item, increment_amt, transferred_bytes=0):
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


class ItemNotFound(DDSUserException):
    pass


class DuplicateNameError(DDSUserException):
    pass
