from __future__ import absolute_import
import logging
from ddsc.sdk.client import Client, FileUpload, PathToFiles, ItemNotFound, DuplicateNameError
from ddsc.config import create_config
from ddsc.core.userutil import UserUtil


class DukeDS(object):
    """
    Contains methods for interacting with DukeDS using standard ddsclient config file and environment variables
    """
    @staticmethod
    def list_projects():
        """
        Return a list of project names
        :return: [str]: list of project names
        """
        return Session().list_projects()

    @staticmethod
    def create_project(name, description):
        """
        Create a project with the specified name and description
        :param name: str: unique name for this project
        :param description: str: long description of this project
        :return: str: name of the project
        """
        return Session().create_project(name, description)

    @staticmethod
    def delete_project(project_name):
        """
        Delete a project with the specified name. Raises ItemNotFound if no such project exists
        :param project_name: str: name of the project to delete
        :return:
        """
        Session().delete_project(project_name)

    @staticmethod
    def list_files(project_name):
        """
        Return a list of file paths that make up project_name
        :param project_name: str: specifies the name of the project to list contents of
        :return: [str]: returns a list of remote paths for all files part of the specified project qq
        """
        return Session().list_files(project_name)

    @staticmethod
    def download_file(project_name, remote_path, local_path=None):
        """
        Download a file from a project
        When local_path is None the file will be downloaded to the base filename
        :param project_name: str: name of the project to download a file from
        :param remote_path: str: remote path specifying which file to download
        :param local_path: str: optional argument to customize where the file will be downloaded to
        """
        Session().download_file(project_name, remote_path, local_path)

    @staticmethod
    def move_file_or_folder(project_name, source_remote_path, target_remote_path):
        """
        Move a file or folder specified by source_remote_path to target_remote_path.
        This operation is loosely modeled after linux 'mv' command.
        :param project_name: str: name of the project containing the file
        :param source_remote_path: str: remote path specifying the file/folder to be moved
        :param target_remote_path: str: remote path specifying where to move the file/folder to
        """
        Session().move_file_or_folder(project_name, source_remote_path, target_remote_path)

    @staticmethod
    def upload_file(project_name, local_path, remote_path=None):
        """
        Upload a file into project creating a new version if it already exists.
        Will also create project and parent folders if they do not exist.
        :param project_name: str: name of the project to upload a file to
        :param local_path: str: path to download the file into
        :param remote_path: str: remote path specifying file to upload to (defaults to local_path basename)
        """
        return Session().upload_file(project_name, local_path, remote_path)

    @staticmethod
    def delete_file(project_name, remote_path):
        """
        Delete a file or folder from a project
        :param project_name: str: name of the project containing a file we will delete
        :param remote_path: str: remote path specifying file to delete
        """
        Session().delete_file(project_name, remote_path)

    @staticmethod
    def can_deliver_to_user_with_email(email_address, logging_func=logging.info):
        """
        Determine if we can deliver a project to a user
        :param email_address: str: email address to lookup
        :param logging_func: func(str): function that will receive log messages
        :return: boolean: True if the specified user can receive deliveries
        """
        return Session().can_deliver_to_user_with_email(email_address, logging_func)

    @staticmethod
    def can_deliver_to_user_with_username(username, logging_func=logging.info):
        """
        Determine if we can deliver a project to a user
        :param username: str: username to lookup
        :param logging_func: func(str): function that will receive log messages
        :return: boolean: True if the specified user can receive deliveries
        """
        return Session().can_deliver_to_user_with_username(username, logging_func)


class Session(object):
    """
    Contains methods for interacting with DukeDS using standard ddsclient config file and environment variables
    Same functionality as DukeDS but caches project list to improve performance.
    """
    def __init__(self, config=create_config()):
        """
        :param config:  ddsc.config.Config: configuration specifying DukeDS endpoint and credential to use
        """
        self.client = Client(config)
        self.projects = None

    def list_projects(self):
        """
        Return a list of project names
        :return: [str]: list of project names
        """
        self._cache_project_list_once()
        return [project.name for project in self.projects]

    def create_project(self, name, description):
        """
        Create a project with the specified name and description
        :param name: str: unique name for this project
        :param description: str: long description of this project
        :return: str: name of the project
        """
        self._cache_project_list_once()
        if name in [project.name for project in self.projects]:
            raise DuplicateNameError("There is already a project named {}".format(name))
        self.client.create_project(name, description)
        self.clear_project_cache()
        return name

    def delete_project(self, project_name):
        """
        Delete a project with the specified name. Raises ItemNotFound if no such project exists
        :param project_name: str: name of the project to delete
        :return:
        """
        project = self._get_project_for_name(project_name)
        project.delete()
        self.clear_project_cache()

    def list_files(self, project_name):
        """
        Return a list of file paths that make up project_name
        :param project_name: str: specifies the name of the project to list contents of
        :return: [str]: returns a list of remote paths for all files part of the specified project qq
        """
        project = self._get_project_for_name(project_name)
        file_path_dict = self._get_file_path_dict_for_project(project)
        return list(file_path_dict)

    def download_file(self, project_name, remote_path, local_path=None):
        """
        Download a file from a project
        When local_path is None the file will be downloaded to the base filename
        :param project_name: str: name of the project to download a file from
        :param remote_path: str: remote path specifying which file to download
        :param local_path: str: optional argument to customize where the file will be downloaded to
        """
        project = self._get_project_for_name(project_name)
        file = project.get_child_for_path(remote_path)
        file.download_to_path(local_path)

    def upload_file(self, project_name, local_path, remote_path=None):
        """
        Upload a file into project creating a new version if it already exists.
        Will also create project and parent folders if they do not exist.
        :param project_name: str: name of the project to upload a file to
        :param local_path: str: path to download the file into
        :param remote_path: str: remote path specifying file to upload to (defaults to local_path basename)
        """
        project = self._get_or_create_project(project_name)
        file_upload = FileUpload(project, remote_path, local_path)
        file_upload.run()

    def _get_or_create_project(self, project_name):
        try:
            return self._get_project_for_name(project_name)
        except ItemNotFound:
            project_description = project_name
            project = self.client.create_project(project_name, project_description)
            self.clear_project_cache()
            return project

    def _cache_project_list_once(self):
        if not self.projects:
            self.projects = self.client.get_projects()

    def clear_project_cache(self):
        """
        Empty project cache so successive methods will re-fetch the list when it is needed
        """
        self.projects = None

    def _get_project_for_name(self, project_name):
        self._cache_project_list_once()
        projects = [project for project in self.projects if project.name == project_name]
        if not projects:
            raise ItemNotFound("No project found with name {}".format(project_name))
        if len(projects) == 1:
            return projects[0]
        raise DuplicateNameError("Multiple projects found with name {}".format(project_name))

    @staticmethod
    def _get_file_path_dict_for_project(project):
        path_to_nodes = PathToFiles()
        path_to_nodes.add_paths_for_children_of_node(project)
        return path_to_nodes.paths

    def delete_file(self, project_name, remote_path):
        """
        Delete a file or folder from a project
        :param project_name: str: name of the project containing a file we will delete
        :param remote_path: str: remote path specifying file to delete
        """
        project = self._get_or_create_project(project_name)
        remote_file = project.get_child_for_path(remote_path)
        remote_file.delete()

    def can_deliver_to_user_with_email(self, email_address, logging_func):
        data_service = self.client.dds_connection.data_service
        dds_user_util = UserUtil(data_service, logging_func=logging_func)
        return dds_user_util.user_or_affiliate_exists_for_email(email_address)

    def can_deliver_to_user_with_username(self, username, logging_func):
        data_service = self.client.dds_connection.data_service
        dds_user_util = UserUtil(data_service, logging_func=logging_func)
        return dds_user_util.user_or_affiliate_exists_for_username(username)

    def move_file_or_folder(self, project_name, source_remote_path, target_remote_path):
        project = self._get_or_create_project(project_name)
        project.move_file_or_folder(source_remote_path, target_remote_path)
