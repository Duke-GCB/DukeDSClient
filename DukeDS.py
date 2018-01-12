"""
Contains methods for interacting with DukeDS using standard ddsclient config file and environment variables
"""
from ddsc.sdk.session import Session


def list_projects():
    """
    Return a list of project names
    :return: [str]: list of project names
    """
    return Session().list_projects()


def create_project(name, description):
    """
    Create a project with the specified name and description
    :param name: str: unique name for this project
    :param description: str: long description of this project
    :return: str: name of the project
    """
    return Session().create_project(name, description)


def delete_project(project_name):
    """
    Delete a project with the specified name. Raises ItemNotFound if no such project exists
    :param project_name: str: name of the project to delete
    :return:
    """
    Session().delete_project(project_name)


def list_files(project_name):
    """
    Return a list of file paths that make up project_name
    :param project_name: str: specifies the name of the project to list contents of
    :return: [str]: returns a list of remote paths for all files part of the specified project qq
    """
    return Session().list_files(project_name)


def download_file(project_name, remote_path, local_path=None):
    """
    Download a file from a project
    When local_path is None the file will be downloaded to the base filename
    :param project_name: str: name of the project to download a file from
    :param remote_path: str: remote path specifying which file to download
    :param local_path: str: optional argument to customize where the file will be downloaded to
    """
    Session().download_file(project_name, remote_path, local_path)


def upload_file(local_path, project_name, remote_path=None):
    """
    Upload a file into project creating a new version if it already exists.
    Will also create project and parent folders if they do not exist.
    :param local_path: str: path to download the file into
    :param project_name: str: name of the project to upload a file to
    :param remote_path: str: remote path specifying file to upload to (defaults to local_path basename)
    """
    return Session().upload_file(local_path, project_name, remote_path)


def delete_file(project_name, remote_path):
    """
    Delete a file or folder from a project
    :param project_name: str: name of the project containing a file we will delete
    :param remote_path: str: remote path specifying file to delete
    """
    Session().delete_file(project_name, remote_path)
