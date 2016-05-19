import os

from ddsc.core.util import ProgressPrinter, ProjectWalker
from ddsc.core.filedownloader import FileDownloader


class ProjectDownload(object):
    """
    Creates local version of remote content.
    """
    def __init__(self, remote_store, project_name, dest_directory):
        self.remote_store = remote_store
        self.project_name = project_name
        self.dest_directory = dest_directory
        self.watcher = None
        self.id_to_path = {}

    def run(self):
        """
        Download the contents of the specified project_name to dest_directory.
        """
        remote_project = self.remote_store.fetch_remote_project(self.project_name, must_exist=True)
        self.walk_project(remote_project)

    def walk_project(self, project):
        """
        For each project, folder, and files send to remote store if necessary.
        :param project: LocalProject project who's contents we want to walk/send.
        """
        # This method will call visit_project, visit_folder, and visit_file below as it walks the project tree.
        counter = RemoteContentCounter(project)
        counter.run()
        self.watcher = ProgressPrinter(counter.count, msg_verb='downloading')
        ProjectWalker.walk_project(project, self)
        self.watcher.finished()

    def try_create_dir(self, path):
        """
        Try to create a directory if it doesn't exist and raise error if there is a non-directory with the same name.
        :param path: str path to the directory
        """
        if not os.path.exists(path):
            os.mkdir(path)
        elif not os.path.isdir(path):
            ValueError("Unable to create directory:" + path + " because a file already exists with the same name.")

    def visit_project(self, item):
        """
        Save off the path for the top level project(dest_directory) into id_to_path.
        Create the directory if necessary.
        :param item: RemoteProject
        """
        self.id_to_path[item.id] = self.dest_directory
        self.try_create_dir(self.dest_directory)

    def visit_folder(self, item, parent):
        """
        Save off the path for item into id_to_path.
        :param item: RemoteFolder item we want to mkdir/add to id_to_path
        :param parent: RemoteProject/RemoteFolder parent of item
        """
        parent_path = self.id_to_path[parent.id]
        path = os.path.join(parent_path, item.name)
        self.id_to_path[item.id] = path
        self.try_create_dir(path)

    def visit_file(self, item, parent):
        """
        Download the file associated with item and make sure we received all of it.
        :param item: RemoteFile file we will download
        :param parent: RemoteProject/RemoteFolder parent of item
        """
        parent_path = self.id_to_path[parent.id]
        path = os.path.join(parent_path, item.name)
        url_json = self.remote_store.data_service.get_file_url(item.id).json()
        downloader = FileDownloader(self.remote_store.config, item, url_json, path, self.watcher)
        downloader.run()
        ProjectDownload.check_file_size(item, path)

    @staticmethod
    def check_file_size(item, path):
        """
        Raise an error if we didn't get all of the file.
        :param item: RemoteFile file we tried to download
        :param path: str path where we downloaded the file to
        """
        stat_info = os.stat(path)
        if stat_info.st_size != item.size:
            format_str = "Error occurred downloading {}. Got a file size {}. Expected file size:{}"
            msg = format_str.format(path, stat_info.st_size, item.size)
            raise ValueError(msg)


class RemoteContentCounter(object):
    """
    Counts up how many bytes we have to download to retrieve the entire project.
    """
    def __init__(self, project):
        """
        Setup to count the bytes in a project.
        :param project: LocalProject project who's contents we want to walk/count.
        """
        self.count = 0
        self.project = project

    def run(self):
        """
        Update internal count finding bytes in the project.
        """
        self.walk_project(self.project)

    def walk_project(self, project):
        """
        For each file update count based on size.
        :param project: LocalProject project who's contents we want to walk/count.
        """
        # This method will call visit_project, visit_folder, and visit_file below as it walks the project tree.
        ProjectWalker.walk_project(project, self)

    def visit_project(self, item):
        """Not making use of this part of ProjectWalker"""
        pass

    def visit_folder(self, item, parent):
        """Not making use of this part of ProjectWalker"""
        pass

    def visit_file(self, item, parent):
        """
        Increment count based on file size.
        :param item: RemoteFile file we want to add size to our total
        :param parent: RemoteProject/RemoteFolder parent of item
        :return:
        """
        self.count += item.size