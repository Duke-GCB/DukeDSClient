import os
from ddsc.core.util import ProgressPrinter
from ddsc.core.filedownloader import FileDownloader
from ddsc.core.pathfilter import PathFilteredProject
from ddsc.core.localstore import PathData


class ProjectDownload(object):
    """
    Creates local version of remote content.
    """
    def __init__(self, remote_store, project, dest_directory, path_filter, file_download_pre_processor=None):
        """
        Setup for downloading a remote project.
        :param remote_store: RemoteStore: which remote store to download the project from
        :param project: RemoteProject: project to download
        :param dest_directory: str: path to where we will save the project contents
        :param path_filter: PathFilter: determines which files will be downloaded
        :param file_download_pre_processor: object: has run(data_service, RemoteFile) method to run before downloading
        """
        self.remote_store = remote_store
        self.project = project
        self.dest_directory = dest_directory
        self.path_filter = path_filter
        self.file_download_pre_processor = file_download_pre_processor
        self.watcher = None

    def run(self):
        """
        Download the contents of the specified project name or id to dest_directory.
        """
        self.walk_project(self.project)

    def walk_project(self, project):
        """
        For each project, folder, and files send to remote store if necessary.
        :param project: LocalProject project who's contents we want to walk/send.
        """
        counter = RemoteContentCounter(project)
        path_filtered_project = PathFilteredProject(self.path_filter, counter)
        path_filtered_project.run(project)  # calls visit_project, visit_folder, visit_file in RemoteContentCounter

        self.watcher = ProgressPrinter(counter.count, msg_verb='downloading')
        path_filtered_project = PathFilteredProject(self.path_filter, self)
        path_filtered_project.run(project)  # calls visit_project, visit_folder, visit_file below
        self.watcher.finished()
        warnings = self.check_warnings()
        if warnings:
            self.watcher.show_warning(warnings)

    def try_create_dir(self, remote_path):
        """
        Try to create a directory if it doesn't exist and raise error if there is a non-directory with the same name.
        :param path: str path to the directory
        :param remote_path: str path as it exists on the remote server
        """
        path = os.path.join(self.dest_directory, remote_path)
        if not os.path.exists(path):
            os.mkdir(path)
        elif not os.path.isdir(path):
            ValueError("Unable to create directory:" + path + " because a file already exists with the same name.")

    def visit_project(self, item):
        """
        Create the parent directory if necessary.
        :param item: RemoteProject
        """
        self.try_create_dir(item.remote_path)

    def visit_folder(self, item, parent):
        """
        Make directory for item.
        :param item: RemoteFolder item we want create a directory for.
        :param parent: RemoteProject/RemoteFolder parent of item
        """
        self.try_create_dir(item.remote_path)

    def visit_file(self, item, parent):
        """
        Download the file associated with item and make sure we received all of it.
        :param item: RemoteFile file we will download
        :param parent: RemoteProject/RemoteFolder parent of item
        """
        if self.file_download_pre_processor:
            self.file_download_pre_processor.run(self.remote_store.data_service, item)
        path = os.path.join(self.dest_directory, item.remote_path)
        if self.file_exists_with_same_hash(item, path):
            # Update progress bar skipping this file
            self.watcher.transferring_item(item, increment_amt=item.size)
        else:
            downloader = FileDownloader(self.remote_store.config, item, path, self.watcher)
            downloader.run()
            ProjectDownload.check_file_size(item, path)

    @staticmethod
    def file_exists_with_same_hash(item, path):
        if os.path.exists(path):
            hash_data = PathData(path).get_hash()
            return hash_data.matches(item.hash_alg, item.file_hash)
        return False

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

    def check_warnings(self):
        unused_paths = self.path_filter.get_unused_paths()
        if unused_paths:
            return 'WARNING: Path(s) not found: {}.'.format(','.join(unused_paths))
        return None


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
