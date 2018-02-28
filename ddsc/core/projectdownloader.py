from __future__ import print_function
import os
from ddsc.core.parallel import TaskExecutor, TaskRunner
from ddsc.sdk.client import DDSConnection, ProjectFileUrl


class DownloadSettings(object):
    def __init__(self, config, dest_directory):
        self.config = config
        self.dest_directory = dest_directory


class DownloadContext(object):
    """
    Values passed to a background worker.
    Contains DownloadSettings and parameters specific to the function to be run.
    """
    def __init__(self, settings, params, message_queue, task_id):
        """
        Setup context so it can be passed.
        :param settings: UploadSettings: project level info
        :param params: tuple: values specific to the function being run
        :param message_queue: Queue: queue background process can send messages to us on
        :param task_id: int: id of this command's task so message will be routed correctly
        """
        #self.data_service_auth_data = settings.get_data_service_auth_data()
        self.config = settings.config
        self.params = params
        self.message_queue = message_queue
        self.task_id = task_id

    def create_dds_connection(self):
        return DDSConnection(self.config)


class ProjectDownload(object):
    """
    Creates local version of remote content.
    """
    def __init__(self, settings, project):
        self.settings = settings
        self.project = project
        self.dest_directory = settings.dest_directory
        self.bytes_per_chunk = self.settings.config.upload_bytes_per_chunk

    def run(self):
        file_urls = self._get_file_urls_different_from_local()
        file_urls.make_local_directories(self.dest_directory)
        #large_file_urls, small_file_urls = file_urls.split_file_urls_by_size(self.bytes_per_chunk)
        runner = TaskRunner(TaskExecutor(self.settings.config.download_workers))
        for command in file_urls.get_download_commands(self.settings):
            runner.add(parent_task_id=None, command=command)
        runner.run()

    def _get_file_urls_different_from_local(self):
        """
        Create FileUrls that contains data for each remote file that is different from those in dest_directory.
        :return: FileUrls
        """
        file_urls_to_download = []
        for file_url in self.project.get_file_urls():
            if not file_url.version_exists_in_directory(self.dest_directory):
                file_urls_to_download.append(file_url)
        return FileUrls(file_urls_to_download)


class FileUrls(object):
    def __init__(self, file_urls):
        """
        :param file_urls: [ddsc.sdk.client.ProjectFileUrl]: file urls
        """
        self.file_urls = file_urls

    def _get_parent_remote_paths(self):
        """
        Get list of remote folders based on the list of all file urls
        :return: set([str]): set of remote folders (that contain files)
        """
        parent_paths = set([item.get_remote_parent_path() for item in self.file_urls])
        if '' in parent_paths:
            parent_paths.remove('')
        return parent_paths

    def make_local_directories(self, destination_directory):
        """
        Create directories underneath the specified destination_directory
        :param destination_directory: str: directory to create folders necessary for downloading these file urls
        """
        for remote_path in self._get_parent_remote_paths():
            local_path = os.path.join(destination_directory, remote_path)
            self._assure_dir_exists(local_path)

    @staticmethod
    def _assure_dir_exists(path):
        """
        If path doesn't exist create it and any necessary parent directories.
        :param path: str: path to a directory to create
        """
        if not os.path.exists(path):
            os.makedirs(path)

    def split_file_urls_by_size(self, size):
        """
        Return tuple that contains a list large files and a list of small files based on size parameter
        :param size: int: size (in bytes) that determines if a file is large or small
        :return: ([ProjectFileUrl],[ProjectFileUrl]): (large file urls, small file urls)
        """
        large_items = []
        small_items = []
        for file_url in self.file_urls:
            if file_url.size >= size:
                large_items.append(file_url)
            else:
                small_items.append(file_url)
        return large_items, small_items

    def get_download_commands(self, settings):
        commands = []
        for file_url in self.file_urls:
            commands.append(DownloadEntireFileCommand(settings, file_url))
        return commands


class DownloadEntireFileCommand(object):
    """
    Create project in DukeDS.
    """
    def __init__(self, settings, file_url):
        """
        Setup passing in all necessary data to create project and update external state.
        :param settings: UploadSettings: settings to be used/updated when we upload the project.
        :param local_project: LocalProject: information about the project(holds remote_id when done)
        """
        self.settings = settings
        self.file_url = file_url
        self.func = download_entire_file_run

    def before_run(self, parent_task_result):
        pass

    def create_context(self, message_queue, task_id):
        """
        Create data needed by upload_project_run(DukeDS connection info).
        :param message_queue: Queue: queue background process can send messages to us on
        :param task_id: int: id of this command's task so message will be routed correctly
        """
        file_url_data_dict = self.file_url.get_data_dict()
        params = (self.settings.dest_directory, file_url_data_dict)
        return DownloadContext(self.settings, params, message_queue, task_id)

    def after_run(self, result_id):
        """
        Save uuid associated with project we just created.
        :param result_id: str: uuid of the project
        """
        pass


def download_entire_file_run(download_context):
    """
    Function run by CreateProjectCommand to create the project.
    Runs in a background process.
    :param upload_context: UploadContext: contains data service setup and project name to create.
    """
    dds_connection = download_context.create_dds_connection()
    destination_dir, file_url_data_dict = download_context.params
    file_url = ProjectFileUrl(dds_connection, file_url_data_dict)
    local_path = file_url.get_local_path(destination_dir)
    file_download = file_url.get_file_download()
    file_download.save_to_path(local_path)
    return 'ok'
