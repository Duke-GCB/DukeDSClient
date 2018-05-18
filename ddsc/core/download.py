import os
import sys
import traceback
import math
import requests
from ddsc.core.util import ProgressPrinter
from ddsc.core.filedownloader import FileDownloader, PartialChunkDownloadError, TooLargeChunkDownloadError
from ddsc.core.localstore import PathData
from ddsc.core.parallel import TaskExecutor, TaskRunner
from ddsc.core.ddsapi import DataServiceAuth, DataServiceApi
from ddsc.core.remotestore import RemoteStore, ProjectFile, RemoteFileUrl

FETCH_EXTERNAL_PUT_RETRY_TIMES = 5
FETCH_EXTERNAL_RETRY_SECONDS = 20
RESOURCE_NOT_CONSISTENT_RETRY_SECONDS = 2
DOWNLOAD_FILE_CHUNK_SIZE = 20 * 1024 * 1024
MIN_DOWNLOAD_CHUNK_SIZE = DOWNLOAD_FILE_CHUNK_SIZE


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
        files_to_download = []
        for project_file in self.remote_store.get_project_files(self.project):
            if self.path_filter.include_path(project_file.path):
                files_to_download.append(project_file)

        total_files_size = 0
        for project_file in files_to_download:
            total_files_size += project_file.size

        self.watcher = ProgressPrinter(total_files_size, msg_verb='downloading')

        # create directory
        self.try_create_dir(self.project.remote_path)

        settings = DownloadSettings(self.remote_store, self.dest_directory)
        file_url_downloader = FileUrlDownloader(settings, files_to_download, self.watcher)
        file_url_downloader.make_local_directories()
        file_url_downloader.make_big_empty_files()
        file_url_downloader.download_files()

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


class DownloadSettings(object):
    """
    Settings used to download a project
    """
    def __init__(self, remote_store, dest_directory):
        self.remote_store = remote_store
        self.config = remote_store.config
        self.dest_directory = dest_directory

    def get_data_service_auth_data(self):
        """
        Serialize data_service setup into something that can be passed to another process.
        :return: tuple of data service settings
        """
        return self.remote_store.data_service.auth.get_auth_data()


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
        self.data_service_auth_data = settings.get_data_service_auth_data()
        self.config = settings.config
        self.params = params
        self.message_queue = message_queue
        self.task_id = task_id

    def create_data_service(self):
        auth = DataServiceAuth(self.config)
        auth.set_auth_data(self.data_service_auth_data)
        return DataServiceApi(auth, self.config.url)

    def create_remote_store(self):
        return RemoteStore(self.config, self.create_data_service())

    def send_message(self, data):
        """
        Sends a message to the command's on_message(data) method.
        :param data: object: data sent to on_message
        """
        self.message_queue.put((self.task_id, data))

    def send_processed_message(self, num_bytes):
        self.send_message(('processed', num_bytes))

    def send_error_message(self, error):
        self.send_message(('error', error))


class FileUrlDownloader(object):
    def __init__(self, settings, file_urls, watcher):
        """
        :param: settings: DownloadSettings
        :param file_urls: [ddsc.sdk.client.ProjectFileUrl]: file urls to be downloaded
        """
        self.settings = settings
        self.file_urls = file_urls
        self.task_runner = TaskRunner(TaskExecutor(settings.config.download_workers))
        self.dest_directory = settings.dest_directory
        self.bytes_per_chunk = self.settings.config.download_bytes_per_chunk
        self.watcher = watcher

    def _get_parent_remote_paths(self):
        """
        Get list of remote folders based on the list of all file urls
        :return: set([str]): set of remote folders (that contain files)
        """
        parent_paths = set([item.get_remote_parent_path() for item in self.file_urls])
        if '' in parent_paths:
            parent_paths.remove('')
        return parent_paths

    def make_local_directories(self):
        """
        Create directories necessary to download the files into dest_directory
        """
        for remote_path in self._get_parent_remote_paths():
            local_path = os.path.join(self.dest_directory, remote_path)
            self._assure_dir_exists(local_path)

    def make_big_empty_files(self):
        """
        Write out a empty file so the workers can seek to where they should write and write their data.
        """
        for file_url in self.file_urls:
            local_path = file_url.get_local_path(self.dest_directory)
            with open(local_path, "wb") as outfile:
                if file_url.size > 0:
                    outfile.seek(int(file_url.size) - 1)
                    outfile.write(b'\0')

    def download_files(self):
        large_file_urls, small_file_urls = self.split_file_urls_by_size(self.bytes_per_chunk)
        self.download_small_files(small_file_urls)
        self.download_large_files(large_file_urls)

    def download_small_files(self, small_file_urls):
        for file_url in small_file_urls:
            command = DownloadFilePartCommand(self.settings, file_url, seek_amt=0, bytes_to_read=file_url.size)
            self.task_runner.add(parent_task_id=None, command=command)
        self.task_runner.run()

    def download_large_files(self, large_file_urls):
        for file_url in large_file_urls:
            for start_range, end_range in self.make_ranges(file_url):
                bytes_to_read = end_range - start_range + 1
                command = DownloadFilePartCommand(self.settings, file_url,
                                                  seek_amt=start_range,
                                                  bytes_to_read=bytes_to_read)
                self.task_runner.add(parent_task_id=None, command=command)
            self.task_runner.run()

    def make_ranges(self, file_url):
        """
        Divides file_url size into an array of ranges to be downloaded by workers.
        :param: file_url: ProjectFileUrl: file url to download
        :return: [(int,int)]: array of (start, end) tuples
        """
        size = file_url.size
        bytes_per_chunk = self.determine_bytes_per_chunk(size)
        start = 0
        ranges = []
        while size > 0:
            amount = bytes_per_chunk
            if amount > size:
                amount = size
            ranges.append((start, start + amount - 1))
            start += amount
            size -= amount
        return ranges

    def determine_bytes_per_chunk(self, size):
        """
        Calculate the size of chunk a worker should download.
        The last worker may download less than this depending on file size.
        :return: int: byte size for a worker
        """
        workers = self.settings.config.download_workers
        if not workers or workers == 'None':
            workers = 1
        bytes_per_chunk = int(math.ceil(size / float(workers)))
        if bytes_per_chunk < MIN_DOWNLOAD_CHUNK_SIZE:
            bytes_per_chunk = MIN_DOWNLOAD_CHUNK_SIZE
        return bytes_per_chunk

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


class DownloadFilePartCommand(object):
    """
    Create project in DukeDS.
    """
    def __init__(self, settings, file_url, seek_amt, bytes_to_read):
        """
        Setup passing in all necessary data to download part of a file.
        :param settings:
        :param file_url:
        :param seek_amt:
        :param bytes_to_read:
        """
        self.settings = settings
        self.file_url = file_url
        self.seek_amt = seek_amt
        self.bytes_to_read = bytes_to_read
        self.func = download_file_part_run

    def before_run(self, parent_task_result):
        pass

    def create_context(self, message_queue, task_id):
        """
        Create data needed by upload_project_run(DukeDS connection info).
        :param message_queue: Queue: queue background process can send messages to us on
        :param task_id: int: id of this command's task so message will be routed correctly
        """
        params = (self.settings.dest_directory, self.file_url.json_data, self.seek_amt, self.bytes_to_read)
        return DownloadContext(self.settings, params, message_queue, task_id)

    def after_run(self, result_id):
        """
        Save uuid associated with project we just created.
        :param result_id: str: uuid of the project
        """
        pass

    def on_message(self, params):
        print("GOT MESSAGE", params)


def download_file_part_run(download_context):
    """
    Function run by CreateProjectCommand to create the project.
    Runs in a background process.
    :param download_context: UploadContext: contains data service setup and project name to create.
    """
    destination_dir, file_url_data_dict, seek_amt, bytes_to_read = download_context.params
    project_file = ProjectFile(file_url_data_dict)
    local_path = project_file.get_local_path(destination_dir)
    retry_chunk_downloader = RetryChunkDownloader(project_file, local_path,
                                                  seek_amt, bytes_to_read,
                                                  download_context)
    retry_chunk_downloader.run()
    return 'ok'


class RetryChunkDownloader(object):
    def __init__(self, project_file, local_path, seek_amt, bytes_to_read, download_context):
        self.project_file = project_file
        self.local_path = local_path
        self.seek_amt = seek_amt
        self.bytes_to_read = bytes_to_read
        self.retry_times = 0
        self.max_retry_times = FETCH_EXTERNAL_PUT_RETRY_TIMES
        self.download_context = download_context
        self.actual_bytes_read = 0
        self.remote_store = download_context.create_remote_store()

    def run(self):
        try:
            return self.retry_download_loop()
        except:
            error_msg = "".join(traceback.format_exception(*sys.exc_info()))
            self.download_context.send_error_message(error_msg)

    def retry_download_loop(self):
        file_download = RemoteFileUrl(self.project_file.file_url)
        while True:
            try:
                url, headers = self.get_url_and_headers_for_range(file_download)
                self.download_chunk(url, headers)
                break
            except (DownloadInconsistentError, PartialChunkDownloadError, requests.exceptions.ConnectionError):
                if self.retry_times < self.max_retry_times:
                    self.retry_times += 1
                    file_download = self.remote_store.get_project_file(self.project_file.id)
                    # continue loop and try downloading again
                else:
                    raise  # run will send the error back to the main process

    def get_url_and_headers_for_range(self, file_download):
        """
        Return url and headers to use for downloading part of a file, adding range headers.
        :param file_download: FileDownload: contains data about file we will download
        :return: str, dict: url to download and headers to use
        """
        headers = self.get_range_headers()
        if file_download.http_headers:
            headers.update(file_download.http_headers)
        url = '{}/{}'.format(file_download.host, file_download.url)
        return url, headers

    def get_range_headers(self):
        range_start = self.seek_amt
        range_end = self.seek_amt + self.bytes_to_read - 1
        return {'Range': 'bytes={}-{}'.format(range_start, range_end)}

    def download_chunk(self, url, headers):
        """
        Download part of a file and write to our file
        :param url: str: URL to download this file
        :param headers: dict: headers used to download this file chunk
        """
        response = requests.get(url, headers=headers, stream=True)
        if response.status_code == 401:
            raise DownloadInconsistentError(response.text)
        response.raise_for_status()
        self._write_response_to_file(response)
        self._verify_download_complete()

    def _write_response_to_file(self, response):
        """
        Write response to the appropriate section of the file at self.local_path.
        :param response: requests.Response: response containing stream-able data
        """
        with open(self.local_path, 'r+b') as outfile:  # open file for read/write (no truncate)
            outfile.seek(self.seek_amt)
            for chunk in response.iter_content(chunk_size=DOWNLOAD_FILE_CHUNK_SIZE):
                if chunk:  # filter out keep-alive chunks
                    outfile.write(chunk)
                    self._on_bytes_read(len(chunk))

    def _on_bytes_read(self, num_bytes_read):
        """
        Record our progress so we can validate that we receive all the data
        :param num_bytes_read: int: number of bytes we received as part of one chunk
        """
        self.actual_bytes_read += num_bytes_read
        if self.actual_bytes_read > self.bytes_to_read:
            raise TooLargeChunkDownloadError(self.actual_bytes_read, self.bytes_to_read, self.local_path)
            self.download_context.send_progress_message(num_bytes_read)

    def _verify_download_complete(self):
        """
        Make sure we received all the data
        """
        if self.actual_bytes_read > self.bytes_to_read:
            raise TooLargeChunkDownloadError(self.actual_bytes_read, self.bytes_to_read, self.local_path)
        elif self.actual_bytes_read < self.bytes_to_read:
            raise PartialChunkDownloadError(self.actual_bytes_read, self.bytes_to_read, self.local_path)

    def revert_progress(self):
        """
        Update progress monitor with negative number so it is accurate since this download failed.
        """
        undo_size = self.actual_bytes_read * -1
        self.download_context.send_progress_message(undo_size)


class DownloadInconsistentError(Exception):
    pass
