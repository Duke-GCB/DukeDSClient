from __future__ import print_function
import os
import sys
import traceback
import requests
import math
from ddsc.sdk.client import DDSConnection, ProjectFileUrl
from ddsc.core.parallel import TaskExecutor, TaskRunner
from ddsc.core.util import ProgressQueue
from ddsc.core.filedownloader import PartialChunkDownloadError, TooLargeChunkDownloadError

FETCH_EXTERNAL_PUT_RETRY_TIMES = 5
FETCH_EXTERNAL_RETRY_SECONDS = 20
RESOURCE_NOT_CONSISTENT_RETRY_SECONDS = 2
DOWNLOAD_FILE_CHUNK_SIZE = 20 * 1024 * 1024
MIN_DOWNLOAD_CHUNK_SIZE = DOWNLOAD_FILE_CHUNK_SIZE
# = 100000


class DownloadSettings(object):
    """
    Settings used to download a project
    """
    def __init__(self, client, dest_directory):
        self.client = client
        self.config = client.dds_connection.config
        self.dest_directory = dest_directory

    def get_data_service_auth_data(self):
        """
        Serialize data_service setup into something that can be passed to another process.
        :return: tuple of data service settings
        """
        return self.client.dds_connection.get_auth_data()


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

    def create_dds_connection(self):
        return DDSConnection.create_from_auth_data(self.config, self.data_service_auth_data)

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


class ProjectDownload(object):
    """
    Creates local version of remote content.
    """
    def __init__(self, settings, project):
        self.settings = settings
        self.project = project
        self.dest_directory = settings.dest_directory

    def run(self):
        file_url_downloader = self._get_file_url_downloader()
        file_url_downloader.make_local_directories()
        file_url_downloader.make_big_empty_files()
        file_url_downloader.download_files()

    def _get_file_url_downloader(self):
        """
        Create FileUrlDownloader that contains data for each remote file that is different from those in dest_directory.
        :return: FileUrlDownloader
        """
        file_urls_to_download = []
        for file_url in self.project.get_file_urls():
            if not file_url.version_exists_in_directory(self.dest_directory):
                file_urls_to_download.append(file_url)
        return FileUrlDownloader(self.settings, file_urls_to_download)

    def _get_non_local_file_urls(self):
        """
        Return a list of FileUrls that are not present or different from those in the destination directory
        :return: [FileUrl]
        """
        pass

class FileUrlDownloader(object):
    def __init__(self, settings, file_urls):
        """
        :param: settings: DownloadSettings
        :param file_urls: [ddsc.sdk.client.ProjectFileUrl]: file urls to be downloaded
        """
        self.settings = settings
        self.file_urls = file_urls
        self.task_runner = TaskRunner(TaskExecutor(settings.config.download_workers))
        self.dest_directory = settings.dest_directory
        #self.bytes_per_chunk = self.settings.config.upload_bytes_per_chunk
        self.bytes_per_chunk = 100000

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
            print("DOWNLOD SMALL FILE {}".format(file_url.name))
            command = DownloadFilePartCommand(self.settings, file_url, seek_amt=0, bytes_to_read=file_url.size)
            self.task_runner.add(parent_task_id=None, command=command)
        self.task_runner.run()

    def download_large_files(self, large_file_urls):
        for file_url in large_file_urls:
            print("DOWNLOD LARGE FILE {}".format(file_url.name))
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
        Setup passing in all necessary data to create project and update external state.
        :param settings: UploadSettings: settings to be used/updated when we upload the project.
        :param local_project: LocalProject: information about the project(holds remote_id when done)
        :param seek_amt: int: offset to be applied when fetching content and writing to the local file
        :param bytes_to_read: number bytes to read from url and write to file at seek_amt
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
        file_url_data_dict = self.file_url.get_data_dict()
        params = (self.settings.dest_directory, file_url_data_dict, self.seek_amt, self.bytes_to_read)
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
    dds_connection = download_context.create_dds_connection()
    destination_dir, file_url_data_dict, seek_amt, bytes_to_read = download_context.params
    file_url = ProjectFileUrl(dds_connection, file_url_data_dict)
    local_path = file_url.get_local_path(destination_dir)
    retry_chunk_downloader = RetryChunkDownloader(file_url, local_path,
                                                  seek_amt, bytes_to_read,
                                                  download_context)
    retry_chunk_downloader.run()
    return 'ok'


class RetryChunkDownloader(object):
    def __init__(self, file_url, local_path, seek_amt, bytes_to_read, download_context):
        self.file_url = file_url
        self.local_path = local_path
        self.seek_amt = seek_amt
        self.bytes_to_read = bytes_to_read
        self.retry_times = 0
        self.max_retry_times = FETCH_EXTERNAL_PUT_RETRY_TIMES
        self.download_context = download_context
        self.actual_bytes_read = 0

    def run(self):
        try:
            return self.retry_download_loop()
        except:
            error_msg = "".join(traceback.format_exception(*sys.exc_info()))
            self.download_context.send_error_message(error_msg)

    def retry_download_loop(self):
        file_download = self.file_url.get_file_download()
        while True:
            try:
                url, headers = self.get_url_and_headers_for_range(file_download)
                self.download_chunk(url, headers)
                break
            except DownloadInconsistentError:
                if self.retry_times < self.max_retry_times:
                    self.retry_times += 1
                    file_download = self.get_file_download(fetch_from_server=True)
                    # continue loop and try downloading again
                else:
                    print("HAD ERRRO")
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
