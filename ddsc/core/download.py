import os
import sys
import traceback
import math
import requests
from ddsc.core.localstore import HashUtil
from ddsc.core.util import ProgressPrinter
from ddsc.core.parallel import TaskExecutor, TaskRunner
from ddsc.core.ddsapi import DataServiceAuth, DataServiceApi
from ddsc.core.remotestore import RemoteStore, ProjectFile, RemoteFileUrl

FETCH_EXTERNAL_PUT_RETRY_TIMES = 5
FETCH_EXTERNAL_RETRY_SECONDS = 20
RESOURCE_NOT_CONSISTENT_RETRY_SECONDS = 2
SWIFT_EXPIRED_STATUS_CODE = 401
S3_EXPIRED_STATUS_CODE = 403


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

    def run(self):
        """
        Download the contents of the specified project name or id to dest_directory.
        """
        files_to_download = self.get_files_to_download()
        if not files_to_download:
            print("All content is already downloaded.")
            return

        if self.file_download_pre_processor:
            self.run_preprocessor(files_to_download)

        self.try_create_dir(self.dest_directory)
        num_files_to_download = len(files_to_download)
        print("Downloading {} files.".format(num_files_to_download))
        self.download_files(files_to_download)
        print("Verifying contents of {} downloaded files using file hashes.".format(num_files_to_download))
        self.check_downloaded_files(files_to_download)

    def get_files_to_download(self):
        files_to_download = []
        for project_file in self.remote_store.get_project_files(self.project):
            if self.include_project_file(project_file):
                local_path = project_file.get_local_path(self.dest_directory)
                file_to_download = FileToDownload(project_file.json_data, local_path)
                files_to_download.append(file_to_download)
        return files_to_download

    def include_project_file(self, project_file):
        if not self.path_filter.include_path(project_file.path):
            return False  # the file has been filtered out
        if self.file_exists_with_same_hash(project_file):
            return False  # the file is already local and has the same hash (no need to download)
        return True

    def file_exists_with_same_hash(self, project_file):
        local_path = project_file.get_local_path(self.dest_directory)
        if os.path.exists(local_path):
            file_hash = FileHash.create_for_first_supported_algorithm(project_file.hashes, local_path)
            return file_hash.status == FileHash.STATUS_OK
        return False

    @staticmethod
    def get_total_files_size(files_to_download):
        return sum([file_to_download.size for file_to_download in files_to_download])

    def run_preprocessor(self, files_to_download):
        """
        Run file_download_pre_processor for each file we are about to download.
        :param files_to_download: [ProjectFile]: files that will be downloaded
        """
        for file_to_download in files_to_download:
            self.file_download_pre_processor.run(self.remote_store.data_service, file_to_download)

    def download_files(self, files_to_download):
        total_files_size = self.get_total_files_size(files_to_download)
        watcher = ProgressPrinter(total_files_size, msg_verb='downloading')
        settings = DownloadSettings(self.remote_store.data_service, self.remote_store.config, watcher)
        file_downloader = FileDownloader(settings, files_to_download)
        file_downloader.run()
        watcher.finished()
        warnings = self.check_warnings()
        if warnings:
            watcher.show_warning(warnings)

    @staticmethod
    def try_create_dir(path):
        """
        Try to create a directory if it doesn't exist and raise error if there is a non-directory with the same name.
        :param path: str path to the directory
        """
        if not os.path.exists(path):
            os.mkdir(path)
        elif not os.path.isdir(path):
            ValueError("Unable to create directory:" + path + " because a file already exists with the same name.")

    def check_warnings(self):
        unused_paths = self.path_filter.get_unused_paths()
        if unused_paths:
            return 'WARNING: Path(s) not found: {}.'.format(','.join(unused_paths))
        return None

    def check_downloaded_files(self, files_to_download):
        """
        Make sure the file contents are correct by hashing file and comparing against hash provided by DukeDS.
        Raises ValueError if there is one or more problematic files.
        """
        had_hash_failures = False
        for file_to_download in files_to_download:
            local_path = file_to_download.get_local_path(self.dest_directory)
            file_hash = FileHash.create_for_first_supported_algorithm(file_to_download.hashes, local_path)
            print(file_hash.get_status_line())
            if file_hash.status == FileHash.STATUS_FAILED:
                had_hash_failures = True
        if had_hash_failures:
            raise ValueError("ERROR: Downloaded file(s) do not match the expected hashes.")
        else:
            print("All downloaded files have been verified successfully.")


class DownloadSettings(object):
    """
    Settings used to download a project
    """
    def __init__(self, data_service, config, watcher):
        self.data_service = data_service
        self.config = config
        self.watcher = watcher

    def get_data_service_auth_data(self):
        """
        Serialize data_service setup into something that can be passed to another process.
        :return: tuple of data service settings
        """
        return self.data_service.auth.get_auth_data()


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


class FileToDownload(ProjectFile):
    """
    Extends ProjectFile to have a destination to write the file to.
    """
    def __init__(self, json_data, local_path):
        super(FileToDownload, self).__init__(json_data)
        self.local_path = local_path


class FileDownloader(object):
    def __init__(self, settings, files_to_download):
        """
        :param: settings: DownloadSettings
        :param files_to_download: [FileToDownload]: files and their destinations to be downloaded
        """
        self.settings = settings
        self.files_to_download = files_to_download
        self.bytes_per_chunk = self.settings.config.download_bytes_per_chunk

    def run(self):
        self.make_local_directories()
        self.make_big_empty_files()
        self.download_files()

    def make_local_directories(self):
        """
        Create directories necessary to download the files into dest_directory
        """
        for file_to_download in self.files_to_download:
            parent_dir = os.path.dirname(file_to_download.local_path)
            if parent_dir:
                self._assure_dir_exists(parent_dir)

    def make_big_empty_files(self):
        """
        Write out a empty file so the workers can seek to where they should write and write their data.
        """
        for file_to_download in self.files_to_download:
            with open(file_to_download.local_path, "wb") as outfile:
                if file_to_download.size > 0:
                    outfile.seek(int(file_to_download.size) - 1)
                    outfile.write(b'\0')

    def download_files(self):
        large_files, small_files = self.group_files_by_size(self.bytes_per_chunk)
        if small_files:
            self.download_small_files(small_files)
        if large_files:
            self.download_large_files(large_files)

    def download_small_files(self, small_files):
        task_runner = self._create_task_runner()
        for file_to_download in small_files:
            command = DownloadFilePartCommand(self.settings, file_to_download,
                                              seek_amt=0,
                                              bytes_to_read=file_to_download.size,
                                              local_path=file_to_download.local_path)
            task_runner.add(parent_task_id=None, command=command)
        task_runner.run()

    def download_large_files(self, large_files):
        task_runner = self._create_task_runner()
        for file_to_download in large_files:
            for start_range, end_range in self.make_ranges(file_to_download):
                bytes_to_read = end_range - start_range + 1
                command = DownloadFilePartCommand(self.settings, file_to_download,
                                                  seek_amt=start_range,
                                                  bytes_to_read=bytes_to_read,
                                                  local_path=file_to_download.local_path)
                task_runner.add(parent_task_id=None, command=command)
        task_runner.run()

    def _create_task_runner(self):
        return TaskRunner(TaskExecutor(self.settings.config.download_workers))

    def make_ranges(self, file_to_download):
        """
        Divides file_to_download size into an array of ranges to be downloaded by workers.
        :param: file_to_download: FileToDownload: file url to download
        :return: [(int,int)]: array of (start, end) tuples
        """
        size = file_to_download.size
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
        if bytes_per_chunk < self.bytes_per_chunk:
            bytes_per_chunk = self.bytes_per_chunk
        return bytes_per_chunk

    @staticmethod
    def _assure_dir_exists(path):
        """
        If path doesn't exist create it and any necessary parent directories.
        :param path: str: path to a directory to create
        """
        if not os.path.exists(path):
            os.makedirs(path)

    def group_files_by_size(self, size):
        """
        Return tuple that contains a list large files and a list of small files based on size parameter
        :param size: int: size (in bytes) that determines if a file is large or small
        :return: ([FileToDownload],[FileToDownload]): (large file urls, small file urls)
        """
        large_items = []
        small_items = []
        for file_to_download in self.files_to_download:
            if file_to_download.size >= size:
                large_items.append(file_to_download)
            else:
                small_items.append(file_to_download)
        return large_items, small_items


class DownloadFilePartCommand(object):
    """
    Create project in DukeDS.
    """
    def __init__(self, settings, file_to_download, seek_amt, bytes_to_read, local_path):
        """
        Setup passing in all necessary data to download part of a file.
        :param settings: DownloadSettings
        :param file_to_download: FileToDownload
        :param seek_amt: int
        :param bytes_to_read: int
        """
        self.settings = settings
        self.file_to_download = file_to_download
        self.seek_amt = seek_amt
        self.bytes_to_read = bytes_to_read
        self.local_path = local_path
        self.func = download_file_part_run

    def before_run(self, parent_task_result):
        pass

    def create_context(self, message_queue, task_id):
        """
        Create data needed by upload_project_run(DukeDS connection info).
        :param message_queue: Queue: queue background process can send messages to us on
        :param task_id: int: id of this command's task so message will be routed correctly
        """
        params = (self.file_to_download.json_data, self.seek_amt, self.bytes_to_read, self.local_path)
        return DownloadContext(self.settings, params, message_queue, task_id)

    def after_run(self, result):
        pass

    def on_message(self, params):
        message_type, message_value = params
        if message_type == 'processed':
            watcher = self.settings.watcher
            watcher.transferring_item(self.file_to_download, message_value)
        elif message_type == 'error':
            raise ValueError(message_value)
        else:
            raise ValueError("Unknown message type {}".format(message_type, message_value))


def download_file_part_run(download_context):
    """
    Function run by CreateProjectCommand to create the project.
    Runs in a background process.
    :param download_context: UploadContext: contains data service setup and project name to create.
    """
    file_to_download_data_dict, seek_amt, bytes_to_read, local_path = download_context.params
    project_file = ProjectFile(file_to_download_data_dict)
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
        self.bytes_per_chunk = download_context.config.download_bytes_per_chunk

    def run(self):
        try:
            return self.retry_download_loop()
        except:
            error_msg = "".join(traceback.format_exception(*sys.exc_info()))
            self.download_context.send_error_message(error_msg)

    def retry_download_loop(self):
        if self.project_file.file_url:
            file_download = RemoteFileUrl(self.project_file.file_url)
        else:
            file_download = self.remote_store.get_file_url(self.project_file.id)
        while True:
            try:
                url, headers = self.get_url_and_headers_for_range(file_download)
                self.download_chunk(url, headers)
                break
            except (DownloadInconsistentError, PartialChunkDownloadError, requests.exceptions.ConnectionError):
                if self.retry_times < self.max_retry_times:
                    self.retry_times += 1
                    file_download = self.remote_store.get_file_url(self.project_file.id)
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
        separator = ""
        if not file_download.url.startswith("/"):
            separator = "/"
        url = '{}{}{}'.format(file_download.host, separator, file_download.url)
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
        if response.status_code == SWIFT_EXPIRED_STATUS_CODE \
                or response.status_code == S3_EXPIRED_STATUS_CODE:
            raise DownloadInconsistentError(response.text)
        response.raise_for_status()
        self.actual_bytes_read = 0
        self._write_response_to_file(response)
        self._verify_download_complete()

    def _write_response_to_file(self, response):
        """
        Write response to the appropriate section of the file at self.local_path.
        :param response: requests.Response: response containing stream-able data
        """
        with open(self.local_path, 'r+b') as outfile:  # open file for read/write (no truncate)
            outfile.seek(self.seek_amt)
            for chunk in response.iter_content(chunk_size=self.bytes_per_chunk):
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
        self.download_context.send_processed_message(num_bytes_read)

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
        self.download_context.send_processed_message(undo_size)


class DownloadInconsistentError(Exception):
    pass


class PartialChunkDownloadError(Exception):
    """
    Raised when we only received part of a file (possibly due to connection errors)
    """

    def __init__(self, actual_bytes, expected_bytes, path):
        self.message = "Received too few bytes downloading part of a file. " \
                       "Actual: {} Expected: {} File:{}".format(actual_bytes, expected_bytes, path)
        super(PartialChunkDownloadError, self).__init__(self.message)


class TooLargeChunkDownloadError(Exception):
    """
    Raised when we only received an unexpectedly large part of a file
    """
    def __init__(self, actual_bytes, expected_bytes, path):
        self.message = "Received too many bytes downloading part of a file. " \
                       "Actual: {} Expected: {} File:{}".format(actual_bytes, expected_bytes, path)
        super(TooLargeChunkDownloadError, self).__init__(self.message)


class MD5FileHash(object):
    algorithm = 'md5'

    @staticmethod
    def get_hash_value(file_path):
        hash_util = HashUtil()
        hash_util.add_file(file_path)
        return hash_util.hash.hexdigest()


class FileHash(object):
    STATUS_OK = "OK"
    STATUS_FAILED = "FAILED"
    algorithm_to_get_hash_value = {
        MD5FileHash.algorithm: MD5FileHash.get_hash_value
    }

    def __init__(self, algorithm, expected_hash_value, file_path):
        self.algorithm = algorithm
        self.expected_hash_value = expected_hash_value
        self.file_path = file_path
        self.status = self.determine_status()

    def _get_hash_value(self):
        get_hash_value_func = self.algorithm_to_get_hash_value.get(self.algorithm)
        if get_hash_value_func:
            return get_hash_value_func(self.file_path)
        raise ValueError("Unsupported algorithm {}.".format(self.algorithm))

    def determine_status(self):
        if self._get_hash_value() == self.expected_hash_value:
            return self.STATUS_OK
        else:
            return self.STATUS_FAILED

    def get_status_line(self):
        return "{} {} {} {}".format(self.file_path, self.expected_hash_value, self.algorithm, self.status)

    def raise_for_status(self):
        if self.status == self.STATUS_FAILED:
            raise ValueError("Hash validation error: {}".format(self.get_status_line()))

    @staticmethod
    def create_for_first_supported_algorithm(dds_hashes, file_path):
        for hash_info in dds_hashes:
            algorithm = hash_info.get('algorithm')
            hash_value = hash_info.get('value')
            if algorithm in FileHash.algorithm_to_get_hash_value:
                return FileHash(algorithm, hash_value, file_path)
        raise ValueError("Unable to validate: No supported hashes found for file {}".format(file_path))
