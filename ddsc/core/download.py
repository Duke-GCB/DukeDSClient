import sys
import requests
from requests.exceptions import HTTPError
import os
import multiprocessing
import time
import queue
from ddsc.core.localstore import HashUtil
from ddsc.core.ddsapi import DDS_TOTAL_HEADER
from ddsc.core.util import humanize_bytes, transfer_speed_str

SWIFT_EXPIRED_STATUS_CODE = 401
S3_EXPIRED_STATUS_CODE = 403
MISMATCHED_FILE_HASH_WARNING = """
NOTICE: Data Service reports multiple hashes for {}.
The downloaded files have been verified and confirmed to match one of these hashes.
You do not need to retry the download.
For more information, visit https://github.com/Duke-GCB/DukeDSClient/wiki/MD5-Hash-Conflicts.
"""


class MD5FileHash(object):
    algorithm = 'md5'

    @staticmethod
    def get_hash_value(file_path):
        hash_util = HashUtil()
        hash_util.add_file(file_path)
        return hash_util.hash.hexdigest()


class FileHash(object):
    algorithm_to_get_hash_value = {
        MD5FileHash.algorithm: MD5FileHash.get_hash_value
    }

    def __init__(self, algorithm, expected_hash_value, file_path):
        self.algorithm = algorithm
        self.expected_hash_value = expected_hash_value
        self.file_path = file_path

    def _get_hash_value(self):
        get_hash_value_func = self.algorithm_to_get_hash_value.get(self.algorithm)
        if get_hash_value_func:
            return get_hash_value_func(self.file_path)
        raise ValueError("Unsupported algorithm {}.".format(self.algorithm))

    def is_valid(self):
        return self._get_hash_value() == self.expected_hash_value

    @staticmethod
    def get_supported_file_hashes(dds_hashes, file_path):
        """
        Returns a list of FileHashes for each dict in dds_hashes.
        :param dds_hashes: [dict]: list of dicts with 'algorithm', and 'value' keys
        :param file_path: str: path to file to have hash checked
        :return: [FileHash]
        """
        file_hashes = []
        for hash_info in dds_hashes:
            algorithm = hash_info.get('algorithm')
            hash_value = hash_info.get('value')
            if algorithm in FileHash.algorithm_to_get_hash_value:
                file_hashes.append(FileHash(algorithm, hash_value, file_path))
        return file_hashes

    @staticmethod
    def separate_valid_and_failed_hashes(file_hashes):
        """
        Given a list of file hashes seperate them into a list of valid and a list of failed.
        :param file_hashes: [FileHash]
        :return: [FileHash], [FileHash]: valid_file_hashes, failed_file_hashes
        """
        valid_file_hashes = []
        failed_file_hashes = []
        for file_hash in file_hashes:
            if file_hash.is_valid():
                valid_file_hashes.append(file_hash)
            else:
                failed_file_hashes.append(file_hash)
        return valid_file_hashes, failed_file_hashes


class FileHashStatus(object):
    STATUS_OK = "OK"
    STATUS_WARNING = "WARNING"
    STATUS_FAILED = "FAILED"

    def __init__(self, file_hash, status):
        self.file_hash = file_hash
        self.status = status

    def has_a_valid_hash(self):
        return self.status in [self.STATUS_OK, self.STATUS_WARNING]

    def get_status_line(self):
        return "{} {} {} {}".format(self.file_hash.file_path,
                                    self.file_hash.expected_hash_value,
                                    self.file_hash.algorithm,
                                    self.status)

    def raise_for_status(self):
        if self.status == self.STATUS_FAILED:
            raise ValueError("Hash validation error: {}".format(self.get_status_line()))

    @staticmethod
    def determine_for_hashes(dds_hashes, file_path):
        """
        Compares dds_hashes against file_path using the associated algorithms recording a status property.
        The status property will bet set as follows:
        STATUS_OK: there are only valid file hashes
        STATUS_FAILED: there are only failed file hashes
        STATUS_WARNING: there are both failed and valid hashes
        Raises ValueError if no hashes found.
        :param dds_hashes: [dict]: list of dicts with 'algorithm', and 'value' keys
        :param file_path: str: path to file to have hash checked
        :return: FileHashStatus
        """
        file_hashes = FileHash.get_supported_file_hashes(dds_hashes, file_path)
        valid_file_hashes, failed_file_hashes = FileHash.separate_valid_and_failed_hashes(file_hashes)
        if valid_file_hashes:
            first_ok_file_hash = valid_file_hashes[0]
            if failed_file_hashes:
                return FileHashStatus(first_ok_file_hash, FileHashStatus.STATUS_WARNING)
            else:
                return FileHashStatus(first_ok_file_hash, FileHashStatus.STATUS_OK)
        else:
            if failed_file_hashes:
                first_failed_file_hash = failed_file_hashes[0]
                return FileHashStatus(first_failed_file_hash, FileHashStatus.STATUS_FAILED)
        raise ValueError("Unable to validate: No supported hashes found for file {}".format(file_path))


class FileDownloadState(object):
    """
    Contains details passed between foreground ProjectFileDownloader and background download_file function
    """
    NEW = 'new'  # initial state before downloading
    DOWNLOADING = 'downloading'  # state when downloading
    GOOD = 'good'  # successfully download and verified the file's hash
    ALREADY_COMPLETE = 'already_complete'  # the file already exists and has a correct md5 sum
    EXPIRED_URL = 'expired_url'  # backend url expired before we got a chance to download it
    ERROR = 'error'  # an error occurred during download

    def __init__(self, project_file, output_path, config):
        self.file_id = project_file.id
        self.size = project_file.size
        self.hashes = project_file.hashes
        self.output_path = output_path
        self.url = project_file.file_url['host'] + project_file.file_url['url']
        self.retries = config.file_download_retries
        self.download_bytes_per_chunk = config.download_bytes_per_chunk
        self.state = self.NEW
        self.status = None
        self.msg = 'New state'

    def calculate_file_hash_status(self):
        return FileHashStatus.determine_for_hashes(self.hashes, self.output_path)

    def is_ok_state(self):
        return self.state == self.GOOD or self.state == self.ALREADY_COMPLETE

    def mark_good(self, status):
        self.state = self.GOOD
        self.status = status
        self.msg = ''
        return self

    def mark_already_complete(self, status):
        self.state = self.ALREADY_COMPLETE
        self.status = status
        self.msg = ''
        return self

    def mark_expired_url(self, msg):
        self.state = self.EXPIRED_URL
        self.status = None
        self.msg = msg
        return self

    def mark_error(self, msg):
        self.state = self.ERROR
        self.status = None
        self.msg = msg
        return self

    def raise_for_status(self):
        if self.status:
            self.status.raise_for_status()
        else:
            raise ValueError(self.msg)


class URLExpiredException(Exception):
    pass


class ProjectFileDownloader(object):
    def __init__(self, config, dest_directory, project, path_filter):
        self.config = config
        self.dest_directory = dest_directory
        self.project = project
        self.dds_connection = project.dds_connection
        self.num_workers = config.download_workers
        self.path_filter = path_filter
        self.async_download_results = []
        self.message_queue = multiprocessing.Manager().Queue()
        self.files_downloaded = 0
        self.files_to_download = None
        self.file_download_statuses = {}
        self.download_status_list = []
        self.spinner_chars = "|/-\\"
        self.start_time = None

    def run(self):
        self.start_time = time.time()
        self._download_files()
        self._show_downloaded_files_status()

    def _download_files(self):
        pool = multiprocessing.Pool(self.num_workers)
        try:
            for project_file in self._get_project_files():
                self._download_file(pool, project_file)
                while self._work_queue_is_full():
                    self._wait_for_and_retry_failed_downloads(pool)
            while self._work_queue_is_not_empty():
                self._wait_for_and_retry_failed_downloads(pool)
        finally:
            pool.close()

    def _show_downloaded_files_status(self):
        print("\nVerifying contents of {} downloaded files using file hashes.".format(self.files_to_download))
        all_good = True
        files_with_mismatched_hashes = 0
        for download_status in self.download_status_list:
            if not download_status.has_a_valid_hash():
                all_good = False
            if download_status.status == FileHashStatus.STATUS_WARNING:
                files_with_mismatched_hashes += 1
            print(download_status.get_status_line())
        if all_good:
            print("All downloaded files have been verified successfully.")
            if files_with_mismatched_hashes:
                print(MISMATCHED_FILE_HASH_WARNING.format(files_with_mismatched_hashes))
        else:
            raise ValueError("ERROR: Downloaded file(s) do not match the expected hashes.")

    def _get_project_files(self):
        project_files_generator = self.project.get_project_files_generator(self.config.page_size)
        if self.path_filter:
            # fetch all files so we can determine an accurate filtered count
            project_files = self._filter_project_files(project_files_generator)
            self._print_path_filter_warnings()
            self.files_to_download = len(project_files)
            self.show_progress_bar()
            for project_file in project_files:
                yield project_file
        else:
            for project_file, headers in project_files_generator:
                if self.files_to_download is None:
                    self.files_to_download = int(headers.get(DDS_TOTAL_HEADER))
                    self.show_progress_bar()
                yield project_file

    def _filter_project_files(self, project_files_generator):
        project_files = []
        for project_file, headers in project_files_generator:
            if self.path_filter.include_path(project_file.path):
                project_files.append(project_file)
        return project_files

    def _print_path_filter_warnings(self):
        if self.path_filter:
            unused_paths = self.path_filter.get_unused_paths()
            if unused_paths:
                print('WARNING: Path(s) not found: {}.'.format(','.join(unused_paths)))

    def _download_file(self, pool, project_file):
        output_path = project_file.get_local_path(self.dest_directory)
        output_path_parent = os.path.dirname(output_path)
        if not os.path.exists(output_path_parent):
            os.makedirs(output_path_parent)
        file_download_state = FileDownloadState(project_file, output_path, self.config)
        self._async_download_file(pool, file_download_state)

    def _async_download_file(self, pool, file_download_state):
        async_result = pool.apply_async(download_file, (file_download_state, self.message_queue))
        self.async_download_results.append(async_result)

    def _work_queue_is_full(self):
        return len(self.async_download_results) >= self.num_workers

    def _work_queue_is_not_empty(self):
        return len(self.async_download_results) > 0

    def _wait_for_and_retry_failed_downloads(self, pool):
        download_results = self._pop_ready_download_results()
        if download_results:
            self._process_download_results(pool, download_results)
        else:
            self._try_process_message_queue()
            time.sleep(0)  # Pause to give up CPU since no results are ready

    def _try_process_message_queue(self):
        try:
            file_id, bytes_downloaded, file_size, file_state = self.message_queue.get_nowait()
            # This might be out of date for a little bit
            self.file_download_statuses[file_id] = (bytes_downloaded, file_size, file_state)
            self.show_progress_bar()
        except queue.Empty:
            pass

    def show_progress_bar(self):
        files_downloaded, total_bytes_downloaded = self.get_download_progress()
        current_time = time.time()
        bytes_progress = '{} {}'.format(
            humanize_bytes(total_bytes_downloaded),
            self.make_download_speed(current_time, total_bytes_downloaded))
        sys.stdout.write("\r{} downloaded {} ({} of {} files complete)".format(
            self.make_spinner_char(current_time),
            bytes_progress.ljust(22),
            files_downloaded,
            self.files_to_download
        ))
        sys.stdout.flush()

    def make_spinner_char(self, current_time):
        half_seconds = int(current_time)
        return self.spinner_chars[half_seconds % 4]

    def make_download_speed(self, current_time, total_bytes_downloaded):
        return transfer_speed_str(
            current_time=current_time,
            start_time=self.start_time,
            transferred_bytes=total_bytes_downloaded
        )

    def get_download_progress(self):
        files_downloaded = 0
        total_bytes_downloaded = 0
        for file_id, download_info in self.file_download_statuses.items():
            bytes_downloaded, file_size, file_state = download_info
            # do not include files that were already downloaded in bytes downloaded
            if file_state != FileDownloadState.ALREADY_COMPLETE:
                total_bytes_downloaded += bytes_downloaded
            if bytes_downloaded == file_size:
                files_downloaded += 1
        return files_downloaded, total_bytes_downloaded

    def _pop_ready_download_results(self):
        ready_results = []
        for async_result in self._get_ready_async_results():
            result = async_result.get()
            # retrieve the value from the async result
            ready_results.append(result)
            # remove the async result from the list to watch
            self.async_download_results.remove(async_result)
        return ready_results

    def _get_ready_async_results(self):
        ready_results = []
        for async_result in self.async_download_results:
            if async_result.ready():
                ready_results.append(async_result)
        return ready_results

    def _process_download_results(self, pool, download_results):
        for file_download_state in download_results:
            if file_download_state.is_ok_state():
                file_id = file_download_state.file_id
                size = file_download_state.size
                status = file_download_state.status
                self.file_download_statuses[file_id] = (size, size, file_download_state.state)
                self.download_status_list.append(status)
            elif file_download_state.retries:
                file_download_state.retries -= 1
                # Refresh url in file_download_state
                file_download = self.dds_connection.get_file_download(file_download_state.file_id)
                file_download_state.url = file_download.host + file_download.url
                # Re-run download process
                self._async_download_file(pool, file_download_state)
            else:
                raise ValueError("Error downloading {}\n{}".format(
                    file_download_state.output_path,
                    file_download_state.msg
                ))
        self.show_progress_bar()


def download_file(file_download_state, message_queue=None):
    if os.path.exists(file_download_state.output_path):
        file_hash_status = file_download_state.calculate_file_hash_status()
        if file_hash_status.has_a_valid_hash():
            return file_download_state.mark_already_complete(file_hash_status)
    try:
        file_download_state.state = FileDownloadState.DOWNLOADING
        written_size = download_url_to_path(file_download_state, message_queue)
        return compute_download_result(file_download_state, written_size)
    except URLExpiredException:
        msg = 'Expired URL: {}'.format(file_download_state.url)
        return file_download_state.mark_expired_url(msg)
    except Exception as error:
        return file_download_state.mark_error(msg=str(error))


def download_url_to_path(file_download_state, message_queue=None):
    try:
        response = requests.get(file_download_state.url, stream=True)
        written_size = 0
        response.raise_for_status()
        with open(file_download_state.output_path, "wb") as outfile:
            for chunk in response.iter_content(chunk_size=file_download_state.download_bytes_per_chunk):
                if chunk:  # filter out keep-alive new chunks
                    outfile.write(chunk)
                    written_size += len(chunk)
                    if message_queue:
                        message_queue.put((file_download_state.file_id, written_size, file_download_state.size,
                                           file_download_state.state))
        return written_size
    except HTTPError:
        if response.status_code == SWIFT_EXPIRED_STATUS_CODE or response.status_code == S3_EXPIRED_STATUS_CODE:
            raise URLExpiredException()
        raise


def compute_download_result(file_download_state, written_size):
    if written_size == file_download_state.size:
        file_hash_status = file_download_state.calculate_file_hash_status()
        if file_hash_status.has_a_valid_hash():
            return file_download_state.mark_good(file_hash_status)
        else:
            return file_download_state.mark_error(msg=file_hash_status.get_status_line())
    else:
        msg = "Downloaded file was wrong size. Expected: {} Actual: {}".format(file_download_state.size, written_size)
        return file_download_state.mark_error(msg=msg)
