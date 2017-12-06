"""
Downloads a file based on ranges.
"""
import math
import time
import requests
from multiprocessing import Process, Queue
from ddsc.core.util import ProgressQueue, wait_for_processes
from ddsc.core.remotestore import RemoteStore
from ddsc.core.ddsapi import retry_until_resource_is_consistent

DOWNLOAD_FILE_CHUNK_SIZE = 20 * 1024 * 1024
MIN_DOWNLOAD_CHUNK_SIZE = DOWNLOAD_FILE_CHUNK_SIZE

PARTIAL_DOWNLOAD_RETRY_TIMES = 5
PARTIAL_DOWNLOAD_RETRY_SECONDS = 20


class FileDownloader(object):
    """
    Downloads a file using a number of worker processes who download different ranges.
    Creates an empty file.
    Each worker seeks to their spot and streams the data from their url data into the file.
    """
    def __init__(self, config, remote_file, path, watcher):
        """
        Setup details on what to download and watcher to notify of progress.
        :param config: Config: configuration settings for download (number workers)
        :param remote_file: RemoteFile: details about DukeDS file we will download
        :param path: str: path to where we will save the file
        :param watcher: ProgressPrinter: we notify of our progress
        """
        self.config = config
        self.remote_file = remote_file
        self.file_size = remote_file.size
        self.path = path
        self.watcher = watcher

    def make_ranges(self):
        """
        Divides file size into an array of ranges to be downloaded by workers.
        :return: [(int,int)]: array of (start, end) tuples
        """
        size = int(self.file_size)
        bytes_per_chunk = self.determine_bytes_per_chunk()
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

    def determine_bytes_per_chunk(self):
        """
        Calculate the size of chunk a worker should download.
        The last worker may download less than this depending on file size.
        :return: int: byte size for a worker
        """
        workers = self.config.download_workers
        if not workers or workers == 'None':
            workers = 1
        size = int(self.file_size)
        bytes_per_chunk = int(math.ceil(size / float(workers)))
        if bytes_per_chunk < MIN_DOWNLOAD_CHUNK_SIZE:
            bytes_per_chunk = MIN_DOWNLOAD_CHUNK_SIZE
        return bytes_per_chunk

    def run(self):
        """
        Download a file using separate processes.
        """
        ranges = self.make_ranges()
        processes = []
        progress_queue = ProgressQueue(Queue())
        self.make_big_empty_file()
        for range_start, range_end in ranges:
            processes.append(self.make_and_start_process(range_start, range_end, progress_queue))
        wait_for_processes(processes, int(self.file_size), progress_queue, self.watcher, self.remote_file)

    def make_big_empty_file(self):
        """
        Write out a empty file so the workers can seek to where they should write and write their data.
        """
        with open(self.path, "wb") as outfile:
            if self.file_size > 0:
                outfile.seek(int(self.file_size) - 1)
                outfile.write(b'\0')

    def make_and_start_process(self, range_start, range_end, progress_queue):
        """
        Create a process that will download the specified range and notify progress_queue of progress or errors.
        :param range_start: int: file offset to download
        :param range_end: int: file ending offset to download
        :param progress_queue: ProgressQueue: queue to notify as we make progress
        :return: Process: the process we created
        """
        range_headers = {'Range': 'bytes={}-{}'.format(range_start, range_end)}
        bytes_to_read = range_end - range_start + 1
        seek_amt = range_start
        process = Process(target=download_async,
                          args=(self.config, self.remote_file.id, range_headers,
                                self.path, seek_amt, bytes_to_read, progress_queue))
        process.start()
        return process


def download_async(config, remote_file_id, range_headers, path, seek_amt, bytes_to_read, progress_queue):
    """
    Called in separate process to download a chunk of a file.
    :param config: Config: configuration settings for download
    :param remote_file_id: str: uuid of the file we will download
    :param range_headers: dict: range request header to filter amount downloaded
    :param path: str: path to where we should save our chunk we download
    :param seek_amt: offset to seek before writing our chunk out to path
    :param bytes_to_read: int: how many bytes of data we will receive from the url
    :param progress_queue: ProgressQueue: queue of tuples we will add progress/errors to
    """
    """
    Called in separate process to download a chunk of a file.
    :param url: str: url to file we should download
    :param headers: dict: header to use with url, should contain Range to limit what we download
    :param path: str: path to where we should save our chunk we download
    :param seek_amt: int: offset to seek before writing our chunk out to path
    :param bytes_to_read: int: how many bytes of data we will receive from the url
    :param progress_queue: ProgressQueue: queue of tuples we will add progress/errors to
    """
    partial_download_failures = 0
    downloader = None
    remote_store = RemoteStore(config)
    while True:
        try:
            url, headers = get_file_chunk_url_and_headers(remote_store, remote_file_id, range_headers, progress_queue)
            downloader = ChunkDownloader(url, headers, path, seek_amt, bytes_to_read, progress_queue)
            downloader.run()
            break
        except (PartialChunkDownloadError, requests.exceptions.ConnectionError) as err:
            # partial downloads can be due to flaky connections so we should retry a few times
            partial_download_failures += 1
            if partial_download_failures <= PARTIAL_DOWNLOAD_RETRY_TIMES:
                if downloader:
                    downloader.revert_progress()  # Notify progress monitor to undo our current progress
                time.sleep(PARTIAL_DOWNLOAD_RETRY_SECONDS)
                # loop will call ChunkDownloader run again
            else:
                progress_queue.error(str(err))
                break
        except Exception as err:
            progress_queue.error(str(err))
            break


def get_file_chunk_url_and_headers(remote_store, remote_file_id, range_headers, progress_queue):
    """
    Return url and headers to use for downloading part of a file.
    :param remote_store: RemoteStore: used to fetch url and default headers
    :param remote_file_id: str: uuid of the file we will download
    :param range_headers: dict: range request header to filter amount downloaded
    :param progress_queue: ProgressQueue: queue of tuples we will add progress/errors to
    :return: str, dict: url to download and headers to use (combines range_headers and those returned by DukeDS)
    """
    get_file_url = GetFileUrl(remote_store.data_service, remote_file_id)
    url_json = retry_until_resource_is_consistent(get_file_url.run, progress_queue)
    url = url_json['host'] + url_json['url']
    additional_headers = url_json['http_headers']
    headers = range_headers
    if additional_headers:
        headers.update(additional_headers)
    return url, headers


class GetFileUrl(object):
    def __init__(self, data_service, remote_file_id):
        self.data_service = data_service
        self.remote_file_id = remote_file_id

    def run(self):
        return self.data_service.get_file_url(self.remote_file_id).json()


class ChunkDownloader(object):
    """
    Downloads part of a file and writes it to a location in a local pre-existing file.
    This runs in a separate process from the main application.
    """
    def __init__(self, url, http_headers, path, seek_amt, bytes_to_read, progress_queue):
        """
        Setup for downloading part of a file.
        :param url: str: url to the file
        :param http_headers: dict: headers for use with the url should contain byte Range
        :param path: str: path to file to write data to
        :param seek_amt: int: offset amount to seek into the file
        :param bytes_to_read: int: how many bytes of data we will receive from the url
        :param progress_queue: ProgressQueue: queue we notify of progress or errors
        """
        self.url = url
        self.http_headers = http_headers
        self.path = path
        self.seek_amt = seek_amt
        self.bytes_to_read = bytes_to_read
        self.actual_bytes_read = 0
        self.progress_queue = progress_queue

    def run(self):
        """
        Download part of a file at self.url and write it to the appropriate section of self.path.
        """
        response = requests.get(self.url, headers=self.http_headers, stream=True)
        response.raise_for_status()
        self._write_response_to_file(response)
        self._verify_download_complete()

    def _write_response_to_file(self, response):
        """
        Write response to the appropriate section of the file at self.path.
        :param response: requests.Response: response containing stream-able data
        """
        with open(self.path, 'r+b') as outfile:  # open file for read/write (no truncate)
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
            raise TooLargeChunkDownloadError(self.actual_bytes_read, self.bytes_to_read, self.path)
        self.progress_queue.processed(num_bytes_read)

    def _verify_download_complete(self):
        """
        Make sure we received all the data
        """
        if self.actual_bytes_read > self.bytes_to_read:
            raise TooLargeChunkDownloadError(self.actual_bytes_read, self.bytes_to_read, self.path)
        elif self.actual_bytes_read < self.bytes_to_read:
            raise PartialChunkDownloadError(self.actual_bytes_read, self.bytes_to_read, self.path)

    def revert_progress(self):
        """
        Update progress monitor with negative number so it is accurate since this download failed.
        """
        undo_size = self.actual_bytes_read * -1
        self.progress_queue.processed(undo_size)


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
