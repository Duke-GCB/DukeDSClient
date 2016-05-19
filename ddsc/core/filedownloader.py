"""
Downloads a file based on ranges.
"""
import math
import tempfile
import requests
from multiprocessing import Process, Queue
from ddsc.core.util import ProgressQueue, wait_for_processes

DOWNLOAD_FILE_CHUNK_SIZE = 20 * 1024 * 1024
MIN_DOWNLOAD_CHUNK_SIZE = DOWNLOAD_FILE_CHUNK_SIZE


class FileDownloader(object):
    """
    Downloads a file using a number of worker processes who download different ranges.
    Creates an empty file.
    Each worker seeks to their spot and streams the data from their url data into the file.
    """
    def __init__(self, config, remote_file, url_parts, path, watcher):
        """
        Setup details on what to download and watcher to notify of progress.
        :param config: Config: configuration settings for download (number workers)
        :param remote_file: RemoteFile: info about the file we are downloading
        :param url_parts: dictionary of fields related to url ('http_verb','host','http_headers') received from duke_data_service
        :param path: str: path to where we will save the file
        :param watcher: ProgressPrinter: we notify of our progress
        """
        self.config = config
        self.remote_file = remote_file
        self.file_size = remote_file.size
        self.url_parts = url_parts
        self.path = path
        self.watcher = watcher
        self.file_parts = []

    @property
    def http_verb(self):
        verb = self.url_parts['http_verb']
        if verb != 'GET':
            raise ValueError("Unsupported download method: {}".format(verb))
        return verb

    @property
    def url(self):
        """
        Returns the full url based on url_parts.
        The 'url' part of url_parts is actually the path.
        """
        return self.url_parts['host'] + self.url_parts['url']

    @property
    def http_headers(self):
        return self.url_parts['http_headers']

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
        self.file_parts = []
        ranges = self.make_ranges()
        processes = []
        progress_queue = ProgressQueue(Queue())
        self.make_big_empty_file()
        for range_start, range_end in ranges:
            (temp_handle, temp_path) = tempfile.mkstemp()
            self.file_parts.append(temp_path)
            processes.append(self.make_and_start_process(range_start, range_end, progress_queue))
        wait_for_processes(processes, int(self.file_size), progress_queue, self.watcher, self.remote_file)

    def make_big_empty_file(self):
        """
        Write out a empty file so the workers can seek to where they should write and write their data.
        """
        with open(self.path, "wb") as outfile:
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
        http_headers = {'Range': 'bytes={}-{}'.format(range_start, range_end)}
        if self.http_headers:
            http_headers.update(self.http_headers)
        seek_amt = range_start
        process = Process(target=download_async,
                          args=(self.url, http_headers, self.path, seek_amt, progress_queue))
        process.start()
        return process


def download_async(url, headers, path, seek_amt, progress_queue):
    """
    Called in separate process to download a chunk of a file.
    :param url: str: url to file we should download
    :param headers: dict: header to use with url, should contain Range to limit what we download
    :param path: str: path to where we should save our chunk we download
    :param seek_amt: int: offset to seek before writing our chunk out to path
    :param progress_queue: ProgressQueue: queue of tuples we will add progress/errors to
    :return:
    """
    downloader = ChunkDownloader(url, headers, path, seek_amt, progress_queue)
    downloader.run()


class ChunkDownloader(object):
    """
    Downloads part of a file and writes it to a location in a local pre-existing file.
    This runs in a separate process from the main application.
    """
    def __init__(self, url, http_headers, path, seek_amt, progress_queue):
        """
        Setup for downloading part of a file.
        :param url: str: url to the file
        :param http_headers: dict: headers for use with the url should contain byte Range
        :param path: str: path to file to write data to
        :param seek_amt: int: offset amount to seek into the file
        :param progress_queue: ProgressQueue: queue we notify of progress or errors
        """
        self.url = url
        self.http_headers = http_headers
        self.path = path
        self.seek_amt = seek_amt
        self.progress_queue = progress_queue

    def run(self):
        try:
            response = requests.get(self.url, headers=self.http_headers, stream=True)
            # open file for read/write without truncating
            with open(self.path, 'r+b') as outfile:
                outfile.seek(self.seek_amt)
                for chunk in response.iter_content(chunk_size=DOWNLOAD_FILE_CHUNK_SIZE):
                    if chunk:  # filter out keep-alive chunks
                        outfile.write(chunk)
                        self.progress_queue.processed(len(chunk))
        except Exception as ex:
            self.progress_queue.error(str(ex))

