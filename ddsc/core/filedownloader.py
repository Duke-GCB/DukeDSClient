"""
Downloads a file based on ranges.
"""
import os
import math
import tempfile
import requests
from multiprocessing import Process, Queue

DOWNLOAD_FILE_CHUNK_SIZE = 20 * 1024 * 1024
MIN_DOWNLOAD_CHUNK_SIZE = 20 * 1024 * 1024


class FileDownloader(object):
    def __init__(self, config, remote_file, url_parts, path, file_size, watcher):
        self.config = config
        self.remote_file = remote_file
        self.url_parts = url_parts
        self.path = path
        self.file_size = file_size
        self.watcher = watcher
        self.file_parts = []

    @property
    def http_verb(self):
        verb = self.url_parts['http_verb']
        if verb != 'GET':
            raise ValueError("Unsupported download method: {}".format(verb))
        return verb

    @property
    def host(self):
        return self.url_parts['host']

    @property
    def url(self):
        return self.url_parts['url']

    @property
    def http_headers(self):
        return self.url_parts['http_headers']

    def make_ranges(self):
        size = int(self.file_size)
        bytes_per_chunk = self.determine_bytes_per_chunk()
        start = 0
        ranges = []
        while size > 0:
            amount = bytes_per_chunk
            if amount > size:
                amount = size
            ranges.append("{}-{}".format(start, start + amount - 1))
            start += amount
            size -= amount
        return ranges

    def determine_bytes_per_chunk(self):
        workers = self.config.download_workers
        if not workers:
            workers = 1
        size = int(self.file_size)
        bytes_per_chunk = int(math.ceil(size / float(workers)))
        if bytes_per_chunk < MIN_DOWNLOAD_CHUNK_SIZE:
            bytes_per_chunk = MIN_DOWNLOAD_CHUNK_SIZE
        return bytes_per_chunk

    def run(self):
        self.file_parts = []
        ranges = self.make_ranges()
        processes = []
        progress_queue = Queue()
        for range in ranges:
            (temp_handle, temp_path) = tempfile.mkstemp()
            self.file_parts.append(temp_path)
            processes.append(self.make_and_start_process(temp_path, range, progress_queue))
        self.wait_for_processes(progress_queue, processes)
        self.merge_file_parts()

    def make_and_start_process(self, temp_path, range, progress_queue):
        http_headers = {'Range': 'bytes=' + range}
        process = Process(target=download_async,
                          args=(self.host + self.url, http_headers, temp_path, progress_queue))
        process.start()
        return process

    def wait_for_processes(self, progress_queue, processes):
        file_bytes = int(self.file_size)
        while file_bytes > 0:
            progress_type, value = progress_queue.get()
            if progress_type == ChunkDownloader.RECEIVED:
                chunk_size = value
                self.watcher.transferring_item(self.remote_file, increment_amt=chunk_size)
                file_bytes -= chunk_size
            else:
                error_message = value
                for process in processes:
                    process.terminate()
                raise ValueError(error_message)
        for process in processes:
            process.join()

    def merge_file_parts(self):
        with open(self.path, "wb") as outfile:
            for temp_path in self.file_parts:
                with open(temp_path, "rb") as infile:
                    data = infile.readlines(MIN_DOWNLOAD_CHUNK_SIZE)
                    if not data:
                        break
                    outfile.write(data)
        for temp_path in self.file_parts:
            os.remove(temp_path)


def download_async(url, headers, path, progress_queue):
    downloader = ChunkDownloader(url, headers, path, progress_queue)
    downloader.run()


class ChunkDownloader(object):
    RECEIVED = 'received'
    ERROR = 'error'

    def __init__(self, url, http_headers, path, progress_queue):
        self.url = url
        self.http_headers = http_headers
        self.path = path
        self.progress_queue = progress_queue

    def run(self):
        response = requests.get(self.url, headers=self.http_headers, stream=True)
        with open(self.path, 'wb') as outfile:
            for chunk in response.iter_content(chunk_size=DOWNLOAD_FILE_CHUNK_SIZE):
                if chunk:  # filter out keep-alive chunks
                    outfile.write(chunk)
                    self.progress_queue.put((ChunkDownloader.RECEIVED, len(chunk)))

