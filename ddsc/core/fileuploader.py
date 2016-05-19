"""
Objects to upload a number of chunks from a file to a remote store as part of an upload.
"""

import math
from multiprocessing import Process, Queue
from ddsc.core.localstore import HashUtil
from ddsc.core.ddsapi import DataServiceAuth, DataServiceApi
from ddsc.core.util import ProgressQueue, wait_for_processes

class FileUploader(object):
    """
    Handles sending the contents of a file to a a remote data_service.
    Process:
    1) It creates an 'upload' with the remote service
    2) Uses a chunk_processor to send the parts
    3) Sends the complete message to finalize the 'upload'
    4) Sends create_file message to remote store with the 'upload' id
    """
    def __init__(self, config, data_service, local_file, watcher):
        """
        Setup for sending to remote store.
        :param config: ddsc.config.Config user configuration settings from YAML file/environment
        :param data_service: DataServiceApi data service we are sending the content to.
        :param local_file: LocalFile file we are sending to remote store
        :param watcher: ProgressPrinter we notify of our progress
        """
        self.config = config
        self.data_service = data_service
        self.local_file = local_file
        self.upload_id = None
        self.watcher = watcher

    def _make_chunk_processor(self):
        """
        Based on the passed in config determine if we should use parallel chunk processor or serial one.
        """
        if self.config.upload_workers:
            if self.config.upload_workers > 1:
                return ParallelChunkProcessor(self)
        return SerialChunkProcessor(self)

    def upload(self, project_id, parent_kind, parent_id):
        """
        Upload file contents to project within a specified parent.
        :param project_id: str project uuid
        :param parent_kind: str type of parent ('dds-project' or 'dds-folder')
        :param parent_id: str uuid of parent
        :return: str uuid of the newly uploaded file
        """
        size = self.local_file.size
        (hash_alg, hash_value) = self.local_file.get_hashpair()
        name = self.local_file.name
        resp = self.data_service.create_upload(project_id, name, self.local_file.mimetype, size, hash_value, hash_alg)
        self.upload_id = resp.json()['id']
        chunk_processor = self._make_chunk_processor()
        chunk_processor.run()
        self.data_service.complete_upload(self.upload_id)
        if self.local_file.remote_id:
            file_id = self.local_file.remote_id
            self.data_service.update_file(file_id, self.upload_id)
            return file_id
        else:
            result = self.data_service.create_file(parent_kind, parent_id, self.upload_id)
            return result.json()['id']

    @staticmethod
    def send_file_external(data_service, url_json, chunk):
        """
        Send chunk to external store specified in url_json.
        Raises ValueError on upload failure.
        :param data_service: data service to use for sending chunk
        :param url_json: dict contains where/how to upload chunk
        :param chunk: data to be uploaded
        """
        http_verb = url_json['http_verb']
        host = url_json['host']
        url = url_json['url']
        http_headers = url_json['http_headers']
        resp = data_service.send_external(http_verb, host, url, http_headers, chunk)
        if resp.status_code != 200 and resp.status_code != 201:
            raise ValueError("Failed to send file to external store. Error:" + str(resp.status_code))

    @staticmethod
    def hash_chunk(chunk):
        """Creates a hash from the bytes in chunk."""
        hash = HashUtil()
        hash.add_chunk(chunk)
        return hash.hexdigest()


class SerialChunkProcessor(object):
    """
    Uploads chunks one at a time without any additional processes.
    """
    def __init__(self, file_uploader):
        """
        Send chunks in the file specified in file_uploader to the remote data service.
        :param file_uploader: FileUploader contains all data we need to upload chunks of a file.
        """
        self.local_file = file_uploader.local_file
        self.bytes_per_chunk = file_uploader.config.upload_bytes_per_chunk
        self.data_service = file_uploader.data_service
        self.watcher = file_uploader.watcher
        self.upload_id = file_uploader.upload_id
        self.chunk_num = 0

    def run(self):
        """
        Sends contents of a local file to a remote data service.
        """
        processor = self.process_chunk
        if self.local_file.size == 0:
            chunk = ''
            (chunk_hash_alg, chunk_hash_value) = FileUploader.hash_chunk(chunk)
            self.process_chunk(chunk, chunk_hash_alg, chunk_hash_value)
        else:
            with open(self.local_file.path, 'rb') as infile:
                number = 0
                for chunk in SerialChunkProcessor.read_in_chunks(infile, self.bytes_per_chunk):
                    (chunk_hash_alg, chunk_hash_value) = FileUploader.hash_chunk(chunk)
                    self.process_chunk(chunk, chunk_hash_alg, chunk_hash_value)

    def process_chunk(self, chunk, chunk_hash_alg, chunk_hash_value):
        """
        Creates 'upload' url and send bytes to that url.
        Raises ValueError on upload failure.
        :param chunk: bytes part of the file to send
        :param chunk_hash_alg: str the algorithm used to hash chunk
        :param chunk_hash_value: str the hash value of chunk
        """
        self.watcher.transferring_item(self.local_file)
        resp = self.data_service.create_upload_url(self.upload_id, self.chunk_num, len(chunk),
                                                   chunk_hash_value, chunk_hash_alg)
        if resp.status_code == 200:
            FileUploader.send_file_external(self.data_service, resp.json(), chunk)
            self.chunk_num += 1
        else:
            raise ValueError("Failed to retrieve upload url status:" + str(resp.status_code))

    @staticmethod
    def read_in_chunks(infile, blocksize):
        """
        Generator to read chunks lazily.
        :param infile: filehandle file to read from
        :param blocksize: int size of blocks to read
        """
        """"""
        while True:
            data = infile.read(blocksize)
            if not data:
                break
            yield data


class ParallelChunkProcessor(object):
    """
    Uploads grouped chunks in separate processes.
    """
    def __init__(self, file_uploader):
        """
        Send chunks in the file specified in file_uploader to the remote data service using multiple processes.
        :param file_uploader: FileUploader contains all data we need to upload chunks of a file.
        """
        self.config = file_uploader.config
        self.data_service = file_uploader.data_service
        self.upload_id = file_uploader.upload_id
        self.watcher = file_uploader.watcher
        self.local_file = file_uploader.local_file

    def run(self):
        """
        Sends contents of a local file to a remote data service.
        """
        processes = []
        progress_queue = ProgressQueue(Queue())
        num_chunks = ParallelChunkProcessor.determine_num_chunks(self.config.upload_bytes_per_chunk,
                                                                 self.local_file.size)
        work_parcels = ParallelChunkProcessor.make_work_parcels(self.config.upload_workers, num_chunks)
        for (index, num_items) in work_parcels:
            processes.append(self.make_and_start_process(index, num_items, progress_queue))
        wait_for_processes(processes, num_chunks, progress_queue, self.watcher, self.local_file)


    @staticmethod
    def determine_num_chunks(chunk_size, file_size):
        """
        Figure out how many pieces we are sending the file in.
        """
        return int(math.ceil(float(file_size) / float(chunk_size)))

    @staticmethod
    def make_work_parcels(upload_workers, num_chunks):
        """
        Make groups so we can split up num_chunks into similar sizes.
        Rounds up trying to keep work evenly split so sometimes it will not use all workers.
        For very small numbers it can result in (upload_workers-1) total workers.
        For example if there are two few items to distribute.
        :param upload_workers: int target number of workers
        :param num_chunks: int number of total items we need to send
        :return [(index, num_items)] -  an array of tuples where array element will be in a separate process.
        """
        chunks_per_worker = int(math.ceil(float(num_chunks) / float(upload_workers)))
        return ParallelChunkProcessor.divide_work(range(num_chunks), chunks_per_worker)

    @staticmethod
    def divide_work(list_of_indexes, batch_size):
        """
        Given a sequential list of indexes split them into num_parts.
        :param list_of_indexes: [int] list of indexes to be divided up
        :param batch_size: number of items to put in batch(not exact obviously)
        :return: [(int,int)] list of (index, num_items) to be processed
        """
        grouped_indexes = [list_of_indexes[i:i + batch_size] for i in range(0, len(list_of_indexes), batch_size)]
        return [(batch[0], len(batch)) for batch in grouped_indexes]

    def make_and_start_process(self, index, num_items, progress_queue):
        """
        Create and start a process to upload num_items chunks from our file starting at index.
        :param index: int offset into file(must be multiplied by upload_bytes_per_chunk to get actual location)
        :param num_items: int number chunks to send
        :param progress_queue: ProgressQueue queue to send notifications of progress or errors
        """
        process = Process(target=upload_async,
                       args=(self.data_service.auth.get_auth_data(), self.config, self.upload_id,
                             self.local_file.path, index, num_items, progress_queue))
        process.start()
        return process


def upload_async(data_service_auth_data, config, upload_id,
                 filename, index, num_chunks_to_send, progress_queue):
    """
    Method run in another process called from ParallelChunkProcessor.make_and_start_process.
    :param data_service_auth_data: tuple of auth data for rebuilding DataServiceAuth
    :param config: dds.Config configuration settings to use during upload
    :param upload_id: uuid unique id of the 'upload' we are uploading chunks into
    :param filename: str path to file who's contents we will be uploading
    :param index: int offset into filename where we will start sending bytes from (must multiply by upload_bytes_per_chunk)
    :param num_chunks_to_send: int number of chunks of config.upload_bytes_per_chunk size to send.
    :param progress_queue: ProgressQueue queue to send notifications of progress or errors
    """
    auth = DataServiceAuth(config)
    auth.set_auth_data(data_service_auth_data)
    data_service = DataServiceApi(auth, config.url)
    sender = ChunkSender(data_service, upload_id, filename, config.upload_bytes_per_chunk, index, num_chunks_to_send, progress_queue)
    error_msg = sender.send()
    if error_msg:
        progress_queue.error(error_msg)


class ChunkSender(object):
    """
    Receives an index and seeks to that part of the file to upload.
    Creates an upload url with the data_service.
    Uploads the bytes at that point in the file.
    Repeats last two steps for each chunk it is supposed to send.
    """
    def __init__(self, data_service, upload_id, filename, chunk_size, index, num_chunks_to_send, progress_queue):
        """
        Sends num_chunks_to_send from filename at offset index*chunk_size.
        :param data_service: DataServiceApi remote service we will be uploading to
        :param upload_id: str upload uuid we are sending chunks part of
        :param filename: str path to file on disk we are uploading parts of
        :param chunk_size: int size of block we will upload
        :param index: int index into filename content(must multiply by chunk_size during seek)
        :param num_chunks_to_send: how many chunks of chunk_size should we upload
        :param progress_queue: ProgressQueue queue we will send updates or errors to.
        """
        self.data_service = data_service
        self.upload_id = upload_id
        self.filename = filename
        self.chunk_size = chunk_size
        self.index = index
        self.num_chunks_to_send = num_chunks_to_send
        self.progress_queue = progress_queue

    def send(self):
        """
        For each chunk we need to send, create upload url and send bytes.
        :return None when everything is ok otherwise returns a string error message.
        """
        sent_chunks = 0
        chunk_num = self.index
        with open(self.filename, 'rb') as infile:
            infile.seek(self.index * self.chunk_size)
            while sent_chunks != self.num_chunks_to_send:
                chunk = infile.read(self.chunk_size)
                error_msg = self._send_chunk(chunk, chunk_num)
                if error_msg:
                    return error_msg
                self.progress_queue.processed(1)
                chunk_num += 1
                sent_chunks += 1
        return None

    def _send_chunk(self, chunk, chunk_num):
        """
        Send a single chunk to the remote service.
        :param chunk: bytes data we are uploading
        :param chunk_num: int number associated with this chunk
        """
        chunk_hash_alg, chunk_hash_value = FileUploader.hash_chunk(chunk)
        resp = self.data_service.create_upload_url(self.upload_id, chunk_num, len(chunk),
                                                   chunk_hash_value, chunk_hash_alg)
        if resp.status_code == 200:
            try:
                FileUploader.send_file_external(self.data_service, resp.json(), chunk)
                return None
            except ValueError as err:
                return str(err)
        else:
            return "Failed to retrieve upload url status:" + str(resp.status_code)

