"""
Objects to upload a number of chunks from a file to a remote store as part of an upload.
"""

import math
from multiprocessing import Process, Queue
from ddsc.core.localstore import HashUtil
from ddsc.core.ddsapi import DataServiceAuth, DataServiceApi


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
        self.filename = local_file.path
        self.content_type = local_file.mimetype
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
        resp = self.data_service.create_upload(project_id, name, self.content_type, size, hash_value, hash_alg)
        self.upload_id = resp.json()['id']
        chunk_processor = self._make_chunk_processor()
        chunk_processor.run()
        self.data_service.complete_upload(self.upload_id)
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
        self.file_uploader = file_uploader
        self.local_file = file_uploader.local_file
        self.config = file_uploader.config
        self.chunk_num = 0
        self.data_service = file_uploader.data_service
        self.watcher = file_uploader.watcher
        self.upload_id = file_uploader.upload_id

    def run(self):
        bytes_per_chunk = self.config.upload_bytes_per_chunk
        processor = self.process_chunk
        if self.local_file.size == 0:
            chunk = ''
            (chunk_hash_alg, chunk_hash_value) = FileUploader.hash_chunk(chunk)
            self.process_chunk(chunk, chunk_hash_alg, chunk_hash_value)
        else:
            with open(self.local_file.path, 'rb') as infile:
                number = 0
                for chunk in SerialChunkProcessor.read_in_chunks(infile, bytes_per_chunk):
                    (chunk_hash_alg, chunk_hash_value) = FileUploader.hash_chunk(chunk)
                    self.process_chunk(chunk, chunk_hash_alg, chunk_hash_value)

    def process_chunk(self, chunk, chunk_hash_alg, chunk_hash_value):
        """
        Method to consume chunks sent by local_file.process_chunks.
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
        self.file_uploader = file_uploader
        self.config = file_uploader.config
        self.file_size = file_uploader.local_file.size
        self.num_workers = file_uploader.config.upload_workers
        self.chunk_size = file_uploader.config.upload_bytes_per_chunk
        self.data_service = file_uploader.data_service
        self.upload_id = file_uploader.upload_id
        self.filename = file_uploader.local_file.path

    def run(self):
        processes = []
        progress_queue = Queue()
        num_chunks = self.determine_num_chunks()
        work_parcels = self.make_work_parcels(num_chunks)
        for (index, num_items) in work_parcels:
            processes.append(self.make_and_start_process(index, num_items, progress_queue))
        self.wait_for_processes(num_chunks, progress_queue, processes)

    def determine_num_chunks(self):
        return int(math.ceil(float(self.file_size) / float(self.chunk_size)))

    def make_work_parcels(self, num_chunks):
        chunks_per_worker = int(math.ceil(float(num_chunks) / float(self.num_workers)))
        return self.divide_work(range(num_chunks), chunks_per_worker)

    def wait_for_processes(self, expected_chunks, progress_queue, processes):
        while expected_chunks > 0:
            progress_type, value = progress_queue.get()
            if progress_type == ChunkSender.SENT:
                chunks_sent = value
                self.file_uploader.watcher.transferring_item(self.file_uploader.local_file, chunks_sent)
                expected_chunks -= chunks_sent
            else:
                error_message = value
                for process in processes:
                    process.terminate()
                raise ValueError(error_message)
        for process in processes:
            process.join()

    def divide_work(self, list_of_indexes, batch_size):
        """
        Given a sequential list of indexes split them into num_parts.
        :param list_of_indexes: [int] list of indexes to be divided up
        :param batch_size: number of items to put in batch(not exact obviously)
        :return: [(int,int)] list of (index, num_items) to be processed
        """
        grouped_indexes = [list_of_indexes[i:i + batch_size] for i in range(0, len(list_of_indexes), batch_size)]
        return [(batch[0], len(batch)) for batch in grouped_indexes]

    def make_and_start_process(self, index, num_items, progress_queue):
        process = Process(target=upload_async,
                       args=(self.data_service.auth.get_auth_data(), self.config,
                       self.upload_id, self.filename, self.chunk_size, index, num_items, progress_queue))
        process.start()
        return process


def upload_async(data_service_auth_data, config, upload_id, filename,
                 chunk_size, index, num_chunks_to_send, progress_queue):
    auth = DataServiceAuth(config)
    auth.set_auth_data(data_service_auth_data)
    data_service = DataServiceApi(auth, config.url)
    sender = ChunkSender(data_service, upload_id, filename, chunk_size, index, num_chunks_to_send, progress_queue)
    error_msg = sender.send()
    if error_msg:
        progress_queue.put((ChunkSender.ERROR, error_msg))


class ChunkSender(object):
    """
    Receives an index and seeks to that part of the file to upload.
    Creates an upload url with the data_service.
    Uploads the bytes at that point in the file.
    Repeats last two steps for each chunk it is supposed to send.
    """
    SENT = "sent"
    ERROR = "error"

    def __init__(self, data_service, upload_id, filename, chunk_size, index, num_chunks_to_send, progress_queue):
        self.data_service = data_service
        self.upload_id = upload_id
        self.filename = filename
        self.chunk_size = chunk_size
        self.index = index
        self.num_chunks_to_send = num_chunks_to_send
        self.progress_queue = progress_queue

    def send(self):
        sent_chunks = 0
        chunk_num = self.index
        with open(self.filename, 'rb') as infile:
            infile.seek(self.index * self.chunk_size)
            while sent_chunks != self.num_chunks_to_send:
                chunk = infile.read(self.chunk_size)
                error_msg = self._send_chunk(chunk, chunk_num)
                if error_msg:
                    return error_msg
                self.progress_queue.put((ChunkSender.SENT, 1))
                chunk_num += 1
                sent_chunks += 1
        return None

    def _send_chunk(self, chunk, chunk_num):
        chunk_hash_alg, chunk_hash_value = ChunkSender.get_hash(chunk)
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

    @staticmethod
    def get_hash(chunk):
        hash = HashUtil()
        hash.add_chunk(chunk)
        return hash.hexdigest()

