"""
Objects to upload a number of chunks from a file to a remote store as part of an upload.
"""

import math
from multiprocessing import Process, Queue
from ddsc.core.ddsapi import DataServiceAuth, DataServiceApi
from ddsc.core.util import ProgressQueue, wait_for_processes
from ddsc.core.localstore import HashData


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
        self.upload_operations = FileUploadOperations(self.data_service)
        self.local_file = local_file
        self.upload_id = None
        self.watcher = watcher

    def upload(self, project_id, parent_kind, parent_id):
        """
        Upload file contents to project within a specified parent.
        :param project_id: str project uuid
        :param parent_kind: str type of parent ('dds-project' or 'dds-folder')
        :param parent_id: str uuid of parent
        :return: str uuid of the newly uploaded file
        """
        path_data = self.local_file.get_path_data()
        hash_data = path_data.get_hash()
        self.upload_id = self.upload_operations.create_upload(project_id, path_data, hash_data)
        ParallelChunkProcessor(self).run()
        parent_data = ParentData(parent_kind, parent_id)
        return self.upload_operations.finish_upload(self.upload_id, hash_data, parent_data, self.local_file.remote_id)


class ParentData(object):
    """
    Holds data about the parent of a file or folder.
    """
    def __init__(self, parent_kind, parent_id):
        """
        DukeDS info about a parent.
        :param parent_kind: str: dds_folder/dds_file
        :param parent_id: str: uuid of the parent
        """
        self.kind = parent_kind
        self.id = parent_id


class FileUploadOperations(object):
    """
    Data Service Upload file operations.
    Wraps up upload process:
    1) create upload
    2) create url for part of file
    3) upload part of file
    4) complete upload then create new file or update existing file
    """
    def __init__(self, data_service):
        """
        Setup with specified data service we will communicate with.
        :param data_service: DataServiceApi data service we are uploading the file to.
        """
        self.data_service = data_service

    def create_upload(self, project_id, path_data, hash_data):
        """
        Create upload so we can send call further methods.
        :param project_id: str: uuid of the project
        :param path_data: PathData: holds file system data about the file we are uploading
        :param hash_data: HashData: contains hash alg and value for the file we are uploading
        :return: str: uuid for the upload
        """
        name = path_data.name()
        mime_type = path_data.mime_type()
        size = path_data.size()
        resp = self.data_service.create_upload(project_id, name, mime_type, size, hash_data.value, hash_data.alg)
        return resp.json()['id']

    def create_file_chunk_url(self, upload_id, chunk_num, chunk):
        """
        Create a url for uploading a particular chunk to the datastore.
        :param upload_id: str: uuid of the upload this chunk is for
        :param chunk_num: int: where in the file does this chunk go
        :param chunk: bytes: data we are going to upload
        :return:
        """
        chunk_len = len(chunk)
        hash_data = HashData.create_from_chunk(chunk)
        resp = self.data_service.create_upload_url(upload_id, chunk_num, chunk_len, hash_data.value, hash_data.alg)
        return resp.json()

    def send_file_external(self, url_json, chunk):
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
        resp = self.data_service.send_external(http_verb, host, url, http_headers, chunk)
        if resp.status_code != 200 and resp.status_code != 201:
            raise ValueError("Failed to send file to external store. Error:" + str(resp.status_code))

    def finish_upload(self, upload_id, hash_data, parent_data, remote_file_id):
        """
        Complete the upload and create or update the file.
        :param upload_id: str: uuid of the upload we are completing
        :param hash_data: HashData: hash info about the file
        :param parent_data: ParentData: info about the parent of this file
        :param remote_file_id: str: uuid of this file if it already exists or None if it is a new file
        :return: str: uuid of this file
        """
        self.data_service.complete_upload(upload_id, hash_data.value, hash_data.alg)
        if remote_file_id:
            self.data_service.update_file(remote_file_id, upload_id)
            return remote_file_id
        else:
            result = self.data_service.create_file(parent_data.kind, parent_data.id, upload_id)
            return result.json()['id']


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
        NOTE: duke-data-service requires an empty chunk to be uploaded for empty files.
        """
        if file_size == 0:
            return 1
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
    sender = ChunkSender(data_service, upload_id, filename, config.upload_bytes_per_chunk, index, num_chunks_to_send,
                         progress_queue)
    return sender.send()


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
        self.upload_operations = FileUploadOperations(self.data_service)
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
                self._send_chunk(chunk, chunk_num)
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
        url_info = self.upload_operations.create_file_chunk_url(self.upload_id, chunk_num, chunk)
        self.upload_operations.send_file_external(url_info, chunk)

