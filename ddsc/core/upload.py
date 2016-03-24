import datetime
from ddsc.core.localstore import LocalProject
from ddsc.core.remotestore import RemoteStore
from ddsc.core.util import ProgressPrinter, ProjectWalker


class ProjectUpload(object):
    """
    Allows uploading a local project to a remote duke-data-service.
    """
    def __init__(self, config, project_name, folders, follow_symlinks=False):
        """
        Setup for uploading folders dictionary of paths to project_name using config.
        :param config: Config configuration for performing the upload(url, keys, etc)
        :param project_name: str name of the project we will upload files to
        :param folders: [str] list of paths of files/folders to upload to the project
        :param follow_symlinks: bool if true we will traverse symbolic linked directories
        """
        self.config = config
        self.remote_store = RemoteStore(config)
        self.project_name = project_name
        self.remote_project = self.remote_store.fetch_remote_project(project_name)
        self.local_project = ProjectUpload._load_local_project(folders, follow_symlinks)
        self.local_project.update_remote_ids(self.remote_project)
        self.different_items = self._count_differences()

    @staticmethod
    def _load_local_project(folders, follow_symlinks):
        local_project = LocalProject(followsymlinks=follow_symlinks)
        local_project.add_paths(folders)
        return local_project

    def _count_differences(self):
        """
        Count how many things we will be sending.
        :param local_project: LocalProject project we will send data from
        :return: LocalOnlyCounter contains counts for various items
        """
        different_items = LocalOnlyCounter(self.config.upload_bytes_per_chunk)
        different_items.walk_project(self.local_project)
        return different_items

    def needs_to_upload(self):
        """
        Is there anything in the local project different from the remote project.
        :return: bool is there any point in calling upload()
        """
        return self.different_items.total_items() != 0

    def run(self):
        """
        Upload different items within local_project to remote store showing a progress bar.
        """
        progress_printer = ProgressPrinter(self.different_items.total_items(), msg_verb='sending')
        sender = RemoteContentSender(self.config, self.remote_store.data_service, self.local_project.remote_id,
                                     self.project_name, progress_printer)
        sender.walk_project(self.local_project)
        progress_printer.finished()

    def get_differences_summary(self):
        """
        Print a summary of what is to be done.
        :param different_items: LocalOnlyCounter item that contains the summary
        """
        return 'Uploading {}.'.format(self.different_items.result_str())

    def get_upload_report(self):
        """
        Generate and print a report onto stdout.
        """
        report = UploadReport(self.project_name)
        report.walk_project(self.local_project)
        return report.get_content()

    def get_url_msg(self):
        """
        Print url to view the project via dds portal.
        """
        msg = 'URL to view project'
        project_id = self.local_project.remote_id
        url = '{}: https://{}/portal/#/project/{}'.format(msg, self.config.get_url_base(), project_id)
        return url


class LocalOnlyCounter(object):
    """
    Visitor that counts items that need to be sent in LocalContent.
    """
    def __init__(self, bytes_per_chunk):
        self.projects = 0
        self.folders = 0
        self.files = 0
        self.chunks = 0
        self.bytes_per_chunk = bytes_per_chunk

    def walk_project(self, project):
        """
        Increment counters for each project, folder, and files calling visit methods below.
        :param project: LocalProject project we will count items of.
        """
        # This method will call visit_project, visit_folder, and visit_file below as it walks the project tree.
        ProjectWalker.walk_project(project, self)

    def visit_project(self, item):
        """
        Increments counter if the project is not already remote.
        :param item: LocalProject
        """
        if not item.remote_id:
            self.projects += 1

    def visit_folder(self, item, parent):
        """
        Increments counter if item is not already remote
        :param item: LocalFolder
        :param parent: LocalFolder/LocalProject
        """
        if not item.remote_id:
            self.folders += 1

    def visit_file(self, item, parent):
        """
        Increments counter if item needs to be sent.
        :param item: LocalFile
        :param parent: LocalFolder/LocalProject
        """
        if item.need_to_send:
            self.files += 1
            self.chunks += item.count_chunks(self.bytes_per_chunk)

    def total_items(self):
        """
        Total number of files/folders/chunks that need to be sent.
        :return: int number of items to be sent.
        """
        return self.projects + self.folders + self.chunks

    def result_str(self):
        """
        Return a string representing the totals contained herein.
        :return: str counts/types string
        """
        return '{}, {}, {}'.format(LocalOnlyCounter.plural_fmt('project', self.projects),
                                   LocalOnlyCounter.plural_fmt('folder', self.folders),
                                   LocalOnlyCounter.plural_fmt('file', self.files))

    @staticmethod
    def plural_fmt(name, cnt):
        """
        pluralize name if necessary and combine with cnt
        :param name: str name of the item type
        :param cnt: int number items of this type
        :return: str name and cnt joined
        """
        if cnt == 1:
            return '{} {}'.format(cnt, name)
        else:
            return '{} {}s'.format(cnt, name)


class UploadReport(object):
    """
    Creates a text report of items that were sent to the remote store.
    """
    def __init__(self, project_name):
        """
        Create report witht the specified project name since the local store doesn't contain that info.
        :param project_name: str project name for the report
        """
        self.report_items = []
        self.project_name = project_name
        self._add_report_item('SENT FILENAME', 'ID', 'SIZE', 'HASH')

    def _add_report_item(self, name, remote_id, size='', file_hash=''):
        self.report_items.append(ReportItem(name, remote_id, size, file_hash))

    def walk_project(self, project):
        """
        Create report items for each project, folder, and files if necessary calling visit_* methods below.
        :param project: LocalProject project we will count items of.
        """
        # This method will call visit_project, visit_folder, and visit_file below as it walks the project tree.
        ProjectWalker.walk_project(project, self)

    def visit_project(self, item):
        """
        Add project to the report if it was sent.
        :param item: LocalContent project level item
        """
        if item.sent_to_remote:
            self._add_report_item('Project', item.remote_id)

    def visit_folder(self, item, parent):
        """
        Add folder to the report if it was sent.
        :param item: LocalFolder folder to possibly add
        :param parent: LocalFolder/LocalContent not used here
        """
        if item.sent_to_remote:
            self._add_report_item(item.path, item.remote_id)

    def visit_file(self, item, parent):
        """
        Add file to the report if it was sent.
        :param item: LocalFile file to possibly add.
        :param parent: LocalFolder/LocalContent not used here
        """
        if item.sent_to_remote:
            (alg, file_hash) = item.get_hashpair()
            self._add_report_item(item.path, item.remote_id, item.size, file_hash)

    def _report_header(self):
        return u"Upload Report for Project: '{}' {}\n".format(self.project_name, datetime.datetime.utcnow())

    def _report_body(self):
        max_name = UploadReport.max_len([item.name for item in self.report_items])
        max_remote_id = UploadReport.max_len([item.remote_id for item in self.report_items])
        max_size = UploadReport.max_len([item.size for item in self.report_items])
        return [item.str_with_sizes(max_name, max_remote_id, max_size) for item in self.report_items]

    @staticmethod
    def max_len(values):
        return max([len(x) for x in values])

    def get_content(self):
        lines = [self._report_header()]
        lines.extend(self._report_body())
        return '\n'.join(lines)


class ReportItem(object):
    """
    Item sent to remote store that is part of the UploadReport.
    """
    def __init__(self, name, remote_id, size='', file_hash=''):
        """
        Setup properties for use in str method
        :param name: str name of the
        :param remote_id: str remote uuid of the item
        :param size: int/str size of the item can be '' if blank
        :return:
        """
        self.name = name
        self.remote_id = remote_id
        self.size = str(size)
        self.file_hash = file_hash

    def str_with_sizes(self, max_name, max_remote_id, max_size):
        """
        Create string for report based on internal properties using sizes to line up columns.
        :param max_name: int width of the name column
        :param max_remote_id: int width of the remote_id column
        :return: str info from this report item
        """
        name_str = self.name.ljust(max_name)
        remote_id_str = self.remote_id.ljust(max_remote_id)
        size_str = self.size.ljust(max_size)
        return u'{}    {}    {}    {}'.format(name_str, remote_id_str, size_str, self.file_hash)


class FileContentSender(object):
    """
    Sends the data that local_file makes up to the remote store in chunks.
    """
    def __init__(self, config, data_service, local_file, watcher):
        """
        Setup for sending to remote store.
        :param config: ddsc.config.Config user configuration settings from YAML file/environment
        :param data_service: DataServiceApi data service we are sending the content to.
        :param local_file: LocalFile file we are sending to remote store
        """
        self.config = config
        self.data_service = data_service
        self.local_file = local_file
        self.filename = local_file.path
        self.content_type = local_file.mimetype
        self.chunk_num = 0
        self.upload_id = None
        self.watcher = watcher
        #self.pool = Pool()

    def upload(self, project_id, parent_kind, parent_id):
        """
        Upload file contents to project within specified parent.
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
        self._send_file_chunks()
        self.data_service.complete_upload(self.upload_id)
        result = self.data_service.create_file(parent_kind, parent_id, self.upload_id)
        return result.json()['id']

    def _send_file_chunks(self):
        """
        Have the file feed us chunks we can upload.
        """
        #file_chink_info = FileChunkInfo(self.filename, 0, 512, self.data_service)
        #pending_resp = self.pool.apply_async(FileContentSender.forkme, [file_chink_info])
        #print(pending_resp.get())

        self.local_file.process_chunks(self.config.upload_bytes_per_chunk, self.process_chunk)

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
            self._send_file_external(resp.json(), chunk)
            self.chunk_num += 1
        else:
            raise ValueError("Failed to retrieve upload url status:" + str(resp.status_code))

    def _send_file_external(self, url_json, chunk):
        """
        Send chunk to external store specified in url_json.
        Raises ValueError on upload failure.
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

    @staticmethod
    def forkme(file_chunk_info):
        return "In fork" + file_chunk_info.some_str()


class FileChunkInfo(object):
    def __init__(self, filename, index, chunk_size, data_service):
        self.filename = filename
        self.index = index
        self.chunk_size = chunk_size
        self.data_service = data_service

    def some_str(self):
        return "{}-{}-{}".format(self.filename, self.index, self.chunk_size)


class RemoteContentSender(object):
    """
    Sends project, folder, and files to remote store.
    """
    def __init__(self, config, data_service, project_id, project_name, watcher):
        """
        Setup to allow remote sending.
        :param config: ddsc.config.Config user configuration settings from YAML file/environment
        :param data_service: DataServiceApi used to query/send data
        :param project_id: str UUID of the project we want to add items too(can be '' for a new project)
        :param project_name: str Name of the project to create if necessary
        :param watcher: ProgressPrinter object we notify of items we are about to send
        """
        self.config = config
        self.data_service = data_service
        self.project_id = project_id
        self.project_name = project_name
        self.watcher = watcher

    def walk_project(self, project):
        """
        For each project, folder, and files send to remote store if necessary.
        :param project: LocalProject project who's contents we want to walk/send.
        """
        # This method will call visit_project, visit_folder, and visit_file below as it walks the project tree.
        ProjectWalker.walk_project(project, self)

    def visit_project(self, item):
        """
        Send a project to remote store if necessary.
        :param item: LocalProject we should send
        :param parent: object always None since a project doesn't have a parent
        """
        if not item.remote_id:
            self.watcher.transferring_item(item)
            result = self.data_service.create_project(self.project_name, self.project_name)
            item.set_remote_id_after_send(result.json()['id'])
            self.project_id = item.remote_id

    def visit_folder(self, item, parent):
        """
        Send a folder to remote store if necessary.
        :param item: LocalFolder we should send
        :param parent: LocalContent/LocalFolder that contains this folder
        """
        if not item.remote_id:
            self.watcher.transferring_item(item)
            result = self.data_service.create_folder(item.name, parent.kind, parent.remote_id)
            item.set_remote_id_after_send(result.json()['id'])

    def visit_file(self, item, parent):
        """
        Send file to remote store if necessary.
        :param item: LocalFile we should send
        :param parent: LocalContent/LocalFolder that contains this file
        """
        if item.need_to_send:
            file_content_sender = FileContentSender(self.config, self.data_service, item, self.watcher)
            remote_id = file_content_sender.upload(self.project_id, parent.kind, parent.remote_id)
            item.set_remote_id_after_send(remote_id)