from ddsc.core.util import ProjectWalker, KindType
from ddsc.core.ddsapi import DataServiceAuth, DataServiceApi
from ddsc.core.fileuploader import FileUploader, FileUploadOperations, ParentData, ParallelChunkProcessor
from ddsc.core.parallel import TaskRunner


class UploadSettings(object):
    """
    Settings used to upload a project
    """
    def __init__(self, config, data_service, watcher, project_name_or_id, file_upload_post_processor):
        """
        :param config: ddsc.config.Config user configuration settings from YAML file/environment
        :param data_service: DataServiceApi: where we will upload to
        :param watcher: ProgressPrinter we notify of our progress
        :param project_name_or_id: ProjectNameOrId: name or id of the project so we can create it if necessary
        :param file_upload_post_processor: object: has run(data_service, file_response) method to run after download
        """
        self.config = config
        self.data_service = data_service
        self.watcher = watcher
        self.project_name_or_id = project_name_or_id
        self.project_id = None
        self.file_upload_post_processor = file_upload_post_processor

    def get_data_service_auth_data(self):
        """
        Serialize data_service setup into something that can be passed to another process.
        :return: tuple of data service settings
        """
        return self.data_service.auth.get_auth_data()

    @staticmethod
    def rebuild_data_service(config, data_service_auth_data):
        """
        Deserialize value into DataServiceApi object.
        :param config:
        :param data_service_auth_data:
        :return:
        """
        auth = DataServiceAuth(config)
        auth.set_auth_data(data_service_auth_data)
        return DataServiceApi(auth, config.url)


class UploadContext(object):
    """
    Values passed to a background worker.
    Contains UploadSettings and parameters specific to the function to be run.
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
        self.project_name_or_id = settings.project_name_or_id
        self.project_id = settings.project_id
        self.params = params
        self.message_queue = message_queue
        self.task_id = task_id

    def make_data_service(self):
        """
        Recreate data service from within background worker.
        :return: DataServiceApi
        """
        return UploadSettings.rebuild_data_service(self.config, self.data_service_auth_data)

    def send_message(self, data):
        """
        Sends a message to the command's on_message(data) method.
        :param data: object: data sent to on_message
        """
        self.message_queue.put((self.task_id, data))

    def start_waiting(self):
        """
        Called when we start waiting for project to be ready for file uploads.
        """
        self.send_message(True)

    def done_waiting(self):
        """
        Called when project is ready for file uploads (after waiting).
        """
        self.send_message(False)


class ProjectUploader(object):
    """
    Uploads a project based on UploadSettings.
    """
    def __init__(self, settings):
        """
        Setup to talk to the data service based on settings.
        :param settings: UploadSettings: settings to use for uploading.
        """
        self.runner = TaskRunner(settings.config.upload_workers)
        self.settings = settings
        self.small_item_task_builder = SmallItemUploadTaskBuilder(self.settings, self.runner)
        self.small_files = []
        self.large_files = []

    def run(self, local_project):
        """
        Upload a project by uploading project, folders, and small files then uploading the large files.
        :param local_project: LocalProject: project to upload
        """
        # Walks project adding project/folder to small_item_task_builder and adding files to small_files/large_files
        ProjectWalker.walk_project(local_project, self)

        self.sort_files_list(self.small_files)
        self.add_small_files_to_task_builder()
        # Run small items in parallel
        self.runner.run()

        # Run parts of each large item in parallel
        self.sort_files_list(self.large_files)
        self.upload_large_files()

    @staticmethod
    def sort_files_list(files_list):
        """
        Sort files that are new first so they will will be processed before files that already exist in DukeDS.
        This is to allow us to immediately begin making progress in retrying an upload.
        :param files_list: [(LocalFile, LocalFolder|LocalProject)]: list of files to upload
        """
        files_list.sort(key=lambda tuple: tuple[0].remote_id)

    # Methods called by ProjectWalker.walk_project
    def visit_project(self, item):
        """
        Add create project to small task list.
        """
        self.small_item_task_builder.visit_project(item)

    def visit_folder(self, item, parent):
        """
        Add create folder to small task list.
        """
        self.small_item_task_builder.visit_folder(item, parent)

    def visit_file(self, item, parent):
        """
        If file is large add it to the large items to be processed after small task list.
        else file is small add it to the small task list.
        """
        if self.is_large_file(item):
            self.large_files.append((item, parent))
        else:
            self.small_files.append((item, parent))

    def is_large_file(self, item):
        return item.size > self.settings.config.upload_bytes_per_chunk

    def add_small_files_to_task_builder(self):
        for local_file, parent in self.small_files:
            self.small_item_task_builder.visit_file(local_file, parent)

    def upload_large_files(self):
        """
        Upload files that were too large.
        """
        for local_file, parent in self.large_files:
            self.settings.watcher.transferring_item(local_file, increment_amt=0, override_msg_verb='checking')
            hash_data = local_file.calculate_local_hash()
            if local_file.hash_matches_remote(hash_data):
                self.file_already_uploaded(local_file)
            else:
                self.settings.watcher.transferring_item(local_file, increment_amt=0)
                self.process_large_file(local_file, parent, hash_data)

    def process_large_file(self, local_file, parent, hash_data):
        """
        Upload a single file using multiple processes to upload multiple chunks at the same time.
        Updates local_file with it's remote_id when done.
        :param local_file: LocalFile: file we are uploading
        :param parent: LocalFolder/LocalProject: parent of the file
        """
        file_content_sender = FileUploader(self.settings.config, self.settings.data_service, local_file, hash_data,
                                           self.settings.watcher, self.settings.file_upload_post_processor)
        remote_id = file_content_sender.upload(self.settings.project_id, parent.kind, parent.remote_id)
        local_file.set_remote_values_after_send(remote_id, hash_data.alg, hash_data.value)

    def file_already_uploaded(self, local_file):
        """
        Updates progress bar for a file that was already uploaded
        :param local_file: LocalFile
        """
        num_chunks = ParallelChunkProcessor.determine_num_chunks(self.settings.config.upload_bytes_per_chunk,
                                                                 local_file.size)
        self.settings.watcher.increment_progress(num_chunks)


class SmallItemUploadTaskBuilder(object):
    """
    Uploads project, folders and small files to DukeDS.
    Does them in parallel ordered based on their requirements.
    """
    def __init__(self, settings, task_runner):
        self.settings = settings
        self.task_runner = task_runner
        self.tasks = []
        self.item_to_id = {}

    def walk_project(self, project):
        """
        Calls visit_* methods of self.
        :param project: project we will visit children of.
        """
        ProjectWalker.walk_project(project, self)

    def visit_project(self, item):
        """
        Adds create project command to task runner if project doesn't already exist.
        """
        if not item.remote_id:
            command = CreateProjectCommand(self.settings, item)
            self.task_runner_add(None, item, command)
        else:
            self.settings.project_id = item.remote_id

    def visit_folder(self, item, parent):
        """
        Adds create folder command to task runner if folder doesn't already exist.
        """
        if not item.remote_id:
            command = CreateFolderCommand(self.settings, item, parent)
            self.task_runner_add(parent, item, command)

    def visit_file(self, item, parent):
        """
        If file is small add create small file command otherwise raise error.
        Large files shouldn't be passed to SmallItemUploadTaskBuilder.
        """
        if item.size > self.settings.config.upload_bytes_per_chunk:
            msg = "Programmer Error: Trying to upload large file as small item size:{} name:{}"
            raise ValueError(msg.format(item.size, item.name))
        else:
            # Create a command to hash the file
            hash_command = HashFileCommand(self.settings, item)
            parent_task_id = self.item_to_id.get(parent)
            hash_task_id = self.task_runner.add(parent_task_id, hash_command)
            # Create a command to upload the file that waits for the results from the HashFileCommand
            send_command = CreateSmallFileCommand(self.settings, item, parent,
                                                  self.settings.file_upload_post_processor)
            self.task_runner.add(hash_task_id, send_command)

    def task_runner_add(self, parent, item, command):
        """
        Add command to task runner with parent's task id createing a task id for item/command.
        Save this item's id to a lookup.
        :param parent: object: parent of item
        :param item: object: item we are running command on
        :param command: parallel TaskCommand we want to have run
        """
        parent_task_id = self.item_to_id.get(parent)
        task_id = self.task_runner.add(parent_task_id, command)
        self.item_to_id[item] = task_id


class CreateProjectCommand(object):
    """
    Create project in DukeDS.
    """
    def __init__(self, settings, local_project):
        """
        Setup passing in all necessary data to create project and update external state.
        :param settings: UploadSettings: settings to be used/updated when we upload the project.
        :param local_project: LocalProject: information about the project(holds remote_id when done)
        """
        self.settings = settings
        self.local_project = local_project
        if not settings.project_name_or_id.is_name:
            raise ValueError('Programming Error: CreateProjectCommand called without project name.')
        self.func = upload_project_run

    def before_run(self, parent_task_result):
        """
        Notify progress bar that we are creating the project.
        """
        self.settings.watcher.transferring_item(self.local_project)

    def create_context(self, message_queue, task_id):
        """
        Create data needed by upload_project_run(DukeDS connection info).
        :param message_queue: Queue: queue background process can send messages to us on
        :param task_id: int: id of this command's task so message will be routed correctly
        """
        return UploadContext(self.settings, (), message_queue, task_id)

    def after_run(self, result_id):
        """
        Save uuid associated with project we just created.
        :param result_id: str: uuid of the project
        """
        self.local_project.set_remote_id_after_send(result_id)
        self.settings.project_id = result_id


def upload_project_run(upload_context):
    """
    Function run by CreateProjectCommand to create the project.
    Runs in a background process.
    :param upload_context: UploadContext: contains data service setup and project name to create.
    """
    data_service = upload_context.make_data_service()
    project_name = upload_context.project_name_or_id.get_name_or_raise()
    result = data_service.create_project(project_name, project_name)
    data_service.close()
    return result.json()['id']


class CreateFolderCommand(object):
    """
    Create folder in DukeDS.
    """
    def __init__(self, settings, remote_folder, parent):
        """
        Setup passing in all necessary data to create folder and update external state.
        :param settings: UploadSettings: contains data_service connection info
        :param remote_folder: object: contains data about the folder we should create
        :param parent: object: contains info about the parent of the folder we will create.
        """
        self.settings = settings
        self.remote_folder = remote_folder
        self.parent = parent
        self.func = upload_folder_run

    def before_run(self, parent_task_result):
        """
        Notify progress bar that we are creating this folder.
        """
        self.settings.watcher.transferring_item(self.remote_folder)

    def create_context(self, message_queue, task_id):
        """
        Create values to be used by upload_folder_run function.
        :param message_queue: Queue: queue background process can send messages to us on
        :param task_id: int: id of this command's task so message will be routed correctly
        """
        params = (self.remote_folder.name, self.parent.kind, self.parent.remote_id)
        return UploadContext(self.settings, params, message_queue, task_id)

    def after_run(self, result_id):
        """
        Save the uuid of our new folder back to our LocalFolder object.
        :param result_id: str: uuid of the folder we just created.
        """
        self.remote_folder.set_remote_id_after_send(result_id)


def upload_folder_run(upload_context):
    """
    Function run by CreateFolderCommand to create the folder.
    Runs in a background process.
    :param upload_context: UploadContext: contains data service setup and folder details.
    """
    data_service = upload_context.make_data_service()
    folder_name, parent_kind, parent_remote_id = upload_context.params
    result = data_service.create_folder(folder_name, parent_kind, parent_remote_id)
    data_service.close()
    return result.json()['id']


class HashFileCommand(object):
    """
    Hashes file and returns result
    """
    def __init__(self, settings, local_file):
        self.settings = settings
        self.local_file = local_file
        self.func = hash_file

    def before_run(self, parent_task_result):
        # Update progress bar that we are checking this file
        self.settings.watcher.transferring_item(self.local_file, increment_amt=0, override_msg_verb='checking')

    def create_context(self, message_queue, task_id):
        params = self.local_file.get_path_data()
        return UploadContext(self.settings, params, message_queue, task_id)

    def after_run(self, result):
        pass


def hash_file(upload_context):
    """
    Function run by HashFileCommand to calculate a file hash.
    :param upload_context: PathData: contains path to a local file to hash
    :return HashData: result of hash (alg + value)
    """
    path_data = upload_context.params
    hash_data = path_data.get_hash()
    return hash_data


class CreateSmallFileCommand(object):
    """
    Creates a small file in the data service.
    This includes:
     1) creating an upload
     2) creating an upload url
     3) posting the contents of the file
     4) completing the upload
     5) creating or updating file version
    """
    def __init__(self, settings, local_file, parent, file_upload_post_processor=None):
        """
        Setup passing in all necessary data to create file and update external state.
        :param settings: UploadSettings: contains data_service connection info
        :param local_file: object: information about the file we will upload
        :param parent: object: parent of the file (folder or project)
        :param file_upload_post_processor: object: has run(data_service, file_response) method to run after download
        """
        self.settings = settings
        self.local_file = local_file
        self.parent = parent
        self.func = create_small_file
        self.file_upload_post_processor = file_upload_post_processor
        self.hash_data = None

    def before_run(self, parent_task_result):
        self.hash_data = parent_task_result

    def create_context(self, message_queue, task_id):
        """
        Create values to be used by create_small_file function.
        :param message_queue: Queue: queue background process can send messages to us on
        :param task_id: int: id of this command's task so message will be routed correctly
       """
        parent_data = ParentData(self.parent.kind, self.parent.remote_id)
        path_data = self.local_file.get_path_data()
        params = parent_data, path_data, self.hash_data, self.local_file.remote_id, \
            self.local_file.remote_file_hash_alg, self.local_file.remote_file_hash

        return UploadContext(self.settings, params, message_queue, task_id)

    def after_run(self, remote_file_data):
        """
        Save uuid and hash values of file to our LocalFile if it was updated. If remote_file_data is None that means
        the file was already up to date.
        :param remote_file_data: dict: DukeDS file data
        """
        if remote_file_data:
            if self.file_upload_post_processor:
                self.file_upload_post_processor.run(self.settings.data_service, remote_file_data)
            remote_file_id = remote_file_data['id']
            remote_hash_dict = remote_file_data['current_version']['upload']['hashes'][0]
            self.local_file.set_remote_values_after_send(remote_file_id,
                                                         remote_hash_dict['algorithm'],
                                                         remote_hash_dict['value'])
            self.settings.watcher.transferring_item(self.local_file, transferred_bytes=self.local_file.size)
        else:
            self.settings.watcher.increment_progress()

    def on_message(self, started_waiting):
        """
        Receives started_waiting boolean from create_small_file method and notifies project_status_monitor in settings.
        :param started_waiting: boolean: True when we start waiting, False when done
        """
        watcher = self.settings.watcher
        if started_waiting:
            watcher.start_waiting()
        else:
            watcher.done_waiting()


def create_small_file(upload_context):
    """
    Function run by CreateSmallFileCommand to create the file.
    Runs in a background process.
    :param upload_context: UploadContext: contains data service setup and file details.
    :return dict: DukeDS file data
    """
    parent_data, path_data, hash_data, remote_file_id, remote_file_hash_alg, remote_file_hash = upload_context.params
    if hash_data.matches(remote_file_hash_alg, remote_file_hash):
        return None

    data_service = upload_context.make_data_service()
    # The small file will fit into one chunk so read into memory and hash it.
    chunk = path_data.read_whole_file()

    # Talk to data service uploading chunk and creating the file.
    upload_operations = FileUploadOperations(data_service, upload_context)
    upload_id, url_info = upload_operations.create_upload_and_chunk_url(
        upload_context.project_id, path_data, hash_data, storage_provider_id=upload_context.config.storage_provider_id)
    upload_operations.send_file_external(url_info, chunk)
    file_response_json = upload_operations.finish_upload(upload_id, hash_data, parent_data, remote_file_id)
    data_service.close()
    return file_response_json


class ProjectUploadDryRun(object):
    """
    Recursively visits children of the project passed to run.
    Builds a list of the names of folders/files that need to be uploaded.
    """
    def __init__(self, local_project):
        self.upload_items = []
        self._run(local_project)

    def add_upload_item(self, name):
        self.upload_items.append(name)

    def _run(self, local_project):
        """
        Appends file/folder paths to upload_items based on the contents of this project that need to be uploaded.
        :param local_project: LocalProject: project we will build the list for
        """
        self._visit_recur(local_project)

    def _visit_recur(self, item):
        """
        Recursively visits children of item.
        :param item: object: project, folder or file we will add to upload_items if necessary.
        """
        if item.kind == KindType.file_str:
            hash_data = item.calculate_local_hash()
            if not item.hash_matches_remote(hash_data):
                self.add_upload_item(item.path)
        else:
            if item.kind == KindType.project_str:
                pass
            else:
                if not item.remote_id:
                    self.add_upload_item(item.path)
            for child in item.children:
                self._visit_recur(child)

    def get_report(self):
        """
        Returns text displaying the items that need to be uploaded or a message saying there are no files/folders
        to upload.
        :return: str: report text
        """
        if not self.upload_items:
            return "\n\nNo changes found. Nothing needs to be uploaded.\n\n"
        else:
            result = "\n\nFiles/Folders that need to be uploaded:\n"
            for item in self.upload_items:
                result += "{}\n".format(item)
            result += "\n"
            return result
