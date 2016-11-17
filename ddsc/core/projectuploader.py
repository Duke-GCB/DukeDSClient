import requests
from ddsc.core.util import ProjectWalker, KindType
from ddsc.core.ddsapi import DataServiceAuth, DataServiceApi
from ddsc.core.fileuploader import FileUploader, FileUploadOperations, ParentData
from ddsc.core.parallel import TaskExecutor, TaskRunner
from ddsc.core.transferdata import TransferDB
from ddsc.core.localstore import PathData
import time
import os

requests_session = requests.Session()


class UploadSettings(object):
    """
    Settings used to upload a project
    """
    def __init__(self, config, data_service, watcher, project_name, project_db):
        """
        :param config: ddsc.config.Config user configuration settings from YAML file/environment
        :param data_service: DataServiceApi: where we will upload to
        :param watcher: ProgressPrinter we notify of our progress
        :param project_name: str: name of the project so we can create it if necessary
        """
        self.config = config
        self.data_service = data_service
        self.watcher = watcher
        self.project_name = project_name
        self.project_id = None
        self.project_db = project_db

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
        return DataServiceApi(auth, config.url, requests_session)

    def commit_session(self):
        self.project_db.commit_session()

class UploadContext(object):
    """
    Values passed to a background worker.
    Contains UploadSettings and parameters specific to the function to be run.
    """
    def __init__(self, settings, params):
        """
        Setup context so it can be passed.
        :param settings: UploadSettings: project level info
        :param params: tuple: values specific to the function being run
        """
        self.data_service_auth_data = settings.get_data_service_auth_data()
        self.config = settings.config
        self.project_name = settings.project_name
        self.project_id = settings.project_id
        self.params = params

    def make_data_service(self):
        """
        Recreate data service from within background worker.
        :return: DataServiceApi
        """
        return UploadSettings.rebuild_data_service(self.config, self.data_service_auth_data)


class ProjectUploader(object):
    """
    Uploads a project based on UploadSettings.
    """
    def __init__(self, settings):
        """
        Setup to talk to the data service based on settings.
        :param settings: UploadSettings: settings to use for uploading.
        """
        self.runner = TaskRunner(TaskExecutor(settings.config.upload_workers))
        self.settings = settings
        self.small_item_task_builder = SmallItemUploadTaskBuilder(self.settings, self.runner)
        self.small_items = []
        self.large_items = []

    def run(self, local_project):
        """
        Upload a project by uploading project, folders, and small files then uploading the large files.
        :param local_project: LocalProject: project to upload
        """
        # Walk project adding small items to runner saving large items to large_items
        ProjectWalker.walk_project(local_project, self)
        # Run small items in parallel
        self.runner.run()
        # Run parts of each large item in parallel
        self.upload_large_items()

    def run2(self, project):
        self.visit_transfer_item(project, None)
        self.runner.run()

    def visit_transfer_item(self, item, parent_item):
        self.process_item(item, parent_item)
        for child in item.children:
            self.visit_transfer_item(child, item)

    def process_item(self, item, parent_item):
        if item.dds_kind == KindType.project_str:
            self.small_item_task_builder.visit_project(item)
        if item.dds_kind == KindType.folder_str:
            self.small_item_task_builder.visit_folder(item, parent_item)
        if item.dds_kind == KindType.file_str:
            self.small_item_task_builder.visit_file(item, parent_item)

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
        self.small_item_task_builder.visit_file(item, parent)

    def upload_large_items(self):
        """
        Upload files that were too large.
        """
        for local_file, parent in self.large_items:
            if local_file.need_to_send:
                self.process_large_file(local_file, parent)

    def process_large_file(self, local_file, parent):
        """
        Upload a single file using multiple processes to upload multiple chunks at the same time.
        Updates local_file with it's remote_id when done.
        :param local_file: LocalFile: file we are uploading
        :param parent: LocalFolder/LocalProject: parent of the file
        """
        file_content_sender = FileUploader(self.settings.config, self.settings.data_service, local_file,
                                           self.settings.watcher)
        remote_id = file_content_sender.upload(self.settings.project_id, parent.kind, parent.dds_id)
        self.local_file.dds_id = remote_id
        self.local_file.need_to_send = False
        self.local_file.was_transferred = True
        self.settings.commit_session()


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
        if not item.dds_id:
            command = CreateProjectCommand(self.settings, item)
            self.task_runner_add(None, item, command)
        else:
            self.settings.project_id = item.dds_id

    def visit_folder(self, item, parent):
        """
        Adds create folder command to task runner if folder doesn't already exist.
        """
        if not item.dds_id:
            command = CreateFolderCommand(self.settings, item, parent)
            self.task_runner_add(parent, item, command)

    def visit_file(self, item, parent):
        """
        If file is small add create small file command otherwise raise error.
        Large files shouldn't be passed to SmallItemUploadTaskBuilder.
        """
        if item.need_to_send:
            command = None
            if item.file_size > self.settings.config.upload_bytes_per_chunk:
                command = CreateLargeFileCommand(self.settings, item, parent)
            else:
                command = CreateSmallFileCommand(self.settings, item, parent)
            self.task_runner_add(parent, item, command)

    def task_runner_add(self, parent, item, command):
        """
        Add command to task runner with parent's task id createing a task id for item/command.
        Save this item's id to a lookup.
        :param parent: object: parent of item
        :param item: object: item we are running command on
        :param command: parallel TaskCommand we want to have run
        :return int,int: task_id, parent_task_id task_id created for this command, and the parent task id(can be None)
        """
        parent_task_id = self.item_to_id.get(parent)
        task_id = self.task_runner.add(parent_task_id, command)
        self.item_to_id[item] = task_id
        return task_id, parent_task_id


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
        self.project_name = settings.project_name
        self.func = upload_project_run
        self.is_serial = False

    def before_run(self, parent_task_result):
        """
        Notify progress bar that we are creating the project.
        """
        self.settings.watcher.transferring_item(self.local_project)

    def create_context(self):
        """
        Create data needed by upload_project_run(DukeDS connection info).
        """
        return UploadContext(self.settings, ())

    def after_run(self, result_id):
        """
        Save uuid associated with project we just created.
        :param result_id: str: uuid of the project
        """
        self.local_project.dds_id = result_id
        self.local_project.need_to_send = False
        self.local_project.was_transferred = True
        self.settings.commit_session()
        self.settings.project_id = result_id



def upload_project_run(upload_context):
    """
    Function run by CreateProjectCommand to create the project.
    Runs in a background process.
    :param upload_context: UploadContext: contains data service setup and project name to create.
    """
    data_service = upload_context.make_data_service()
    project_name = upload_context.project_name
    result = data_service.create_project(project_name, project_name)
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
        self.is_serial = False

    def before_run(self, parent_task_result):
        """
        Notify progress bar that we are creating this folder.
        """
        self.settings.watcher.transferring_item(self.remote_folder)

    def create_context(self):
        """
        Create values to be used by upload_folder_run function.
        """
        folder_name = os.path.basename(self.remote_folder.remote_path)
        params = (folder_name, self.parent.dds_kind, self.parent.dds_id)
        return UploadContext(self.settings, params)

    def after_run(self, result_id):
        """
        Save the uuid of our new folder back to our LocalFolder object.
        :param result_id: str: uuid of the folder we just created.
        """
        self.remote_folder.dds_id = result_id
        self.remote_folder.need_to_send = False
        self.remote_folder.was_transferred = True
        self.settings.commit_session()


def upload_folder_run(upload_context):
    """
    Function run by CreateFolderCommand to create the folder.
    Runs in a background process.
    :param upload_context: UploadContext: contains data service setup and folder details.
    """
    data_service = upload_context.make_data_service()
    folder_name, parent_kind, parent_remote_id = upload_context.params
    result = data_service.create_folder(folder_name, parent_kind, parent_remote_id)
    return result.json()['id']


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
    def __init__(self, settings, local_file, parent):
        """
        Setup passing in all necessary data to create file and update external state.
        :param settings: UploadSettings: contains data_service connection info
        :param local_file: object: information about the file we will upload
        :param parent: object: parent of the file (folder or project)
        """
        self.settings = settings
        self.local_file = local_file
        self.parent = parent
        self.func = create_small_file
        self.is_serial = False

    def before_run(self, parent_task_result):
        pass

    def create_context(self):
        """
        Create values to be used by create_small_file function.
        """
        parent_data = ParentData(self.parent.dds_kind, self.parent.dds_id)
        path_data = PathData(self.local_file.local_path)
        params = parent_data, path_data, self.local_file.dds_id, self.local_file.upload_id
        return UploadContext(self.settings, params)

    def after_run(self, remote_file_id):
        """
        Save uuid of file to our LocalFile
        :param remote_file_id: uuid of the file we just created/updated.
        """
        self.settings.watcher.transferring_item(self.local_file)
        self.local_file.dds_id = remote_file_id
        self.local_file.need_to_send = False
        self.local_file.was_transferred = True
        self.settings.commit_session()


def create_small_file(upload_context):
    """
    Function run by CreateSmallFileCommand to create the file.
    Runs in a background process.
    :param upload_context: UploadContext: contains data service setup and file details.
    """

    data_service = upload_context.make_data_service()
    parent_data, path_data, remote_file_id, upload_id = upload_context.params
    # The small file will fit into one chunk so read into memory and hash it.
    chunk_num = 1
    chunk = path_data.read_whole_file()
    hash_data = path_data.get_hash()

    # Talk to data service uploading chunk and creating the file.
    upload_operations = FileUploadOperations(data_service)
    mark_upload_complete = True
    if upload_id:
        if upload_operations.is_upload_complete(upload_id):
            mark_upload_complete = False
    else:
        upload_id = upload_operations.create_upload(upload_context.project_id, path_data, hash_data)
    url_info = upload_operations.create_file_chunk_url(upload_id, chunk_num, chunk)
    upload_operations.send_file_external(url_info, chunk)
    return upload_operations.finish_upload(upload_id, hash_data, parent_data, remote_file_id, mark_upload_complete)


class CreateLargeFileCommand(object):
    def __init__(self, settings, local_file, parent):
        self.settings = settings
        self.local_file = local_file
        self.parent = parent
        self.func = create_large_file_func
        self.is_serial = True

    def create_context(self):
        return self

    def run(self):
        file_content_sender = FileUploader(self.settings.config, self.settings.data_service, self.local_file,
                                           self.settings.watcher, self.settings.project_db)
        remote_id = file_content_sender.upload(self.settings.project_id, self.parent.dds_kind, self.parent.dds_id)
        self.local_file.dds_id = remote_id
        self.local_file.need_to_send = False
        self.local_file.was_transferred = True
        self.settings.commit_session()


def create_large_file_func(large_file_cmd):
    """
    Uploads a large file in parallel.
    Calls run method on large_file_cmd.
    :param large_file_cmd: CreateLargeFileCommand
    """
    large_file_cmd.run()