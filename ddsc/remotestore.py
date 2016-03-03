import os
from ddsc.ddsapi import KindType
from ddsc.localstore import ProjectWalker
from ddsc.util import ProgressPrinter

FETCH_ALL_USERS_PAGE_SIZE = 25
DOWNLOAD_FILE_CHUNK_SIZE = 1024


class RemoteStore(object):
    """
    Fetches project tree data from remote store.
    """
    def __init__(self, data_service):
        """
        Setup to allow fetching project tree.
        :param data_service: DataServiceApi we will use for querying project content.
        """
        self.data_service = data_service

    def fetch_remote_project(self, project_name, must_exist=False):
        """
        Retrieve the project via project_name
        :param project_name: str name of the project to try and download
        :return: RemoteProject project requested or None if not found
        """
        project = self._get_my_project(project_name)
        if project:
            self._add_project_children(project)
        else:
            if must_exist:
                raise ValueError(u'There is no project with the name {}'.format(project_name).encode('utf-8'))
        return project

    def _get_my_project(self, project_name):
        """
        Return project tree root for project_name.
        :param project_name: str name of the project to download
        :return: RemoteProject project we found or None
        """
        response = self.data_service.get_projects().json()
        for project in response['results']:
            if project['name'] == project_name:
                return RemoteProject(project)
        return None

    def _add_project_children(self, project):
        """
        Add the rest of the project tree from the remote store to the project object.
        :param project: RemoteProject root of the project tree to add children too
        """
        response = self.data_service.get_project_children(project.id, '').json()
        for child in response['results']:
            self._add_child(project, child)

    def _add_child(self, parent, child):
        """
        Add file or folder(child) to parent.
        :param parent: RemoteProject/RemoteFolder to add child to
        :param child: dict JSON data back from remote store
        """
        kind = child['kind']
        if kind == KindType.folder_str:
            parent.add_child(self._read_folder(child))
        elif kind == KindType.file_str:
            parent.add_child(self._read_file_metadata(child))
        else:
            raise ValueError("Unknown child type {}".format(kind))

    def _read_folder(self, folder_json):
        """
        Create RemoteFolder and query it's children.
        :param folder_json: dict JSON data back from remote store
        :return: RemoteFolder folder we filled in
        """
        folder = RemoteFolder(folder_json)
        response = self.data_service.get_folder_children(folder.id, '').json()
        for child in response['results']:
            self._add_child(folder, child)
        return folder

    def _read_file_metadata(self, file_json):
        """
        Create RemoteFile based on file_json and fetching it's hash.
        :param file_json: dict JSON data back from remote store
        :return: RemoteFile file we created from file_json
        """
        remote_file = RemoteFile(file_json)
        response = self.data_service.get_file(remote_file.id)
        file_hash = response.json()['upload']['hash']
        if file_hash:
            remote_file.set_hash(file_hash['value'], file_hash['algorithm'])
        return remote_file

    def upload_differences(self, local_project, project_name, progress_printer):
        sender = RemoteContentSender(self.data_service, local_project.remote_id, project_name, progress_printer)
        sender.walk_project(local_project)

    def lookup_user_by_name(self, full_name):
        """
        Query remote store for a single user with the name full_name or raise error.
        :param full_name: str Users full name separated by a space.
        :return: RemoteUser user info for single user with full_name
        """
        res = self.data_service.get_users_by_full_name(full_name)
        json_data = res.json()
        results = json_data['results']
        found_cnt = len(results)
        if found_cnt == 0:
            raise ValueError("User not found:" + full_name)
        elif found_cnt > 1:
            raise ValueError("Multiple users with name:" + full_name)
        user = RemoteUser(results[0])
        if user.full_name.lower() != full_name.lower():
            raise ValueError("User not found:" + full_name)
        return user

    def lookup_user_by_username(self, username):
        """
        Finds the single user who has this username or raises ValueError.
        :param username: str username we are looking for
        :return: RemoteUser user we found
        """
        matches = [user for user in self.fetch_all_users() if user.username == username]
        if not matches:
            raise ValueError('Username not found: {}.'.format(username))
        if len(matches) > 1:
            raise ValueError('Multiple users with same username found: {}.'.format(username))
        return matches[0]

    def lookup_user_by_email(self, email):
        """
        Finds the single user who has this email or raises ValueError.
        :param email: str email we are looking for
        :return: RemoteUser user we found
        """
        matches = [user for user in self.fetch_all_users() if user.email == email]
        if not matches:
            raise ValueError('Email not found: {}.'.format(email))
        if len(matches) > 1:
            raise ValueError('Multiple users with same email found: {}.'.format(email))
        return matches[0]

    def fetch_all_users(self):
        """
        Retrieves all users from data service.
        :return: [RemoteUser] list of all users we downloaded
        """
        page = 1
        per_page = FETCH_ALL_USERS_PAGE_SIZE
        users = []
        while True:
            result = self.data_service.get_users_by_page_and_offset(page, per_page)
            user_list_json = result.json()
            for user_json in user_list_json['results']:
                users.append(RemoteUser(user_json))
            total_pages = int(result.headers["x-total-pages"])
            result_page = int(result.headers["x-page"])
            if result_page == total_pages:
                break;
            page += 1
        return users

    def set_user_project_permission(self, project, user, auth_role):
        """
        Update remote store for user giving auth_role permissions on project.
        :param project: RemoteProject project to give permissions to
        :param user: RemoteUser user who we are giving permissions to
        :param auth_role: str type of authorization to give user(project_admin)
        """
        self.data_service.set_user_project_permission(project.id, user.id, auth_role)

    def download_file(self, remoteFile, path, watcher):
        """
        Download a remote file associated with the remote uuid(file_id) into local path.
        :param file_id: str uuid of the file to retrieve
        :param path: str file system path to save the contents to.
        :param watcher: object implementing send_item(item, increment_amt) that updates UI
        """
        url_json = self.data_service.get_file_url(remoteFile.id).json()
        http_verb = url_json['http_verb']
        host = url_json['host']
        url = url_json['url']
        http_headers = url_json['http_headers']
        response = self.data_service.receive_external(http_verb, host, url, http_headers)
        with open(path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=DOWNLOAD_FILE_CHUNK_SIZE):
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
                    watcher.sending_item(remoteFile, increment_amt=len(chunk))
        #TODO check size


class RemoteProject(object):
    """
    Project data from a remote store projects request.
    Represents the top of a tree.
    Has kind property to allow project tree traversal with ProjectWalker.
    """
    def __init__(self, json_data):
        """
        Set properties based on json_data.
        :param json_data: dict JSON data containing project info
        """
        self.id = json_data['id']
        self.kind = json_data['kind']
        self.name = json_data['name']
        self.description = json_data['description']
        self.is_deleted = json_data['is_deleted']
        self.children = []

    def add_child(self, child):
        """
        Add a file or folder to our remote project.
        :param child: RemoteFolder/RemoteFile child to add.
        """
        self.children.append(child)

    def __str__(self):
        return 'project: {} id:{} {}'.format(self.name, self.id, self.children)


class RemoteFolder(object):
    """
    Folder data from a remote store project_id_children or folder_id_children request.
    Represents a leaf or branch in a project tree.
    Has kind property to allow project tree traversal with ProjectWalker.
    """
    def __init__(self, json_data):
        """
        Set properties based on json_data.
        :param json_data: dict JSON data containing folder info
        """
        self.id = json_data['id']
        self.kind = json_data['kind']
        self.name = json_data['name']
        self.is_deleted = json_data['is_deleted']
        self.children = []

    def add_child(self, child):
        """
        Add remote file or folder to this folder.
        :param child: RemoteFolder or remoteFile to add.
        """
        self.children.append(child)

    def __str__(self):
        return 'folder: {} id:{} {}'.format(self.name, self.id, self.children)


class RemoteFile(object):
    """
    File data from a remote store project_id_children or folder_id_children request.
    Represents a leaf in a project tree.
    Has kind property to allow project tree traversal with ProjectWalker.
    """
    def __init__(self, json_data):
        """
        Set properties based on json_data.
        :param json_data: dict JSON data containing file info
        """
        self.id = json_data['id']
        self.kind = json_data['kind']
        self.name = json_data['name']
        self.path = self.name # for compatibilty with ProgressPrinter
        self.is_deleted = json_data['is_deleted']
        self.size = json_data['upload']['size']
        self.file_hash = None
        self.hash_alg = None

    def set_hash(self, file_hash, hash_alg):
        """
        Set the hash value and algorithm for the contents of the file.
        :param file_hash: str hash value
        :param hash_alg: str name of the hash algorithm(md5)
        """
        self.file_hash = file_hash
        self.hash_alg = hash_alg

    def __str__(self):
        return 'file: {} id:{} size:{}'.format(self.name, self.id, self.size)


class RemoteUser(object):
    """
    User who can download/upload/edit project on remote store.
    """
    def __init__(self, json_data):
        """
        Set properties based on json_data.
        :param json_data: dict JSON data containing file info
        """
        self.id = json_data['id']
        self.username = json_data['username']
        self.full_name = json_data['full_name']
        self.email = json_data['email']

    def __str__(self):
        return 'id:{} username:{} full_name:{}'.format(self.id, self.username, self.full_name)


class FileContentSender(object):
    """
    Sends the data that local_file makes up to the remote store in chunks.
    """
    def __init__(self, data_service, local_file, watcher):
        """
        Setup for sending to remote store.
        :param data_service: DataServiceApi data service we are sending the content to.
        :param local_file: LocalFile file we are sending to remote store
        """
        self.data_service = data_service
        self.local_file = local_file
        self.filename = local_file.path
        self.content_type = local_file.mimetype
        self.chunk_num = 0
        self.upload_id = None
        self.watcher = watcher

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
        self.local_file.process_chunks(self.data_service.bytes_per_chunk, self.process_chunk)

    def process_chunk(self, chunk, chunk_hash_alg, chunk_hash_value):
        """
        Method to consume chunks sent by local_file.process_chunks.
        Raises ValueError on upload failure.
        :param chunk: bytes part of the file to send
        :param chunk_hash_alg: str the algorithm used to hash chunk
        :param chunk_hash_value: str the hash value of chunk
        """
        self.watcher.sending_item(self.local_file)
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


class RemoteContentSender(object):
    """
    Sends project, folder, and files to remote store.
    """
    def __init__(self, data_service, project_id, project_name, watcher):
        """
        Setup to allow remote sending.
        :param data_service: DataServiceApi used to query/send data
        :param project_id: str UUID of the project we want to add items too(can be '' for a new project)
        :param project_name: str Name of the project to create if necessary
        :param watcher: ProgressPrinter object we notify of items we are about to send
        """
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
            self.watcher.sending_item(item)
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
            self.watcher.sending_item(item)
            result = self.data_service.create_folder(item.name, parent.kind, parent.remote_id)
            item.set_remote_id_after_send(result.json()['id'])

    def visit_file(self, item, parent):
        """
        Send file to remote store if necessary.
        :param item: LocalFile we should send
        :param parent: LocalContent/LocalFolder that contains this file
        """
        if item.need_to_send:
            file_content_sender = FileContentSender(self.data_service, item, self.watcher)
            remote_id = file_content_sender.upload(self.project_id, parent.kind, parent.remote_id)
            item.set_remote_id_after_send(remote_id)

class RemoteContentDownloader(object):
    """
    Creates local version of remote content.
    """
    def __init__(self, remote_store, dest_directory):
        self.remote_store = remote_store
        self.dest_directory = dest_directory
        self.watcher = None
        self.id_to_path = {}

    def walk_project(self, project):
        """
        For each project, folder, and files send to remote store if necessary.
        :param project: LocalProject project who's contents we want to walk/send.
        """
        # This method will call visit_project, visit_folder, and visit_file below as it walks the project tree.
        counter = RemoteContentCounter()
        counter.walk_project(project)
        self.watcher = ProgressPrinter(counter.count, msg_verb='downloading')
        ProjectWalker.walk_project(project, self)
        self.watcher.finished()

    def try_create_dir(self, path):
        """
        Try to create a directory if it doesn't exist and raise error if there is a non-directory with the same name.
        :param path: str path to the directory
        """
        if not os.path.exists(path):
            os.mkdir(path)
        elif not os.path.isdir(path):
            ValueError("Unable to create directory:" + path + " because a file already exists with the same name.")

    def visit_project(self, item):
        """
        Save off the path for the top level project(dest_directory) into id_to_path.
        Create the directory if necessary.
        :param item: RemoteProject
        """
        self.id_to_path[item.id] = self.dest_directory
        self.try_create_dir(self.dest_directory)

    def visit_folder(self, item, parent):
        """
        Save off the path for item into id_to_path.
        :param item: RemoteFolder item we want to mkdir/add to id_to_path
        :param parent: RemoteProject/RemoteFolder parent of item
        """
        parent_path = self.id_to_path[parent.id]
        path = os.path.join(parent_path, item.name)
        self.id_to_path[item.id] = path
        self.try_create_dir(path)

    def visit_file(self, item, parent):
        """
        Download the file associated with item and make sure we received all of it.
        :param item: RemoteFile file we will download
        :param parent: RemoteProject/RemoteFolder parent of item
        """
        parent_path = self.id_to_path[parent.id]
        path = os.path.join(parent_path, item.name)
        self.remote_store.download_file(item, path, self.watcher)
        RemoteContentDownloader.check_file_size(item, path)


    @staticmethod
    def check_file_size(item, path):
        """
        Raise an error if we didn't get all of the file.
        :param item: RemoteFile file we tried to download
        :param path: str path where we downloaded the file to
        """
        stat_info = os.stat(path)
        if stat_info.st_size != item.size:
            format_str = "Error occurred downloading {}. Got a file size {}. Expected file size:{}"
            msg = format_str.format(path, stat_info.st_size, item.size)
            raise ValueError(msg)


class RemoteContentCounter(object):
    """
    Counts up how many bytes we have to download to download a project.
    """
    def __init__(self):
        self.count = 0

    def walk_project(self, project):
        """
        For each file update count based on size.
        :param project: LocalProject project who's contents we want to walk/count.
        """
        # This method will call visit_project, visit_folder, and visit_file below as it walks the project tree.
        ProjectWalker.walk_project(project, self)

    def visit_project(self, item):
        """Not making use of this part of ProjectWalker"""
        pass

    def visit_folder(self, item, parent):
        """Not making use of this part of ProjectWalker"""
        pass

    def visit_file(self, item, parent):
        """
        Increment count based on file size.
        :param item: RemoteFile file we want to add size to our total
        :param parent: RemoteProject/RemoteFolder parent of item
        :return:
        """
        self.count += item.size