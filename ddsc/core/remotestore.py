from ddsc.core.ddsapi import DataServiceApi, DataServiceError, DataServiceAuth
from ddsc.core.util import KindType

FETCH_ALL_USERS_PAGE_SIZE = 25
DOWNLOAD_FILE_CHUNK_SIZE = 20 * 1024 * 1024


class RemoteStore(object):
    """
    Fetches project tree data from remote store.
    """
    def __init__(self, config):
        """
        Setup to allow fetching project tree.
        :param config: ddsc.config.Config settings to use for connecting to the dataservice.
        """
        self.config = config
        auth = DataServiceAuth(self.config)
        self.data_service = DataServiceApi(auth, self.config.url)

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

    def fetch_remote_project_by_id(self, id):
        """
        Retrieves project from via id
        :param id: str id of project from data service
        :return: RemoteProject we downloaded
        """
        response = self.data_service.get_project_by_id(id).json()
        return RemoteProject(response)

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
        file_hash = RemoteFile.get_upload_from_json(response.json())['hash']
        if file_hash:
            remote_file.set_hash(file_hash['value'], file_hash['algorithm'])
        return remote_file

    def lookup_user_by_email_or_username(self, email, username):
        if username:
            return self.lookup_user_by_username(username)
        else:
            return self.lookup_user_by_email(email)

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

    def get_current_user(self):
        """
        Fetch info about the current user
        :return: RemoteUser user who we are logged in as(auth determines this).
        """
        response = self.data_service.get_current_user().json()
        return RemoteUser(response)

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

    def fetch_user(self, id):
        """
        Retrieves user from data service having a specific id
        :param id: str id of user from data service
        :return: RemoteUser user we downloaded
        """
        response = self.data_service.get_user_by_id(id).json()
        return RemoteUser(response)

    def set_user_project_permission(self, project, user, auth_role):
        """
        Update remote store for user giving auth_role permissions on project.
        :param project: RemoteProject project to give permissions to
        :param user: RemoteUser user who we are giving permissions to
        :param auth_role: str type of authorization to give user(project_admin)
        """
        self.data_service.set_user_project_permission(project.id, user.id, auth_role)

    def revoke_user_project_permission(self, project, user):
        """
        Update remote store for user removing auth_role permissions on project.
        :param project: RemoteProject project to remove permissions from
        :param user: RemoteUser user who we are removing permissions from
        """
        # Server errors out with 500 if a user isn't found.
        try:
            resp = self.data_service.get_user_project_permission(project.id, user.id)
            self.data_service.revoke_user_project_permission(project.id, user.id)
        except DataServiceError as e:
            if e.status_code != 404:
                raise

    def download_file(self, remote_file, path, watcher):
        """
        Download a remote file associated with the remote uuid(file_id) into local path.
        :param remote_file: RemoteFile file to retrieve
        :param path: str file system path to save the contents to.
        :param watcher: object implementing send_item(item, increment_amt) that updates UI
        """
        url_json = self.data_service.get_file_url(remote_file.id).json()
        http_verb = url_json['http_verb']
        host = url_json['host']
        url = url_json['url']
        http_headers = url_json['http_headers']
        response = self.data_service.receive_external(http_verb, host, url, http_headers)
        with open(path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=DOWNLOAD_FILE_CHUNK_SIZE):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
                    watcher.transferring_item(remote_file, increment_amt=len(chunk))


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
        self.size = RemoteFile.get_upload_from_json(json_data)['size']
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

    @staticmethod
    def get_upload_from_json(json_data):
        if 'current_version' in json_data:
            return json_data['current_version']['upload']
        else:
            if 'upload' in json_data:
                return json_data['upload']
            else:
                raise ValueError("Invalid file json data, unable to find upload.")

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
