import os
from ddsc.core.ddsapi import DataServiceApi, DataServiceError, DataServiceAuth
from ddsc.core.util import KindType, REMOTE_PATH_SEP, RemotePath
from ddsc.core.localstore import HashUtil
from ddsc.core.userutil import UserUtil

FETCH_ALL_USERS_PAGE_SIZE = 25
DOWNLOAD_FILE_CHUNK_SIZE = 20 * 1024 * 1024

# Response content that does not need to be generated from the /project/<id>/children list.
PROJECT_LIST_EXCLUDE_RESPONSE_FIELDS = [
    'audit',
    'ancestors',
    'project',
]
# There is also a 'current_file_version' option that is still needed since it contains the
# file hash and size.


class RemoteStore(object):
    """
    Fetches project tree data from remote store.
    """
    def __init__(self, config, data_service=None):
        """
        Setup to allow fetching project tree.
        :param config: ddsc.config.Config settings to use for connecting to the dataservice.
        :param data_service: DataServiceApi: optional param to specify an existing DataServiceApi object
        """
        self.config = config
        if data_service:
            self.data_service = data_service
        else:
            auth = DataServiceAuth(self.config)
            self.data_service = DataServiceApi(auth, self.config.url)

    def fetch_remote_project(self, project_name_or_id, must_exist=False, include_children=True):
        """
        Retrieve the project via project name or id.
        :param project_name_or_id: ProjectNameOrId name or id of the project to fetch
        :param must_exist: should we error if the project doesn't exist
        :param include_children: should we read children(folders/files)
        :return: RemoteProject project requested or None if not found(and must_exist=False)
        """
        project = self._get_my_project(project_name_or_id)
        if project:
            if include_children:
                self._add_project_children(project, PROJECT_LIST_EXCLUDE_RESPONSE_FIELDS)
        else:
            if must_exist:
                project_description = project_name_or_id.description()
                raise NotFoundError(u'There is no project with the {}'.format(project_description))
        return project

    def fetch_remote_project_by_id(self, id):
        """
        Retrieves project from via id
        :param id: str id of project from data service
        :return: RemoteProject we downloaded
        """
        response = self.data_service.get_project_by_id(id).json()
        return RemoteProject(response)

    def _get_my_project(self, project_name_or_id):
        """
        Return project tree root for project_name_or_id.
        :param project_name_or_id: ProjectNameOrId name or id of the project to lookup
        :return: RemoteProject project we found or None
        """
        response = self.data_service.get_projects().json()
        for project in response['results']:
            if project_name_or_id.contained_in_dict(project):
                return RemoteProject(project)
        return None

    def _add_project_children(self, project, exclude_response_fields=None):
        """
        Add the rest of the project tree from the remote store to the project object.
        :param project: RemoteProject root of the project tree to add children too
        :param exclude_response_fields: [str]: list of fields to exclude in the children response items
        """
        response = self.data_service.get_project_children(project.id, '', exclude_response_fields).json()
        project_children = RemoteProjectChildren(project.id, response['results'])
        for child in project_children.get_tree():
            project.add_child(child)

    def lookup_or_register_user_by_email_or_username(self, email, username):
        """
        Lookup user by email or username. Only fill in one field.
        For both cases it will try to register if not found.
        :param email: str: email address of the user
        :param username: netid of the user to find
        :return: RemoteUser
        """
        if username:
            return self.get_or_register_user_by_username(username)
        else:
            return self.get_or_register_user_by_email(email)

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
            raise NotFoundError("User not found:" + full_name)
        elif found_cnt > 1:
            raise ValueError("Multiple users with name:" + full_name)
        user = RemoteUser(results[0])
        if user.full_name.lower() != full_name.lower():
            raise NotFoundError("User not found:" + full_name)
        return user

    def get_or_register_user_by_username(self, username):
        """
        Try to lookup user by username. If not found try registering the user.
        :param username: str: username to lookup
        :return: RemoteUser: user we found
        """
        util = UserUtil(self.data_service)
        user_json = util.find_user_by_username(username)
        if not user_json:
            user_json = util.register_user_by_username(username)
        return RemoteUser(user_json)

    def get_or_register_user_by_email(self, email):
        """
        Try to lookup user by email. If not found try registering the user.
        Raises ValueError when unable to find/register a user for the email.
        :param email: str: email to lookup or register a user for
        :return: RemoteUser: user we found
        """
        util = UserUtil(self.data_service)
        user_json = util.find_user_by_email(email)
        if not user_json:
            affiliate = util.find_affiliate_by_email(email)
            if affiliate:
                user_json = util.register_user_by_username(affiliate['uid'])
            else:
                raise ValueError("Unable to find or register a user with email {}".format(email))
        return RemoteUser(user_json)

    def get_auth_providers(self):
        """
        Return the list of authorization providers.
        :return: [RemoteAuthProvider]: list of remote auth providers
        """
        providers = []
        response = self.data_service.get_auth_providers().json()
        for data in response['results']:
            providers.append(RemoteAuthProvider(data))
        return providers

    def get_current_user(self):
        """
        Fetch info about the current user
        :return: RemoteUser user who we are logged in as(auth determines this).
        """
        response = self.data_service.get_current_user().json()
        return RemoteUser(response)

    def fetch_users(self, email=None, username=None):
        """
        Retrieves users with optional email and/or username filtering from data service.
        :param email: str: optional email to filter by
        :param username: str: optional username to filter by
        :return: [RemoteUser] list of all users we downloaded
        """
        users = []
        result = self.data_service.get_users(email=email, username=username)
        user_list_json = result.json()
        for user_json in user_list_json['results']:
            users.append(RemoteUser(user_json))
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
            self.data_service.get_user_project_permission(project.id, user.id)
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

    def get_project_names(self):
        """
        Return a list of names of the remote projects owned by this user.
        :return: [str]: the list of project names
        """
        names = []
        response = self.data_service.get_projects().json()
        for project in response['results']:
            names.append(project['name'])
        return names

    def get_projects_details(self):
        """
        Return list of top level details for all projects
        """
        return self.data_service.get_projects().json()['results']

    def get_projects_with_auth_role(self, auth_role):
        """
        Return the list of projects that have the specified auth role from the list that the current user has access to.
        :param auth_role: str: auth role we are filtering for
        :return: [dict]: list of projects that have auth_role permissions for the current user
        """
        user = self.get_current_user()
        # user.id
        projects = []
        response = self.data_service.get_projects().json()
        for project in response['results']:
            project_id = project['id']
            permissions = self.data_service.get_user_project_permission(project_id, user.id).json()
            if auth_role == permissions['auth_role']['id']:
                projects.append(project)
        return projects

    def delete_project(self, project_name_or_id):
        """
        Find the project with project_name_or_id and delete it raise error if not found.
        :param project_name_or_id: ProjectNameOrId: name or id of the project we want to be deleted
        """
        project = self._get_my_project(project_name_or_id)
        if project:
            self.data_service.delete_project(project.id)
        else:
            raise ValueError("No project with {} found.\n".format(project_name_or_id.description()))

    def get_active_auth_roles(self, context):
        """
        Retrieve non-deprecated authorization roles based on a context.
        Context should be RemoteAuthRole.PROJECT_CONTEXT or RemoteAuthRole.SYSTEM_CONTEXT.
        :param context: str: context for which auth roles to retrieve
        :return: [RemoteAuthRole]: list of active auth_role objects
        """
        response = self.data_service.get_auth_roles(context).json()
        return self.get_active_auth_roles_from_json(response)

    @staticmethod
    def get_active_auth_roles_from_json(json_data):
        """
        Given a json blob response containing a list of authorization roles return the active ones
        in an array of RemoteAuthRole objects.
        :param json_data: list of dictionaries - data from dds in auth_role format
        :return: [RemoteAuthRole] list of active auth_role objects
        """
        result = []
        for auth_role_properties in json_data['results']:
            auth_role = RemoteAuthRole(auth_role_properties)
            if not auth_role.is_deprecated:
                result.append(auth_role)
        return result

    def get_project_files(self, project):
        """
        Returns a list of project files (files in the project including their download links)
        :param project: RemoteProject
        :return: [ProjectFile]: files in the specified project
        """
        files = []
        result = self.data_service.get_project_files(project.id)
        user_list_json = result.json()
        for user_json in user_list_json['results']:
            files.append(ProjectFile(user_json))
        return files

    def get_file_url(self, file_id):
        """
        Given a file id return the RemoteFileUrl (file download url)
        :param file_id: str: DukeDS file uuid
        :return: RemoteFileUrl
        """
        return RemoteFileUrl(self.data_service.get_file_url(file_id).json())


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
        self.remote_path = REMOTE_PATH_SEP

    def add_child(self, child):
        """
        Add a file or folder to our remote project.
        :param child: RemoteFolder/RemoteFile child to add.
        """
        self.children.append(child)

    def get_project_name_or_id(self):
        """
        :return: ProjectNameOrId: contains key of id
        """
        return ProjectNameOrId.create_from_remote_project(self)

    def __str__(self):
        return 'project: {} id:{} {}'.format(self.name, self.id, self.children)


class RemoteFolder(object):
    """
    Folder data from a remote store project_id_children or folder_id_children request.
    Represents a leaf or branch in a project tree.
    Has kind property to allow project tree traversal with ProjectWalker.
    """
    def __init__(self, json_data, parent_remote_path):
        """
        Set properties based on json_data.
        :param json_data: dict JSON data containing folder info
        :param parent_remote_path: remote_path path to this folder's parent
        """
        self.id = json_data['id']
        self.kind = json_data['kind']
        self.name = json_data['name']
        self.is_deleted = json_data['is_deleted']
        self.children = []
        self.remote_path = os.path.join(parent_remote_path, self.name)

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
    def __init__(self, json_data, parent_remote_path):
        """
        Set properties based on json_data.
        :param json_data: dict JSON data containing file info
        :param parent_remote_path: remote_path path to this file's parent
        """
        self.id = json_data['id']
        self.file_version_id = json_data['current_version']['id']
        self.kind = json_data['kind']
        self.name = json_data['name']
        self.path = self.name  # for compatibility with ProgressPrinter
        self.is_deleted = json_data['is_deleted']
        upload = RemoteFile.get_upload_from_json(json_data)
        self.size = upload['size']
        self.file_hash = None
        self.hash_alg = None
        hash_data = RemoteFile.get_hash_from_upload(upload)
        if hash_data:
            self.file_hash = hash_data.get('value')
            self.hash_alg = hash_data.get('algorithm')
        self.remote_path = os.path.join(parent_remote_path, self.name)

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

    @staticmethod
    def get_hash_from_upload(upload, target_algorithm=HashUtil.HASH_NAME):
        """
        Find hash value in upload dictionary.
        Older upload format stores a single hash in 'hash' property.
        New upload format stores multiple under 'hashes' property for this one we look for a particular algorithm.
        :param upload: dictionary: contains hash data in DukeDS upload format.
        :param target_algorithm: str: name of the algorithm to look for if there are more than one hash
        :return: dictionary of hash information, keys: "algorithm" and  "value"
        """
        hash_info = upload.get('hash')
        if hash_info:
            return hash_info
        hashes_array = upload.get('hashes')
        if hashes_array:
            for hash_info in hashes_array:
                algorithm = hash_info.get('algorithm')
                if algorithm == target_algorithm:
                    return hash_info
        return None

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
        self.first_name = json_data['first_name']
        self.last_name = json_data['last_name']

    def __str__(self):
        return 'id:{} username:{} full_name:{}'.format(self.id, self.username, self.full_name)


class RemoteAuthRole(object):
    PROJECT_CONTEXT = "project"
    SYSTEM_CONTEXT = "system"
    """
    Permissions a user can be given on a project.
    """
    def __init__(self, json_data):
        """
        Set properties based on json_data.
        :param json_data: dict JSON data containing auth_role info
        """
        self.id = json_data['id']
        self.name = json_data['name']
        self.description = json_data['description']
        self.is_deprecated = json_data['is_deprecated']

    def __str__(self):
        return 'id:{} name:{} description:{}'.format(self.id, self.name, self.description)


class RemoteProjectChildren(object):
    """
    Creates RemoteFolders and RemoteFiles as tree structure based on DukeDS recursive project children data.
    """
    def __init__(self, project_id, data):
        """
        Specify the project_id and the array of item dictionaries.
        :param project_id: str: uuid of the project
        :param data: [object]: DukeDS recursive project children
        """
        self.project_id = project_id
        self.data = data

    def _get_children_for_parent(self, parent_id):
        """
        Given a parent uuid return a list of dictionaries.
        :param parent_id: str: uuid of the parent
        :return: [dict]: children in this list with parent_id parent
        """
        children = []
        for child in self.data:
            parent = child['parent']
            if parent['id'] == parent_id:
                children.append(child)
        return children

    def get_tree(self):
        """
        Return array of RemoteFolders(with appropriate children)/RemoteFiles based on the values from constructor.
        :return: [RemoteFolder/RemoteFile]
        """
        return self.get_tree_recur(self.project_id, REMOTE_PATH_SEP)

    def get_tree_recur(self, parent_id, parent_path):
        """
        Recursively create array RemoteFolders/RemoteFiles.
        :param parent_id: str: uuid if the parent to find children for
        :param parent_path: str: remote path of parent to build child paths
        :return: [RemoteFolder/RemoteFile]
        """
        children = []
        for child_data in self._get_children_for_parent(parent_id):
            if child_data['kind'] == KindType.folder_str:
                folder = RemoteFolder(child_data, parent_path)
                for grand_child in self.get_tree_recur(child_data['id'], folder.remote_path):
                    folder.add_child(grand_child)
                children.append(folder)
            else:
                file = RemoteFile(child_data, parent_path)
                children.append(file)
        return children


class RemoteAuthProvider(object):
    def __init__(self, json_data):
        """
        Set properties based on json_data.
        :param json_data: dict JSON data containing auth_role info
        """
        self.id = json_data['id']
        self.service_id = json_data['service_id']
        self.name = json_data['name']
        self.is_deprecated = json_data['is_deprecated']
        self.is_default = json_data['is_default']
        self.is_deprecated = json_data['is_deprecated']
        self.login_initiation_url = json_data['login_initiation_url']


class NotFoundError(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message


class ProjectNameOrId(object):
    """
    Contains either a project name or a project id(uuid).
    If it contains a project ID the project is assumed to already exist (only DukeDS makes up project uuid).
    If it contains a project name they project may or may not exist.
    """
    def __init__(self, value, is_name):
        self.value = value
        self.is_name = is_name

    def description(self):
        format_str = 'name {}'
        if not self.is_name:
            format_str = 'id {}'
        return format_str.format(self.value)

    def contained_in_dict(self, data):
        if self.is_name:
            return self.value == data.get('name')
        else:
            return self.value == data.get('id')

    def get_name_or_raise(self):
        if self.is_name:
            return self.value
        raise ValueError("Programming Error: Cannot return name. value is project id.")

    def get_id_or_raise(self):
        if not self.is_name:
            return self.value
        raise ValueError("Programming Error: Cannot return id. value is project name.")

    @staticmethod
    def create_from_name(name):
        return ProjectNameOrId(value=name, is_name=True)

    @staticmethod
    def create_from_project_id(project_id):
        return ProjectNameOrId(value=project_id, is_name=False)

    @staticmethod
    def create_from_remote_project(project):
        return ProjectNameOrId.create_from_project_id(project.id)


class ProjectFile(object):
    def __init__(self, json_data):
        """
        Set properties based on json_data.
        :param json_data: dict JSON data containing auth_role info
        """
        self.id = json_data['id']
        self.name = json_data['name']
        self.size = json_data['size']
        self.file_url = json_data['file_url']
        self.hashes = json_data['hashes']
        self.ancestors = json_data['ancestors']
        self.json_data = json_data
        self.kind = KindType.file_str

    @property
    def path(self):
        names = self._get_remote_parent_folder_names()
        names.append(self.name)
        return RemotePath.add_leading_slash(REMOTE_PATH_SEP.join(names))

    def get_remote_parent_path(self):
        parent_folders_path = REMOTE_PATH_SEP.join(self._get_remote_parent_folder_names())
        return RemotePath.add_leading_slash(parent_folders_path)

    def _get_remote_parent_folder_names(self):
        return [item['name'] for item in self.ancestors if item['kind'] == KindType.folder_str]

    def get_local_path(self, directory_path):
        # Removing leading slash from self.path because os.path.join ignores preceding paths if it encounters an
        # absolute path.
        path_without_leading_slash = RemotePath.strip_leading_slash(self.path)
        return os.path.join(directory_path, path_without_leading_slash)

    def get_hash(self):
        return RemoteFile.get_hash_from_upload(self.json_data)

    @staticmethod
    def create_for_dds_file_dict(file_dict):
        """
        Create a ProjectFile for a DukeDS File dict specifying None for file_url since this data is not present.
        :param file_dict: dict: DukeDS API file dict
        :return: ProjectFile
        """
        # create a DukeDS ProjectFile dict based on input DukeDS File dict
        project_file_dict = {
            "id": file_dict["id"],
            "name": file_dict["name"],
            "size": file_dict["current_version"]["upload"]["size"],
            "hashes": file_dict["current_version"]["upload"]["hashes"],
            "ancestors": file_dict["ancestors"],
            "file_url": None,

        }
        return ProjectFile(project_file_dict)


class RemoteFileUrl(object):
    def __init__(self, json_data):
        self.http_verb = json_data['http_verb']
        self.host = json_data['host']
        self.url = json_data['url']
        self.http_headers = json_data['http_headers']
