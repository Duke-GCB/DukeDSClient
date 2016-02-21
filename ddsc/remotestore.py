from localstore import LocalContent, HashUtil


class RemoteContentFetch(object):
    def __init__(self, data_service):
        self.data_service = data_service

    def fetch_remote_project(self, project_name, path_list):
        project = self._get_my_project(project_name)
        if project:
            self._add_project_children(project)
        return project

    def _get_my_project(self, project_name):
        response = self.data_service.get_projects().json()
        for project in response['results']:
            if project['name'] == project_name:
                return RemoteProject(project)
        return None

    def _add_project_children(self, project):
        response = self.data_service.get_project_children(project.id, '').json()
        for child in response['results']:
            self._add_child_recur(project, child)

    def _add_child_recur(self, parent, child):
            kind = child['kind']
            if kind == 'dds-folder':
                parent.add_child(self._read_folder(child))
            elif kind == 'dds-file':
                parent.add_child(RemoteFile(child))
            else:
                raise ValueError("Unknown child type {}".format(kind))

    def _read_folder(self, folder_json):
        folder = RemoteFolder(folder_json)
        response = self.data_service.get_folder_children(folder.id, '').json()
        for child in response['results']:
            self._add_child_recur(folder, child)
        return folder


class RemoteProject(object):
    """
    Project data from a remote store projects request.
    Represents the top of a tree.
    """
    def __init__(self, json_data):
        self.id = json_data['id']
        self.kind = json_data['kind']
        self.name = json_data['name']
        self.description = json_data['description']
        self.is_deleted = json_data['is_deleted']
        self.children = []

    def add_child(self, child):
        self.children.append(child)

    def get_paths(self):
        paths = set()
        for child in self.children:
            paths.update(child.get_paths(''))
        return paths

    def __str__(self):
        return 'project: {} id:{} {}'.format(self.name, self.id, self.children)


class RemoteFolder(object):
    """
    Folder data from a remote store project_id_children or folder_id_children request.
    Represents a leaf or branch in a project tree.
    """
    def __init__(self, json_data):
        self.id = json_data['id']
        self.kind = json_data['kind']
        self.name = json_data['name']
        self.is_deleted = json_data['is_deleted']
        self.children = []

    def add_child(self, child):
        self.children.append(child)

    def get_paths(self, parent):
        paths = set()
        my_path = parent + '/' + self.name
        paths.add(my_path)
        for child in self.children:
            paths.update(child.get_paths(my_path))
        return paths

    def __str__(self):
        return 'folder: {} id:{} {}'.format(self.name, self.id, self.children)



class RemoteFile(object):
    """
    File data from a remote store project_id_children or folder_id_children request.
    Represents a leaf in a project tree.
    """
    def __init__(self, json_data):
        self.id = json_data['id']
        self.kind = json_data['kind']
        self.name = json_data['name']
        self.is_deleted = json_data['is_deleted']
        self.size = json_data['upload']['size']

    def get_paths(self, parent):
        paths = set()
        paths.add(parent + '/' + self.name)
        return paths

    def __str__(self):
        return 'file: {} id:{} size:{}'.format(self.name, self.id, self.size)


    def create_remote_content(self, local_content):
        self._try_create_remote_project(local_content)
        for local_child in local_content.children:
            self._try_create_remote_child(local_content.remote_id, local_content, local_child)

    def _try_create_remote_project(self, local_content):
        if not local_content.remote_id:
            result = self.data_service.create_project(self.project_name, self.project_name)
            local_content.remote_id = result.json()['id']

    def _try_create_remote_child(self, project_id, parent, local_child):
            if local_child.is_file:
                file_on_disk = FileOnDisk(self.data_service, local_child)
                file_on_disk.upload(project_id, parent.kind, parent.remote_id)
            else:
                if not local_child.remote_id:
                    result = self.data_service.create_folder(local_child.name, parent.kind, parent.remote_id)
                    local_child.remote_id = result.json()['id']
                for sub_child in local_child.children:
                    self._try_create_remote_child(project_id, local_child, sub_child)


class FileOnDisk(object):
    """Return a chunks lazily."""
    def __init__(self, dsa, local_file):
        self.dsa = dsa
        self.local_file = local_file
        self.filename = local_file.path
        self.content_type = local_file.mimetype

    def upload(self, project_id, parent_kind, parent_id):
        size = self.local_file.size
        (hash_alg, hash_value) = self.local_file.get_hashpair()
        name = self.local_file.name
        resp = self.dsa.create_upload(project_id, name, self.content_type, size, hash_value, hash_alg)
        upload_id = resp.json()['id']
        self._send_file_chunks(upload_id)
        self.dsa.complete_upload(upload_id)
        result = self.dsa.create_file(parent_kind, parent_id, upload_id)
        return result.json()['id']

    def _send_file_chunks(self, upload_id):
        with open(self.filename,'rb') as infile:
            number = 0
            for chunk in read_in_chunks(infile, self.dsa.bytes_per_chunk):
                (chunk_hash_alg, chunk_hash_value) = hash_chunk(chunk)
                resp = self.dsa.create_upload_url(upload_id, number, len(chunk),
                                                  chunk_hash_value, chunk_hash_alg)
                if resp.status_code == 200:
                    self._send_file_external(resp.json(), chunk)
                    number += 1
                else:
                    raise ValueError("Failed to retrieve upload url status:" + str(resp.status_code))

    def _send_file_external(self, url_json, chunk):
        http_verb = url_json['http_verb']
        host = url_json['host']
        url = url_json['url']
        http_headers = url_json['http_headers']
        resp = self.dsa.send_external(http_verb, host, url, http_headers, chunk)
        if resp.status_code != 200 and resp.status_code != 201:
            raise ValueError("Failed to send file to external store. Error:" + str(resp.status_code))

    def _send_file_external(self, url_json, chunk):
        http_verb = url_json['http_verb']
        host = url_json['host']
        url = url_json['url']
        http_headers = url_json['http_headers']
        resp = self.dsa.send_external(http_verb, host, url, http_headers, chunk)
        if resp.status_code != 200 and resp.status_code != 201:
            raise ValueError("Failed to send file to external store. Error:" + str(resp.status_code))

def hash_chunk(chunk):
    """Creates a hash from the bytes in chunk."""
    hash = HashUtil()
    hash.add_chunk(chunk)
    return hash.hexdigest()


def read_in_chunks(infile, blocksize):
    """Return a chunks lazily."""
    while True:
        data = infile.read(blocksize)
        if not data:
            break
        yield data


class RemoteContentSender(object):
    def __init__(self, data_service, project_id, project_name, watcher):
        self.data_service = data_service
        self.project_id = project_id
        self.project_name = project_name
        self.watcher = watcher

    def visit_project(self, item, parent):
        if not item.remote_id:
            self.watcher.sending_item(item)
            result = self.data_service.create_project(self.project_name, self.project_name)
            item.remote_id = result.json()['id']
            item.sent_to_remote = True
            self.project_id = item.remote_id

    def visit_folder(self, item, parent):
        if not item.remote_id:
            self.watcher.sending_item(item)
            result = self.data_service.create_folder(item.name, parent.kind, parent.remote_id)
            item.remote_id = result.json()['id']
            item.sent_to_remote = True

    def visit_file(self, item, parent):
        # Always sending files right, no way to know if different without downloading.
        self.watcher.sending_item(item)
        file_on_disk = FileOnDisk(self.data_service, item)
        item.remote_id = file_on_disk.upload(self.project_id, parent.kind, parent.remote_id)
        item.sent_to_remote = True
