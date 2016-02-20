class RemoteContent(object):
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

    def __repr__(self):
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

    def __repr__(self):
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

    def __repr__(self):
        return 'file: {} id:{} size:{}'.format(self.name, self.id, self.size)
