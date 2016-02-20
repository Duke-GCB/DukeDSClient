import os
import hashlib
import mimetypes

class LocalContent(object):
    """
    Represents a list of folder/file trees on the filesystem.
    """
    def __init__(self):
        self.remote_id = ''
        self.kind = 'dds-project'
        self.children = []

    def add_path(self, path):
        abspath = os.path.abspath(path)
        self.children.append(_build_project_tree(abspath))

    def __repr__(self):
        return 'project: {}'.format(self.children)

    def update_remote_ids(self, remote_project):
        if remote_project:
            self.remote_id = remote_project.id
            _update_remote_children(remote_project, self.children)

def _name_to_child_map(children):
    name_to_child = {}
    for child in children:
        name_to_child[child.name] = child
    return name_to_child


def _update_remote_children(remote_parent, children):
    name_to_child = _name_to_child_map(children)
    for remote_child in remote_parent.children:
        local_child = name_to_child.get(remote_child.name)
        if local_child:
            local_child.update_remote_ids(remote_child)


def _build_project_tree(path):
    result = None
    if os.path.isfile(path):
        result = LocalFile(path)
    else:
        result = _build_folder_tree(os.path.abspath(path))
    return result


def _build_folder_tree(top_abspath):
    path_to_content = {}
    child_to_parent = {}
    for dir_name, child_dirs, child_files in os.walk(top_abspath):
        abspath = os.path.abspath(dir_name)
        folder = LocalFolder(abspath)
        path_to_content[abspath] = folder
        # If we have a parent add us to it.
        parent_path = child_to_parent.get(abspath)
        if parent_path:
            path_to_content[parent_path].add_child(folder)
        for child_dir in child_dirs:
            # Record dir_name as the parent of child_dir so we can call add_child when get to it.
            abs_child_path = os.path.abspath(os.path.join(dir_name, child_dir))
            child_to_parent[abs_child_path] = abspath
        for child_filename in child_files:
            folder.add_child(LocalFile(os.path.join(dir_name, child_filename)))
    return path_to_content.get(top_abspath)


class LocalFolder(object):
    """
    A folder on disk.
    """
    def __init__(self, path):
        self.path = os.path.abspath(path)
        self.name = os.path.basename(self.path)
        self.children = []
        self.remote_id = ''
        self.is_file = False
        self.kind = 'dds-folder'

    def add_child(self, child):
        self.children.append(child)

    def update_remote_ids(self, remote_folder):
        self.remote_id = remote_folder.id
        _update_remote_children(remote_folder, self.children)

    def __repr__(self):
        return 'folder:{} {}'.format(self.name, self.children)


class LocalFile(object):
    """
    Represents a file on disk.
    """
    def __init__(self, path):
        self.path = os.path.abspath(path)
        self.name = os.path.basename(self.path)
        self.size = os.path.getsize(self.path)
        self.need_to_send = True
        self.remote_id = ''
        self.is_file = True
        (mimetype, encoding) = mimetypes.guess_type(self.path)
        if not mimetype:
            mimetype = 'application/octet-stream'
        self.mimetype = mimetype
        self.kind = 'dds-file'

    def get_hashpair(self):
        hash = HashUtil()
        hash.add_file(self.path)
        return hash.hexdigest()

    def update_remote_ids(self, remote_file):
        self.remote_id = remote_file.id


    def __repr__(self):
        return 'file:{}'.format(self.name)


class HashUtil(object):
    """
    Utility to create hash pair (name, hash) for a file or chunk.
    """
    def __init__(self):
        self.hash = hashlib.md5()

    def add_file(self, filename, block_size=4096):
        with open(filename, "rb") as f:
            for chunk in iter(lambda: f.read(block_size), b""):
                self.hash.update(chunk)

    def add_chunk(self, chunk):
        self.hash.update(chunk)

    def hexdigest(self):
        return "md5", self.hash.hexdigest()