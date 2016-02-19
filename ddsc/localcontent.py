import os
import hashlib


class LocalContent(object):
    """
    Represents a list of folder/file trees on the filesystem.
    """
    def __init__(self):
        self.children = []

    def add_path(self, path):
        abspath = os.path.abspath(path)
        self.children.append(_build_project_tree(abspath))

    def __repr__(self):
        return 'project: {}'.format(self.children)


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

    def add_child(self, child):
        self.children.append(child)

    def __repr__(self):
        return 'folder:{} {}'.format(self.name, self.children)


class LocalFile(object):
    """
    Represents a file on disk.
    """
    def __init__(self, path):
        self.path = os.path.abspath(path)
        self.name = os.path.basename(self.path)

    def get_hashpair(self):
        hash = HashUtil()
        hash.add_file(self.path)
        return hash.hexdigest()

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
                hash.update(chunk)

    def add_chunk(self, chunk):
        hash.update(chunk)

    def hexdigest(self):
        return "md5", hash.hexdigest()