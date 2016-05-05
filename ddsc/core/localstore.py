import hashlib
import math
import mimetypes
import os

from ddsc.core.util import KindType


class LocalProject(object):
    """
    Represents a list of folder/file trees on the filesystem.
    Has kind property to allow project tree traversal with ProjectWalker.
    """
    def __init__(self, followsymlinks):
        """
        Creates a list of local file system content that can be sent to a remote project.
        :param followsymlinks: bool follow symbolic links when looking for content
        """
        self.remote_id = ''
        self.kind = KindType.project_str
        self.children = []
        self.sent_to_remote = False
        self.followsymlinks = followsymlinks

    def add_path(self, path):
        """
        Add the path and any children files/folders to the list of content.
        :param path: str path to add
        """
        abspath = os.path.abspath(path)
        self.children.append(_build_project_tree(abspath, self.followsymlinks))

    def add_paths(self, path_list):
        """
        Add a list of paths to the list of content.
        :param path_list: [str] list of file system paths
        """
        for path in path_list:
            self.add_path(path)

    def update_remote_ids(self, remote_project):
        """
        Compare against remote_project saving off the matching uuids of of matching content.
        :param remote_project: RemoteProject project to compare against
        """
        if remote_project:
            self.remote_id = remote_project.id
            _update_remote_children(remote_project, self.children)

    def set_remote_id_after_send(self, remote_id):
        """
        Save remote_id after creating on remote side.
        :param remote_id: str uuid of the project
        """
        self.remote_id = remote_id
        self.sent_to_remote = True

    def __str__(self):
        child_str = ', '.join([str(child) for child in self.children])
        return 'project: [{}]'.format(child_str)


def _name_to_child_map(children):
    """
    Create a map of name to child based on a list.
    :param children [LocalFolder/LocalFile] list of children:
    :return: map child.name -> child
    """
    name_to_child = {}
    for child in children:
        name_to_child[child.name] = child
    return name_to_child


def _update_remote_children(remote_parent, children):
    """
    Update remote_ids based on on parent matching up the names of children.
    :param remote_parent: RemoteProject/RemoteFolder who has children
    :param children: [LocalFolder,LocalFile] children to set remote_ids based on remote children
    """
    name_to_child = _name_to_child_map(children)
    for remote_child in remote_parent.children:
        local_child = name_to_child.get(remote_child.name)
        if local_child:
            local_child.update_remote_ids(remote_child)


def _build_project_tree(path, followsymlinks):
    """
    Build a tree of LocalFolder with children or just a LocalFile based on a path.
    :param path: str path to a directory to walk
    :param followsymlinks: bool should we follow symlinks when walking
    :return: the top node of the tree LocalFile or LocalFolder
    """
    result = None
    if os.path.isfile(path):
        result = LocalFile(path)
    else:
        result = _build_folder_tree(os.path.abspath(path), followsymlinks)
    return result


def _build_folder_tree(top_abspath, followsymlinks):
    """
    Build a tree of LocalFolder with children based on a path.
    :param top_abspath: str path to a directory to walk
    :param followsymlinks: bool should we follow symlinks when walking
    :return: the top node of the tree LocalFolder
    """
    path_to_content = {}
    child_to_parent = {}
    for dir_name, child_dirs, child_files in os.walk(top_abspath, followlinks=followsymlinks):
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
    Has kind property to allow project tree traversal with ProjectWalker.
    """
    def __init__(self, path):
        """
        Setup folder based on a path.
        :param path: str path to filesystem directory
        """
        self.path = os.path.abspath(path)
        self.name = os.path.basename(self.path)
        self.children = []
        self.remote_id = ''
        self.is_file = False
        self.kind = KindType.folder_str
        self.sent_to_remote = False

    def add_child(self, child):
        """
        Add a child to this folder.
        :param child: LocalFolder/LocalFile to add
        """
        self.children.append(child)

    def update_remote_ids(self, remote_folder):
        """
        Set remote id based on remote_folder and check children against this folder's children.
        :param remote_folder: RemoteFolder to compare against
        """
        self.remote_id = remote_folder.id
        _update_remote_children(remote_folder, self.children)

    def set_remote_id_after_send(self, remote_id):
        """
        Set remote id after we sent this folder to a remote store.
        :param remote_id: str uuid of this folder created on remote store
        """
        self.sent_to_remote = True
        self.remote_id = remote_id

    def __str__(self):
        child_str = ', '.join([str(child) for child in self.children])
        return 'folder:{} [{}]'.format(self.name, child_str)


class LocalFile(object):
    """
    Represents a file on disk.
    Has kind property to allow project tree traversal with ProjectWalker.
    """
    def __init__(self, path):
        """
        Setup file based on filesystem path.
        :param path: path to a file on the filesystem
        """
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
        self.kind = KindType.file_str
        self.sent_to_remote = False

    def get_hashpair(self):
        """
        Return a tuple of algorithm name(md5) and a hash of the entire file.
        :return: (str, str) format (<algorithm>, <hashvalue>)
        """
        hash = HashUtil()
        hash.add_file(self.path)
        return hash.hexdigest()

    def update_remote_ids(self, remote_file):
        """
        Based on a remote file try to assign a remote_id and compare hash info.
        :param remote_file: RemoteFile remote data pull remote_id from
        """
        self.remote_id = remote_file.id
        (alg, file_hash) = self.get_hashpair()
        if alg == remote_file.hash_alg and file_hash == remote_file.file_hash:
            self.need_to_send = False

    def set_remote_id_after_send(self, remote_id):
        """
        Set remote_id to specific value after this file has been sent to remote store.
        :param remote_id: str uuid of the file in the remote store
        """
        self.sent_to_remote = True
        self.remote_id = remote_id

    def count_chunks(self, bytes_per_chunk):
        return math.ceil(float(self.size) / float(bytes_per_chunk))

    def __str__(self):
        return 'file:{}'.format(self.name)


class HashUtil(object):
    """
    Utility to create hash pair (name, hash) for a file or chunk.
    """
    def __init__(self):
        self.hash = hashlib.md5()

    def add_file(self, filename, block_size=4096):
        """
        Add an entire file to this hash.
        :param filename: str filename of the file to hash
        :param block_size: int size of chunks when reading the file
        """
        with open(filename, "rb") as f:
            for chunk in iter(lambda: f.read(block_size), b""):
                self.hash.update(chunk)

    def add_chunk(self, chunk):
        """
        Add a single block of memory to the hash.
        :param chunk: str data to hash
        :return:
        """
        self.hash.update(chunk)

    def hexdigest(self):
        """
        return a hash pair
        :return: (str,str) -> (algorithm,value)
        """
        return "md5", self.hash.hexdigest()
