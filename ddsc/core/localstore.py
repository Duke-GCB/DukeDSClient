import hashlib
import math
import mimetypes
import os
from os.path import isfile, isdir
from ddsc.core.ignorefile import FileFilter, IgnoreFilePatterns
from ddsc.core.util import KindType, ProjectWalker, plural_fmt, join_with_commas_and_and


class LocalProject(object):
    """
    Represents a list of folder/file trees on the filesystem.
    Has kind property to allow project tree traversal with ProjectWalker.
    """
    def __init__(self, followsymlinks, file_exclude_regex):
        """
        Creates a list of local file system content that can be sent to a remote project.
        :param followsymlinks: bool follow symbolic links when looking for content
        :param file_exclude_regex: str: regex that should be used to filter out files we do not want to upload
        """
        self.remote_id = ''
        self.kind = KindType.project_str
        self.children = []
        self.sent_to_remote = False
        self.followsymlinks = followsymlinks
        self.file_filter = FileFilter(file_exclude_regex)

    def add_path(self, path):
        """
        Add the path and any children files/folders to the list of content.
        :param path: str path to add
        """
        abspath = os.path.abspath(path)
        child = _build_project_tree(abspath, self.followsymlinks, self.file_filter)
        if child:
            self.children.append(child)

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

    def count_local_items(self):
        return LocalItemsCounter(self)

    def count_items_to_send(self, upload_bytes_per_chunk):
        return ItemsToSendCounter(self, upload_bytes_per_chunk)

    def __str__(self):
        child_str = ', '.join([str(child) for child in self.children])
        return 'project: [{}]'.format(child_str)


class LocalItemsCounter(object):
    def __init__(self, local_project):
        self.folders = 0
        self.files = 0
        for child in local_project.children:
            self._count_recur(child)

    def _count_recur(self, item):
        if KindType.is_file(item):
            self.files += 1
        else:
            self.folders += 1
            for child in item.children:
                self._count_recur(child)

    def to_str(self, prefix):
        parts = []
        if self.files:
            parts.append(plural_fmt('file', self.files))
        if self.folders:
            parts.append(plural_fmt('folder', self.folders))
        combined_parts = join_with_commas_and_and(parts)
        return "{} {}.".format(prefix, combined_parts)


class ItemsToSendCounter(object):
    """
    Visitor that counts items that need to be sent in LocalContent.
    """
    def __init__(self, local_project, bytes_per_chunk):
        self.projects = 0
        self.folders = 0
        self.existing_folders = 0
        self.files = 0
        self.existing_files = 0
        self.chunks = 0
        self.bytes_per_chunk = bytes_per_chunk
        self._walk_project(local_project)

    def _walk_project(self, project):
        """
        Increment counters for each project, folder, and files calling visit methods below.
        :param project: LocalProject project we will count items of.
        """
        # This method will call visit_project, visit_folder, and visit_file below as it walks the project tree.
        ProjectWalker.walk_project(project, self)

    def visit_project(self, item):
        """
        Increments counter if the project is not already remote.
        :param item: LocalProject
        """
        if not item.remote_id:
            self.projects += 1

    def visit_folder(self, item, parent):
        """
        Increments counter if item is not already remote
        :param item: LocalFolder
        :param parent: LocalFolder/LocalProject
        """
        if not item.remote_id:
            self.folders += 1
        else:
            self.existing_folders += 1

    def visit_file(self, item, parent):
        """
        Increments counter if item needs to be sent.
        :param item: LocalFile
        :param parent: LocalFolder/LocalProject
        """
        self.files += 1
        self.chunks += item.count_chunks(self.bytes_per_chunk)
        if item.remote_id:
            self.existing_files += 1

    def total_items(self):
        """
        Total number of files/folders/chunks that need to be sent.
        :return: int number of items to be sent.
        """
        return self.projects + self.folders + self.chunks

    def to_str(self, prefix, local_items_count):
        parts = []
        new_files = local_items_count.files - self.existing_files
        existing_files = self.existing_files
        new_folders = local_items_count.folders - self.existing_folders
        existing_folders = self.existing_folders

        if new_files:
            parts.append(plural_fmt('new file', new_files))
        if existing_files:
            parts.append(plural_fmt('existing file', existing_files))
        if new_folders:
            parts.append(plural_fmt('new folder', new_folders))
        if existing_folders:
            parts.append(plural_fmt('existing folder', existing_folders))
        combined_parts = join_with_commas_and_and(parts)
        return "{} {}.".format(prefix, combined_parts)


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


def _build_project_tree(path, followsymlinks, file_filter):
    """
    Build a tree of LocalFolder with children or just a LocalFile based on a path.
    :param path: str path to a directory to walk
    :param followsymlinks: bool should we follow symlinks when walking
    :param file_filter: FileFilter: include method returns True if we should include a file/folder
    :return: the top node of the tree LocalFile or LocalFolder
    """
    result = None
    if isfile(path):
        result = LocalFile(path)
    elif isdir(path):
        result = _build_folder_tree(os.path.abspath(path), followsymlinks, file_filter)
    else:
        _on_non_regular_file(path)
    return result


def _on_non_regular_file(path):
    print("Warning: Skipping {}. This is an unsupported type of file.".format(path))


def _build_folder_tree(top_abspath, followsymlinks, file_filter):
    """
    Build a tree of LocalFolder with children based on a path.
    :param top_abspath: str path to a directory to walk
    :param followsymlinks: bool should we follow symlinks when walking
    :param file_filter: FileFilter: include method returns True if we should include a file/folder
    :return: the top node of the tree LocalFolder
    """
    path_to_content = {}
    child_to_parent = {}
    ignore_file_patterns = IgnoreFilePatterns(file_filter)
    ignore_file_patterns.load_directory(top_abspath, followsymlinks)
    for dir_name, child_dirs, child_files in os.walk(top_abspath, followlinks=followsymlinks):
        abspath = os.path.abspath(dir_name)
        folder = LocalFolder(abspath)
        path_to_content[abspath] = folder
        # If we have a parent add us to it.
        parent_path = child_to_parent.get(abspath)
        if parent_path:
            path_to_content[parent_path].add_child(folder)
        remove_child_dirs = []
        for child_dir in child_dirs:
            # Record dir_name as the parent of child_dir so we can call add_child when get to it.
            abs_child_path = os.path.abspath(os.path.join(dir_name, child_dir))
            if ignore_file_patterns.include(abs_child_path, is_file=False):
                child_to_parent[abs_child_path] = abspath
            else:
                remove_child_dirs.append(child_dir)
        for remove_child_dir in remove_child_dirs:
            child_dirs.remove(remove_child_dir)
        for child_filename in child_files:
            abs_child_filename = os.path.join(dir_name, child_filename)
            if isfile(abs_child_filename):
                if ignore_file_patterns.include(abs_child_filename, is_file=True):
                    folder.add_child(LocalFile(abs_child_filename))
            else:
                _on_non_regular_file(abs_child_filename)
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
        self.path_data = PathData(self.path)
        self.name = self.path_data.name()
        self.size = self.path_data.size()
        self.mimetype = self.path_data.mime_type()
        self.remote_id = ''
        self.remote_file_hash_alg = None
        self.remote_file_hash = None
        self.is_file = True
        self.kind = KindType.file_str
        self.sent_to_remote = False

    def get_path_data(self):
        """
        Return PathData created from internal path.
        """
        return self.path_data

    def get_hash_value(self):
        """
        Return the current hash value for our path.
        :return: str: hash value
        """
        return self.path_data.get_hash().value

    def update_remote_ids(self, remote_file):
        """
        Based on a remote file try to assign a remote_id
        :param remote_file: RemoteFile remote data pull remote_id from
        """
        self.remote_id = remote_file.id
        self.remote_file_hash_alg = remote_file.hash_alg
        self.remote_file_hash = remote_file.file_hash

    def calculate_local_hash(self):
        return self.path_data.get_hash()

    def hash_matches_remote(self, hash_data):
        if self.remote_file_hash and self.remote_file_hash_alg:
            return hash_data.matches(self.remote_file_hash_alg, self.remote_file_hash)
        return False

    def set_remote_values_after_send(self, remote_id, remote_hash_alg, remote_file_hash):
        """
        Set remote_id to specific value after this file has been sent to remote store.
        :param remote_id: str uuid of the file in the remote store
        """
        self.sent_to_remote = True
        self.remote_id = remote_id
        self.remote_file_hash_alg = remote_hash_alg
        self.remote_file_hash = remote_file_hash

    def count_chunks(self, bytes_per_chunk):
        """
        Based on the size of the file determine how many chunks we will need to upload.
        For empty files 1 chunk is returned (DukeDS requires an empty chunk for empty files).
        :param bytes_per_chunk: int: how many bytes should chunks to spglit the file into
        :return: int: number of chunks that will need to be sent
        """
        chunks = math.ceil(float(self.size) / float(bytes_per_chunk))
        return max(chunks, 1)

    def __str__(self):
        return 'file:{}'.format(self.name)


class HashData(object):
    """
    Hash info about a file.
    """
    def __init__(self, hash_util):
        """
        Create hash info from hash_util with data already loaded.
        :param hash_util: HashUtil with data populated
        """
        alg, value = hash_util.hexdigest()
        self.alg = alg
        self.value = value

    def matches(self, hash_alg, hash_value):
        """
        Does our algorithm and hash value match the specified arguments.
        :param hash_alg: str: hash algorithm
        :param hash_value: str: hash value
        :return: boolean
        """
        return self.alg == hash_alg and self.value == hash_value

    @staticmethod
    def create_from_path(path):
        """
        Hash the local file at path and return HashData with results.
        :param path: str: path to file we will hash
        :return: HashData: hash alg and value
        """
        hash_util = HashUtil()
        hash_util.add_file(path)
        return HashData(hash_util)

    @staticmethod
    def create_from_chunk(chunk):
        """
        Hash chunk and return HashData with results.
        :param chunk: bytes/str: data to hash
        :return: HashData: hash alg and value
        """
        hash_util = HashUtil()
        hash_util.add_chunk(chunk)
        return HashData(hash_util)


class PathData(object):
    """
    Various information that can be derived from a filesystem path to a file.
    """
    def __init__(self, path):
        """
        Setup with path pointing to existing file.
        :param path: str: path
        """
        self.path = path

    def name(self):
        """
        Get the name portion of the file(remove directory).
        :return: str: filename
        """
        return os.path.basename(self.path)

    def mime_type(self):
        """
        Guess the mimetype of a file or 'application/octet-stream' if unable to guess.
        :return: str: mimetype
        """
        mime_type, encoding = mimetypes.guess_type(self.path)
        if not mime_type:
            mime_type = 'application/octet-stream'
        return mime_type

    def size(self):
        """
        Return the file size.
        :return: int: size of file
        """
        return os.path.getsize(self.path)

    def get_hash(self):
        """
        Create HashData for the file
        :return: HashData: alg and value of contents of the file
        """
        return HashData.create_from_path(self.path)

    def read_whole_file(self):
        """
        Slurp the whole file into memory.
        Should only be used with relatively small files.
        :return: str: file contents
        """
        chunk = None
        with open(self.path, 'rb') as infile:
            chunk = infile.read()
        return chunk


class HashUtil(object):
    HASH_NAME = "md5"
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
        return HashUtil.HASH_NAME, self.hash.hexdigest()
