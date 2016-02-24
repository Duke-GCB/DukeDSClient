import os
import datetime
import hashlib
import mimetypes
from util import KindType


class LocalContent(object):
    """
    Represents a list of folder/file trees on the filesystem.
    """
    def __init__(self, allow_symlink):
        """
        Creates a list of local file system content that can be sent to a remote project.
        :param allow_symlink: bool follow symbolic links when looking for content
        """
        self.remote_id = ''
        self.kind = KindType.project_str
        self.children = []
        self.sent_to_remote = False
        self.allow_symlink = allow_symlink

    def add_path(self, path):
        """
        Add the path and any children files/folders to the list of content.
        :param path: str path to add
        """
        abspath = os.path.abspath(path)
        self.children.append(_build_project_tree(abspath, self.allow_symlink))

    def add_paths(self, path_list):
        """
        Add a list of paths to the list of content.
        :param path_list: [str] list of file system paths
        """
        for path in path_list:
            self.add_path(path)

    def accept(self, visitor):
        """
        Traverse the content trees with a visitor.
        :param visitor: object who implements visit_project, visit_folder, visit_file
        """
        _visit_content(self, None, visitor)

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


def _visit_content(item, parent, visitor):
    """
    Recursively visit nodes in the project tree.
    :param item: LocalContent/LocalFolder/LocalFile we are traversing down from
    :param parent: LocalContent/LocalFolder parent or None
    :param visitor: object visiting the tree
    """
    if KindType.is_project(item):
        visitor.visit_project(item, parent)
    elif KindType.is_folder(item):
        visitor.visit_folder(item, parent)
    else:
        visitor.visit_file(item, parent)
    if not KindType.is_file(item):
        for child in item.children:
            _visit_content(child, item, visitor)


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
    Update remote_ids based on on parent.
    :param remote_parent: RemoteProject/RemoteFolder who has children
    :param children: [LocalFolder,LocalFile] children to set remote_ids based on remote children
    """
    name_to_child = _name_to_child_map(children)
    for remote_child in remote_parent.children:
        local_child = name_to_child.get(remote_child.name)
        if local_child:
            local_child.update_remote_ids(remote_child)


def _build_project_tree(path, allow_symlink):
    """
    Build a tree of LocalFolder with children or just a LocalFile based on a path.
    :param path: str path to a directory to walk
    :param allow_symlink: bool should we follow symlinks when walking
    :return: the top node of the tree LocalFile or LocalFolder
    """
    result = None
    if os.path.isfile(path):
        result = LocalFile(path)
    else:
        result = _build_folder_tree(os.path.abspath(path), allow_symlink)
    return result


def _build_folder_tree(top_abspath, allow_symlink):
    """
    Build a tree of LocalFolder with children based on a path.
    :param top_abspath: str path to a directory to walk
    :param allow_symlink: bool should we follow symlinks when walking
    :return: the top node of the tree LocalFolder
    """
    path_to_content = {}
    child_to_parent = {}
    for dir_name, child_dirs, child_files in os.walk(top_abspath, followlinks=allow_symlink):
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
        Return a tupil of algorithm name(md5) and a hash of the entire file.
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
        # Since right now the remote server allows duplicates
        # this could be called multiple times for the same local file
        # as long as one matches we have the file uploaded.

        # if we don't have a uuid yet any will do
        if not self.need_to_send:
            self.remote_id = remote_file.id
        # but we prefer the one that matches our hash
        (alg, file_hash) = self.get_hashpair()
        if alg == remote_file.hash_alg and file_hash == remote_file.file_hash:
            self.need_to_send = False
            self.remote_id = remote_file.id

    def set_remote_id_after_send(self, remote_id):
        """
        Set remote_id to specific value after this file has been sent to remote store.
        :param remote_id: str uuid of the file in the remote store
        """
        self.sent_to_remote = True
        self.remote_id = remote_id

    def process_chunks(self, bytes_per_chunk, processor):
        """
        Lazily processes the contents of the file given a max size per chunk.
        :param bytes_per_chunk: int size of chunks
        :param processor: function to process the data
        """
        with open(self.path,'rb') as infile:
            number = 0
            for chunk in LocalFile.read_in_chunks(infile, bytes_per_chunk):
                (chunk_hash_alg, chunk_hash_value) = LocalFile.hash_chunk(chunk)
                processor(chunk, chunk_hash_alg, chunk_hash_value)

    @staticmethod
    def read_in_chunks(infile, blocksize):
        """
        Generator to read chunks lazily.
        :param infile: filehandle file to read from
        :param blocksize: int size of blocks to read
        """
        """"""
        while True:
            data = infile.read(blocksize)
            if not data:
                break
            yield data

    @staticmethod
    def hash_chunk(chunk):
        """Creates a hash from the bytes in chunk."""
        hash = HashUtil()
        hash.add_chunk(chunk)
        return hash.hexdigest()

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


class LocalOnlyCounter(object):
    """
    Visitor that counts items that need to be sent in LocalContent.
    Meant to be passed to the LocalContent.accept method.
    """
    def __init__(self):
        self.projects = 0
        self.folders = 0
        self.files = 0

    def visit_project(self, item, parent):
        """
        Increments counter if the project is not already remote.
        :param item: LocalProject
        :param parent: None always None since projects don't have parents
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

    def visit_file(self, item, parent):
        """
        Increments counter if item needs to be sent.
        :param item: LocalFile
        :param parent: LocalFolder/LocalProject
        """
        if item.need_to_send:
            self.files += 1

    def total_items(self):
        """
        Total number of items that need to be sent.
        :return: int number of items to be sent.
        """
        return self.projects + self.folders + self.files

    def result_str(self):
        """
        Return a string representing the totals contained herein.
        :return: str counts/types string
        """
        return '{}, {}, {}'.format(LocalOnlyCounter.plural_fmt('project', self.projects),
                                   LocalOnlyCounter.plural_fmt('folder', self.folders),
                                   LocalOnlyCounter.plural_fmt('file', self.files))

    @staticmethod
    def plural_fmt(name, cnt):
        """
        pluralize name if necessary and combine with cnt
        :param name: str name of the item type
        :param cnt: int number items of this type
        :return: str name and cnt joined
        """
        if cnt == 1:
            return '{} {}'.format(cnt, name)
        else:
            return '{} {}s'.format(cnt, name)


class UploadReport(object):
    """
    Creates a text report of items that were sent to the remote store.
    Does this via LocalStore.accept visit_* methods.
    """
    def __init__(self, project_name):
        """
        Create report witht the specified project name since the local store doesn't contain that info.
        :param project_name: str project name for the report
        """
        self.report_items = []
        self.project_name = project_name
        self._add_report_item('Sent filename','ID', 'SIZE')

    def _add_report_item(self, name, remote_id, size):
        self.report_items.append(ReportItem(name, remote_id, size))

    def visit_project(self, item, parent):
        """
        Add project to the report if it was sent.
        :param item: LocalContent project level item
        :param parent: None not used here
        """
        if item.sent_to_remote:
            self._add_report_item('Project', item.remote_id, '')

    def visit_folder(self, item, parent):
        """
        Add folder to the report if it was sent.
        :param item: LocalFolder folder to possibly add
        :param parent: LocalFolder/LocalContent not used here
        """
        if item.sent_to_remote:
            self._add_report_item(item.path, item.remote_id, '')

    def visit_file(self, item, parent):
        """
        Add file to the report if it was sent.
        :param item: LocalFile file to possibly add.
        :param parent: LocalFolder/LocalContent not used here
        """
        if item.sent_to_remote:
            self._add_report_item(item.path, item.remote_id, item.size)

    def report_header(self):
        return "Upload Report for Project: '{}' {}".format(self.project_name, datetime.datetime.utcnow())

    def report_body(self):
        max_name = max([len(item.name) for item in self.report_items])
        max_remote_id = max([len(item.remote_id) for item in self.report_items])
        return [item.str_with_sizes(max_name, max_remote_id) for item in self.report_items]

    def __str__(self):
        lines = [self.report_header()]
        lines.extend(self.report_body())
        return '\n'.join(lines)


class ReportItem(object):
    """
    Item sent to remote store that is part of the UploadReport.
    """
    def __init__(self, name, remote_id, size):
        """
        Setup properties for use in str method
        :param name: str name of the
        :param remote_id: str remote uuid of the item
        :param size: int/str size of the item can be '' if blank
        :return:
        """
        self.name = name
        self.remote_id = remote_id
        self.size = size

    def str_with_sizes(self, max_name, max_remote_id):
        """
        Create string for report based on internal properties using sizes to line up columns.
        :param max_name: int width of the name column
        :param max_remote_id: int width of the remote_id column
        :return: str info from this report item
        """
        name_str = self.name.ljust(max_name)
        remote_id_str = self.remote_id.ljust(max_remote_id)
        return '{}    {}    {}'.format(name_str, remote_id_str, self.size)