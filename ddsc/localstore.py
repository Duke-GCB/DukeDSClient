import os
import datetime
import hashlib
import mimetypes
from util import KindType


class LocalContent(object):
    """
    Represents a list of folder/file trees on the filesystem.
    """
    def __init__(self):
        self.remote_id = ''
        self.kind = KindType.project_str
        self.children = []
        self.sent_to_remote = False

    def add_path(self, path):
        abspath = os.path.abspath(path)
        self.children.append(_build_project_tree(abspath))

    def add_paths(self, path_list):
        for path in path_list:
            self.add_path(path)

    def accept(self, visitor):
        _visit_content(self, None, visitor)

    def __str__(self):
        child_str = ', '.join([str(child) for child in self.children])
        return 'project: [{}]'.format(child_str)

    def update_remote_ids(self, remote_project):
        if remote_project:
            self.remote_id = remote_project.id
            _update_remote_children(remote_project, self.children)

    def set_remote_id_after_send(self, remote_id):
        self.remote_id = remote_id
        self.sent_to_remote = True

    def count_items(self):
        projects = 0
        folders = 0
        files = 0
        if self.sent_to_remote:
            projects = 1
        for child in self.children:
            (child_folders, child_files) = child.count_items()
            folders += child_folders
            files += child_files
        return projects, folders, files


def _visit_content(item, parent, visitor):
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
        self.sent_to_remote = False

    def add_child(self, child):
        self.children.append(child)

    def update_remote_ids(self, remote_folder):
        self.remote_id = remote_folder.id
        _update_remote_children(remote_folder, self.children)

    def set_remote_id_after_send(self, remote_id):
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
        self.sent_to_remote = False

    def get_hashpair(self):
        hash = HashUtil()
        hash.add_file(self.path)
        return hash.hexdigest()

    def update_remote_ids(self, remote_file):
        self.remote_id = remote_file.id
        (alg, hash) = self.get_hashpair()
        if alg == remote_file.hash_alg and hash == remote_file.hash:
            self.need_to_send = False

    def set_remote_id_after_send(self, remote_id):
        self.sent_to_remote = True
        self.remote_id = remote_id

    def process_chunks(self, bytes_per_chunk, processor):
        with open(self.path,'rb') as infile:
            number = 0
            for chunk in LocalFile.read_in_chunks(infile, bytes_per_chunk):
                (chunk_hash_alg, chunk_hash_value) = LocalFile.hash_chunk(chunk)
                processor(chunk, chunk_hash_alg, chunk_hash_value)

    @staticmethod
    def read_in_chunks(infile, blocksize):
        """Return a chunks lazily."""
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
        with open(filename, "rb") as f:
            for chunk in iter(lambda: f.read(block_size), b""):
                self.hash.update(chunk)

    def add_chunk(self, chunk):
        self.hash.update(chunk)

    def hexdigest(self):
        return "md5", self.hash.hexdigest()


class LocalOnlyCounter(object):
    def __init__(self):
        self.projects = 0
        self.folders = 0
        self.files = 0

    def visit_project(self, item, parent):
        if not item.remote_id:
            self.projects += 1

    def visit_folder(self, item, parent):
        if not item.remote_id:
            self.folders += 1

    def visit_file(self, item, parent):
        if item.need_to_send:
            self.files += 1

    def total_items(self):
        return self.projects + self.folders + self.files

    def result_str(self):
        return '{}, {}, {}'.format(self.plural_fmt('project', self.projects),
                                 self.plural_fmt('folder', self.folders),
                                 self.plural_fmt('file', self.files))

    def plural_fmt(self, name, cnt):
        if cnt == 1:
            return '{} {}'.format(cnt, name)
        else:
            return '{} {}s'.format(cnt, name)


class UploadReport(object):
    def __init__(self, project_name):
        self.report_items = []
        self.project_name = project_name
        self._add_report_item('Sent filename','ID', 'SIZE')

    def _add_report_item(self, name, remote_id, size):
        self.report_items.append(ReportItem(name, remote_id, size))

    def visit_project(self, item, parent):
        if item.sent_to_remote:
            self._add_report_item('Project', item.remote_id, '')

    def visit_folder(self, item, parent):
        if item.sent_to_remote:
            self._add_report_item(item.path, item.remote_id, '')

    def visit_file(self, item, parent):
        if item.sent_to_remote:
            self._add_report_item(item.path, item.remote_id, item.size)

    def report_header(self):
        return "Upload Report for Project: '{}' {}".format(self.project_name, datetime.datetime.utcnow())

    def report_content(self):
        max_name = max([len(item.name) for item in self.report_items])
        max_remote_id = max([len(item.remote_id) for item in self.report_items])
        return [item.str(max_name, max_remote_id) for item in self.report_items]

    def __str__(self):
        lines = [self.report_header()]
        lines.extend(self.report_content())
        return '\n'.join(lines)


class ReportItem(object):
    def __init__(self, name, remote_id, size):
        self.name = name
        self.remote_id = remote_id
        self.size = size

    def str(self, max_name, max_remote_id):
        name_str = self.name.ljust(max_name)
        remote_id_str = self.remote_id.ljust(max_remote_id)
        return '{}    {}    {}'.format(name_str, remote_id_str, self.size)


