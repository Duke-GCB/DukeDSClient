from __future__ import print_function
import os
import re
import uuid
import math
import datetime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, PrimaryKeyConstraint, ForeignKeyConstraint, Enum, Boolean
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.schema import ForeignKey
from ddsc.core.util import ProjectWalker
from ddsc.core.util import KindType
from ddsc.core.localstore import FileFilter, HashData, HashUtil

Base = declarative_base()


class TransferDirection:
    upload = "upload"
    download = "download"

# DO I REALLY NEED THIS?
class Transfer(Base):
    __tablename__ = 'transfer'
    id = Column(String, primary_key=True)
    direction = Column(Enum(TransferDirection.upload, TransferDirection.download))
    project_transfer_id = Column(String, ForeignKey('transfer_item.id'))


class TransferItem(Base):
    __tablename__ = 'transfer_item'
    id = Column(String, primary_key=True)
    parent_id = Column(String, ForeignKey('transfer_item.id'))
    dds_kind = Column(Enum(KindType.project_str, KindType.folder_str, KindType.file_str))
    dds_id = Column(String)
    name = Column(String) # only used for project type
    remote_path = Column(String) # used for folders and files
    local_path = Column(String) # used for folders and files
    file_hash_value = Column(String) # only used for files
    file_size = Column(Integer)  # only used for files
    upload_id = Column(String) # only used for filesrm
    need_to_send = Column(Boolean)
    was_transferred = Column(Boolean)
    children = relationship("TransferItem", cascade='all')
    chunks = relationship("TransferChunk", cascade='all')

    def __repr__(self):
        return "{} {} {} {} {} {}".format(self.id, self.parent_id, self.dds_kind, self.dds_id, self.name,
                                       self.local_path, self.remote_path)


class TransferChunk(Base):
    __tablename__ = 'transfer_chunk'
    item_id = Column(String, ForeignKey('transfer_item.id'), primary_key=True)
    chunk_num = Column(Integer, primary_key=True)


class ProjectDB(object):
    def __init__(self, storage_engine_url):
        engine = create_engine(storage_engine_url, echo=False)
        session = sessionmaker(bind=engine)
        Base.metadata.create_all(engine)
        self.session = session()

    def add_transfer_item(self, transfer_id, transfer_parent_id, dds_kind, name=None, local_path=None,
                          remote_path=None, file_hash_value=None, file_size=None):
        transfer = TransferItem(id=transfer_id, parent_id=transfer_parent_id, dds_kind=dds_kind,
                                name=name, local_path=local_path, remote_path=remote_path,
                                need_to_send=True, file_hash_value=file_hash_value,
                                file_size=file_size)
        self.session.add(transfer)
        self.session.commit()
        return transfer

    def create_project(self, project_name):
        project_id = str(uuid.uuid4())
        return self.add_transfer_item(transfer_id=project_id, transfer_parent_id=None,
                                      dds_kind=KindType.project_str, name=project_name)

    def save_id(self, transfer_item, id):
        transfer_item.id = id
        self.session.commit()

    def commit_session(self):
        self.session.commit()

    def delete_project(self, project):
        self.session.delete(project)
        self.session.commit()

    def add_folder(self, parent, local_path, remote_path):
        folder_id = str(uuid.uuid4())
        return self.add_transfer_item(transfer_id=folder_id, transfer_parent_id=parent.id,
                                      dds_kind=KindType.folder_str,
                                      local_path=local_path, remote_path=remote_path)

    def add_file(self, parent, local_path, remote_path):
        folder_id = str(uuid.uuid4())
        file_hash_value = HashData.create_from_path(local_path).value
        return self.add_transfer_item(transfer_id=folder_id, transfer_parent_id=parent.id,
                                      dds_kind=KindType.file_str,
                                      local_path=local_path, remote_path=remote_path,
                                      file_hash_value=file_hash_value,
                                      file_size=os.path.getsize(local_path))

    def create_file_chunk(self, transfer_item, chunk_num):
        transfer_chunk = TransferChunk(item_id=transfer_item.id, chunk_num=chunk_num)
        self.session.add(transfer_chunk)
        self.session.commit()
        return transfer_chunk

    def get_projects(self):
        return self.session.query(TransferItem).filter(TransferItem.dds_kind == KindType.project_str).all()

    def get_project_by_name(self, name):
        return self.session.query(TransferItem).filter(TransferItem.dds_kind == KindType.project_str,
                                                       TransferItem.name == name).first()


    def get_chunks(self, transfer_item):
        return self.session.query(TransferChunk).filter(TransferChunk.item_id == transfer_item.id).all()


class ProjectUtil(object):
    def __init__(self, project_db):
        self.followsymlinks = False
        self.file_include = FileFilter('').include
        self.project_db = project_db

    def create_project_tree_for_paths(self, project_name, paths):
        project = self.project_db.create_project(project_name)
        for path in paths:
            local_path = os.path.abspath(path)
            remote_path = os.path.basename(path)
            if os.path.isfile(path):
                self.project_db.add_file(parent=project,
                                         local_path=local_path,
                                         remote_path=remote_path)
            else:
                self.create_children_for_path(project, local_path)
        return project

    def create_children_for_path(self, project, top_local_path):
        child_to_parent = {}
        parent_local_path = os.path.abspath(top_local_path + '/..')
        for dir_name, child_dirs, child_files in os.walk(top_local_path, followlinks=self.followsymlinks):
            local_dir_path = os.path.abspath(dir_name)
            remote_dir_path = self.create_remote_path(parent_local_path, local_dir_path)
            parent = child_to_parent.get(local_dir_path, project)
            folder = self.project_db.add_folder(parent=parent,
                                                local_path=local_dir_path,
                                                remote_path=remote_dir_path)
            for child_dir in child_dirs:
                # Record dir_name as the parent of child_dir so we can call add_child when get to it.
                abs_child_path = os.path.abspath(os.path.join(dir_name, child_dir))
                child_to_parent[abs_child_path] = folder
            for child_filename in child_files:
                if self.file_include(child_filename):
                    local_file_path = os.path.join(dir_name, child_filename)
                    remote_file_path = self.create_remote_path(parent_local_path, local_file_path)
                    self.project_db.add_file(parent=folder,
                                             local_path=local_file_path,
                                             remote_path=remote_file_path)

    def update_project_info(self, project, project_json):
        if project_json:
            project.dds_id = project_json['id']
            project.need_to_send = False
            self.project_db.commit_session()

    def update_project_content(self, project, project_contents_json):
        remote_path_to_dds = {}
        for item in project_contents_json:
            parent_path = '/'.join([parent['name'] for parent in item['ancestors'] if parent['kind'] != 'dds-project'])
            remote_path = os.path.join(parent_path, item['name'])
            remote_path_to_dds[remote_path] = item
        self.recursive_set_dds_id(project, remote_path_to_dds)

    def recursive_set_dds_id(self, item, remote_path_to_dds):
        dds_data = remote_path_to_dds.get(item.remote_path)
        if dds_data:
            remote_id = dds_data['id']
            if item.dds_kind == KindType.folder_str:
                item.need_to_send = False
            if item.dds_kind == KindType.file_str:
                hashes = dds_data['current_version']['upload']['hashes']
                for hash in hashes:
                    alg = hash['algorithm']
                    value = hash['value']
                    if alg == HashUtil.HASH_NAME and item.file_hash_value == value:
                        item.need_to_send = False
            item.dds_id = remote_id
        for child in item.children:
            self.recursive_set_dds_id(child, remote_path_to_dds)


    @staticmethod
    def create_remote_path(top_local_path, local_path):
        return re.sub('^' + os.path.abspath(top_local_path) + '/', '', os.path.abspath(local_path))

    @staticmethod
    def get_project_url_str(project, config):
        msg = 'URL to view project'
        project_id = project.dds_id
        url_str = '{}: https://{}/portal/#/project/{}'.format(msg, config.get_portal_url_base(), project_id)
        return url_str

    @staticmethod
    def get_upload_report(project, config):
        report = UploadReport2(project.name)
        report.walk_project(project)
        return report.get_content()


class LocalOnlyCounter2(object):
    """
    Visitor that counts items that need to be sent in LocalContent.
    """
    def __init__(self, bytes_per_chunk):
        self.projects = 0
        self.folders = 0
        self.files = 0
        self.chunks = 0
        self.bytes_per_chunk = bytes_per_chunk

    def count_items(self, project):
        self._count_recursive(project)

    def _count_recursive(self, item):
        if item.need_to_send:
            if item.dds_kind == KindType.project_str:
                self.projects += 1
            if item.dds_kind == KindType.folder_str:
                self.folders += 1
            if item.dds_kind == KindType.file_str:
                self.files += 1
                self.chunks += math.ceil(float(item.file_size) / float(self.bytes_per_chunk))
        for child in item.children:
            self._count_recursive(child)

    def total_items(self):
        """
        Total number of files/folders/chunks that need to be sent.
        :return: int number of items to be sent.
        """
        return self.projects + self.folders + self.chunks

    def result_str(self):
        """
        Return a string representing the totals contained herein.
        :return: str counts/types string
        """
        return '{}, {}, {}'.format(LocalOnlyCounter2.plural_fmt('project', self.projects),
                                   LocalOnlyCounter2.plural_fmt('folder', self.folders),
                                   LocalOnlyCounter2.plural_fmt('file', self.files))

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


class UploadReport2(object):
    """
    Creates a text report of items that were sent to the remote store.
    """
    def __init__(self, project_name):
        """
        Create report witht the specified project name since the local store doesn't contain that info.
        :param project_name: str project name for the report
        """
        self.report_items = []
        self.project_name = project_name
        self._add_report_item('SENT FILENAME', 'ID', 'SIZE', 'HASH')

    def _add_report_item(self, name, remote_id, size='', file_hash=''):
        self.report_items.append(ReportItem2(name, remote_id, size, file_hash))

    def walk_project(self, project):
        self.visit_item(project, None)

    def visit_item(self, item, parent):
        if item.dds_kind == KindType.project_str:
            self.visit_project(item)
        if item.dds_kind == KindType.folder_str:
            self.visit_folder(item, parent)
        if item.dds_kind == KindType.file_str:
            self.visit_file(item, parent)
        for child in item.children:
            self.visit_item(child, item)

    def visit_project(self, item):
        """
        Add project to the report if it was sent.
        :param item: LocalContent project level item
        """
        if item.was_transferred:
            self._add_report_item('Project', item.dds_id)

    def visit_folder(self, item, parent):
        """
        Add folder to the report if it was sent.
        :param item: LocalFolder folder to possibly add
        :param parent: LocalFolder/LocalContent not used here
        """
        if item.was_transferred:
            self._add_report_item(item.local_path, item.dds_id)

    def visit_file(self, item, parent):
        """
        Add file to the report if it was sent.
        :param item: LocalFile file to possibly add.
        :param parent: LocalFolder/LocalContent not used here
        """
        if item.was_transferred:
            self._add_report_item(item.local_path, item.dds_id, item.file_size, item.file_hash_value)

    def _report_header(self):
        return u"Upload Report for Project: '{}' {}\n".format(self.project_name, datetime.datetime.utcnow())

    def _report_body(self):
        max_name = UploadReport2.max_len([item.name for item in self.report_items])
        max_remote_id = UploadReport2.max_len([item.remote_id for item in self.report_items])
        max_size = UploadReport2.max_len([item.size for item in self.report_items])
        return [item.str_with_sizes(max_name, max_remote_id, max_size) for item in self.report_items]

    @staticmethod
    def max_len(values):
        return max([len(x) for x in values])

    def get_content(self):
        lines = [self._report_header()]
        lines.extend(self._report_body())
        return '\n'.join(lines)


class ReportItem2(object):
    """
    Item sent to remote store that is part of the UploadReport2.
    """
    def __init__(self, name, remote_id, size='', file_hash=''):
        """
        Setup properties for use in str method
        :param name: str name of the
        :param remote_id: str remote uuid of the item
        :param size: int/str size of the item can be '' if blank
        :return:
        """
        self.name = name
        self.remote_id = remote_id
        self.size = str(size)
        self.file_hash = file_hash

    def str_with_sizes(self, max_name, max_remote_id, max_size):
        """
        Create string for report based on internal properties using sizes to line up columns.
        :param max_name: int width of the name column
        :param max_remote_id: int width of the remote_id column
        :return: str info from this report item
        """
        name_str = self.name.ljust(max_name)
        remote_id_str = self.remote_id.ljust(max_remote_id)
        size_str = self.size.ljust(max_size)
        return u'{}    {}    {}    {}'.format(name_str, remote_id_str, size_str, self.file_hash)
