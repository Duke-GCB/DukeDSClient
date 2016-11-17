from __future__ import print_function
import uuid
import datetime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, PrimaryKeyConstraint, ForeignKeyConstraint, Enum
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.schema import ForeignKey
from ddsc.core.util import ProjectWalker

Base = declarative_base()


class TransferItemType:
    PROJECT = 'PROJECT'
    FOLDER = 'FOLDER'
    FILE = 'FILE'


class Transfer(Base):
    __tablename__ = 'transfer'
    uuid = Column(String, primary_key=True)
    created = Column(DateTime, nullable=False)
    project_id = Column(String)
    project_name = Column(String, nullable=False)
    items = relationship("TransferItem", cascade='all')
    def __repr__(self):
        return "{} {} {} {}".format(self.uuid, self.created, self.project_id, self.project_name)


class TransferItem(Base):
    __tablename__ = 'transfer_item'
    transfer_uuid = Column(String, ForeignKey("transfer.uuid"), primary_key=True)
    task_id = Column(Integer, primary_key=True)
    name = Column(String)
    item_type = Column(Enum(TransferItemType.PROJECT, TransferItemType.FOLDER, TransferItemType.FILE))
    chunks = relationship("TransferedChunk", cascade='all',
                          foreign_keys="[TransferedChunk.transfer_uuid,TransferedChunk.task_id]")

    def __repr__(self):
        return "{} {}".format(self.transfer_uuid, self.task_id)


class TransferedChunk(Base):
    __tablename__ = 'transfered_chunk'
    transfer_uuid = Column(String, ForeignKey("transfer_item.transfer_uuid"), primary_key=True)
    task_id = Column(Integer, ForeignKey("transfer_item.task_id"), primary_key=True)
    chunk_num = Column(Integer, primary_key=True)
    __table_args__ = (ForeignKeyConstraint([transfer_uuid, task_id],
                                           [TransferItem.transfer_uuid, TransferItem.task_id]),
                      {})
    def __repr__(self):
        return "{} {} {}".format(self.transfer_uuid, self.task_id, self.chunk_num)


class TransferDB(object):
    def __init__(self, storage_engine_url):
        engine = create_engine(storage_engine_url, echo=False)
        Session = sessionmaker(bind=engine)
        Base.metadata.create_all(engine)
        self.session = Session()

    def create_transfer(self, project_id, project_name):
        transfer = Transfer(uuid=str(uuid.uuid4()), created=datetime.datetime.now(),
                            project_id=project_id,
                            project_name=project_name)
        self.session.add(transfer)
        return transfer

    def delete_transfer(self, transfer):
        self.session.delete(transfer)

    def list_transfers(self):
        return self.session.query(Transfer).all()

    def create_transfer_item(self, transfer, task_id, item_type):
        transfer_item = TransferItem(transfer_uuid=transfer.uuid, task_id=task_id, item_type=item_type)
        self.session.add(transfer_item)
        return transfer_item

    def create_transferred_chunk(self, transfer_item, chunk_num):
        transferred_chunk = TransferedChunk(transfer_uuid=transfer_item.transfer_uuid,
                                           task_id=transfer_item.task_id, chunk_num=chunk_num)
        self.session.add(transferred_chunk)
        return transferred_chunk


class SaveProjectUpload(object):
    def __init__(self, transfer_db):
        self.transfer_db = transfer_db

    def run(self, local_project):
        ProjectWalker.walk_project(local_project, self)

    def visit_project(self, item):
        """
        Adds create project command to task runner if project doesn't already exist.
        """
        if not item.remote_id:
            pass
            #command = CreateProjectCommand(self.settings, item)
            #self.task_runner_add(None, item, command)
        #else:
        #    self.settings.project_id = item.remote_id

    def visit_folder(self, item, parent):
        """
        Adds create folder command to task runner if folder doesn't already exist.
        """
        if not item.remote_id:
            pass
            #command = CreateFolderCommand(self.settings, item, parent)
            #self.task_runner_add(parent, item, command)

    def visit_file(self, item, parent):
        """
        If file is small add create small file command otherwise raise error.
        Large files shouldn't be passed to SmallItemUploadTaskBuilder.
        """
        #if item.need_to_send:
        pass