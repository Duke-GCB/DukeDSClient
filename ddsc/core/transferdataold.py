# use sqlalchemy instead ?
import os
import sqlite3

SCHEMA_SQL = """
create table transfer (
    uuid text PRIMARY KEY,
    created    text,
    project_id text,
    project_name text,
    local_path text,
    transfer_type integer
);

create table transfer_item (
    transfer_uuid text,
    task_id integer,
    task_parent_id integer,
    item_type integer,
    path text,
    status integer,

    upload_id text,

    FOREIGN KEY(transfer_uuid) REFERENCES transfer(uuid)
);
"""

GET_TRANSFERS_SQL = """
select uuid, created, project_id, project_name, local_path, transfer_type from transfer order by created;
"""

INSERT_TRANSFER_SQL = """
insert into transfer (uuid, created, project_id, project_name, local_path, transfer_type) values (?,?,?,?,?,?);
"""

class TransferType(object):
    UPLOAD = 1
    DOWNLOAD = 2


class ItemType(object):
    PROJECT = 1
    FOLDER = 2
    FILE = 3


class ItemStatus(object):
    NEW = 1
    STARTED = 2
    COMPLETE = 3


class Transfer(object):
    def __init__(self, uuid, created, project_id, project_name, local_path, transfer_type):
        self.uuid = uuid
        self.created = created
        self.project_id = project_id
        self.project_name = project_name
        self.local_path = local_path
        self.transfer_type = transfer_type


class TransferItem(object):
    pass


class TransferDB(object):
    def __init__(self, db_path):
        self.db_path = db_path

    def get_transfers(self):
        result = []
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(GET_TRANSFERS_SQL)
        for row in cursor.fetchall():
            uuid, created, project_id, project_name, local_path, transfer_type = row
            result.append(Transfer(uuid, created, project_id, project_name, local_path, transfer_type))
        cursor.close()
        conn.close()
        return result

    def create_transfer(self, transfer):
        conn = self._connect()
        cursor = conn.cursor()
        resp = cursor.execute(INSERT_TRANSFER_SQL,
                              (transfer.uuid, transfer.created,
                               transfer.project_id, transfer.project_name,
                               transfer.local_path, transfer.transfer_type))
        cursor.close()
        conn.commit()
        conn.close()
        return resp

    def _connect(self):
        is_new = not os.path.exists(self.db_path)
        conn = sqlite3.connect(self.db_path)
        if is_new:
            conn.executescript(SCHEMA_SQL)
        return conn



