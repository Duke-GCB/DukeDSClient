from localcontent import LocalContent, HashUtil
from remotecontent import RemoteProject, RemoteFolder, RemoteFile, RemoteContent
import ddsapi
import uploadtool
import sys

class DDSClient(object):
    def __init__(self, data_service, project_name, path_list):
        self.data_service = data_service
        self.project_name = project_name
        self.path_list = path_list

    def read_local_content(self):
        content = LocalContent()
        for path in self.path_list:
            content.add_path(path)
        return content
        """
    def fetch_remote_content(self):
        project = self._get_my_project()
        if project:
            self._add_project_children(project)
        return project

    def _get_my_project(self):
        response = self.data_service.get_projects().json()
        for project in response['results']:
            if project['name'] == self.project_name:
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
        """

    def create_remote_content(self, local_content):
        self._try_create_remote_project(local_content)
        for local_child in local_content.children:
            self._try_create_remote_child(local_content.remote_id, local_content, local_child)

    def _try_create_remote_project(self, local_content):
        if not local_content.remote_id:
            result = self.data_service.create_project(self.project_name, self.project_name)
            local_content.remote_id = result.json()['id']

    def _try_create_remote_child(self, project_id, parent, local_child):
            if local_child.is_file:
                file_on_disk = FileOnDisk(self.data_service, local_child)
                file_on_disk.upload(project_id, parent.kind, parent.remote_id)
            else:
                if not local_child.remote_id:
                    result = self.data_service.create_folder(local_child.name, parent.kind, parent.remote_id)
                    local_child.remote_id = result.json()['id']
                for sub_child in local_child.children:
                    self._try_create_remote_child(project_id, local_child, sub_child)



class FileOnDisk(object):
    """Return a chunks lazily."""
    def __init__(self, dsa, local_file):
        self.dsa = dsa
        self.local_file = local_file
        self.filename = local_file.path
        self.content_type = local_file.mimetype

    def upload(self, project_id, parent_kind, parent_id):
        size = self.local_file.size
        (hash_alg, hash_value) = self.local_file.get_hashpair()
        name = self.local_file.name
        resp = self.dsa.create_upload(project_id, name, self.content_type, size, hash_value, hash_alg)
        upload_id = resp.json()['id']
        self._send_file_chunks(upload_id)
        self.dsa.complete_upload(upload_id)
        self.dsa.create_file(parent_kind, parent_id, upload_id)

    def _send_file_chunks(self, upload_id):
        with open(self.filename,'rb') as infile:
            number = 0
            for chunk in read_in_chunks(infile, self.dsa.bytes_per_chunk):
                (chunk_hash_alg, chunk_hash_value) = hash_chunk(chunk)
                resp = self.dsa.create_upload_url(upload_id, number, len(chunk),
                                                  chunk_hash_value, chunk_hash_alg)
                if resp.status_code == 200:
                    self._send_file_external(resp.json(), chunk)
                    number += 1
                else:
                    raise ValueError("Failed to retrieve upload url status:" + str(resp.status_code))

    def _send_file_external(self, url_json, chunk):
        http_verb = url_json['http_verb']
        host = url_json['host']
        url = url_json['url']
        http_headers = url_json['http_headers']
        resp = self.dsa.send_external(http_verb, host, url, http_headers, chunk)
        if resp.status_code != 200 and resp.status_code != 201:
            raise ValueError("Failed to send file to external store. Error:" + str(resp.status_code))


def hash_chunk(chunk):
    """Creates a hash from the bytes in chunk."""
    hash = HashUtil()
    hash.add_chunk(chunk)
    return hash.hexdigest()


def read_in_chunks(infile, blocksize):
    """Return a chunks lazily."""
    while True:
        data = infile.read(blocksize)
        if not data:
            break
        yield data

    def _send_file_external(self, url_json, chunk):
        http_verb = url_json['http_verb']
        host = url_json['host']
        url = url_json['url']
        http_headers = url_json['http_headers']
        resp = self.dsa.send_external(http_verb, host, url, http_headers, chunk)
        if resp.status_code != 200 and resp.status_code != 201:
            raise ValueError("Failed to send file to external store. Error:" + str(resp.status_code))


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    config = uploadtool.Configuration(args)
    data_service = ddsapi.DataServiceApi(config.auth, config.url)
    tool = DDSClient(data_service, config.project_name, [config.folder]) #ugh repeating this below
    local_content = tool.read_local_content()
    remote_content = RemoteContent(data_service)
    remote_project = remote_content.fetch_remote_project(config.project_name, [config.folder])
    local_content.update_remote_ids(remote_project)
    tool.create_remote_content(local_content)

if __name__ == '__main__':
    main()



