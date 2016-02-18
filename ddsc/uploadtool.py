import os
import argparse
import hashlib
import mimetypes


class Configuration(object):
    """Holds configuration to perform folder upload operation."""
    def __init__(self, args):
        self.auth = os.environ.get('DUKE_DATA_SERVICE_AUTH', None)
        if not self.auth:
            raise ValueError('Set DUKE_DATA_SERVICE_AUTH environment variable to valid key.')

        self.url = os.environ.get('DUKE_DATA_SERVICE_URL', None)
        if not self.url:
            self.url = 'https://uatest.dataservice.duke.edu/api/v1'

        description = ("Uploads a folder to duke data service. "
                       "Specify an auth token via DUKE_DATA_SERVICE_AUTH environment variable. "
                       "Specify an alternate url via DUKE_DATA_SERVICE_URL environment variable if necessary. "
                      )
        parser = argparse.ArgumentParser(description=description)
        parser.add_argument("project_name", help="Name of the project to create or add to.")
        parser.add_argument("folder", help="The folder you want to add to the project.")
        parsed_args = parser.parse_args(args=args)
        self.project_name = parsed_args.project_name
        self.folder = parsed_args.folder
        self._check_args()
        self.project_desc = self.project_name

    def _check_args(self):
        if not os.path.exists(self.folder):
            raise ValueError('You tried to upload ' + self.folder + ', but that folder does not exist.')


class ProjectList(object):
    """
    Contains a list of the projects queried from the dataservice and can create new projects for the current user.
    """
    def __init__(self, dsa):
        self.dsa = dsa
        self._fetch_list()

    def _fetch_list(self):
        self.projects = []
        response = self.dsa.get_projects()
        if response.status_code == 200:
            data = response.json()
            for project in data['results']:
                (proj_name, proj_id,desc,kind_str) = project['name'], project['id'], project['description'], project['kind']
                self.projects.append(Project(self.dsa, proj_name, proj_id, desc, kind_str))
        else:
            print(str(response))

    def get_project_by_name(self, name):
        for project in self.projects:
            if project.name == name:
                return project
        return None

    def create_project(self, proj_name, proj_desc):
        self.dsa.create_project(proj_name, proj_desc)
        self._fetch_list()
        return self.get_project_by_name(proj_name)


class Project(object):
    """A single project's data queried from the dataservice."""
    def __init__(self, dsa, name, id, desc, kind):
        self.dsa = dsa
        self.name = name
        self.id = id
        self.desc = desc
        self.kind = kind

    def add_folder(self, folder_name):
        self.dsa.create_folder(folder_name, self.kind, self.id)

    def fetch_children(self, name_contains=''):
        result = []
        resp = self.dsa.get_project_children(self.id, name_contains)
        if resp.status_code == 200:
            for child in resp.json()['results']:
                (folder_name, folder_id,kind_str) = child['name'], child['id'], child['kind']
                result.append(Child(self.dsa, folder_name, folder_id, kind_str))
            return result
        else:
            raise ValueError("Failed to fetch children:" + str(resp.status_code))

    def __repr__(self):
        return self.name + ":" + self.id


class Child(object):
    """A child of either a project or a folder within a project."""
    def __init__(self, dsa, name, id, kind):
        self.dsa = dsa
        self.name = name
        self.id = id
        self.kind = kind

    def __repr__(self):
        return self.name + ":" + self.id

    def fetch_children(self, name_contains=''):
        result = []
        if self.kind == 'dds-folder':
            resp = self.dsa.get_folder_children(self.id, name_contains)
            if resp.status_code == 200:
                for child in resp.json()['results']:
                    (folder_name, folder_id, kind_str) = child['name'], child['id'], child['kind']
                    result.append(Child(self.dsa, folder_name, folder_id, kind_str))
            else:
                raise ValueError("Failed to fetch children:" + str(resp.status_code))
        return result

    def add_folder(self, folder_name):
        if self.kind == 'dds-folder':
            self.dsa.create_folder(folder_name, self.kind, self.id)


def hash_filename(filename, blocksize=4096):
    """Reads contents of filename and returns a hash."""
    hash = hashlib.md5()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(blocksize), b""):
            hash.update(chunk)
    return "md5", hash.hexdigest()


def hash_chunk(chunk):
    """Creates a hash from the bytes in chunk."""
    hash = hashlib.md5()
    hash.update(chunk)
    return "md5", hash.hexdigest()


def read_in_chunks(infile, blocksize):
    """Return a chunks lazily."""
    while True:
        data = infile.read(blocksize)
        if not data:
            break
        yield data


class FileOnDisk(object):
    """Return a chunks lazily."""
    def __init__(self, dsa, filename, content_type):
        self.filename = filename
        self.dsa = dsa
        self.content_type = content_type

    def upload(self, project_id, parent_kind, parent_id):
        size = os.path.getsize(self.filename)
        (hash_alg, hash_value) = hash_filename(self.filename)
        basename = os.path.basename(self.filename)
        resp = self.dsa.create_upload(project_id, basename, self.content_type, size, hash_value, hash_alg)
        if resp.status_code != 201:
            raise ValueError("Failed to create upload:" + str(resp.status_code))
        upload_id = resp.json()['id']
        self._send_file_chunks(upload_id)
        resp = self.dsa.complete_upload(upload_id)
        if resp.status_code != 200:
            raise ValueError("Failed to complete upload:" + str(resp.status_code))
        resp = self.dsa.create_file(parent_kind, parent_id, upload_id)
        if resp.status_code != 201:
            raise ValueError("Failed to create file after upload:" + str(resp.status_code))

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


class ProjectUploadTool(object):
    def __init__(self, dsa, project_name, project_desc):
        self.dsa = dsa
        self.project_name = project_name
        self.project_desc = project_desc
        self.parent_lookup = {}
        self.proj = None

    def upload(self, folder_path):
        self.parent_lookup = {}
        self._upload_project()
        for dirname, dirnames, filenames in os.walk(folder_path):
            if self.include_path(dirname):
                self._upload_directory(dirname, top=True)
                for subdirname in dirnames:
                    if self.include_path(subdirname):
                        self._upload_directory(os.path.join(dirname, subdirname))
                for filename in filenames:
                    if self.include_path(filename):
                        self.upload_file(os.path.join(dirname, filename))

    def _upload_project(self):
        project_list = ProjectList(self.dsa)
        proj = project_list.get_project_by_name(self.project_name)
        if not proj:
            print "creating project"
            proj = project_list.create_project(self.project_name, self.project_desc)
            if not proj:
                raise ValueError("Failed to create project.")
        self.proj = proj
        self.parent_lookup[''] = proj

    def _find_parent(self, path, top):
        if top:
            path = ''
        parent = self.parent_lookup.get(path, None)
        if not parent:
            raise ValueError("no parent found for" + path)
        return parent

    def _upload_directory(self, path, top=False):
        basename = os.path.basename(path)
        folder_name = os.path.dirname(path)
        print "upload directory:", folder_name, basename
        parent = self._find_parent(folder_name, top)
        child = self._find_child_by_name(parent, basename)
        if not child:
            parent.add_folder(basename)
            child = self._find_child_by_name(parent, basename)
            if not child:
                raise ValueError('Failed to find child after creating it:', basename)
        print "parent_lookup_add :", path, child.id
        self.parent_lookup[path] = child

    def _find_child_by_name(self, parent, child_name):
        children = parent.fetch_children(name_contains=child_name)
        if children:
            for child in children:
                if child.name == child_name:
                    return child
        return None

    def upload_file(self, path, top=False):
        basename = os.path.basename(path)
        parent_name = os.path.dirname(path)
        print "upload file:", parent_name, basename
        parent = self._find_parent(parent_name, top)
        (mimetype, encoding) = mimetypes.guess_type(path)
        if not mimetype:
            mimetype = 'application/octet-stream'
        print mimetype, basename, parent_name
        file_on_disk = FileOnDisk(self.dsa, path, mimetype)
        print 'try upload',
        file_on_disk.upload(self.proj.id, parent.kind, parent.id)
        print 'done upload'

    def include_path(self, path):
        basename = os.path.basename(path)
        return not basename.startswith(".")
