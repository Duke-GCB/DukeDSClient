from ddsc.exceptions import DDSUserException


class Azure(object):
    def __init__(self, config):
        # placeholder data until Azure backend is live
        self._placeholder_projects = [
            AzureProject(self, name="user1/Mouse", auth_role="file_downloader"),
            AzureProject(self, name="user1/rna", auth_role="project_admin")
        ]
        self._placeholder_files = {
            "user1/Mouse": [
                AzureFile(self, "/file1.dat", md5="a1335de16b6efeb0f0dba271521c1f9d"),
                AzureFile(self, "/data/SRN01.fastq.gz", md5="b1335de16b6efeb0f0dba271521c1f9d"),
                AzureFile(self, "/data/SRN02.fastq.gz", md5="c1335de16b6efeb0f0dba271521c1f9d"),
            ],
            "user1/rna": [
                AzureFile(self, "/one/SRN01.fastq.gz", md5="d1335de16b6efeb0f0dba271521c1f9d"),
                AzureFile(self, "/two/SRN02.fastq.gz", md5="e1335de16b6efeb0f0dba271521c1f9d"),
                AzureFile(self, "/three/SRN03.fastq.gz", md5="f1335de16b6efeb0f0dba271521c1f9d"),
                AzureFile(self, "/three/nested/SRN04.fastq.gz", md5="g1335de16b6efeb0f0dba271521c1f9d"),
            ],
        }
        self._auth_roles = [
            AzureAuthRole(self, "project_admin",
                          "Can update project details, delete project, manage project level permissions "
                          "and perform all file operations"),
            AzureAuthRole(self, "project_viewer", "Can only view project and file meta-data"),
            AzureAuthRole(self, "file_downloader", "Can download files"),
            AzureAuthRole(self, "file_editor", "Can view download create update and delete files"),
            AzureAuthRole(self, "file_uploader", "Can update files"),
        ]

    def get_projects(self):
        return self._placeholder_projects

    def get_project(self, project_name):
        items = [p for p in self._placeholder_projects if p.name == project_name]
        if not items:
            raise ItemNotFound("Unable to find project named '{}'.".format(project_name))
        return items[0]

    def get_files(self, project):
        return self._placeholder_files.get(project.name, [])

    def get_auth_roles(self):
        return self._auth_roles

    def upload_files(self, project, paths, follow_symlinks, dry_run):
        print("Upload files/folders")
        print("project", project)
        print("paths", paths)
        print("follow_symlinks", follow_symlinks)
        print("dry_run", dry_run)

    def add_user(self, project, netid, auth_role):
        print("Add user")
        print("project", project)
        print("netid", netid)
        print("auth_role", auth_role)

    def remove_user(self, project, netid):
        print("Remove User")
        print("project", project)
        print("netid", netid)

    def download_files(self, project, include_paths, exclude_paths, destination):
        print("Download")
        print("project", project)
        print("include_paths", include_paths)
        print("exclude_paths", exclude_paths)
        print("destination", destination)

    def share(self, project, netid, auth_role):
        print("Share")
        print("project", project)
        print("netid", netid)
        print("auth_role", auth_role)

    def deliver(self, project, netid, copy_project, resend, msg_file, share_usernames):
        print("Deliver ")
        print("project", project)
        print("netid", netid)
        print("copy_project", copy_project)
        print("resend", resend)
        print("msg_file", msg_file)
        print("share_usernames", share_usernames)

    def delete(self, project, remote_path):
        print("Delete")
        print("project", project)
        print("remote_path", remote_path)

    def move(self, project, source_remote_path, target_remote_path):
        print("Move")
        print("project", project)
        print("source_remote_path", source_remote_path)
        print("target_remote_path", target_remote_path)


class AzureProject(object):
    def __init__(self, azure, name, auth_role):
        self.azure = azure
        self.name = name
        self.auth_role = auth_role

    def get_url(self):
        return " TODO"

    def size_str(self):
        print()
        print("Name:", self.name)
        print("URL:", self.get_url())
        print("Size:", self.get_size_str())
        print()

    def get_size_str(self):
        return "TODO"

    def __str__(self):
        return "<AzureProject name={} auth_role={}>".format(self.name, self.auth_role)


class AzureFile(object):
    def __init__(self, azure, name, md5):
        self.azure = azure
        self.name = name
        self.md5 = md5

    def __str__(self):
        return "<AzureFile name={} md5={}>".format(self.name, self.md5)


class AzureAuthRole(object):
    def __init__(self, azure, id, description):
        self.azure = azure
        self.id = id
        self.description = description


class ItemNotFound(DDSUserException):
    pass
