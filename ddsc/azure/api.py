import re
import json
import os.path
from datetime import datetime, timedelta
from azure.mgmt.storage import StorageManagementClient
from azure.storage.filedatalake import DataLakeServiceClient, generate_file_system_sas
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import TokenCachePersistenceOptions, SharedTokenCacheCredential, ChainedTokenCredential, \
    DeviceCodeCredential
from msgraph.core import GraphClient
from ddsc.azure.azcopy import create_azcopy, group_by_dirname
from ddsc.azure.delivery import DataDelivery
from ddsc.azure.exceptions import AzureUserNotFoundException
from ddsc.core.util import plural_fmt, humanize_bytes


DUKE_EMAIL_SUFFIX = "@duke.edu"
GRAPH_CLIENT_SCOPES = ["https://graph.microsoft.com/.default"]
DLS_SCOPES = ["https://dfs.core.windows.net/.default"]


def strip_top_directory(path):
    # Removes top directory from azure remote path
    return re.sub("^.*?/", "", path)


def make_acl(user_id, permissions=None, apply_default=True):
    if permissions:
        acl = f"user:{user_id}:{permissions}"
    else:
        acl = f"user:{user_id}"
    if apply_default:
        return f"{acl},default:{acl}"
    else:
        return acl


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


class AzureAuthRole(object):
    def __init__(self, id, permissions, description):
        self.id = id
        self.permissions = permissions
        self.description = description

    def get_acl(self, user_id, apply_default=True):
        return make_acl(user_id, self.permissions, apply_default=apply_default)

    def __repr__(self):
        return json.dumps({"id": self.id, "permissions": self.permissions, "description": self.description,
                           "is_auth_role": True})


class AuthRoles(object):
    project_admin = AzureAuthRole("project_admin", "rwx",
                                  "Can update project details, delete project, manage project level permissions "
                                  "and perform all file operations")
    project_viewer = AzureAuthRole("project_viewer", "r--", "Can only view project and file meta-data")
    file_downloader = AzureAuthRole("file_downloader", "r-x", "Can download files")
    file_editor = AzureAuthRole("file_editor", "rwx", "Can view download create update and delete files")
    file_uploader = AzureAuthRole("file_uploader", "rwx", "Can update files")


class Users(object):
    def __init__(self, credential):
        self.graph_client = GraphClient(credential=credential, scopes=GRAPH_CLIENT_SCOPES)

    def get_current_user_netid(self):
        response = self.graph_client.get('/me')
        response.raise_for_status()
        netid_email = response.json()["userPrincipalName"]
        return netid_email.replace(DUKE_EMAIL_SUFFIX, "")

    def get_user_for_netid(self, netid):
        response = self.graph_client.get(f'/users/{netid}{DUKE_EMAIL_SUFFIX}')
        if response.status_code == 404:
            raise AzureUserNotFoundException(f'ERROR: User with NetID {netid} not found.')
        response.raise_for_status()
        return response.json()

    def get_id_and_name(self, netid):
        user = self.get_user_for_netid(netid)
        return user["id"], user["displayName"]

    def ensure_user_exists(self, netid):
        self.get_user_for_netid(netid)


class Bucket(object):
    def __init__(self, credential, subscription_id, resource_group, storage_account, container_name):
        self.resource_group = resource_group
        self.storage_mgmt_client = StorageManagementClient(credential=credential, subscription_id=subscription_id)
        self.service = DataLakeServiceClient(f"https://{storage_account}.dfs.core.windows.net/", credential=credential,
                                             scopes=DLS_SCOPES)
        self.file_system = self.service.get_file_system_client(file_system=container_name)
        self.azcopy = create_azcopy()

    def get_paths(self, path, recursive=False):
        return self.file_system.get_paths(path, recursive=recursive)

    def get_file_paths(self, path):
        paths = self.get_paths(path, recursive=True)
        return [path for path in paths if not path["is_directory"]]

    def get_directory_properties(self, path):
        return self.file_system.get_directory_client(path).get_directory_properties()

    def move_directory(self, source, destination):
        directory_client = self.file_system.get_directory_client(source)
        directory_client.rename_directory(new_name=f"{self.file_system.file_system_name}/{destination}")

    def get_file_properties(self, file_path):
        return self.file_system.get_file_client(file_path).get_file_properties()

    def get_storage_account_key1(self):
        return self.storage_mgmt_client.storage_accounts.list_keys(
            resource_group_name=self.resource_group,
            account_name=self.service.account_name).keys[0].value

    def get_sas_token(self, hours=6):
        account_key = self.get_storage_account_key1()
        return generate_file_system_sas(
            account_name=self.service.account_name,
            credential=account_key,
            file_system_name=self.file_system.file_system_name,
            permission="rwdl",
            protocol='https',
            expiry=datetime.utcnow() + timedelta(hours=hours)
        )

    def get_sas_url(self, path, hours=6):
        account_name = self.service.account_name
        bucket_name = self.file_system.file_system_name
        token = self.get_sas_token(hours=hours)
        return f"https://{account_name}.blob.core.windows.net/{bucket_name}/{path}?{token}"

    def get_url(self, path):
        account_name = self.service.account_name
        bucket_name = self.file_system.file_system_name
        return f"https://{account_name}.blob.core.windows.net/{bucket_name}/{path}"

    def update_access_control_recursive(self, path, acl):
        directory_client = self.file_system.get_directory_client(path)
        directory_client.update_access_control_recursive(acl=acl)

    def remove_access_control_recursive(self, path, acl):
        directory_client = self.file_system.get_directory_client(path)
        directory_client.remove_access_control_recursive(acl=acl)

    def upload_paths(self, project_path, paths, dry_run):
        file_paths = []
        for path in paths:
            if os.path.isfile(path):
                file_paths.append(path)
            else:
                # azcopy can only upload one directory at a time so we will make multiple calls
                simple_path = path.rstrip("/").rstrip("\\")
                directory_name = os.path.basename(simple_path)
                destination_url = self.get_url(f"{project_path}/{directory_name}")
                self.azcopy.upload_directory(source=simple_path, destination=destination_url, dry_run=dry_run)
        file_destination_url = self.get_url(project_path)
        # group files to upload by their parent directory so azcopy uploads as fast as possible
        for parent_dir, file_paths in group_by_dirname(file_paths).items():
            filenames = [os.path.basename(file_path) for file_path in file_paths]
            self.azcopy.upload_files(source_parent_dir=parent_dir, source_filenames=filenames,
                                     destination=file_destination_url, dry_run=dry_run)

    def download_paths(self, project_path, include_paths, exclude_paths, destination, dry_run):
        source_url = self.get_url(project_path)
        self.azcopy.download_directory(source=source_url, include_paths=include_paths, exclude_paths=exclude_paths,
                                       destination=destination, dry_run=dry_run)

    def move_path(self, project_path, source_remote_path, target_remote_path):
        file_client = self.file_system.get_file_client(f"{project_path}/{source_remote_path}")
        full_target_path = f"{self.file_system.file_system_name}/{project_path}/{target_remote_path}"
        file_client.rename_file(full_target_path)

    def delete_path(self, path):
        directory_client = self.file_system.get_directory_client(path)
        directory_client.delete_directory()


class AzureProject(object):
    def __init__(self, api, path_dict):
        self.api = api
        self.path_dict = path_dict
        self.path = path_dict["name"]
        # remove netid parent directory
        self.name = strip_top_directory(self.path)

    def get_url(self):
        return self.api.get_url(self.path_dict["name"])

    def get_size_str(self):
        return self.api.get_size_str(project=self)

    def get_file_paths(self):
        return self.api.get_file_paths(self.path)

    def __repr__(self):
        return str(self.path_dict)


class AzureFile(object):
    def __init__(self, api, path_dict):
        self.api = api
        self.path_dict = path_dict
        self.name = path_dict["name"]
        netid_path = strip_top_directory(self.name)
        self.project_path = strip_top_directory(netid_path)

    def get_properties(self):
        return self.api.get_file_properties(self.name)

    def get_md5(self):
        return self.get_properties()["content_settings"]["content_md5"].hex()


class AzureProjectSummary(object):
    def __init__(self):
        self.total_size = 0
        self.file_count = 0
        self.folder_count = 0
        self.root_folder_count = 0
        self.sub_folder_count = 0

    def apply_path_dict(self, path_dict):
        if path_dict["is_directory"]:
            self.folder_count += 1
            # Remote paths with three parts are top level directories "netid/projectname/dirname"
            if len(path_dict["name"].split('/')) == 3:
                self.root_folder_count += 1
            else:
                self.sub_folder_count += 1
        else:
            self.total_size += path_dict["content_length"]
            self.file_count += 1

    def __str__(self):
        parts = []
        if self.folder_count:
            parts.append(plural_fmt("top level folder", self.root_folder_count))
            parts.append(plural_fmt("subfolder", self.sub_folder_count))
        else:
            parts.append(plural_fmt("folder", self.folder_count))
        files_str = plural_fmt("file", self.file_count)
        files_str += " ({})".format(humanize_bytes(self.total_size))
        parts.append(files_str)
        return ", ".join(parts)


class AzureApi(object):
    def __init__(self, config, credential, subscription_id, resource_group, storage_account, container_name):
        self.config = config
        self.users = Users(credential)
        self.current_user_netid = self.users.get_current_user_netid()
        self.bucket = Bucket(credential, subscription_id, resource_group, storage_account, container_name)

    def list_projects(self):
        path_dicts = self.bucket.get_paths(path=self.current_user_netid, recursive=False)
        return [AzureProject(self, path_dict) for path_dict in path_dicts if path_dict["is_directory"] is True]

    def get_project_by_name(self, name):
        path = name
        if "/" not in path:
            path = f"{self.current_user_netid}/{name}"
        try:
            return AzureProject(self, self.bucket.get_directory_properties(path))
        except ResourceNotFoundError:
            return None

    def get_url(self, path):
        return self.bucket.get_url(path)

    def get_container_url(self):
        return self.get_url(path="").rstrip("/")

    def get_auth_roles(self):
        return [
            AuthRoles.project_admin,
            AuthRoles.project_viewer,
            AuthRoles.file_downloader,
            AuthRoles.file_editor,
            AuthRoles.file_uploader
        ]

    def get_auth_role_by_id(self, auth_role):
        for role in self.get_auth_roles():
            if role.id == auth_role:
                return role
        return None

    def get_file_paths(self, path):
        path_dicts = self.bucket.get_paths(path=path, recursive=True)
        return [AzureFile(self, path_dict) for path_dict in path_dicts]

    def get_file_properties(self, file_path):
        return self.bucket.get_file_properties(file_path)

    def get_sas_url(self):
        return self.bucket.get_sas_url(path=self.current_user_netid)

    def add_user_to_project(self, project, netid, auth_role):
        user_id, user_name = self.users.get_id_and_name(netid)
        role = self.get_auth_role_by_id(auth_role)
        self.bucket.update_access_control_recursive(path=project.path, acl=role.get_acl(user_id))
        print(f'Gave user {user_name} {auth_role} permissions for project {project.path}.')

    def remove_user_from_project(self, project, netid):
        user_id, user_name = self.users.get_id_and_name(netid)
        self.bucket.remove_access_control_recursive(path=project.path, acl=make_acl(user_id))
        print(f'Removed permissions from user {user_name} for project {project.path}.')

    def upload_paths(self, project_name, paths, dry_run):
        project_path = project_name
        if "/" not in project_path:
            project_path = f"{self.current_user_netid}/{project_name}/"
        self.bucket.upload_paths(project_path, paths, dry_run)
        print("\nUpload complete.\nSee azcopy log file for details about transferred files.\n\n")

    def download_paths(self, project_name, include_paths, exclude_paths, destination, dry_run):
        project_path = project_name
        if "/" not in project_path:
            project_path = f"{self.current_user_netid}/{project_name}/"
        self.bucket.download_paths(project_path, include_paths, exclude_paths, destination, dry_run=dry_run)
        print("\nDownload complete.\nSee azcopy log file for details about transferred files.\n\n")

    def move_path(self, project, source_remote_path, target_remote_path):
        self.bucket.move_path(project.path, source_remote_path, target_remote_path)
        print(f"\nMoved {source_remote_path} to {target_remote_path} in project {project.name}\n")

    def delete_remote_path(self, project, remote_path):
        self.bucket.delete_path(path=f"{project.path}/{remote_path}")

    def delete_project(self, project):
        self.bucket.delete_path(project.path)

    def get_size_str(self, project):
        summary = AzureProjectSummary()
        for path_dict in self.bucket.get_paths(path=project.path, recursive=True):
            summary.apply_path_dict(path_dict)
        return str(summary)

    def deliver(self, project, netid, resend, user_message, share_usernames):
        data_delivery = DataDelivery(self.config, self)
        data_delivery.deliver(project.path, to_netid=netid, user_message=user_message,
                              share_user_ids=share_usernames, resend=resend)


def create_azure_api(config):
    # Setup to use token cache or prompt user to login with a URL (DeviceCodeCredential)
    cache_persistence_options = TokenCachePersistenceOptions(allow_unencrypted_storage=True)
    credential = ChainedTokenCredential(
        SharedTokenCacheCredential(),
        DeviceCodeCredential(cache_persistence_options=cache_persistence_options))
    return AzureApi(
        config=config,
        credential=credential,
        subscription_id=config.azure_subscription_id,
        resource_group=config.azure_resource_group,
        storage_account=config.azure_storage_account,
        container_name=config.azure_container_name)
