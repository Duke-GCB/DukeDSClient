from __future__ import print_function
import os
import sys
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from ddsc.localstore import LocalProject, LocalOnlyCounter, UploadReport
from ddsc.remotestore import RemoteStore
from ddsc.ddsapi import DataServiceApi, KindType, SWIFT_BYTES_PER_CHUNK
from ddsc.cmdparser import CommandParser


DDS_DEFAULT_URL = 'https://uatest.dataservice.duke.edu/api/v1'


class Config(object):
    """
    Global configuration for data service api.
    """
    def get_auth(self):
        """
        Return an authorization token
        :return: str auth token to be used with DataServiceApi
        """
        auth = os.environ.get('DUKE_DATA_SERVICE_AUTH', None)
        if not auth:
            raise ValueError('Set DUKE_DATA_SERVICE_AUTH environment variable to valid key.')
        return auth

    def get_data_service_url(self):
        """
        Return base url to data service.
        :return: str url for use with DataServiceApi
        """
        url = os.environ.get('DUKE_DATA_SERVICE_URL', None)
        if not url:
            url = DDS_DEFAULT_URL
        return url

    def get_url_base(self):
        """
        Determine root url of the data service from the url specified.
        :return: str root url of the data service (eg: https://uatest.dataservice.duke.edu)
        """
        url = self.get_data_service_url()
        return urlparse(url).hostname


class DDSClient(object):
    """
    Runs various commands based on arguments.
    """
    def __init__(self, config):
        """
        Pass in the configuration for the data service.
        :param config: Config configuration object to be used with the DataServiceApi.
        """
        self.config = config

    def run_command(self, args):
        """
        Create a parser and have it parse the args then run the appropriate command.
        :param args: [str] command line args
        """
        parser = self._create_parser()
        parser.run_command(args)

    def _create_parser(self):
        """
        Create a parser hooking up the command methods below to be run when chosen.
        :return: CommandParser parser with commands attached.
        """
        parser = CommandParser()
        parser.register_upload_command(self.upload)
        parser.register_add_user_command(self.add_user)
        return parser

    def upload(self, project_name, folders, follow_symlinks):
        """
        Run upload command for the specified arguments and a remote store based on config.
        :param project_name: str name of the remote project to create/upload items to
        :param folders: [str] list of paths to upload to remote store if necessary
        :param follow_symlinks: bool should we include symbolically linked directories
        """
        command = UploadCommand(self.create_remote_store(), self.config.get_url_base())
        command.upload(project_name, folders, follow_symlinks)

    def add_user(self, project_name, user_full_name, auth_role):
        """
        Run add_user command for the specified arguments and a remote store based on config.
        :param project_name: str name of the pre-existing remote project to add this user to
        :param user_full_name: str full name (firstname lastname) of the pre-existing user we want to add
        :param auth_role: str auth_role we want to give to the user(project_admin)
        """
        command = AddUserCommand(self.create_remote_store())
        command.add_user(project_name, user_full_name, auth_role)

    def create_remote_store(self):
        """
        Create a remote store based on config.
        :return: RemoteStore remote store based on config settings.
        """
        data_service = DataServiceApi(self.config.get_auth(), self.config.get_data_service_url())
        return RemoteStore(data_service)


class UploadCommand(object):
    def __init__(self, remote_store, base_url):
        """
        Pass in the remote_store so we can access the remote data.
        :param remote_store: RemoteStore data store we will be sending and receiving data from.
        :param base_url: str base url for creating a portal access url
        """
        self.remote_store = remote_store
        self.base_url = base_url

    def upload(self, project_name, folders, follow_symlinks):
        """
        Upload contents of folders to a project with project_name on remote store.
        If follow_symlinks we will traverse symlinked directories.
        If content is already on remote site it will not be sent.
        :param project_name:
        :param folders:
        :param follow_symlinks:
        """
        remote_project = self.remote_store.fetch_remote_project(project_name)
        local_project = self._load_local_project(folders, follow_symlinks)
        local_project.update_remote_ids(remote_project)
        different_items = self._count_differences(local_project)
        self._print_differences_summary(different_items)
        if different_items.total_items() != 0:
            self._upload_differences(local_project, project_name, different_items.total_items())
            self._print_report(project_name, local_project)
        self._print_url(local_project)

    def _load_local_project(self, folders, follow_symlinks):
        local_project = LocalProject(followsymlinks=follow_symlinks)
        local_project.add_paths(folders)
        return local_project

    def _count_differences(self, local_project):
        """
        Count how many things we will be sending.
        :param local_project: LocalProject project we will send data from
        :return: LocalOnlyCounter contains counts for various items
        """
        different_items = LocalOnlyCounter(SWIFT_BYTES_PER_CHUNK)
        different_items.walk_project(local_project)
        return different_items

    def _print_differences_summary(self, different_items):
        """
        Print a summary of what is to be done.
        :param different_items: LocalOnlyCounter item that contains the summary
        """
        print('Uploading {}.'.format(different_items.result_str()))

    def _upload_differences(self, local_project, project_name, different_items_cnt):
        """
        Send different items within local_project to remote store
        :param local_project: LocalProject project we will send data from
        :param different_items_cnt: int count of items to be sent for progress bar
        """
        progress_printer = ProgressPrinter(different_items_cnt)
        self.remote_store.upload_differences(local_project,
                                             project_name,
                                             progress_printer)
        progress_printer.finished()

    def _print_report(self, project_name, local_project):
        """
        Generate and print a report onto stdout.
        """
        report = UploadReport(project_name)
        report.walk_project(local_project)
        print('\n')
        print(report.get_content())
        print('\n')

    def _print_url(self, local_project):
        """
        Print url to view the project via dds portal.
        """
        msg = 'URL to view project'
        project_id = local_project.remote_id
        url = '{}: https://{}/portal/#/project/{}'.format(msg, self.base_url, project_id)
        print(url)


class AddUserCommand(object):
    def __init__(self, remote_store):
        """
        Pass in the remote_store so we can access the remote data.
        :param remote_store: RemoteStore data store we will be sending and receiving data from.
        """
        self.remote_store = remote_store

    def add_user(self, project_name, user_full_name, auth_role):
        """
        Give the user with user_full_name the auth_role permissions on the remote project with project_name.
        :param project_name: str name of the pre-existing project to set permissions on
        :param user_full_name: str full name (LastName FirstName) of the user you want to give permissions to
        :param auth_role: str type of permission(project_admin)
        """
        project = self._fetch_project(project_name)
        user = self.remote_store.lookup_user_by_name(user_full_name)
        self.remote_store.set_user_project_permission(project, user, auth_role)
        print(u'Gave user {} {} permissions for {}.'.format(user_full_name, auth_role, project_name))

    def _fetch_project(self, project_name):
        remote_project = self.remote_store.fetch_remote_project(project_name)
        if not remote_project:
            raise ValueError(u'There is no project with the name {}'.format(project_name).encode('utf-8'))
        return remote_project


class ProgressPrinter(object):
    """
    Prints a progress bar(percentage) to the terminal, expects to have sending_item and finished called.
    Replaces the same line again and again as progress changes.
    """
    def __init__(self, total):
        """
        Setup printer expecting to have sending_item called total times.
        :param total: int the number of items we are expecting, used to determine progress
        """
        self.total = total
        self.cnt = 0
        self.max_width = 0

    def sending_item(self, item):
        """
        Update progress that item is about to be sent.
        :param item: LocalFile, LocalFolder, or LocalContent(project) that is about to be sent.
        """
        percent_done = int(float(self.cnt)/float(self.total) * 100.0)
        name = ''
        if KindType.is_project(item):
            name = 'project'
        else:
            name = item.path
        # left justify message so we cover up the previous one
        message = '\rProgress: {}% - sending {}'.format(percent_done, name)
        self.max_width = max(len(message), self.max_width)
        sys.stdout.write(message.ljust(self.max_width))
        sys.stdout.flush()
        self.cnt += 1

    def finished(self):
        """
        Must be called to print final progress label.
        """
        sys.stdout.write('\rDone: 100%'.ljust(self.max_width) + '\n')
        sys.stdout.flush()