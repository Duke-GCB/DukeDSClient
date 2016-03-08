from __future__ import print_function
import os
import sys
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from ddsc.localstore import LocalProject, LocalOnlyCounter, UploadReport
from ddsc.remotestore import RemoteStore, RemoteContentDownloader
from ddsc.ddsapi import DataServiceApi, KindType, SWIFT_BYTES_PER_CHUNK
from ddsc.cmdparser import CommandParser, path_does_not_exist_or_is_empty, replace_invalid_path_chars
from ddsc.util import ProgressPrinter


DDS_DEFAULT_URL = 'https://dataservice.duke.edu/api/v1'


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
        :return: str root url of the data service (eg: https://dataservice.duke.edu)
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
        parser.register_upload_command(self._setup_run_command(UploadCommand))
        parser.register_add_user_command(self._setup_run_command(AddUserCommand))
        parser.register_download_command(self._setup_run_command(DownloadCommand))
        return parser

    def _setup_run_command(self, command_constructor):
        """
        Create f(args) to run that will create the specified object and call run when invoked.
        The reason for this complexity is deferring the creation of expensive objects until
        we have decided to run a command. For instance setting up the data service api if we are just running -h.
        :param command_constructor: class specifies object to create and pass args to(eventually).
        :return: func function that will let the command created by command_constructor run with arguments.
        """
        return lambda args: self._run_command(command_constructor, args)

    def _run_command(self, command_constructor, args):
        """
        Run command_constructor and call run(args) on the resulting object
        :param command_constructor: class of an object that implements run(args)
        :param args: object arguments for specific command created by CommandParser
        """
        command = command_constructor(self)
        command.run(args)

    def create_remote_store(self):
        """
        Create a remote store based on config.
        :return: RemoteStore remote store based on config settings.
        """
        data_service = DataServiceApi(self.config.get_auth(), self.config.get_data_service_url())
        return RemoteStore(data_service)


class UploadCommand(object):
    """
    Uploads a folder to a remote project.
    """
    def __init__(self, ddsclient):
        """
        Pass in the parent who can create a remote_store/url so we can access the remote data.
        :param ddsclient: DDSClient parent who can create objects based on config for us.
        """
        self.remote_store = ddsclient.create_remote_store()
        self.base_url = ddsclient.config.get_url_base()

    def run(self, args):
        """
        Upload contents of folders to a project with project_name on remote store.
        If follow_symlinks we will traverse symlinked directories.
        If content is already on remote site it will not be sent.
        :param args: Namespace arguments parsed from the command line.
        """
        project_name = args.project_name        # name of the remote project to create/upload to
        folders = args.folders                  # list of local files/folders to upload into the project
        follow_symlinks = args.follow_symlinks  # should we follow symlinks when traversing folders

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
        progress_printer = ProgressPrinter(different_items_cnt, msg_verb='sending')
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


class DownloadCommand(object):
    """
    Downloads the content from a remote project into a folder.
    """
    def __init__(self, ddsclient):
        """
        Pass in the parent who can create a remote_store so we can access the remote data.
        :param ddsclient: DDSClient parent who can create objects based on config for us.
        """
        self.remote_store = ddsclient.create_remote_store()

    def run(self, args):
        """
        Download a project based on passed in args.
        :param args: Namespace arguments parsed from the command line.
        """
        project_name = args.project_name    # name of the pre-existing project to set permissions on
        folder = args.folder                # path to a folder to download data into
        # Default to project name with spaces replaced with '_' if not specified
        if not folder:
            fixed_path = replace_invalid_path_chars(project_name.replace(' ', '_'))
            folder = path_does_not_exist_or_is_empty(fixed_path)
        remote_project = self.remote_store.fetch_remote_project(project_name, must_exist=True)
        downloader = RemoteContentDownloader(self.remote_store, folder)
        downloader.walk_project(remote_project)


class AddUserCommand(object):
    """
    Adds a user to a pre-existing remote project.
    """
    def __init__(self, ddsclient):
        """
        Pass in the parent who can create a remote_store so we can access the remote data.
        :param ddsclient: DDSClient parent who can create objects based on config for us.
        """
        self.remote_store = ddsclient.create_remote_store()

    def run(self, args):
        """
        Give the user with user_full_name the auth_role permissions on the remote project with project_name.
        :param args Namespace arguments parsed from the command line
        """
        project_name = args.project_name    # name of the pre-existing project to set permissions on
        username = args.username            # username of person to give permissions, will be None if email is specified
        email = args.email                  # email of person to give permissions, will be None if username is specified
        auth_role = args.auth_role          # type of permission(project_admin)
        project = self.remote_store.fetch_remote_project(project_name, must_exist=True)
        user = None
        if username:
            user = self.remote_store.lookup_user_by_username(username)
        else :
            user = self.remote_store.lookup_user_by_email(email)
        self.remote_store.set_user_project_permission(project, user, auth_role)
        print(u'Gave user {} {} permissions for {}.'.format(user.full_name, auth_role, project_name))


