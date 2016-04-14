""" Runs the appropriate command for a user based on arguments. """
from __future__ import print_function
import datetime
from ddsc.core.handover import ProjectHandover, HandoverError
from ddsc.core.remotestore import RemoteStore
from ddsc.core.upload import ProjectUpload
from ddsc.cmdparser import CommandParser, path_does_not_exist_or_is_empty, replace_invalid_path_chars
from ddsc.core.download import ProjectDownload


class DDSClient(object):
    """
    Runs various commands based on arguments.
    """
    def __init__(self, config):
        """
        Pass in the configuration for the data service.
        :param config: ddsc.config.Config configuration object to be used with the DataServiceApi.
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
        parser.register_mail_draft_command(self._setup_run_command(MailDraftCommand))
        parser.register_handover_command(self._setup_run_command(HandoverCommand))

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
        command = command_constructor(self.config)
        command.run(args)


class UploadCommand(object):
    """
    Uploads a folder to a remote project.
    """
    def __init__(self, config):
        """
        Pass in the config containing remote_store/url so we can access the remote data.
        :param config: Config global configuration for use with this command.
        """
        self.remote_store = RemoteStore(config)
        self.config = config

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

        project_upload = ProjectUpload(self.config, project_name, folders, follow_symlinks=follow_symlinks)
        print(project_upload.get_differences_summary())
        if project_upload.needs_to_upload():
            project_upload.run()
            print('\n')
            print(project_upload.get_upload_report())
            print('\n')
        print(project_upload.get_url_msg())


class DownloadCommand(object):
    """
    Downloads the content from a remote project into a folder.
    """
    def __init__(self, config):
        """
        Pass in the config who can create a remote_store so we can access the remote data.
        :param config: Config global configuration for use with this command.
        """
        self.remote_store = RemoteStore(config)

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
        project_download = ProjectDownload(self.remote_store, project_name, folder)
        project_download.run()


class AddUserCommand(object):
    """
    Adds a user to a pre-existing remote project.
    """
    def __init__(self, config):
        """
        Pass in the config who can create a remote_store so we can access the remote data.
        :param config: Config global configuration for use with this command.
        """
        self.remote_store = RemoteStore(config)

    def run(self, args):
        """
        Give the user with user_full_name the auth_role permissions on the remote project with project_name.
        :param args Namespace arguments parsed from the command line
        """
        project_name = args.project_name    # name of the pre-existing project to set permissions on
        email = args.email                  # email of person to give permissions, will be None if username is specified
        username = args.username            # username of person to give permissions, will be None if email is specified
        auth_role = args.auth_role          # type of permission(project_admin)
        project = self.remote_store.fetch_remote_project(project_name, must_exist=True)
        user = self.remote_store.lookup_user_by_email_or_username(email, username)
        self.remote_store.set_user_project_permission(project, user, auth_role)
        print(u'Gave user {} {} permissions for {}.'.format(user.full_name, auth_role, project_name))


class MailDraftCommand(object):
    """
    Send email that draft project is ready for a user.
    """
    def __init__(self, config):
        """
        Pass in the config who can create a remote_store so we can access the remote data.
        :param config: Config global configuration for use with this command.
        """
        self.remote_store = RemoteStore(config)
        self.project_handover = ProjectHandover(config, self.remote_store, print_func=print)

    def run(self, args):
        """
        Send email that draft project is ready for the user.
        :param args Namespace arguments parsed from the command line
        """
        project_name = args.project_name    # name of the pre-existing project to set permissions on
        email = args.email                  # email of person to send email to
        username = args.username            # username of person to send email to, will be None if email is specified
        force_send = args.resend            # is this a resend so we should force sending
        to_user = self.remote_store.lookup_user_by_email_or_username(email, username)
        try:
            dest_email = self.project_handover.mail_draft(project_name, to_user, force_send)
            print("Email draft sent to " + dest_email)
        except HandoverError as ex:
            if ex.warning:
                print(ex.message)
            else:
                raise


class HandoverCommand(object):
    """
    Send handover email that project is ready for a user to receive.
    """
    def __init__(self, config):
        """
        Pass in the config who can create a remote_store so we can access the remote data.
        :param config: Config global configuration for use with this command.
        """
        self.remote_store = RemoteStore(config)
        self.project_handover = ProjectHandover(config, self.remote_store, print_func=print)

    def run(self, args):
        """
        Send handover email that project is ready for a user to receive.
        :param args Namespace arguments parsed from the command line
        """
        project_name = args.project_name    # name of the pre-existing project to set permissions on
        email = args.email                  # email of person to handover to, will be None if username is specified
        username = args.username            # username of person to handover to, will be None if email is specified
        skip_copy_project = args.skip_copy_project  # should we skip the copy step
        force_send = args.resend            # is this a resend so we should force sending
        new_project_name = None
        if not skip_copy_project:
            new_project_name = self.get_new_project_name(project_name)
        to_user = self.remote_store.lookup_user_by_email_or_username(email, username)
        try:
            dest_email = self.project_handover.handover(project_name, new_project_name, to_user, force_send)
            print("Handover message sent to " + dest_email)
        except HandoverError as ex:
            if ex.warning:
                print(ex.message)
            else:
                raise

    def get_new_project_name(self, project_name):
        """
        Return a unique project name for the copy.
        :param project_name:
        :return:
        """
        timestamp_str = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M')
        return "{} {}".format(project_name, timestamp_str)

