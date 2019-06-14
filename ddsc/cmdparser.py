"""
Command line parser for the application.
"""
import os
import argparse
import six
from builtins import str

DESCRIPTION_STR = "DukeDSClient ({}) Manage projects/folders/files in the duke-data-service"
INVALID_PATH_CHARS = (':', '/', '\\')


def replace_invalid_path_chars(path):
    """
    Converts bad path characters to '_'.
    :param path: str path to fix
    :return: str fixed path
    """
    for bad_char in INVALID_PATH_CHARS:
        path = path.replace(bad_char, '_')
    return path


def to_unicode(s):
    """
    Convert a command line string to utf8 unicode.
    :param s: string to convert to unicode
    :return: unicode string for argument
    """
    return s if six.PY3 else str(s, 'utf-8')


def add_project_name_arg(arg_parser, required, help_text):
    """
    Adds project_name parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    :param help_text: str label displayed in usage
    """
    arg_parser.add_argument("-p", '--project-name',
                            metavar='ProjectName',
                            type=to_unicode,
                            dest='project_name',
                            help=help_text,
                            required=required)


def add_project_id_arg(arg_parser, required, help_text):
    """
    Adds project_name parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    :param help_text: str label displayed in usage
    """
    arg_parser.add_argument("-i", '--project-id',
                            metavar='ProjectUUID',
                            type=to_unicode,
                            dest='project_id',
                            help=help_text,
                            required=required)


def add_project_name_or_id_arg(arg_parser, required=True, help_text_suffix="manage"):
    """
    Adds project name or project id argument. These two are mutually exclusive.
    :param arg_parser:
    :param required:
    :param help_text:
    :return:
    """
    project_name_or_id = arg_parser.add_mutually_exclusive_group(required=required)
    name_help_text = "Name of the project to {}.".format(help_text_suffix)
    add_project_name_arg(project_name_or_id, required=False, help_text=name_help_text)
    id_help_text = "ID of the project to {}.".format(help_text_suffix)
    add_project_id_arg(project_name_or_id, required=False, help_text=id_help_text)


def _paths_must_exists(path):
    """
    Raises error if path doesn't exist.
    :param path: str path to check
    :return: str same path passed in
    """
    path = to_unicode(path)
    if not os.path.exists(path):
        raise argparse.ArgumentTypeError("{} is not a valid file/folder.".format(path))
    return path


def format_destination_path(path):
    """
    Formats command line destination path.
    :param path: str path to check
    :return: str path
    """
    path = to_unicode(path)
    return _path_has_ok_chars(path)


def _path_has_ok_chars(path):
    """
    Validate path for invalid characters.
    :param path: str possible filesystem path
    :return: path if it was ok otherwise raises error
    """
    basename = os.path.basename(path)
    if any([bad_char in basename for bad_char in INVALID_PATH_CHARS]):
            raise argparse.ArgumentTypeError("{} contains invalid characters for a directory.".format(path))
    return path


def _add_folders_positional_arg(arg_parser):
    """
    Adds folders and/or filenames parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("folders",
                            metavar='Folders',
                            nargs="+",
                            help="Names of the files and/or folders to upload to the remote project.",
                            type=_paths_must_exists)


def _add_folder_positional_arg(arg_parser):
    """
    Adds folders and/or filenames parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("folder",
                            metavar='Folder',
                            help="Name of the folder to download the project contents into. "
                                 "If not specified it will use the name of the project with spaces translated to '_'. ",
                            nargs='?')


def _add_follow_symlinks_arg(arg_parser):
    """
    Adds optional follow_symlinks parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("--follow-symlinks",
                            help="Follow symbolic links(experimental).",
                            action='store_true',
                            dest='follow_symlinks')


def add_user_arg(arg_parser):
    """
    Adds username parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("--user",
                            metavar='Username',
                            type=to_unicode,
                            dest='username',
                            help="Username(NetID) to update permissions on. "
                                 "You must specify either --email or this flag.")


def add_email_arg(arg_parser):
    """
    Adds user_email parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("--email",
                            metavar='UserEmail',
                            type=to_unicode,
                            dest='email',
                            help="Email of the person you want to update permissions on."
                                 " You must specify either --user or this flag.")


def add_share_usernames_arg(arg_parser):
    arg_parser.add_argument("--share-users",
                            metavar='ShareUsers',
                            type=to_unicode,
                            nargs='+',
                            dest='share_usernames',
                            help="Usernames(NetIDs) of the people you want to share with upon delivery acceptance.")


def add_share_emails_arg(arg_parser):
    arg_parser.add_argument("--share-emails",
                            metavar='ShareEmails',
                            type=to_unicode,
                            nargs='+',
                            dest='share_emails',
                            help="Email of the person you want to share with upon delivery acceptance.")


def _add_auth_role_arg(arg_parser, default_permissions):
    """
    Adds optional auth_role parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    :param default_permissions: default value to use for this argument
    """
    help_text = "Specifies which project permissions to give to the user. Example: 'project_admin'. "
    help_text += "See command list_auth_roles for AuthRole values."
    arg_parser.add_argument("--auth-role",
                            metavar='AuthRole',
                            type=to_unicode,
                            dest='auth_role',
                            help=help_text,
                            default=default_permissions)


def _add_project_filter_auth_role_arg(arg_parser):
    """
    Adds optional auth_role filtering parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    help_text = "Filters project listing to just those projects with the specified role. "
    help_text += "See command list_auth_roles for AuthRole values."
    arg_parser.add_argument("--auth-role",
                            metavar='AuthRole',
                            type=to_unicode,
                            dest='auth_role',
                            help=help_text,
                            default=None)


def _add_copy_project_arg(arg_parser):
    """
    Adds optional copy_project parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("--copy",
                            help="Instead of delivering the specified project, deliver a copy of the project.",
                            action='store_true',
                            default=False,
                            dest='copy_project')


def _add_resend_arg(arg_parser, resend_help):
    """
    Adds resend parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    :param type_str
    """
    arg_parser.add_argument("--resend",
                            action='store_true',
                            default=False,
                            dest='resend',
                            help=resend_help)


def _add_force_arg(arg_parser, help_text):
    """
    Adds optional force parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    :param help_text: str label displayed in usage
    """
    arg_parser.add_argument("--force",
                            help=help_text,
                            action='store_true',
                            dest='force')


def _add_include_arg(arg_parser):
    """
    Adds optional repeatable include parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("--include",
                            metavar='Path',
                            action='append',
                            type=to_unicode,
                            dest='include_paths',
                            help="Specifies a single path to include. This argument can be repeated.",
                            default=[])


def _add_exclude_arg(arg_parser):
    """
    Adds optional repeatable exclude parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("--exclude",
                            metavar='Path',
                            action='append',
                            type=to_unicode,
                            dest='exclude_paths',
                            help="Specifies a single path to exclude. This argument can be repeated.",
                            default=[])


def _add_dry_run(arg_parser, help_text):
    """
    Adds optional --dry-run parameter to a parser. Stored as 'dry_run'.
    :param arg_parser: ArgumentParser parser to add this argument to.
    :param help_text: str label displayed in usage
    """
    arg_parser.add_argument("--dry-run",
                            help=help_text,
                            action='store_true',
                            dest='dry_run')


def _skip_config_file_permission_check(arg_parser):
    """
    Adds optional follow_symlinks parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("--allow-insecure-config-file",
                            help="Do not check the config file ~/.ddsclient permissions.",
                            action='store_true',
                            dest='allow_insecure_config_file',
                            default=False)


def _add_message_file(arg_parser, help_text):
    """
    Add mesage file argument with help_text to arg_parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    :param help_text: str: help text for this argument
    """
    arg_parser.add_argument('--msg-file',
                            type=argparse.FileType('r'),
                            help=help_text)


def _add_long_format_option(arg_parser, help_text):
    """
    Adds optional follow_symlinks parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("-l",
                            help=help_text,
                            action='store_true',
                            dest='long_format')


class CommandParser(object):
    """
    Root command line parser. Supports the following commands: upload and add_user.
    You must register external functions to called for the various commands.
    Commands must be registered to appear in help.
    """
    def __init__(self, version_str):
        self.parser = argparse.ArgumentParser(description=DESCRIPTION_STR.format(version_str))
        _skip_config_file_permission_check(self.parser)
        self.subparsers = self.parser.add_subparsers()
        self.upload_func = None
        self.add_user_func = None
        self.download_func = None

    def register_upload_command(self, upload_func):
        """
        Add the upload command to the parser and call upload_func(project_name, folders, follow_symlinks) when chosen.
        :param upload_func: func Called when this option is chosen: upload_func(project_name, folders, follow_symlinks).
        """
        description = "Uploads local files and folders to a remote host."
        upload_parser = self.subparsers.add_parser('upload', description=description)
        _add_dry_run(upload_parser, help_text="Instead of uploading displays a list of folders/files that "
                                              "need to be uploaded.")
        add_project_name_or_id_arg(upload_parser, help_text_suffix="upload files/folders to.")
        _add_folders_positional_arg(upload_parser)
        _add_follow_symlinks_arg(upload_parser)
        upload_parser.set_defaults(func=upload_func)

    def register_add_user_command(self, add_user_func):
        """
        Add the add-user command to the parser and call add_user_func(project_name, user_full_name, auth_role)
        when chosen.
        :param add_user_func: func Called when this option is chosen: upload_func(project_name, user_full_name, auth_role).
        """
        description = "Gives user permission to access a remote project."
        add_user_parser = self.subparsers.add_parser('add-user', description=description)
        add_project_name_or_id_arg(add_user_parser, help_text_suffix="add a user to")
        user_or_email = add_user_parser.add_mutually_exclusive_group(required=True)
        add_user_arg(user_or_email)
        add_email_arg(user_or_email)
        _add_auth_role_arg(add_user_parser, default_permissions='project_admin')
        add_user_parser.set_defaults(func=add_user_func)

    def register_remove_user_command(self, remove_user_func):
        """
        Add the remove-user command to the parser and call remove_user_func(project_name, user_full_name) when chosen.
        :param remove_user_func: func Called when this option is chosen: remove_user_func(project_name, user_full_name).
        """
        description = "Removes user permission to access a remote project."
        remove_user_parser = self.subparsers.add_parser('remove-user', description=description)
        add_project_name_or_id_arg(remove_user_parser, help_text_suffix="remove a user from")
        user_or_email = remove_user_parser.add_mutually_exclusive_group(required=True)
        add_user_arg(user_or_email)
        add_email_arg(user_or_email)
        remove_user_parser.set_defaults(func=remove_user_func)

    def register_download_command(self, download_func):
        """
        Add 'download' command for downloading a project to a directory.
        For non empty directories it will download remote files replacing local files.
        :param download_func: function to run when user choses this option
        """
        description = "Download the contents of a remote remote project to a local folder."
        download_parser = self.subparsers.add_parser('download', description=description)
        add_project_name_or_id_arg(download_parser, help_text_suffix="download")
        _add_folder_positional_arg(download_parser)
        include_or_exclude = download_parser.add_mutually_exclusive_group(required=False)
        _add_include_arg(include_or_exclude)
        _add_exclude_arg(include_or_exclude)
        download_parser.set_defaults(func=download_func)

    def register_share_command(self, share_func):
        """
        Add 'share' command for adding view only project permissions and sending email via another service.
        :param share_func: function to run when user choses this option
        """
        description = "Share a project with another user with specified  permissions. " \
                      "Sends the other user an email message via D4S2 service. " \
                      "If not specified this command gives user download permissions."
        share_parser = self.subparsers.add_parser('share', description=description)
        add_project_name_or_id_arg(share_parser)
        user_or_email = share_parser.add_mutually_exclusive_group(required=True)
        add_user_arg(user_or_email)
        add_email_arg(user_or_email)
        _add_auth_role_arg(share_parser, default_permissions='file_downloader')
        _add_resend_arg(share_parser, "Resend share")
        _add_message_file(share_parser, "Filename containing a message to be sent with the share. "
                                        "Pass - to read from stdin.")
        share_parser.set_defaults(func=share_func)

    def register_deliver_command(self, deliver_func):
        """
        Add 'deliver' command for transferring a project to another user.,
        :param deliver_func: function to run when user choses this option
        """
        description = "Initiate delivery of a project to another user. Removes other user's current permissions. " \
                      "Send message to D4S2 service to send email and allow access to the project once user " \
                      "acknowledges receiving the data."
        deliver_parser = self.subparsers.add_parser('deliver', description=description)
        add_project_name_or_id_arg(deliver_parser)
        user_or_email = deliver_parser.add_mutually_exclusive_group(required=True)
        add_user_arg(user_or_email)
        add_email_arg(user_or_email)
        add_share_usernames_arg(deliver_parser)
        add_share_emails_arg(deliver_parser)
        _add_copy_project_arg(deliver_parser)
        _add_resend_arg(deliver_parser, "Resend delivery")
        include_or_exclude = deliver_parser.add_mutually_exclusive_group(required=False)
        _add_include_arg(include_or_exclude)
        _add_exclude_arg(include_or_exclude)
        _add_message_file(deliver_parser, "Filename containing a message to be sent with the delivery. "
                                          "Pass - to read from stdin.")
        deliver_parser.set_defaults(func=deliver_func)

    def register_list_command(self, list_func):
        """
        Add 'list' command to get a list of projects or details about one project.
        :param list_func: function: run when user choses this option.
        """
        description = "Show a list of project names or folders/files of a single project."
        list_parser = self.subparsers.add_parser('list', description=description)
        project_name_or_auth_role = list_parser.add_mutually_exclusive_group(required=False)
        _add_project_filter_auth_role_arg(project_name_or_auth_role)
        add_project_name_or_id_arg(project_name_or_auth_role, required=False,
                                   help_text_suffix="show details for")
        _add_long_format_option(list_parser, 'Display long format.')
        list_parser.set_defaults(func=list_func)

    def register_delete_command(self, delete_func):
        """
        Add 'delete' command delete a project from the remote store.
        :param delete_func: function: run when user choses this option.
        """
        description = "Permanently delete a project."
        delete_parser = self.subparsers.add_parser('delete', description=description)
        add_project_name_or_id_arg(delete_parser, help_text_suffix="delete")
        _add_force_arg(delete_parser, "Do not prompt before deleting.")
        delete_parser.set_defaults(func=delete_func)

    def register_list_auth_roles_command(self, list_auth_roles_func):
        """
        Add 'list_auth_roles' command to list project authorization roles that can be used with add_user.
        :param list_auth_roles_func: function: run when user choses this option.
        """
        description = "List authorization roles for use with add_user command."
        list_auth_roles_parser = self.subparsers.add_parser('list-auth-roles', description=description)
        list_auth_roles_parser.set_defaults(func=list_auth_roles_func)

    def register_move_command(self, move_func):
        """
        Add 'move' command to move a file/folder within a remote project.
        :param move_func: function: run when user choses this option.
        """
        description = "Moves and/or renames a file or folder within a project. \n" \
                      "When Target is a directory Source will be moved into it that directory. " \
                      "Otherwise Source will be renamed to the filename of Target and moved into the parent" \
                      " directory of Target. Target and Source are remote paths that must start with a '/'."
        parser = self.subparsers.add_parser('move', description=description)
        add_project_name_or_id_arg(parser, help_text_suffix="containing a file/folder to move")
        parser.add_argument("source_remote_path",
                            metavar='Source',
                            type=to_unicode,
                            help='remote path specifying the file/folder to be moved')
        parser.add_argument("target_remote_path",
                            metavar='Target',
                            type=to_unicode,
                            help='remote path specifying where to move the file/folder to')
        parser.set_defaults(func=move_func)

    def run_command(self, args):
        """
        Parse command line arguments and run function registered for the appropriate command.
        :param args: [str] command line arguments
        """
        parsed_args = self.parser.parse_args(args)
        if hasattr(parsed_args, 'func'):
            parsed_args.func(parsed_args)
        else:
            self.parser.print_help()
