"""
Command line parser for the application.
"""
import os
import sys
import argparse


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
    if sys.version_info >= (3,0,0):
        return str(s)
    else:
        if type(s) != unicode:
            return unicode(s, 'utf8')
        return s


def add_project_name_arg(arg_parser):
    """
    Adds project_name parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("-p",
                           metavar='ProjectName',
                           type=to_unicode,
                           dest='project_name',
                           help="Name of the remote project to manage.",
                           required=True)


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


def path_does_not_exist_or_is_empty(path):
    """
    Raises error if the directory the path exists and contains any files.
    :param path: str path to check
    :return: str path
    """
    if os.path.exists(path):
        if os.listdir(path):
            raise argparse.ArgumentTypeError("{} already exists and is not an empty directory.".format(path))
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
                                "If not specified it will use the name of the project with spaces translated to '_'. "
                                "This folder must be empty or not exist(will be created).",
                           type=path_does_not_exist_or_is_empty,
                           nargs='?')


def _add_follow_symlinks_arg(arg_parser):
    """
    Adds optional follow_symlinks parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("--follow_symlinks",
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
                            help="Username(NetID) to give permissions. "
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
                            help="Email of the person you want to give permission."
                                 " You must specify either --user or this flag.")

def _add_auth_role_arg(arg_parser):
    """
    Adds optional auth_role parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("--auth_role",
                            metavar='AuthRole',
                            type=to_unicode,
                            dest='auth_role',
                            help="Specifies authorization role for the user ('project_admin').",
                            default='project_admin')

def _add_copy_project_arg(arg_parser):
    """
    Adds optional copy_project parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("--skip_copy_project",
                            help="Should we just send the handover email and skip copying the project.",
                            action='store_true',
                            default=False,
                            dest='skip_copy_project')

def _add_resend_arg(arg_parser, type_str):
    """
    Adds resend parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    :param type_str
    """
    arg_parser.add_argument("--resend",
                            action='store_true',
                            default=False,
                            dest='resend',
                            help="Resend {}. ".format(type_str))

class CommandParser(object):
    """
    Root command line parser. Supports the following commands: upload and add_user.
    You must register external functions to called for the various commands.
    Commands must be registered to appear in help.
    """
    def __init__(self):
        self.parser = argparse.ArgumentParser()
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
        add_project_name_arg(upload_parser)
        _add_folders_positional_arg(upload_parser)
        _add_follow_symlinks_arg(upload_parser)
        upload_parser.set_defaults(func=upload_func)

    def register_add_user_command(self, add_user_func):
        """
        Add the add_user command to the parser and call add_user_func(project_name, user_full_name, auth_role)
        when chosen.
        :param add_user_func: func Called when this option is chosen: upload_func(project_name, user_full_name, auth_role).
        """
        description = "Gives user permission to access a remote project."
        add_user_parser = self.subparsers.add_parser('add_user', description=description)
        add_project_name_arg(add_user_parser)
        user_or_email = add_user_parser.add_mutually_exclusive_group(required=True)
        add_user_arg(user_or_email)
        add_email_arg(user_or_email)
        _add_auth_role_arg(add_user_parser)
        add_user_parser.set_defaults(func=add_user_func)

    def register_download_command(self, download_func):
        """
        Add 'download' command for downloading a project to a non-existing or empty directory.
        :param download_func: function to run when user choses this option
        """
        description = "Download the contents of a remote remote project to a local folder."
        download_parser = self.subparsers.add_parser('download', description=description)
        add_project_name_arg(download_parser)
        _add_folder_positional_arg(download_parser)
        download_parser.set_defaults(func=download_func)

    def register_mail_draft_command(self, mail_draft_func):
        """
        Add 'mail_draft' command for adding view only project permissions and sending email via another service.
        :param mail_draft_func: function to run when user choses this option
        """
        description = "Send email about draft project being ready and give user view only permissions."
        mail_draft_parser = self.subparsers.add_parser('mail_draft', description=description)
        add_project_name_arg(mail_draft_parser)
        user_or_email = mail_draft_parser.add_mutually_exclusive_group(required=True)
        add_user_arg(user_or_email)
        add_email_arg(user_or_email)
        _add_resend_arg(mail_draft_parser, "email draft")
        mail_draft_parser.set_defaults(func=mail_draft_func)

    def register_handover_command(self, handover_func):
        """
        Add 'handover' command for removing project permissions, possibly copying the project,
        and sending email via another service that will give the user access once they agree.
        :param handover_func: function to run when user choses this option
        """
        description = "Initiate handover of a project to another user. Removes other user's current permissions. " \
                      "Makes a copy of the project. Send message to handover server to send email and allow " \
                      "access to the copy of the project once user acknowledges receiving the data."
        handover_parser = self.subparsers.add_parser('handover', description=description)
        add_project_name_arg(handover_parser)
        user_or_email = handover_parser.add_mutually_exclusive_group(required=True)
        add_user_arg(user_or_email)
        add_email_arg(user_or_email)
        _add_copy_project_arg(handover_parser)
        _add_resend_arg(handover_parser, "handover")
        handover_parser.set_defaults(func=handover_func)

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

