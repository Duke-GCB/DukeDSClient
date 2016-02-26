"""
Command line parser for the application.
"""
import os
import argparse


def _add_project_name_arg(arg_parser):
    """
    Adds project_name parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("-p",
                           metavar='ProjectName',
                           dest='project_name',
                           help="Name of the remote project to upload local files to.",
                           required=True)


def _paths_must_exists(path):
    """
    Raises error if path doesn't exist.
    :param path: str path to check
    :return: str same path passed in
    """
    if not os.path.exists(path):
     raise argparse.ArgumentTypeError("{} is not a valid file/folder.".format(path))
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


def _add_follow_symlinks_arg(arg_parser):
    """
    Adds optional follow_symlinks parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("--follow_symlinks",
                            help="Follow symbolic links(experimental).",
                            action='store_true',
                            dest='follow_symlinks')


def _add_user_full_name_arg(arg_parser):
    """
    Adds user_full_name parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("-user_full_name",
                            metavar='UserFullName',
                            dest='user_full_name',
                            help="Specifies full name of the person in 'Firstname LastName' format.",
                            required=True)


def _add_auth_role_arg(arg_parser):
    """
    Adds optional auth_role parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("--auth_role",
                            metavar='AuthRole',
                            dest='auth_role',
                            help="Specifies authorization role for the user ('project_admin').",
                            default='project_admin')


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

    def register_upload_command(self, upload_func):
        """
        Add the upload command to the parser and call upload_func(project_name, folders, follow_symlinks) when chosen.
        :param upload_func: func Called when this option is chosen: upload_func(project_name, folders, follow_symlinks).
        """
        description = "Uploads local files and folders to a remote host."
        upload_parser = self.subparsers.add_parser('upload', description=description)
        _add_project_name_arg(upload_parser)
        _add_folders_positional_arg(upload_parser)
        _add_follow_symlinks_arg(upload_parser)
        self.upload_func = upload_func
        upload_parser.set_defaults(func=self._upload)

    def _upload(self, upload_args):
        """
        Call upload function passing values from upload_args.
        :param args: Namespace arguments parsed from command line.
        """
        project_name = upload_args.project_name
        folders = upload_args.folders
        follow_symlinks = upload_args.follow_symlinks
        if self.upload_func:
            self.upload_func(project_name, folders, follow_symlinks)

    def register_add_user_command(self, add_user_func):
        """
        Add the add_user command to the parser and call add_user_func(project_name, user_full_name, auth_role)
        when chosen.
        :param add_user_func: func Called when this option is chosen: upload_func(project_name, user_full_name, auth_role).
        """
        add_user_parser = self.subparsers.add_parser('add_user')
        _add_project_name_arg(add_user_parser)
        _add_user_full_name_arg(add_user_parser)
        _add_auth_role_arg(add_user_parser)
        self.add_user_func = add_user_func
        add_user_parser.set_defaults(func=self._add_user)

    def _add_user(self, add_user_args):
        """
        Call add_user function passing values from add_user_args.
        :param args: Namespace arguments parsed from command line.
        """
        project_name = add_user_args.project_name
        user_full_name = add_user_args.user_full_name
        auth_role = add_user_args.auth_role
        if self.add_user_func:
            self.add_user_func(project_name, user_full_name, auth_role)

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