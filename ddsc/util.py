import os
import sys
import argparse
import urlparse


class Configuration(object):
    """Holds configuration to perform folder upload operation."""
    def __init__(self, args):
        self.auth = os.environ.get('DUKE_DATA_SERVICE_AUTH', None)
        if not self.auth:
            raise ValueError('Set DUKE_DATA_SERVICE_AUTH environment variable to valid key.')

        self.url = os.environ.get('DUKE_DATA_SERVICE_URL', None)
        if not self.url:
            self.url = 'https://uatest.dataservice.duke.edu/api/v1'

        parsed_args = ArgParser.parse_args(args=args)
        self.project_name = parsed_args.project_name
        self.project_desc = self.project_name
        self.folders = parsed_args.folders
        self.add_username = parsed_args.add_username
        self.add_user_type = parsed_args.add_user_type
        self.allow_symlink = parsed_args.allow_symlink
        self._check_args()

    def _check_args(self):
        for path in self.folders:
            if not os.path.exists(path):
                raise ValueError('You tried to upload ' + path + ', but that path does not exist.')

    def get_url_base(self):
        return urlparse.urlparse(self.url).hostname


class ArgParser(object):

    @staticmethod
    def parse_args(args):
        parser = ArgParser.create_parser()
        return parser.parse_args(args=args)

    @staticmethod
    def create_parser():
        description = ("Uploads a folder to duke data service. "
                       "Specify an auth token via DUKE_DATA_SERVICE_AUTH environment variable. "
                       "Specify an alternate url via DUKE_DATA_SERVICE_URL environment variable if necessary. "
                      )
        parser = argparse.ArgumentParser(description=description)
        parser.add_argument("project_name",
                            help="Name of the project to create or add to.")
        parser.add_argument("folders",
                            help="The folders/files you want to add to the project.",
                            nargs='*')
        parser.add_argument("--adduser",
                            help="Add a user to the project with their full name.",
                            dest='add_username')
        parser.add_argument("--addusertype",
                            help="Sets the auth_role of user to add if --adduser is specified.",
                            default="project_admin", #other roles not really working as specified yet
                            dest='add_user_type')
        parser.add_argument("--allowsymlink",
                            help="Follow symbolic links(experimental).",
                            action='store_true',
                            dest='allow_symlink')
        parser.set_defaults(allow_symlink=False)
        return parser

class KindType(object):
    file_str = 'dds-file'
    folder_str = 'dds-folder'
    project_str = 'dds-project'

    @staticmethod
    def is_file(item):
        return item.kind == KindType.file_str

    @staticmethod
    def is_folder(item):
        return item.kind == KindType.folder_str

    @staticmethod
    def is_project(item):
        return item.kind == KindType.project_str


class ProgressPrinter(object):
    def __init__(self, total):
        self.total = total
        self.cnt = 0
        self.max_width = 0

    def sending_item(self, item):
        percent_done = int(float(self.cnt)/float(self.total)* 100.0)
        name = ''
        if KindType.is_project(item):
            name = 'project'
        else:
            name = item.path
        #left justify message so we cover up the previous one
        message = '\rProgress: {}% - sending {}'.format(percent_done, name)
        self.max_width = max(len(message), self.max_width)
        sys.stdout.write(message.ljust(self.max_width))
        sys.stdout.flush()
        self.cnt += 1

    def finished(self):
        sys.stdout.write('\rDone: 100%'.ljust(self.max_width) + '\n')
        sys.stdout.flush()


