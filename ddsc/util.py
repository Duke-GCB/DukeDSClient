import os
import sys
import argparse


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
        self.folders = [parsed_args.folder]
        self._check_args()
        self.project_desc = self.project_name

    def _check_args(self):
        for path in self.folders:
            if not os.path.exists(path):
                raise ValueError('You tried to upload ' + path + ', but that path does not exist.')


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

    def sending_item(self, item):
        percent_done = int(float(self.cnt)/float(self.total)* 100.0)
        name = ''
        if KindType.is_project(item):
            name = 'project'
        else:
            name = item.path
        sys.stdout.write('\rProgress: {}% - sending {}'.format(percent_done, name))
        sys.stdout.flush()
        self.cnt += 1

    def finished(self):
        sys.stdout.write('\rDone: 100%\n')
        sys.stdout.flush()


