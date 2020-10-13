import datetime
from ddsc.core.remotestore import RemoteStore
from ddsc.core.util import ProgressPrinter, ProjectWalker, plural_fmt
from ddsc.core.projectuploader import UploadSettings, ProjectUploader
from ddsc.core.localstore import LocalProject


class ProjectUpload(object):
    """
    Allows uploading a local project to a remote duke-data-service.
    """
    def __init__(self, config, project_name_or_id, local_project, items_to_send_count, file_upload_post_processor=None):
        """
        Setup for uploading folders dictionary of paths to project_name using config.
        :param config: Config configuration for performing the upload(url, keys, etc)
        :param project_name_or_id: ProjectNameOrId: name or id of the project we will upload files to
        :param local_project: LocalProject: contains files and folders to upload
        :param items_to_send_count: ItemsToSendCounter
        :param file_upload_post_processor: object: has run(data_service, file_response) method to run after uploading
        """
        self.config = config
        self.remote_store = RemoteStore(config)
        self.project_name_or_id = project_name_or_id
        self.local_project = local_project
        self.items_to_send_count = items_to_send_count
        self.file_upload_post_processor = file_upload_post_processor

    @staticmethod
    def create_for_paths(config, remote_store, project_name_or_id, paths, follow_symlinks=False,
                         file_upload_post_processor=None):
        local_project = LocalProject(followsymlinks=follow_symlinks, file_exclude_regex=config.file_exclude_regex)
        local_project.add_paths(paths)
        remote_project = remote_store.fetch_remote_project(project_name_or_id)
        local_project.update_remote_ids(remote_project)
        items_to_send_count = local_project.count_items_to_send(config.upload_bytes_per_chunk)
        return ProjectUpload(config, project_name_or_id, local_project, items_to_send_count,
                             file_upload_post_processor=file_upload_post_processor)

    def run(self):
        """
        Upload different items within local_project to remote store showing a progress bar.
        """
        progress_printer = ProgressPrinter(self.items_to_send_count.total_items(), msg_verb='sending')
        upload_settings = UploadSettings(self.config, self.remote_store.data_service, progress_printer,
                                         self.project_name_or_id, self.file_upload_post_processor)
        project_uploader = ProjectUploader(upload_settings)
        project_uploader.run(self.local_project)
        progress_printer.finished()

    def get_upload_report(self):
        """
        Generate and print a report onto stdout.
        """
        project = self.remote_store.fetch_remote_project(self.project_name_or_id,
                                                         must_exist=True,
                                                         include_children=False)
        report = UploadReport(project.name)
        report.walk_project(self.local_project)
        return report

    def get_url_msg(self):
        """
        Print url to view the project via dds portal.
        """
        msg = 'URL to view project'
        project_id = self.local_project.remote_id
        url = '{}: {}'.format(msg, self.remote_store.data_service.portal_url(project_id))
        return url

    def cleanup(self):
        self.remote_store.close()


class UploadReport(object):
    """
    Creates a text report of items that were sent to the remote store.
    """
    def __init__(self, project_name):
        """
        Create report with the specified project name since the local store doesn't contain that info.
        :param project_name: str project name for the report
        """
        self.report_items = [ReportItem('SENT FILENAME', 'ID', 'SIZE', 'HASH')]
        self.project_name = project_name
        self.sent_data = False
        self.sent_files = 0
        self.up_to_date_files = 0
        self.sent_folders = 0
        self.up_to_date_folders = 0

    def _add_sent_item(self, name, remote_id, size='', file_hash=''):
        self.report_items.append(ReportItem(name, remote_id, size, file_hash))
        self.sent_data = True

    def walk_project(self, project):
        """
        Create report items for each project, folder, and files if necessary calling visit_* methods below.
        :param project: LocalProject project we will count items of.
        """
        # This method will call visit_project, visit_folder, and visit_file below as it walks the project tree.
        ProjectWalker.walk_project(project, self)

    def visit_project(self, item):
        """
        Add project to the report if it was sent.
        :param item: LocalContent project level item
        """
        if item.sent_to_remote:
            self._add_sent_item('Project', item.remote_id)

    def visit_folder(self, item, parent):
        """
        Add folder to the report if it was sent.
        :param item: LocalFolder folder to possibly add
        :param parent: LocalFolder/LocalContent not used here
        """
        if item.sent_to_remote:
            self._add_sent_item(item.path, item.remote_id)
            self.sent_folders += 1
        else:
            self.up_to_date_folders += 1

    def visit_file(self, item, parent):
        """
        Add file to the report if it was sent.
        :param item: LocalFile file to possibly add.
        :param parent: LocalFolder/LocalContent not used here
        """
        if item.sent_to_remote:
            self._add_sent_item(item.path, item.remote_id, item.size, item.remote_file_hash)
            self.sent_files += 1
        else:
            self.up_to_date_files += 1

    def _report_header(self):
        return u"Upload Report for Project: '{}' {}\n".format(self.project_name, datetime.datetime.utcnow())

    def _report_body(self):
        max_name = UploadReport.max_len([item.name for item in self.report_items])
        max_remote_id = UploadReport.max_len([item.remote_id for item in self.report_items])
        max_size = UploadReport.max_len([item.size for item in self.report_items])
        return [item.str_with_sizes(max_name, max_remote_id, max_size) for item in self.report_items]

    @staticmethod
    def max_len(values):
        return max([len(x) for x in values])

    def get_content(self):
        lines = [self._report_header()]
        lines.extend(self._report_body())
        return '\n'.join(lines)

    def summary(self):
        parts = []
        uploaded_str = self.uploaded_items_str()
        if uploaded_str:
            parts.append(uploaded_str)
        up_to_date_str = self.up_to_date_items_str()
        if up_to_date_str:
            parts.append(up_to_date_str)
        return ' '.join(parts)

    def uploaded_items_str(self):
        parts = []
        if self.sent_files:
            parts.append(plural_fmt('file', self.sent_files))
        if self.sent_folders:
            parts.append(plural_fmt('folder', self.sent_folders))
        if parts:
            return "Uploaded {}.".format(" and ".join(parts))
        else:
            return ""

    def up_to_date_items_str(self):
        parts = []
        if self.up_to_date_files:
            parts.append(plural_fmt('file', self.up_to_date_files))
        if self.up_to_date_folders:
            parts.append(plural_fmt('folder', self.up_to_date_folders))
        if parts:
            return "{} are already up to date.".format(" and ".join(parts))
        else:
            return ""


class ReportItem(object):
    """
    Item sent to remote store that is part of the UploadReport.
    """
    def __init__(self, name, remote_id, size='', file_hash=''):
        """
        Setup properties for use in str method
        :param name: str name of the
        :param remote_id: str remote uuid of the item
        :param size: int/str size of the item can be '' if blank
        :return:
        """
        self.name = name
        self.remote_id = remote_id
        self.size = str(size)
        self.file_hash = file_hash

    def str_with_sizes(self, max_name, max_remote_id, max_size):
        """
        Create string for report based on internal properties using sizes to line up columns.
        :param max_name: int width of the name column
        :param max_remote_id: int width of the remote_id column
        :return: str info from this report item
        """
        name_str = self.name.ljust(max_name)
        remote_id_str = self.remote_id.ljust(max_remote_id)
        size_str = self.size.ljust(max_size)
        return u'{}    {}    {}    {}'.format(name_str, remote_id_str, size_str, self.file_hash).rstrip()
