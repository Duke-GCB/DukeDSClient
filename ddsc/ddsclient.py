from localstore import LocalContent, LocalOnlyCounter, UploadReport
from remotestore import RemoteContentFetch, RemoteContentSender, RemoteUser
from util import ProgressPrinter
from ddsapi import DataServiceApi


class DDSClient(object):
    def __init__(self, config):
        self.config = config
        self.data_service = DataServiceApi(config.auth, config.url)
        self.remote_fetch = RemoteContentFetch(self.data_service)
        self.local_content = None

    def run(self):
        user = None
        if self.config.add_username:
            user = self.remote_fetch.lookup_user_by_name(self.config.add_username)
        self._load_local_content()
        self._fetch_remote_content()
        self._compare_local_with_remote()
        self._send_different_local_content()
        self._print_report()
        self._print_url()
        if user:
            self._add_user_to_project(user)

    def _load_local_content(self):
        self.local_content = LocalContent()
        self.local_content.add_paths(self.config.folders)

    def _fetch_remote_content(self):
        self.remote_project = self.remote_fetch.fetch_remote_project(self.config.project_name, self.config.folders)

    def _compare_local_with_remote(self):
        self.local_content.update_remote_ids(self.remote_project)

    def _send_different_local_content(self):
        counter = LocalOnlyCounter()
        self.local_content.accept(counter)
        print 'Uploading {}.'.format(counter.result_str())
        progress_printer = ProgressPrinter(counter.total_items())
        sender = RemoteContentSender(self.data_service, self.local_content.remote_id, self.config.project_name, progress_printer)
        self.local_content.accept(sender)
        progress_printer.finished()

    def _print_report(self):
        report = UploadReport(self.config.project_name)
        self.local_content.accept(report)
        print(report)

    def _print_url(self):
        msg = 'URL to view project'
        url_base = self.config.get_url_base()
        project_id = self.local_content.remote_id
        url = '{}: https://{}/portal/#/project/{}'.format(msg, url_base, project_id)
        print(url)

    def _add_user_to_project(self, user):
        self.data_service.set_user_project_permission(self.local_content.remote_id, user.id, 'file_downloader')

#Error 400 on /projects/a4a6ec53-2034-4a6b-b7a9-0ffcf821479d/permissions/77882934-5923-4d4b-b202-e4cf671e5439 Reason: Suggestion:

#project:               a4a6ec53-2034-4a6b-b7a9-0ffcf821479d
#user:                                                                   77882934-5923-4d4b-b202-e4cf671e5439