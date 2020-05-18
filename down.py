from ddsc.sdk.client import Client
from ddsc.core.download import FileHashStatus, SWIFT_EXPIRED_STATUS_CODE, S3_EXPIRED_STATUS_CODE
import sys
import requests
import os
import multiprocessing
import time


DOWNLOAD_FILE_CHUNK_SIZE = 20 * 1024 * 1024


class FileDownloader(object):
    def __init__(self, dest_directory, project, num_workers=8):
        self.dest_directory = dest_directory
        self.dds_connection = project.dds_connection
        self.project = project
        self.num_workers = num_workers
        self.page_size = num_workers
        self.async_download_results = []

    def run(self):
        with multiprocessing.Pool(self.num_workers) as pool:
            for project_file in self.project.get_project_files_generator(self.page_size):
                self._download_file(pool, project_file)
                while self._work_queue_is_full():
                    self._wait_for_and_retry_failed_downloads(pool)

    def _download_file(self, pool, project_file):
        output_path = project_file.get_local_path(self.dest_directory)
        print("downloading {}".format(output_path))
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        download_file_dict = self._make_download_file_dict(project_file, output_path)
        async_result = pool.apply_async(download_file, (download_file_dict,))
        self.async_download_results.append(async_result)

    def _work_queue_is_full(self):
        return len(self.async_download_results) >= self.num_workers

    def _wait_for_and_retry_failed_downloads(self, pool):
        download_results = self._pop_ready_download_results()
        if download_results:
            self._retry_failed_downloads(pool, download_results)
        else:
            time.sleep(0)  # Pause to give up CPU since nothing is ready

    def _pop_ready_download_results(self):
        ready_results = []
        for async_result in self._get_ready_async_results():
            # retrieve the value from the async result
            ready_results.append(async_result.get())
            # remove the async result from the list to watch
            self.async_download_results.remove(async_result)
        return ready_results

    def _get_ready_async_results(self):
        ready_results = []
        for async_result in self.async_download_results:
            if async_result.ready():
                ready_results.append(async_result)
        return ready_results


    def _retry_failed_downloads(self, pool, download_results):
        for download_result in download_results:
            if download_result['state'] == 'retry':
                download_file_dict = download_result['download_file_dict']
                # Refresh url in download_file_dict
                file_id = download_file_dict['file_id']
                print("retrying {}".format(download_file_dict['output_path']))
                file_download = self.dds_connection.get_file_download(file_id)
                download_file_dict['url'] = file_download.host + file_download.url
                # Re-run download process
                async_result = pool.apply_async(download_file, (download_file_dict,))
                self.async_download_results.append(async_result)
            elif download_result['state'] == 'error':
                raise ValueError(download_result['msg'])
            else:
                print(download_result['status'].get_status_line())

    @staticmethod
    def _make_download_file_dict(project_file, output_path):
        return {
            'state': 'new',
            'file_id': project_file.id,
            'size': project_file.size,
            'hashes': project_file.hashes,
            'output_path': output_path,
            'url': project_file.file_url['host'] + project_file.file_url['url'],
        }


def download_file(download_file_dict):
    hashes = download_file_dict['hashes']
    output_path = download_file_dict['output_path']
    url = download_file_dict['url']
    response = requests.get(url, stream=True)
    try:
        response.raise_for_status()
        with open(output_path, "wb") as outfile:
            for chunk in response.iter_content(chunk_size=DOWNLOAD_FILE_CHUNK_SIZE):
                if chunk:  # filter out keep-alive new chunks
                    outfile.write(chunk)
        # check size here
        return download_file_result(state='ok', download_file_dict=download_file_dict,
                                    status=FileHashStatus.determine_for_hashes(hashes, output_path))
    except requests.exceptions.HTTPError as error:
        if response.status_code == SWIFT_EXPIRED_STATUS_CODE or response.status_code == S3_EXPIRED_STATUS_CODE:
            return download_file_result(state='retry', download_file_dict=download_file_dict)
        return download_file_result(state='error', download_file_dict=download_file_dict, msg=str(error))


def download_file_result(state, download_file_dict, msg=None, status=None):
    result = dict(download_file_dict)
    result['state'] = state
    if msg:
        result['msg'] = msg
    if status:
        result['status'] = status
    return result


def main():
    project_name = sys.argv[1]
    dest_directory = sys.argv[2]
    num_workers = 4
    client = Client()
    project = client.get_project_by_name(project_name)
    file_downloader = FileDownloader(dest_directory, project, num_workers=num_workers)
    os.makedirs(dest_directory, exist_ok=True)
    file_downloader.run()
    #project.get_files_with_callback(page_size=num_workers,
    #                                callback=download_pool.download_file_in_background)
    #download_pool.wait_for_downloads()


if __name__ == '__main__':
    main()
