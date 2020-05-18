from ddsc.sdk.client import Client
from ddsc.core.download import FileHashStatus
import sys
import requests
import os
import multiprocessing


from multiprocessing import Pool
DOWNLOAD_FILE_CHUNK_SIZE = 20 * 1024 * 1024


class DownloadPool(object):
    def __init__(self, dest_directory, num_workers=8):
        self.dest_directory = dest_directory
        self.responses = []
        self.pool = multiprocessing.Pool(num_workers)
        self.message_queue = multiprocessing.Manager().Queue()

    def download_file_in_background(self, project_file):
        url = project_file.file_url['host'] + project_file.file_url['url']
        output_path = project_file.get_local_path(self.dest_directory)
        print("Downloading {}".format(output_path))
        payload = (url, output_path, project_file.size, project_file.hashes)
        response = self.pool.apply_async(download_file, payload)
        self.responses.append(response)

    def wait_for_downloads(self):
        self.pool.close()
        for res in self.responses:
            val = res.get()
            print(val)


def download_file(url, output_path, size, file_hashes):
    response = requests.get(url, stream=True)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "wb") as outfile:
        for chunk in response.iter_content(chunk_size=DOWNLOAD_FILE_CHUNK_SIZE):
            if chunk:  # filter out keep-alive new chunks
                outfile.write(chunk)
    return FileHashStatus.determine_for_hashes(file_hashes, output_path).get_status_line()


def main():
    project_name = sys.argv[1]
    dest_directory = sys.argv[2]
    client = Client()
    download_pool = DownloadPool(dest_directory)
    os.makedirs(dest_directory, exist_ok=True)
    project = client.get_project_by_name(project_name)
    project.get_files_with_callback(download_pool.download_file_in_background)
    download_pool.wait_for_downloads()


if __name__ == '__main__':
    main()
