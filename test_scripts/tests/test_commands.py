import unittest
import subprocess
import shutil
import hashlib
import os
from ddsc.ddsclient import NO_PROJECTS_FOUND_MESSAGE


def ddsclient_cmd(str_args):
    return subprocess.check_output(["bash", "-c", "ddsclient {}".format(str_args)]).decode("utf-8")


def diff_items(item1, item2):
    return subprocess.check_output(["bash", "-c", "diff -r {} {}".format(item1, item2)])


def hash_file(filename):
    hash = hashlib.sha256()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(2048), b""):
            hash.update(chunk)
    return hash.hexdigest()


@unittest.skipIf(os.environ.get('INTEGRATION_TESTS') != "Y", "skipping integration tests")
class TestUploadDownloadSingleFile(unittest.TestCase):
    def assertFilesSame(self, filename1, filename2):
        self.assertEqual(hash_file(filename1), hash_file(filename2))

    def assertDirSame(self, dir1, dir2):
        diff_items(dir1, dir2)

    def assertUploadWorks(self, upload_cmd):
        result = ddsclient_cmd(upload_cmd)
        self.assertIn("Upload Report for Project", result)
        self.assertIn("Done: 100%", result)

    def assertDownloadWorks(self, download_cmd):
        result = ddsclient_cmd(download_cmd)
        self.assertIn("Done: 100%", result)

    def listProjects(self):
        result = ddsclient_cmd("list")
        if result.strip() == NO_PROJECTS_FOUND_MESSAGE:
            return []
        return [project_name for project_name in result.split("\n") if project_name]

    def list_project_details(self, project_name):
        result = ddsclient_cmd("list -p {}".format(project_name))
        return result.strip()

    def deleteProject(self, project_name):
        result = ddsclient_cmd("delete -p {} --force".format(project_name))
        self.assertEqual("", result)

    def test_diff_items(self):
        # compare against self shouldn't throw
        diff_items("docs", "docs")
        # compare against diff should throw
        with self.assertRaises(subprocess.CalledProcessError) as context:
            diff_items("docs", "ddsc")

    def test_hash_file(self):
        self.assertEqual(hash_file("requirements.txt"), hash_file("requirements.txt"))
        self.assertNotEqual(hash_file("requirements.txt"), hash_file("setup.py"))

    def test_requirements_file(self):
        """
        Upload a single file into a project named 'req'.
        Download into the default directory name of 'req'.
        Make sure the content is the same.
        Download into an explicit directory name of '/tmp/myreq'.
        """
        self.assertUploadWorks("upload -p req requirements.txt")

        self.assertDownloadWorks("download -p req")
        self.assertFilesSame('requirements.txt', './req/requirements.txt')

        self.assertDownloadWorks("download -p req /tmp/myreq")
        self.assertFilesSame('requirements.txt', '/tmp/myreq/requirements.txt')

        #remove test directories
        shutil.rmtree("/tmp/myreq")
        shutil.rmtree("./req")

    def test_upload_download_docs(self):
        self.assertUploadWorks("upload -p my_docs docs")
        self.assertDownloadWorks("download -p my_docs /tmp/my_docs")
        self.assertDirSame("docs", "/tmp/my_docs/docs")
        self.assertUploadWorks("upload -p my_docs2 docs/")
        self.assertDownloadWorks("download -p my_docs2 ../my_docs")
        self.assertDirSame("docs", "../my_docs/docs")

        # remove test directories
        shutil.rmtree("/tmp/my_docs")
        shutil.rmtree("../my_docs")

    def test_upload_download_bigfile(self):
        """
        Uploads a really big file. This takes quite a while to run.
        """
        self.assertUploadWorks("upload -p bigfile /tmp/DukeDSClientData/bigfile.tar")
        self.assertDownloadWorks("download -p bigfile /tmp/bf")
        self.assertFilesSame('/tmp/DukeDSClientData/bigfile.tar', '/tmp/bf/bigfile.tar')

    def test_update_file(self):
        """
        Test that we can update the contents of a file after uploading it.
        """
        with open("/tmp/abc.txt", "w") as data_file:
            data_file.write("one line")
        self.assertUploadWorks("upload -p change_it /tmp/abc.txt")
        with open("/tmp/abc.txt", "w") as data_file:
            data_file.write("one line")
            data_file.write("two line")
        self.assertUploadWorks("upload -p change_it /tmp/abc.txt")
        self.assertDownloadWorks("download -p change_it /tmp/change_it")
        self.assertFilesSame('/tmp/abc.txt', '/tmp/change_it/abc.txt')

    def test_list_and_delete(self):
        """
        Test multiple rounds of list and delete.
        """
        self.assertUploadWorks("upload -p someProj1 requirements.txt")
        self.assertUploadWorks("upload -p someProj2 requirements.txt")
        project_names = self.listProjects()
        self.assertIn("someProj1", project_names)
        self.assertIn("someProj2", project_names)
        for project_name in project_names:
            result = ddsclient_cmd("delete -p {} --force".format(project_name))
            self.assertEqual("", result)
        self.assertEqual([], self.listProjects())
        self.assertUploadWorks("upload -p someProj1 requirements.txt")
        self.assertEqual(["someProj1"], self.listProjects())
        self.assertEqual("Project someProj1 Contents:\nrequirements.txt", self.list_project_details("someProj1"))

if __name__ == '__main__':
    unittest.main()
