import unittest
import subprocess
import shutil
import hashlib
import os


def ddsclient_cmd(str_args):
    return str(subprocess.check_output(["bash", "-c", "ddsclient {}".format(str_args)]))


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
        self.assertUploadWorks("upload -p bigfile /tmp/DukeDSClientData/bigfile.tar")
        self.assertDownloadWorks("download -p bigfile /tmp/bf")
        self.assertFilesSame('/tmp/DukeDSClientData/bigfile.tar', '/tmp/bf/bigfile.tar')

if __name__ == '__main__':
    unittest.main()
