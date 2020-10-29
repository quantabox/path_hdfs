"""Test for io.path"""

import os
import tempfile

from unittest import skipUnless

from test import base
import pyarrow
from io.path import Path


class MockFS(base.TestCase):
    """Mock FileSystem"""

    def test_io_path_string(self):
        """Test string and equality"""
        self.assertEqual(Path("foo/bar"), "foo/bar")
        self.assertEqual(Path("foo", "bar"), "foo/bar")
        self.assertEqual(Path("hdfs://root", "foo/bar"), "hdfs://root/foo/bar")

    def test_io_path_parent(self):
        """Test parent method"""
        path_localfs = Path("foo", "bar")
        path_hdfs = Path("hdfs://root", "foo", "bar")
        self.assertEqual(path_localfs.parent, "foo")
        self.assertEqual(path_hdfs.parent, "hdfs://root/foo")

    def test_io_path_local(self):
        """Test path write -> read on local file system"""
        with tempfile.TemporaryDirectory() as tmpdirname:
            path = os.path.join(tmpdirname, "test.txt")
            # write a temporary file to local FS
            with Path(path).open("w") as file:
                file.write("test_local")
            # check for contents on local FS
            with Path(path).open() as file:
                self.assertEqual(file.read(), "test_local")

    @skipUnless(pyarrow.have_libhdfs(), "Test Skipped! No LibHDFS found")
    def test_io_path_hdfs(self):
        """Test path write / read on hdfs"""
        with tempfile.TemporaryDirectory() as tmpdirname:
            path = os.path.join(tmpdirname, "test.txt")
            # write a temporary file to hdfs
            with Path("hdfs://" + path).open("w") as file:
                file.write("test_hdfs")
            # check for contents on hdfs
            with Path("hdfs://" + path).open() as file:
                self.assertEqual(file.read(), "test_hdfs")

    @skipUnless(pyarrow.have_libhdfs(), "Test Skipped! No LibHDFS found")
    def test_io_copy_file_local_to_hdfs(self):
        """Test path to copy file to hdfs"""
        with tempfile.TemporaryDirectory() as tmpdirname:
            path = Path(tmpdirname, "test.txt")
            # create a local file in a temp folder
            with path.open("w") as file:
                file.write("test_file_local_to_hdfs")
                # copy local dir to hdfs
            path.copy_file("hdfs://" + tmpdirname)
            # check for contents on hdfs
            with Path("hdfs://", path).open() as file:
                self.assertEqual(file.read(), "test_file_local_to_hdfs")

    @skipUnless(pyarrow.have_libhdfs(), "Test Skipped! No LibHDFS found")
    def test_io_copy_file_hdfs_to_local(self):
        """Test path to copy file to hdfs"""
        with tempfile.TemporaryDirectory() as tmpdirname:
            local_path = Path(tmpdirname, "test.txt")
            hdfs_path = Path("hdfs://" + str(local_path))
            # create a hdfs temp folder with a file
            with hdfs_path.open("w") as file:
                file.write("test_file_hdfs_to_local")
            # copy hdfs to local
            hdfs_path.copy_file(local_path.parent.path)
            # check for contents on hdfs
            with local_path.open() as file:
                self.assertEqual(file.read(), "test_file_hdfs_to_local")

    @skipUnless(pyarrow.have_libhdfs(), "Test Skipped! No LibHDFS found")
    def test_io_copy_dir_local_to_hdfs(self):
        """Test path to move content to hdfs"""
        with tempfile.TemporaryDirectory() as tmpdirname:
            path = Path(tmpdirname, "test.txt")
            # create a local temp folder with a file
            with path.open("w") as file:
                file.write("test_dir_local_to_hdfs")
                # copy local dir to hdfs
                path.parent.copy_dir("hdfs://" + path.parent.path)
            # check for contents on hdfs
            with Path("hdfs://", path).open() as file:
                self.assertEqual(file.read(), "test_dir_local_to_hdfs")

    # @TODO Akbar to find a solution for hadoop glob patterns
    # def test_io_copy_dir_hdfs_to_local(self):
    #     """Test path to move content from hdfs"""
    #     with tempfile.TemporaryDirectory() as tmpdirname:
    #         local_path = Path(tmpdirname, "test.txt")
    #         hdfs_path = Path("hdfs://" + str(local_path))
    #         # create a hdfs temp folder with a file
    #         with hdfs_path.open("w") as file:
    #             file.write("test_dir_hdfs_to_local")
    #             # copy hdfs to local
    #             hdfs_path.parent.copy_dir(local_path.parent.path)
    #         # check for contents on local
    #         with local_path.open() as file:
    #             self.assertEqual(file.read(), "test_dir_hdfs_to_local")

    def test_io_path_local_delete_file(self):
        """Test path write -> read on local file system"""
        with tempfile.TemporaryDirectory() as tmpdirname:
            path = os.path.join(tmpdirname, "test.txt")
            # write a temporary file to local FS
            with Path(path).open("w") as file:
                file.write("test_local")
            # Check file exisits
            self.assertTrue(Path(path).exists())
            # check for contents on local FS
            Path(path).delete()
            self.assertFalse(Path(path).exists())

    def test_io_path_local_delete_dir(self):
        """Test path write -> read on local file system"""
        with tempfile.TemporaryDirectory() as tmpdirname:
            model_id = "12345"
            path = os.path.join(tmpdirname, model_id)
            Path(path).mkdir()
            # write a temporary file to local FS
            with Path(path + "/test.txt").open("w") as file:
                file.write("test_local")
                # Check file exists
            self.assertTrue(Path(path).parent.exists())
            # check for contents on local FS
            Path(path).delete_dir()
            self.assertFalse(Path(path).exists())