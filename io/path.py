"""Path Utilities

Allows you to interact with either local or HDFS files.
This is similar to pathlib.Path

Requirements: All pyarrow environment variables need to be configured
including HADOOP_HOME, ARROW_LIBHDFS_DIR, CLASSPATH with hdfs binary
"""

from contextlib import contextmanager
from typing import Union, Generator, Optional
import os
import pathlib
from urllib import parse
import shutil

import pyarrow
from pyarrow.filesystem import FileSystem
from io.hdfs import HDFSContext, HDFSFile


class Path:
    """Equivalent of pathlib.Path for local and HDFS FileSystem
    Allows you to work with local / HDFS files in an agnostic manner.
    """
    def __init__(self, *args: Union[str, pathlib.Path, "Path"]):
        self.path = os.path.join(*[str(arg) for arg in args])

    def __str__(self) -> str:
        return self.path

    def __repr__(self) -> str:
        return f"Path({str(self)})"

    def __eq__(self, other) -> bool:
        return str(self) == str(other)

    def __truediv__(self, other) -> "Path":
        """Syntactic sugar for path definition."""
        return Path(self, other)

    @property
    def name(self) -> str:
        """Final path component."""
        return os.path.basename(self.path)

    @property
    def parent(self):
        """Path to the parent of the current path"""
        return Path("/".join(self.path.split("/")[:-1]))

    @property
    def is_hdfs(self) -> bool:
        """Return True if the path points to an HDFS location"""
        scheme = parse.urlparse(str(self)).scheme
        return scheme in {"hdfs", "viewfs"}

    @property
    def is_local(self) -> bool:
        """Return True if the path points to a local file or dir."""
        return not self.is_hdfs

    @property
    def suffix(self):
        """File extension of the file if any."""
        return pathlib.Path(str(self)).suffix

    @property
    def file_system(self):
        if self.is_hdfs:
            with HDFSContext as hdfs:
                yield hdfs

    def exists(self, filesystem: FileSystem = None) -> bool:
        """Return True if the path points to an existing file or dir."""
        if filesystem is not None:
            return filesystem.exists(str(self))
        if self.is_hdfs:
            with HDFSContext() as hdfs:
                return hdfs.exists(str(self))
        return pathlib.Path(str(self)).exists()

    def is_dir(self, filesystem: FileSystem = None) -> bool:
        """Return True if the path points to a regular directory."""
        if filesystem is not None:
            return filesystem.isdir(str(self))
        if self.is_hdfs:
            with HDFSContext() as hdfs:
                return hdfs.isdir(str(self))
        return pathlib.Path(str(self)).is_dir()

    def is_file(self, filesystem: FileSystem = None) -> bool:
        """Return True if the path points to a regular file."""
        if filesystem is not None:
            return filesystem.isfile(str(self))
        if self.is_hdfs:
            with HDFSContext() as hdfs:
                return hdfs.isfile(str(self))
        return pathlib.Path(str(self)).is_file()

    def mkdir(self,
              parents: bool = False,
              exist_ok: bool = False,
              filesystem: FileSystem = None):
        """Create directory"""
        if self.is_dir(filesystem=filesystem):
            if exist_ok:
                return
            else:
                raise OSError(f"Directory {self} already exists.")
        if filesystem is not None:
            filesystem.mkdir(str(self))
        else:
            if self.is_hdfs:
                with HDFSContext() as hdfs:
                    hdfs.mkdir(str(self))
            else:
                pathlib.Path(str(self)).mkdir(parents=parents,
                                              exist_ok=exist_ok)

    def delete_dir(self, filesystem: FileSystem = None):
        """Delete dir from filesystem"""
        if not self.is_dir(filesystem=filesystem):
            raise FileNotFoundError(str(self))
        if filesystem is not None:
            filesystem.rm(str(self), recursive=True)
        else:
            if self.is_hdfs:
                with HDFSContext() as hdfs:
                    hdfs.rm(str(self), recursive=True)
            else:
                shutil.rmtree(str(self))

    def delete(self, filesystem: FileSystem = None):
        """Delete file from filesystem"""
        if not self.is_file(filesystem=filesystem):
            raise FileNotFoundError(str(self))
        if filesystem is not None:
            filesystem.delete(str(self))
        else:
            if self.is_hdfs:
                with HDFSContext() as hdfs:
                    hdfs.delete(str(self))
            else:
                pathlib.Path(str(self)).unlink()

    def copy_file(self, dest, filesystem: FileSystem = None):
        """Copy current file to dest (target directory must exist)."""
        if not self.is_file(filesystem=filesystem):
            raise FileNotFoundError(str(self))
        elif self.is_hdfs:
            dest = os.path.join(str(dest), self.name)
            pyarrow.hdfs.HadoopFileSystem().download(stream=dest,
                                                     path=str(self))
        elif self.is_local:
            dest = os.path.join(str(dest), self.name)
            with open(str(self), 'rb') as file:
                pyarrow.hdfs.HadoopFileSystem().upload(dest,
                                                       file,
                                                       buffer_size=4096)
        else:
            shutil.copy(str(self), str(dest))

    def copy_dir(self,
                 dest,
                 recursive: bool = False,
                 filesystem: FileSystem = None):
        """Copy current files and directories if recursive to dest.
        @TODO: self.glob needs hdfs glob pattern matching"""
        if not self.is_dir(filesystem=filesystem):
            raise FileNotFoundError(str(self))
        Path(dest).mkdir(parents=True, exist_ok=True, filesystem=filesystem)
        for path in self.glob("*"):
            if path.is_file(filesystem):
                path.copy_file(Path(dest) / path.name, filesystem=filesystem)
            elif path.is_dir(filesystem):
                if recursive:
                    path.copy_dir(Path(dest) / path.name,
                                  recursive=recursive,
                                  filesystem=filesystem)
            else:
                raise Exception(f"Unable to copy {path}")

    def glob(self, pattern) -> Generator["Path", None, None]:
        """Retrieve directory content matching pattern
        @TODO: glob pattern matching on HDFS seems tricky
               maybe incorporate tensorflow.io.gfile.glob for is_hdfs"""
        if not self.is_hdfs:
            return (Path(path)
                    for path in pathlib.Path(str(self)).glob(pattern))

    def iterdir(self, filesystem: FileSystem = None
                ) -> Generator["Path", None, None]:
        """Retrieve directory content."""
        if filesystem is not None:
            return (Path(path) for path in list(filesystem.ls(str(self))))
        else:
            if self.is_hdfs:
                with HDFSContext() as hdfs:
                    return (Path(path) for path in list(hdfs.ls(str(self))))
            else:
                return (Path(str(path))
                        for path in pathlib.Path(str(self)).iterdir())

    @contextmanager
    def open(self,
             mode: str = "r",
             encoding: Optional[str] = "utf-8",
             filesystem: FileSystem = None):
        """Open file on both HDFS and Local File Systems."""

        if "b" in mode:
            encoding = None  # mypy: ignore
        if filesystem is not None:
            with HDFSFile(filesystem=filesystem,
                          path=str(self),
                          mode=mode,
                          encoding=encoding) as file:
                yield file
        else:
            if self.is_hdfs:
                with HDFSContext() as hdfs:
                    with HDFSFile(filesystem=hdfs,
                                  path=str(self),
                                  mode=mode,
                                  encoding=encoding) as file:
                        yield file
            else:
                with pathlib.Path(str(self)).open(mode=mode,
                                                  encoding=encoding) as file:
                    yield file