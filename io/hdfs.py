"""HDFS Utilities"""

from typing import Optional

import pyarrow
from pyarrow.fs import FileSystem


class HDFSContext:
    """Context aware HDFS FileSystem using pyarrow.hdfs
    """

    def __init__(self):
        self._hdfs = None

    def __enter__(self):
        self._hdfs = pyarrow.hdfs.connect()
        return self._hdfs

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._hdfs.close()

    def __getattr__(self, name):
        # Expose self._hdfs methods and attributes (mimic inheritance)
        return getattr(self._hdfs, name)


class HDFSFile:
    """Read or write file from any filesystem."""

    def __init__(self,
                 filesystem: FileSystem,
                 path: str,
                 mode: str = "rb",
                 encoding: Optional[str] = "utf-8"):
        """ HDFSFile construct

        Args:
            filesystem : FileSystem instance
            path : Path to file
            mode : read or write mode.
                    Supported: "r", "rb" (default), "w", "wb".
        """
        self.filesystem = filesystem
        self.path = path
        self.mode = mode
        self.encoding = None if "b" in mode else encoding
        self._file = filesystem.open(self.path,
                                     mode={
                                         "r": "rb",
                                         "w": "wb"
                                     }.get(mode, mode))

    def __iter__(self):
        yield from self.readlines()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        # pylint: disable=redefined-builtin
        return self._file.__exit__(type, exc_value, exc_traceback)

    def __getattr__(self, name):
        # Expose self._file methods and attributes (mimic inheritance)
        return getattr(self._file, name)

    def write(self, data, *args, **kwargs):
        if self.mode == "w":
            self._file.write(data.encode(encoding=self.encoding), *args,
                             **kwargs)
        elif self.mode == "wb":
            self._file.write(data, *args, **kwargs)
        else:
            raise ValueError(f"Mode {self.mode} unkown (must be 'w' or 'wb').")

    def read(self, *args, **kwargs):
        if self.mode == "r":
            return self._file.read(*args,
                                   **kwargs).decode(encoding=self.encoding)
        if self.mode == "rb":
            return self._file.read(*args, **kwargs)
        raise ValueError(f"Mode {self.mode} unkown (must be 'r' or 'rb')")