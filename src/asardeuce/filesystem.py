import hashlib
import io
import itertools
import json
import os

from pathlib import Path
from typing import Annotated, Any, Literal, Final

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, SkipValidation, StringConstraints

from .pickle import Pickle


Sha256Hash = Annotated[str, StringConstraints(to_upper=True, pattern=r'^[0-9a-fA-F]{64}$')]

BLOCK_SIZE = 4096
EMPTY_FILE_HASH = "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855"


class Filesystem:
    def __init__(self, fp):
        sizePickle = Pickle(fp.read(8)).create_iterator()
        size = sizePickle.read_uint32()
        headerPickle = Pickle(fp.read(size)).create_iterator()
        header = json.loads(headerPickle.read_string())
        assert isinstance(header, dict) and "files" in header
        self.header = header
        self.position = self.headerSize = 8 + size
        self.fp = fp

    def __iter__(self):
        cwd = Path.cwd()
        pardir = Path(os.pardir)
        queue = [(Path("."), iter(self.header['files'].items()))]

        while True:
            while True:
                try:
                    parent, it = queue[0]
                except IndexError:
                    return

                try:
                    name, info = next(it)
                except StopIteration:
                    queue.pop(0)
                else:
                    break

            fullpath = parent / name
            assert os.sep not in name
            assert fullpath.name != os.curdir
            assert fullpath.name != os.pardir and pardir not in fullpath.parents

            if "link" in info:
                yield Symlink(filesystem=self, fullpath=fullpath, **info)
            elif "files" in info:
                yield Folder(filesystem=self, fullpath=fullpath)

                # Prepend the children to the queue,
                # to iterate in depth-first order.
                queue.insert(0, (fullpath, iter(info['files'].items())))
            else:
                yield File(filesystem=self, fullpath=fullpath, **info)

    def seek(self, position):
        position += self.headerSize
        if position < self.position:
            raise RuntimeError("Cannot rewind stream")

        # Try to use seek() first for performance reason,
        # but fall back to read() if necessary.
        try:
            self.fp.seek(position)
        except io.UnsupportedOperation:
            size = position - self.position
            for _ in self.read(size):
                pass
        else:
            self.position = position

    def read(self, size):
        if not size:
            yield b''

        while size > 0:
            data = self.fp.read(min(BLOCK_SIZE, size))
            if data == '':
                raise IOError()

            data_length = len(data)
            self.position += data_length
            size -= data_length
            yield data


class Integrity(BaseModel):
    model_config = ConfigDict(extra='forbid')
    algorithm: Literal["SHA256"]
    hash: Sha256Hash
    blockSize: Annotated[int, Field(strict=True, gt=0)]
    blocks: list[Sha256Hash] = Field(exclude=True)


class Node(BaseModel):
    model_config = ConfigDict(extra='forbid')
    filesystem: SkipValidation = Field(exclude=True)
    fullpath: Path


class Symlink(Node):
    model_config = ConfigDict(extra='forbid')
    link: str


class Folder(Node):
    model_config = ConfigDict(extra='forbid')


class File(Node):
    size: Annotated[int, Field(strict=True, ge=0)]
    offset: Annotated[int, Field(exclude=True, ge=0)]
    executable: bool = False
    integrity: Integrity

    def extract(self, fp):
        total_size = self.size
        global_hash = hashlib.sha256()
        self.filesystem.seek(self.offset)

        # Quick execution path for empty files
        if total_size == 0:
            if self.integrity.hash != EMPTY_FILE_HASH or \
                len(self.integrity.blocks) != 1 or \
                self.integrity.blocks[0] != EMPTY_FILE_HASH:
                raise RuntimeError(f"Invalid hash for '{self.fullpath}'")
            return

        block = 0
        while total_size > 0:
            block_size = min(self.integrity.blockSize, total_size)
            block_hash = hashlib.sha256()
            for data in self.filesystem.read(block_size):
                fp.write(data)
                block_hash.update(data)
                global_hash.update(data)

            expected = self.integrity.blocks[block]
            actual = block_hash.hexdigest().upper()
            if actual != expected:
                raise RuntimeError(
                    f"Invalid hash for block #{block} in '{self.fullpath}' "
                    f"(expected: {expected}, actual: {actual})"
                )
            total_size -= block_size
            block += 1

        expected = self.integrity.hash
        actual = global_hash.hexdigest().upper()
        if actual != expected:
            raise RuntimeError(f"Invalid hash for '{self.fullpath}' (expected: {expected}, actual: {actual})")


__all__ = ('Filesystem', 'Node', 'Symlink', 'File')
