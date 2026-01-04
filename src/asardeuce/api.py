import errno
import hashlib
import json
import os
import sys
import textwrap
import tempfile

from enum import StrEnum
from pathlib import Path

from typing_extensions import BinaryIO, Callable, Iterable, Generator, TextIO, Optional, Union

from .filesystem import File, Filesystem, Folder, Integrity, Node, Symlink
from .filesystem import EMPTY_FILE_HASH
from .pickle import Pickle


class ListFormat(StrEnum):
    SHORT: str = "short"
    VERBOSE: str = "verbose"
    JSON: str = "json"
    PRETTY_JSON: str = "pretty-json"


class OpenMode(StrEnum):
    READ: str = "rb"
    WRITE: str = "wb"


def open_archive(
    archive_path: Union[BinaryIO, Path, str],
    mode: OpenMode = OpenMode.READ,
) -> BinaryIO:
    if isinstance(archive_path, str):
        archive_path = Path(archive_path)
    if isinstance(archive_path, Path):
        return archive_path.open(str(mode))
    return archive_path


def handle_file(
    path: Path,
    dir_fd: int,
    block_size: int = 2**22,  # 4 MiB
) -> Iterable[Union[tuple[File, BinaryIO], tuple[Symlink, None]]]:
    filename = path.name
    stats = os.stat(filename, dir_fd=dir_fd, follow_symlinks=False)
    try:
        target = os.readlink(filename, dir_fd=dir_fd)
    except OSError as e:
        if e.errno != errno.EINVAL:
            raise
        target = None

    if target is None:
        fd = os.open(filename, flags=getattr(os, 'O_BINARY', 0) | os.O_RDONLY, dir_fd=dir_fd)
        with os.fdopen(fd, 'rb') as fp:
            yield (
                File(
                    filesystem=None,
                    fullpath=path,
                    size=stats.st_size,
                    offset=0,
                    executable=os.access(filename, os.X_OK, dir_fd=dir_fd, effective_ids=True),
                    integrity=Integrity(
                        algorithm='SHA256',
                        hash=EMPTY_FILE_HASH,
                        blockSize=block_size,
                        blocks=[],
                    )
                ),
                fp
            )
    else:
        yield (Symlink(filesystem=None, fullpath=path, link=target), None)


def handle_folder(
    path: Path,
    dir_fd: int,
) -> Iterable[Union[tuple[Folder, None], tuple[Symlink, None]]]:
    dirname = path.name
    stats = os.stat(dirname, dir_fd=dir_fd, follow_symlinks=False)
    try:
        target = os.readlink(dirname, dir_fd=dir_fd)
    except OSError as e:
        if e.errno != errno.EINVAL:
            raise
        target = None

    if target is None:
        yield (Folder(filesystem=None, fullpath=path), None)
    else:
        yield (Symlink(filesystem=None, fullpath=path, link=target), None)


def walk_dir(
    base_dir: Union[Path, str],
    exclude_hidden: bool,
    block_size: int = 2**22,  # 4 MiB
) -> Generator[BinaryIO, None, None]:
    assert block_size >= 1024

    for dirpath, dirnames, filenames, dirfd in os.fwalk(base_dir):
        dirnames.sort()
        filenames.sort()

        for filename in filenames:
            if exclude_hidden and filename.startswith('.'):
                continue
            try:
                relative = (Path(dirpath) / filename).relative_to(base_dir)
                yield from handle_file(relative, dirfd, block_size)
            except OSError:
                raise  # should we ignore it instead?

        for dirname in dirnames:
            if exclude_hidden and dirname.startswith('.'):
                continue
            try:
                relative = (Path(dirpath) / dirname).relative_to(base_dir)
                yield from handle_folder(relative, dirfd)
            except OSError:
                raise  # should we ignore it instead?


def create_package(
    base_dir: Union[Path, str],
    dest: Union[Path, str, BinaryIO],
    exclude_hidden: bool,
    transform: Optional[Callable] = None,
    stream: Optional[TextIO] = None,
) -> None:
    create_package_from_files(walk_dir(base_dir, exclude_hidden), dest, transform, stream)


def process_file(
    tmpfile: BinaryIO,
    offset: int,
    entry: File,
    fp: BinaryIO,
) -> int:
    if entry.size <= 0:
        entry.integrity.blocks.append(EMPTY_FILE_HASH)
        return offset

    sofar = 0
    buf = b''
    global_hasher = hashlib.sha256()
    block_hasher = hashlib.sha256()
    while sofar < entry.size:
        block_size = min(entry.integrity.blockSize, entry.size % entry.integrity.blockSize)
        read_size = block_size - (sofar % entry.integrity.blockSize)
        data = fp.read(read_size)
        if data == b'':
            raise RuntimeError(f"Premature end-of-file for {entry!r}")

        sofar += len(data)
        buf += data
        tmpfile.write(data)
        global_hasher.update(data)
        block_hasher.update(data)

        if len(buf) == block_size:
            entry.integrity.blocks.append(block_hasher.hexdigest())
            block_hasher = hashlib.sha256()

    # offset may be larger than 2**32-1, which cannot be represented reliably in JSON,
    # so we serialize it as a string instead, per asar's format specification.
    entry.offset = str(offset)
    entry.integrity.hash = global_hasher.hexdigest()
    offset += entry.size
    return offset


def create_index(
    src: Iterable[tuple[Node, Optional[BinaryIO]]],
    tmpfile: BinaryIO,
    stream: Optional[TextIO] = None,
) -> dict:
    index = {"files": {}}
    offset = 0
    for entry, fp in src:
        container = index["files"]

        # Parent folders are returned in reverse order.
        # Also, the top-most parent will always be ".".
        for parent in reversed(entry.fullpath.parents[:-1]):
            container = container[parent.name]["files"]

        if entry.fullpath.name in container:
            raise FileExistsError(entry.fullpath)
        if isinstance(entry, Folder):
            container[entry.fullpath.name] = {"files": {}}
            if stream:
                print(f"[DIR]  {entry.fullpath}/")
        elif isinstance(entry, Symlink):
            container[entry.fullpath.name] = entry.model_dump(exclude=("fullpath", ))
            if stream:
                print(f"[LINK] {entry.fullpath} -> {entry.link}")
        elif isinstance(entry, File):
            offset = process_file(tmpfile, offset, entry, fp)
            container[entry.fullpath.name] = entry.model_dump(exclude=("fullpath", ), serialize_as_any=True)
            if stream:
                print(f"[FILE] {entry.fullpath}")
        else:
            raise RuntimeError(f"This should never happen ({entry!r})")
    return index


def create_package_from_files(
    src: Iterable[tuple[Node, Optional[BinaryIO]]],
    dest: Union[Path, str, BinaryIO],
    transform: Optional[Callable] = None,
    stream: Optional[TextIO] = None,
) -> None:
    with tempfile.TemporaryFile(mode='w+b', prefix='aserdeuce.') as tmp:
        if isinstance(dest, (str, Path)):
            fp = Path(dest).open('wb')
        else:
            fp = dest
        try:
            index = create_index(src, tmp, stream)
            payload_pickle = Pickle()
            payload_pickle.write_string(json.dumps(index, separators=(',', ':')))
            payload = bytes(payload_pickle)
            header_pickle = Pickle()
            header_pickle.write_uint32(len(payload))
            fp.write(bytes(header_pickle))
            fp.write(payload)
            tmp.seek(0)
            while (data := tmp.read(2*12)) != b'':
                fp.write(data)
        finally:
            if isinstance(dest, (str, Path)):
                fp.close()


def list_files_short(fs: Filesystem, stream: TextIO) -> None:
    for entry in fs:
        fullpath = str(entry.fullpath).removesuffix(os.sep)
        if isinstance(entry, Folder):
            fullpath += os.sep
        elif isinstance(entry, Symlink):
            fullpath += f" -> {entry.link}"
        print(fullpath, file=stream)


def list_files_verbose(fs: Filesystem, stream: TextIO) -> None:
    print("Type", "SHA-256".ljust(64), "Executable", "Size".rjust(12), "Name", file=stream)
    print("----", "-------".ljust(64), "----------", "----".rjust(12), "----", file=stream)
    for entry in fs:
        fullpath = str(entry.fullpath).removesuffix(os.sep)
        if isinstance(entry, Folder):
            fullpath += os.sep
            print("DIR".ljust(93), fullpath, file=stream)
        elif isinstance(entry, Symlink):
            print("LINK".ljust(93), fullpath, "->", entry.link, file=stream)
        elif isinstance(entry, File):
            print(
                "FILE",
                entry.integrity.hash,
                str(entry.executable).ljust(10),
                str(entry.size).rjust(12),
                str(entry.fullpath),
                file=stream,
            )
        else:
            raise RuntimeError(f"This should never happen ({entry})")


def list_files_json(fs: Filesystem, stream: TextIO) -> None:
    exclude = {
        "integrity": ("blockSize", "blocks"),
        "offset": True,
    }
    print("[", end="", file=stream)
    for i, entry in enumerate(fs):
        if i > 0:
            print(",", end="", file=stream)
        print(textwrap.indent(entry.model_dump_json(exclude=exclude), "  "), end="", file=stream)
    print("]", file=stream)


def list_files_pretty_json(fs: Filesystem, stream: TextIO) -> None:
    exclude = {
        "integrity": ("blockSize", "blocks"),
        "offset": True,
    }
    print("[", file=stream)
    for i, entry in enumerate(fs):
        if i > 0:
            print(",", file=stream)
        print(entry.model_dump_json(exclude=exclude, indent=2), end="", file=stream)
    print("]", file=stream)


def list_files(
    archive_path: Union[BinaryIO, Path, str],
    fmt: ListFormat,
    stream: Optional[TextIO] = sys.stdout,
) -> None:
    fp = open_archive(archive_path)
    fs = Filesystem(fp)
    if fmt == ListFormat.SHORT:
        list_files_short(fs, stream)
    elif fmt == ListFormat.VERBOSE:
        list_files_verbose(fs, stream)
    elif fmt == ListFormat.JSON:
        list_files_json(fs, stream)
    elif fmt == ListFormat.PRETTY_JSON:
        list_files_pretty_json(fs, stream)
    else:
        raise RuntimeError(f"This should never happen ({fmt})")


def extract_file(
    archive_path: Union[BinaryIO, Path, str],
    filename: Union[Path, str],
    output: BinaryIO,
) -> None:
    fp = open_archive(archive_path)
    fs = Filesystem(fp)
    for entry in fs:
        if isinstance(entry, File) and str(entry.fullpath) == str(filename):
            entry.extract(output)
            output.flush()
            return
    raise FileNotFoundError(f"No such file or directory: '{filename}'")


def extract_all(
    archive_path: Union[BinaryIO, Path, str],
    dest: Union[Path, str],
    stream: Optional[TextIO] = None,
) -> None:
    fp = open_archive(archive_path)
    fs = Filesystem(fp)

    if not isinstance(dest, Path):
        dest = Path(dest)
    if not dest.exists():
        dest.mkdir(parents=True, exist_ok=True)

    for entry in fs:
        fullpath = dest / entry.fullpath
        if isinstance(entry, File):
            with fullpath.open('wb') as fp:
                entry.extract(fp)
                if stream:
                    print(f"[F] {entry.fullpath}", file=stream)
        elif isinstance(entry, Folder):
            fullpath.mkdir(parents=False, exist_ok=True)
            if stream:
                print(f"[D] {entry.fullpath}", file=stream)
        elif isinstance(entry, Symlink):
            fullpath.symlink_to(entry.link)
            if stream:
                print(f"[L] {entry.fullpath}", file=stream)
        else:
            raise RuntimeError(f"This should never happen ({entry!r})")


__all__ = ('list_files', 'extract_file', 'extract_all', 'ListFormat')
