import os
import sys
import textwrap

from enum import StrEnum
from pathlib import Path

from typing_extensions import BinaryIO, TextIO, Optional, Union

from .filesystem import File, Filesystem, Folder, Symlink


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
            print("dir".ljust(93), fullpath, file=stream)
        elif isinstance(entry, Symlink):
            print("link".ljust(93), fullpath, "->", entry.link, file=stream)
        elif isinstance(entry, File):
            print(
                "file",
                entry.integrity.hash,
                str(entry.executable).ljust(10),
                str(entry.size).rjust(12),
                str(entry.fullpath),
                file=stream,
            )
        else:
            raise RuntimeError(f"This should never happen ({entry})")


def list_files_json(fs: Filesystem, stream: TextIO) -> None:
    print("[", end="", file=stream)
    for i, entry in enumerate(fs):
        if i > 0:
            print(",", end="", file=stream)
        print(textwrap.indent(entry.model_dump_json(), "  "), end="", file=stream)
    print("]", file=stream)


def list_files_pretty_json(fs: Filesystem, stream: TextIO) -> None:
    print("[", file=stream)
    for i, entry in enumerate(fs):
        if i > 0:
            print(",", file=stream)
        print(entry.model_dump_json(indent=2), end="", file=stream)
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
    raise FileNotFoundError(str(filename))


def extract_all(
    archive_path: Union[BinaryIO, Path, str],
    destination: Union[Path, str],
    stream: Optional[TextIO] = None,
) -> None:
    fp = open_archive(archive_path)
    fs = Filesystem(fp)

    if not isinstance(destination, Path):
        destination = Path(destination)
    if not destination.exists():
        destination.mkdir(parents=True, exist_ok=True)

    for entry in fs:
        fullpath = destination / entry.fullpath
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
