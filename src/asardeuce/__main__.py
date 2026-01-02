import argparse
import sys
import textwrap

from enum import StrEnum
from importlib.metadata import version
from pathlib import Path

from .filesystem import File, Filesystem, Folder, Symlink


class ListFormat(StrEnum):
    SHORT = "short"
    VERBOSE = "verbose"
    JSON = "json"
    PRETTY_JSON = "pretty-json"


class ArgumentParser(argparse.ArgumentParser):
    def format_usage(self):
        formatter = self._get_formatter()
        formatter.add_usage(self.usage, self._actions,
                            self._mutually_exclusive_groups, prefix="Usage: ")
        return formatter.format_help()

    def format_help(self):
        formatter = self._get_formatter()

        # usage
        formatter.add_usage(self.usage, self._actions,
                            self._mutually_exclusive_groups, prefix="Usage: ")

        # description
        formatter.add_text(self.description)

        # positionals, optionals and user-defined groups
        for action_group in self._action_groups:
            formatter.start_section(action_group.title)
            formatter.add_text(action_group.description)
            formatter.add_arguments(action_group._group_actions)
            formatter.end_section()

        # epilog
        formatter.add_text(self.epilog)

        # determine help from format above
        return formatter.format_help()


def open_archive(archive_path, mode: str = 'rb'):
    if isinstance(archive_path, str):
        archive_path = Path(archive_path)
    if isinstance(archive_path, Path):
        return archive_path.open(mode)
    return archive_path


def list_files(archive_path, fmt):
    fp = open_archive(archive_path)
    fs = Filesystem(fp)

    if fmt == ListFormat.SHORT:
        for entry in fs:
            print(str(entry.fullpath))
        return

    if fmt == ListFormat.VERBOSE:
        print("Name".ljust(90), "Executable", "Size".rjust(20), "SHA-256")
        print("----".ljust(90), "----------", "----".rjust(20), "-------")
        for entry in fs:
            print(
                str(entry.fullpath).ljust(90),
                str(entry.executable).ljust(10),
                str(entry.size).rjust(20),
                entry.integrity.hash
            )
        return

    if fmt == ListFormat.JSON:
        print("[", end="")
        for i, entry in enumerate(fs):
            if i > 0:
                print(",", end="")
            print(textwrap.indent(entry.model_dump_json(), "  "), end="")
        print("]")
        return

    if fmt == ListFormat.PRETTY_JSON:
        print("[")
        for i, entry in enumerate(fs):
            if i > 0:
                print(",")
            print(entry.model_dump_json(indent=2), end="")
        print("]")
        return

    raise RuntimeError(f"This should never happen ({fmt})")


def extract_file(archive_path, filename, output):
    fp = open_archive(archive_path)
    fs = Filesystem(fp)
    for entry in fs:
        if isinstance(entry, File) and str(entry.fullpath) == filename:
            entry.extract(output)
            output.flush()
            return
    print(f"ERROR: file not found: {filename}", file=sys.stderr)
    sys.exit(1)


def extract_all(archive_path, destination):
    fp = open_archive(archive_path)
    fs = Filesystem(fp)
    if not destination.exists():
        destination.mkdir(parents=True, exist_ok=True)

    for entry in fs:
        fullpath = destination / entry.fullpath
        if isinstance(entry, File):
            with fullpath.open('wb') as fp:
                entry.extract(fp)
                print(f"[F] {entry.fullpath}")
        elif isinstance(entry, Folder):
            fullpath.mkdir(parents=False, exist_ok=True)
            print(f"[D] {entry.fullpath}")
        elif isinstance(entry, Symlink):
            fullpath.symlink_to(entry.link)
            print(f"[L] {entry.fullpath}")
        else:
            raise RuntimeError(f"This should never happen ({entry!r})")


def main() -> None:
    v = version('asardeuce')
    parser = ArgumentParser(prog='asardeuce', add_help=False, usage="%(prog)s [options] [command]")
    subparsers = parser.add_subparsers(dest="command", title="Commands", metavar=" ")

    # pack|p
    # @TODO

    # list|l
    list_cmd = subparsers.add_parser(
        'list', aliases=['l'],
        help='list files of asar archive',
    )
    list_cmd.add_argument(
        '--format', '-f',
        choices=[v.value for v in ListFormat],
        type=ListFormat,
        default=ListFormat.SHORT,
    )
    list_cmd.add_argument("archive", type=argparse.FileType('rb', 0))

    # extract-file|ef
    extract_file_cmd = subparsers.add_parser(
        'extract-file', aliases=['ef'],
        help='extract one file from archive',
    )
    extract_file_cmd.add_argument("--output", "-o", type=argparse.FileType('wb', 0), default=sys.stdout.buffer)
    extract_file_cmd.add_argument("archive", type=argparse.FileType('rb', 0))
    extract_file_cmd.add_argument("filename")

    # extract|e
    extract_cmd = subparsers.add_parser(
        'extract', aliases=['e'],
        help='extract archive',
    )
    extract_cmd.add_argument("--verbose", "-v", action="store_true", default=True)
    extract_cmd.add_argument("--quiet", "-q", dest="verbose", action="store_false")
    extract_cmd.add_argument("archive", type=argparse.FileType('rb', 0))
    extract_cmd.add_argument("dest", type=Path)

    options = parser.add_argument_group("Options", description=" ")
    options.add_argument(
        "--help", "-h", action='help',
        help="output usage information"
    )
    options.add_argument(
        '--version', '-V', action='version',
        version=f'%(prog)s {v}',
        help="output the version number",
    )

    args = parser.parse_args()
    if args.command is None:
        parser.print_help()
        return

    if args.command in ('list', 'l'):
        list_files(args.archive, args.format)
    elif args.command in ('extract-file', 'ef'):
        extract_file(args.archive, args.filename, args.output)
    elif args.command in ('extract', 'e'):
        extract_all(args.archive, args.dest)
    else:
        raise RuntimeError(f"This should never happen ({args.command})")


if __name__ == '__main__':
    main()
