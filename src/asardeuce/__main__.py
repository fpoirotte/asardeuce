import argparse
import sys

from importlib.metadata import version
from pathlib import Path
from typing_extensions import Self

from .api import create_package, extract_all, extract_file, list_files, ListFormat


class ArgumentParser(argparse.ArgumentParser):
    """
    Custom argument parser that slightly changes the way the usage message & help
    are displayed, to match those of the original "asar" command.

    This is done purely for cosmetic reasons.
    """

    def format_usage(self: Self) -> str:
        formatter = self._get_formatter()
        formatter.add_usage(self.usage, self._actions,
                            self._mutually_exclusive_groups, prefix="Usage: ")
        return formatter.format_help()

    def format_help(self: Self) -> str:
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


def main() -> None:
    """
    Entrypoint for when asardeuce is executed as a standalone program.
    """
    v = version('asardeuce')
    parser = ArgumentParser(prog='asardeuce', add_help=False, usage="%(prog)s [options] [command]")
    subparsers = parser.add_subparsers(dest="command", title="Commands", metavar=" ")

    # pack|p
    pack_cmd = subparsers.add_parser(
        'pack', aliases=['p'],
        help='create asar archive',
    )
    pack_cmd.add_argument("dir", type=Path)
    pack_cmd.add_argument("output")
    pack_cmd.add_argument("--exclude-hidden", action="store_true", help="exclude hidden files")
    pack_cmd.add_argument("--force", "-f", action="store_true", help="overwrite output file")

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

    try:
        if args.command in ('pack', 'p'):
            output = args.output
            if output == "-":
                output = sys.stdout.buffer
            else:
                try:
                    output = open(output, "wb" if args.force else "xb")
                except FileExistsError:
                    parser.exit(1, f"'{output}' already exists. Use --force to overwrite\n")
            create_package(args.dir, output, exclude_hidden=args.exclude_hidden, stream=sys.stdout)
        elif args.command in ('list', 'l'):
            list_files(args.archive, args.format, stream=sys.stdout)
        elif args.command in ('extract-file', 'ef'):
            extract_file(args.archive, args.filename, args.output)
        elif args.command in ('extract', 'e'):
            extract_all(args.archive, args.dest, stream=sys.stdout)
        else:
            raise RuntimeError(f"This should never happen ({args.command})")
    except Exception as e:
        parser.exit(1, str(e))
    parser.exit()


if __name__ == '__main__':
    main()
