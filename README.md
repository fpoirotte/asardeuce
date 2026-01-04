# asardeuce

asardeuce (pronunced /æ.zɚ.djuːs/) is a port of Electron's [`asar` project](https://github.com/electron/asar/) to Python.

It tries to keep an API similar to that of `asar`, with some provisions to make it feel more Pythonic.

## Installation

The installation instructions may vary depending on the Python package manager in use.
Refer to your package manager's documentation for more information.

The following command shows how to install asardeuce using [The Python Package Installer (pip)](https://github.com/pypa/pip):

```sh
pip3 install git+https://github.com/fpoirotte/asardeuce.git@main
```

## Usage

### As a standalone program

The following snippet shows how to use asardeuce's command-line interface (CLI):

```sh
$ asardeuce --help

Usage: asardeuce [options] [command]

Commands:
   
    list (l)           list files of asar archive
    extract-file (ef)  extract one file from archive
    extract (e)        extract archive

Options:

  --help, -h           output usage information
  --version, -V        output the version number
```

**Note:** alternatively, asardeuce's CLI may be called as a Python module:

```sh
$ python -m asardeuce --help
```

### As a library

TODO

## License

asardeuce uses the same license as the original [`asar` project](https://github.com/electron/asar/).
Namely, it is provided under the MIT license. See [LICENSE.md] for more information.
