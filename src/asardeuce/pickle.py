"""
This module is a direct port of https://github.com/electron/asar/blob/77c0a70/src/pickle.ts
from TypeScript to Python.

The TypeScript version itself was already a port from the JavaScript code found
at https://github.com/electron/node-chromium-pickle-js/blame/master/lib/pickle.js.

The original TypeScript version (and supposedly, the JavaScript version too?)
was released under the MIT license.

Here's a summary of changes compared to the TypeScript version:
* Method names have been converted from camelCase to snake_case
  and Pickle.toBuffer() has been replaced with Pickle.__bytes__(),
  to better fit with Python's conventions/semantics.
* The SIZE_* constants have been replaced with FMT_* constants.
  Theses constants are instances of "struct.Struct".
  The actual size can be retrieved using their "size" attribute.
"""

import struct

from typing_extensions import Final, Optional, Self, TypeVar, Union


Pickle = TypeVar("Pickle", bound="Pickle")

FMT_INT32: Final[str]   = struct.Struct("<i")
FMT_UINT32: Final[str]  = struct.Struct("<L")
FMT_INT64: Final[str]   = struct.Struct("<q")
FMT_UINT64: Final[str]  = struct.Struct("<Q")
FMT_FLOAT: Final[str]   = struct.Struct("<f")
FMT_DOUBLE: Final[str]  = struct.Struct("<d")

PAYLOAD_UNIT: int = 64

CAPACITY_READ_ONLY: int = 9007199254740992


def align_int(i: int, alignment: int) -> int:
    return i + ((alignment - (i % alignment)) % alignment)


class PickleIterator:
    payload: bytearray
    payloadOffset: int
    readIndex: int
    endIndex: int

    def __init__(self: Self, pickle: Pickle) -> None:
        self.payload = pickle.get_header()
        self.payloadOffset = pickle.get_header_size()
        self.readIndex = 0
        self.endIndex = pickle.get_payload_size()

    def read_bool(self: Self) -> bool:
        return (self.read_int() != 0)

    def read_int(self: Self) -> int:
        return FMT_INT32.unpack(self.read_bytes(FMT_INT32))[0]

    # Create an alias for consistency with other read_XintY() methods
    read_int32 = read_int

    def read_uint32(self: Self) -> int:
        return FMT_UINT32.unpack(self.read_bytes(FMT_UINT32))[0]

    def read_int64(self: Self) -> int:
        return FMT_INT64.unpack(self.read_bytes(FMT_INT64))[0]

    def read_uint64(self: Self) -> int:
        return FMT_UINT64.unpack(self.read_bytes(FMT_UINT64))[0]

    def read_float(self: Self) -> float:
        return FMT_FLOAT.unpack(self.read_bytes(FMT_FLOAT))[0]

    def read_double(self: Self) -> float:
        return FMT_DOUBLE.unpack(self.read_bytes(FMT_DOUBLE))[0]

    def read_string(self: Self) -> str:
        size: int = self.read_int()
        assert size >= 0
        return self.read_bytes(size).decode('utf-8')

    def read_bytes(self: Self, length: Union[int, struct.Struct]) -> bytes:
        if isinstance(length, struct.Struct):
            length = length.size
        readPayloadOffset: int = self.get_read_payload_offset_and_advance(length)
        return bytes(self.payload[readPayloadOffset : readPayloadOffset + length])

    def get_read_payload_offset_and_advance(self: Self, length: int) -> int:
        if length > self.endIndex - self.readIndex:
            self.readIndex = self.endIndex
            raise RuntimeError(f"Failed to read data with length of {length}")
        readPayloadOffset: int = self.payloadOffset + self.readIndex
        self.advance(length)
        return readPayloadOffset

    def advance(self: Self, size: int) -> None:
        alignedSize: int = align_int(size, FMT_UINT32.size)
        if self.endIndex < self.readIndex + alignedSize:
            self.readIndex = self.endIndex
        else:
            self.readIndex += alignedSize


class Pickle:
    header: bytearray
    headerSize: int
    capacityAfterHeader: int
    writeOffset: int

    def __init__(self: Self, buf: Optional[Union[bytes, bytearray]] = None) -> None:
        if buf is not None:
            bufferLen: int = len(buf)
            self.header = bytearray(buf)
            self.headerSize = bufferLen - self.get_payload_size()
            self.capacityAfterHeader = CAPACITY_READ_ONLY
            self.writeOffset = 0

            if self.headerSize > bufferLen:
                self.headerSize = 0
            if self.headerSize != align_int(self.headerSize, FMT_UINT32.size):
                self.headerSize = 0
            if self.headerSize == 0:
                self.header = bytearray()
        else:
            self.header = bytearray()
            self.headerSize = FMT_UINT32.size
            self.capacityAfterHeader = 0
            self.writeOffset = 0
            self.resize(PAYLOAD_UNIT)
            self.set_payload_size(0)

    @staticmethod
    def create_empty() -> Pickle:
        return Pickle()

    @staticmethod
    def create_from_buffer(buf: Union[bytes, bytearray]) -> Pickle:
        return Pickle(buf)

    def get_header(self: Self) -> bytearray:
        return self.header

    def get_header_size(self: Self) -> int:
        return self.headerSize

    def create_iterator(self: Self) -> PickleIterator:
        return PickleIterator(self)

    def __bytes__(self: Self):
        return bytes(self.header[:self.headerSize + self.get_payload_size()])

    def write_bool(self: Self, value: bool) -> bool:
        return self.write_int(int(value))

    def write_int(self: Self, value: int) -> bool:
        return self.write_bytes(FMT_INT32.pack(value))

    # Create an alias for consistency with other write_XintY() methods
    write_int32 = write_int

    def write_uint32(self: Self, value: int) -> bool:
        return self.write_bytes(FMT_UINT32.pack(value))

    def write_int64(self: Self, value: int) -> bool:
        return self.write_bytes(FMT_INT64.pack(value))

    def write_uint64(self: Self, value: int) -> bool:
        return self.write_bytes(FMT_UINT64.pack(value))

    def write_float(self: Self, value: float) -> bool:
        return self.write_bytes(FMT_FLOAT.pack(value))

    def write_double(self: Self, value: float) -> bool:
        return self.write_bytes(FMT_DOUBLE.pack(value))

    def write_string(self: Self, value: str) -> bool:
        value = value.encode('utf-8')
        length: int = len(value)
        if not self.write_int(length):
            return False
        return self.write_bytes(value)

    def set_payload_size(self: Self, payload_size: int) -> bool:
        self.header[:FMT_UINT32.size] = FMT_UINT32.pack(payload_size)
        return FMT_UINT32.size

    def get_payload_size(self: Self) -> int:
        return FMT_UINT32.unpack_from(self.header)[0]

    def write_bytes(self: Self, data: bytes) -> bool:
        length = len(data)
        dataLength: int = align_int(length, FMT_UINT32.size)
        newSize: int = self.writeOffset + dataLength
        if newSize > self.capacityAfterHeader:
            self.resize(max(self.capacityAfterHeader * 2, newSize))

        startOffset: int = self.headerSize + self.writeOffset
        self.header[startOffset : startOffset + length] = data
        endOffset: int = startOffset + length
        self.header = self.header.ljust(endOffset + dataLength - length)
        self.set_payload_size(newSize)
        self.writeOffset = newSize
        return True

    def resize(self: Self, new_capacity: int) -> None:
        new_capacity = align_int(new_capacity, PAYLOAD_UNIT)
        self.header += bytearray(new_capacity)
        self.capacityAfterHeader = new_capacity


__all__ = ('Pickle',)
