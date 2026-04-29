#!/usr/bin/env python3

import json
import struct
import sys
from pathlib import Path

INT = struct.Struct("=i")
PROCESS = struct.Struct("=iiii1024s")
SMALL = struct.Struct("=diiiiii")
LARGE = struct.Struct("=diiiiiiiiii")

DATETIME_LENGTH = 64
STRING_LENGTH = 1024
LABEL_LENGTH = 24


def _read_exact(f, n):
    data = f.read(n)
    if len(data) != n:
        raise EOFError("Expected {} bytes, got {}".format(n, len(data)))
    return data


def _cstr(raw):
    return raw.split(b"\0", 1)[0].decode("utf-8", errors="replace")


def parse_trace(path):
    path = Path(path)

    with path.open("rb") as f:
        world_size = INT.unpack(_read_exact(f, INT.size))[0]
        datetime = _cstr(_read_exact(f, DATETIME_LENGTH))
        programname = _cstr(_read_exact(f, STRING_LENGTH))

        processes = []
        for _ in range(world_size):
            rank, process_id, core, chip, hostname = PROCESS.unpack(
                _read_exact(f, PROCESS.size)
            )
            processes.append(
                {
                    "rank": rank,
                    "process_id": process_id,
                    "core": core,
                    "chip": chip,
                    "hostname": _cstr(hostname),
                }
            )

        sections = {}
        for _ in range(world_size):
            rank = INT.unpack(_read_exact(f, INT.size))[0]

            small_label = _cstr(_read_exact(f, LABEL_LENGTH))
            nsmall = INT.unpack(_read_exact(f, INT.size))[0]
            small = []
            for _ in range(nsmall):
                time, rec_id, msg_type, sender, receiver, count, nbytes = SMALL.unpack(
                    _read_exact(f, SMALL.size)
                )
                small.append(
                    {
                        "time": time,
                        "id": rec_id,
                        "message_type": msg_type,
                        "sender": sender,
                        "receiver": receiver,
                        "count": count,
                        "bytes": nbytes,
                    }
                )

            large_label = _cstr(_read_exact(f, LABEL_LENGTH))
            nlarge = INT.unpack(_read_exact(f, INT.size))[0]
            large = []
            for _ in range(nlarge):
                (
                    time,
                    rec_id,
                    msg_type,
                    sender1,
                    receiver1,
                    count1,
                    bytes1,
                    sender2,
                    receiver2,
                    count2,
                    bytes2,
                ) = LARGE.unpack(_read_exact(f, LARGE.size))
                large.append(
                    {
                        "time": time,
                        "id": rec_id,
                        "message_type": msg_type,
                        "sender1": sender1,
                        "receiver1": receiver1,
                        "count1": count1,
                        "bytes1": bytes1,
                        "sender2": sender2,
                        "receiver2": receiver2,
                        "count2": count2,
                        "bytes2": bytes2,
                    }
                )

            sections[rank] = {
                "small_label": small_label,
                "small": small,
                "large_label": large_label,
                "large": large,
            }

        leftover = f.read()
        if leftover not in (b"",):
            raise ValueError(
                "Unexpected trailing data in trace: {} bytes".format(len(leftover))
            )

    return {
        "path": str(path),
        "world_size": world_size,
        "datetime": datetime,
        "programname": programname,
        "processes": processes,
        "sections": sections,
    }


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: {} TRACE_FILE".format(sys.argv[0]), file=sys.stderr)
        sys.exit(2)

    trace = parse_trace(sys.argv[1])
    print(json.dumps(trace, indent=2, sort_keys=True))

