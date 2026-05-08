#!/usr/bin/env python3

import argparse
import gzip
import json
import math
import os
import shlex
import shutil
import struct
import subprocess
import sys
from pathlib import Path

# -----------------------------------------------------------------------------
# MPI message type constants
# -----------------------------------------------------------------------------

MPI_SEND_TYPE = 13
MPI_RECV_TYPE = 14
MPI_BSEND_TYPE = 15
MPI_SSEND_TYPE = 16
MPI_RSEND_TYPE = 17
MPI_ISEND_TYPE = 18
MPI_IBSEND_TYPE = 19
MPI_ISSEND_TYPE = 20
MPI_IRSEND_TYPE = 21
MPI_IRECV_TYPE = 22
MPI_SENDRECV_TYPE = 23
MPI_WAIT_TYPE = 24
MPI_WAITALL_TYPE = 25
MPI_BARRIER_TYPE = 26
MPI_BCAST_TYPE = 27
MPI_REDUCE_TYPE = 28
MPI_ALLREDUCE_TYPE = 29
MPI_GATHER_TYPE = 30
MPI_SCATTER_TYPE = 31
MPI_ALLGATHER_TYPE = 32
MPI_WAITANY_TYPE = 33
MPI_WAITSOME_TYPE = 34
MPI_TEST_TYPE = 35
MPI_TESTANY_TYPE = 36
MPI_TESTALL_TYPE = 37
MPI_TESTSOME_TYPE = 38

# -----------------------------------------------------------------------------
# Current MPIC file format constants
# -----------------------------------------------------------------------------

MPIC_V2_MAGIC = b"MPICv002"
MPIC_V2_VERSION = 2

SMALL_LABEL_TEXT = "P2P Small Type Messages"
LARGE_LABEL_TEXT = "P2P Large Type Messages"

# -----------------------------------------------------------------------------
# Binary layouts matching the C writer
# -----------------------------------------------------------------------------

PROCESS_INFO_FMT = "=iiii1024s"
PROCESS_INFO_SIZE = struct.calcsize(PROCESS_INFO_FMT)

# int rank; padding 4; double mpi_time_zero; int64_t unix_time_zero_ns;
PROCESS_TIME_ANCHOR_FMT = "=i4xdq"
PROCESS_TIME_ANCHOR_SIZE = struct.calcsize(PROCESS_TIME_ANCHOR_FMT)

# small_node_no_link_t:
# double time;
# int id, message_type, comm, tag, sender, receiver, count, bytes;
P2P_SMALL_FMT = "=diiiiiiii"
P2P_SMALL_SIZE = struct.calcsize(P2P_SMALL_FMT)

# large_node_no_link_t:
# double time;
# int id, message_type, comm,
#     sender1, receiver1, count1, bytes1, tag1,
#     sender2, receiver2, count2, bytes2, tag2;
# native C size is 64 bytes => add 4 bytes tail padding
P2P_LARGE_FMT = "=diiiiiiiiiiiii4x"
P2P_LARGE_SIZE = struct.calcsize(P2P_LARGE_FMT)

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def run(cmd, cwd=None, env=None):
    cmd = [str(x) for x in cmd]
    print("+", " ".join(cmd))
    subprocess.run(cmd, cwd=cwd, env=env, check=True)

def describe_records(records):
    return json.dumps(records, indent=2, sort_keys=True)

def require(condition, message):
    if not condition:
        raise AssertionError(message)

def require_one(records, **expected):
    matches = [
        r for r in records
        if all(r.get(k) == v for k, v in expected.items())
    ]
    if len(matches) != 1:
        raise AssertionError(
            "Expected exactly one record matching {}, found {}\nAvailable records:\n{}".format(
                expected, len(matches), describe_records(records)
            )
        )
    return matches[0]

def require_n(records, n, **expected):
    matches = [
        r for r in records
        if all(r.get(k) == v for k, v in expected.items())
    ]
    if len(matches) != n:
        raise AssertionError(
            "Expected {} records matching {}, found {}\nAvailable records:\n{}".format(
                n, expected, len(matches), describe_records(records)
            )
        )
    return matches

def require_none(records, **expected):
    matches = [
        r for r in records
        if all(r.get(k) == v for k, v in expected.items())
    ]
    if len(matches) != 0:
        raise AssertionError(
            "Expected no records matching {}, found {}\nAvailable records:\n{}".format(
                expected, len(matches), describe_records(records)
            )
        )

def require_same_value(a, a_key, b, b_key=None, context="value mismatch"):
    if b_key is None:
        b_key = a_key
    av = a.get(a_key)
    bv = b.get(b_key)
    if av != bv:
        raise AssertionError("{}: {}={} != {}={}".format(context, a_key, av, b_key, bv))

def require_all_same(records, key, context=None):
    if not records:
        return
    vals = [r.get(key) for r in records]
    if any(v != vals[0] for v in vals[1:]):
        raise AssertionError("{}: values for {} were {}".format(
            context or "all-same check failed", key, vals
        ))

def require_zero(record, key, context=None):
    val = record.get(key)
    if val != 0:
        raise AssertionError("{}: expected {}=0, got {}".format(
            context or "zero check failed", key, val
        ))

def require_valid_small_record(record):
    for key in ("time", "id", "message_type", "comm", "tag", "sender", "receiver", "count", "bytes"):
        require(key in record, "small record missing field {}".format(key))
    require(isinstance(record["comm"], int), "small record comm should be int")
    require(isinstance(record["tag"], int), "small record tag should be int")

def require_valid_large_record(record):
    for key in ("time", "id", "message_type", "comm",
                "sender1", "receiver1", "count1", "bytes1", "tag1",
                "sender2", "receiver2", "count2", "bytes2", "tag2"):
        require(key in record, "large record missing field {}".format(key))
    require(isinstance(record["comm"], int), "large record comm should be int")
    require(isinstance(record["tag1"], int), "large record tag1 should be int")
    require(isinstance(record["tag2"], int), "large record tag2 should be int")

def require_small_match(a, b, compare_comm=False, compare_tag=True, context="small match"):
    if compare_comm:
        require_same_value(a, "comm", b, "comm", context + " comm")
    if compare_tag:
        require_same_value(a, "tag", b, "tag", context + " tag")

def require_large_same_comm(a, b, context="large comm"):
    require_same_value(a, "comm", b, "comm", context)

def require_large_zero_tags(record, context="large zero tags"):
    require_zero(record, "tag1", context)
    require_zero(record, "tag2", context)

def _open_maybe_gzip(filepath):
    with open(filepath, "rb") as probe:
        magic = probe.read(2)
    if str(filepath).endswith(".gz") or magic == b"\x1f\x8b":
        return gzip.open(filepath, "rb")
    return open(filepath, "rb")

def _cstr(raw_bytes):
    return raw_bytes.split(b"\x00", 1)[0].decode("utf-8", errors="ignore")

def _read_exact(buf, offset, size, context):
    end = offset + size
    if end > len(buf):
        raise ValueError(
            "Unexpected EOF while reading {}: need {} bytes at offset {}, only {} remain".format(
                context, size, offset, len(buf) - offset
            )
        )
    return buf[offset:end], end

# -----------------------------------------------------------------------------
# Trace parsing
# -----------------------------------------------------------------------------

def _parse_header_v2(raw):
    offset = 0

    chunk, offset = _read_exact(raw, offset, 8, "magic")
    if chunk != MPIC_V2_MAGIC:
        raise ValueError("not MPICv002")

    chunk, offset = _read_exact(raw, offset, 4, "format version")
    version = struct.unpack("=I", chunk)[0]
    if version != MPIC_V2_VERSION:
        raise ValueError("unsupported MPIC version {}".format(version))

    chunk, offset = _read_exact(raw, offset, 4, "world size")
    world_size = struct.unpack("=i", chunk)[0]
    if world_size < 0:
        raise ValueError("negative world size")

    chunk, offset = _read_exact(raw, offset, 64, "date")
    run_date = _cstr(chunk)

    chunk, offset = _read_exact(raw, offset, 1024, "program")
    program = _cstr(chunk)

    topology_by_rank = [None] * world_size
    anchors_by_rank = [None] * world_size

    for idx in range(world_size):
        chunk, offset = _read_exact(raw, offset, PROCESS_INFO_SIZE, "process_info[{}]".format(idx))
        rank, pid, core, chip, hostname_b = struct.unpack(PROCESS_INFO_FMT, chunk)
        hostname = _cstr(hostname_b)

        require(0 <= rank < world_size, "invalid process_info rank {}".format(rank))
        require(topology_by_rank[rank] is None, "duplicate process_info rank {}".format(rank))

        topology_by_rank[rank] = {
            "rank": rank,
            "pid": pid,
            "core": core,
            "chip": chip,
            "hostname": hostname,
        }

    for idx in range(world_size):
        chunk, offset = _read_exact(raw, offset, PROCESS_TIME_ANCHOR_SIZE, "process_time_anchor[{}]".format(idx))
        rank, mpi_time_zero, unix_time_zero_ns = struct.unpack(PROCESS_TIME_ANCHOR_FMT, chunk)

        require(0 <= rank < world_size, "invalid anchor rank {}".format(rank))
        require(anchors_by_rank[rank] is None, "duplicate anchor rank {}".format(rank))

        anchors_by_rank[rank] = {
            "rank": rank,
            "mpi_time_zero": mpi_time_zero,
            "unix_time_zero_ns": unix_time_zero_ns,
        }

    return {
        "magic": MPIC_V2_MAGIC.decode("ascii"),
        "format_version": version,
        "world_size": world_size,
        "date": run_date,
        "program": program,
        "topology": topology_by_rank,
        "anchors": anchors_by_rank,
        "sections_offset": offset,
    }

def _parse_header_v1(raw):
    offset = 0

    header_fmt = "=i64s1024s"
    header_size = struct.calcsize(header_fmt)
    chunk, offset = _read_exact(raw, offset, header_size, "legacy header")
    world_size, raw_date, raw_program = struct.unpack(header_fmt, chunk)

    if world_size < 0:
        raise ValueError("negative world size")

    run_date = _cstr(raw_date)
    program = _cstr(raw_program)

    topology_by_rank = [None] * world_size
    anchors_by_rank = [None] * world_size

    for idx in range(world_size):
        chunk, offset = _read_exact(raw, offset, PROCESS_INFO_SIZE, "legacy process_info[{}]".format(idx))
        rank, pid, core, chip, hostname_b = struct.unpack(PROCESS_INFO_FMT, chunk)
        hostname = _cstr(hostname_b)

        require(0 <= rank < world_size, "invalid process_info rank {}".format(rank))
        require(topology_by_rank[rank] is None, "duplicate process_info rank {}".format(rank))

        topology_by_rank[rank] = {
            "rank": rank,
            "pid": pid,
            "core": core,
            "chip": chip,
            "hostname": hostname,
        }

    return {
        "magic": None,
        "format_version": 1,
        "world_size": world_size,
        "date": run_date,
        "program": program,
        "topology": topology_by_rank,
        "anchors": anchors_by_rank,
        "sections_offset": offset,
    }

def _parse_sections(raw_sections, world_size, anchors):
    offset = 0
    sections = [None] * world_size

    for sec_idx in range(world_size):
        chunk, offset = _read_exact(raw_sections, offset, 4, "section rank")
        rank = struct.unpack("=i", chunk)[0]

        require(0 <= rank < world_size, "invalid section rank {}".format(rank))
        require(sections[rank] is None, "duplicate section rank {}".format(rank))

        chunk, offset = _read_exact(raw_sections, offset, 24, "small label")
        small_label = _cstr(chunk)
        require(small_label == SMALL_LABEL_TEXT,
                "bad small label for rank {}: {!r}".format(rank, small_label))

        chunk, offset = _read_exact(raw_sections, offset, 4, "small count")
        num_small = struct.unpack("=i", chunk)[0]
        require(num_small >= 0, "negative small count for rank {}".format(rank))

        small_records = []
        for i in range(num_small):
            chunk, offset = _read_exact(raw_sections, offset, P2P_SMALL_SIZE, "small record {}".format(i))
            time_val, msg_id, message_type, comm, tag, sender, receiver, count, bytes_vol = struct.unpack(
                P2P_SMALL_FMT, chunk
            )
            rec = {
                "time": time_val,
                "id": msg_id,
                "message_type": message_type,
                "comm": comm,
                "tag": tag,
                "sender": sender,
                "receiver": receiver,
                "count": count,
                "bytes": bytes_vol,
            }
            require_valid_small_record(rec)
            small_records.append(rec)

        chunk, offset = _read_exact(raw_sections, offset, 24, "large label")
        large_label = _cstr(chunk)
        require(large_label == LARGE_LABEL_TEXT,
                "bad large label for rank {}: {!r}".format(rank, large_label))

        chunk, offset = _read_exact(raw_sections, offset, 4, "large count")
        num_large = struct.unpack("=i", chunk)[0]
        require(num_large >= 0, "negative large count for rank {}".format(rank))

        large_records = []
        for i in range(num_large):
            chunk, offset = _read_exact(raw_sections, offset, P2P_LARGE_SIZE, "large record {}".format(i))
            unpacked = struct.unpack(P2P_LARGE_FMT, chunk)
            (
                time_val, msg_id, message_type, comm,
                sender1, receiver1, count1, bytes1, tag1,
                sender2, receiver2, count2, bytes2, tag2
            ) = unpacked

            rec = {
                "time": time_val,
                "id": msg_id,
                "message_type": message_type,
                "comm": comm,
                "sender1": sender1,
                "receiver1": receiver1,
                "count1": count1,
                "bytes1": bytes1,
                "tag1": tag1,
                "sender2": sender2,
                "receiver2": receiver2,
                "count2": count2,
                "bytes2": bytes2,
                "tag2": tag2,
            }
            require_valid_large_record(rec)
            large_records.append(rec)

        sections[rank] = {
            "rank": rank,
            "anchor": anchors[rank] if anchors and 0 <= rank < len(anchors) else None,
            "small": small_records,
            "large": large_records,
        }

    for rank in range(world_size):
        require(sections[rank] is not None, "missing section for rank {}".format(rank))

    trailing = raw_sections[offset:]
    if any(b != 0 for b in trailing):
        raise ValueError("unexpected non-zero trailing bytes after final section")

    return sections

def parse_trace(path):
    with _open_maybe_gzip(path) as f:
        raw = f.read()

    if len(raw) >= 8 and raw[:8] == MPIC_V2_MAGIC:
        header = _parse_header_v2(raw)
    else:
        header = _parse_header_v1(raw)

    sections = _parse_sections(raw[header["sections_offset"]:], header["world_size"], header["anchors"])

    return {
        "magic": header["magic"],
        "format_version": header["format_version"],
        "world_size": header["world_size"],
        "date": header["date"],
        "program": header["program"],
        "topology": header["topology"],
        "anchors": header["anchors"],
        "sections": sections,
    }

# -----------------------------------------------------------------------------
# Common validation
# -----------------------------------------------------------------------------

def validate_common_trace(trace):
    require(trace["world_size"] > 0, "trace world size should be positive")
    require(len(trace["sections"]) == trace["world_size"],
            "sections length should equal world size")
    require(len(trace["topology"]) == trace["world_size"],
            "topology length should equal world size")
    require(len(trace["anchors"]) == trace["world_size"],
            "anchors length should equal world size")

    for rank in range(trace["world_size"]):
        section = trace["sections"][rank]
        topo = trace["topology"][rank]

        require(section is not None, "missing section for rank {}".format(rank))
        require(section["rank"] == rank, "section rank mismatch at {}".format(rank))
        require(topo is not None, "missing topology for rank {}".format(rank))
        require(topo["rank"] == rank, "topology rank mismatch at {}".format(rank))

        for rec in section["small"]:
            require_valid_small_record(rec)
        for rec in section["large"]:
            require_valid_large_record(rec)

    if trace["format_version"] >= 2:
        require(trace["magic"] == "MPICv002", "v2 trace should have MPICv002 magic")
        for rank in range(trace["world_size"]):
            anchor = trace["anchors"][rank]
            require(anchor is not None, "missing time anchor for rank {}".format(rank))
            require(anchor["rank"] == rank, "anchor rank mismatch at {}".format(rank))
            require(math.isfinite(anchor["mpi_time_zero"]),
                    "anchor mpi_time_zero should be finite for rank {}".format(rank))
            require(anchor["unix_time_zero_ns"] > 0,
                    "anchor unix_time_zero_ns should be positive for rank {}".format(rank))

# -----------------------------------------------------------------------------
# Validators
# -----------------------------------------------------------------------------

def validate_send_recv(trace):
    require(trace["world_size"] == 2, "send_recv: world size should be 2")

    rank0 = trace["sections"][0]
    rank1 = trace["sections"][1]

    send = require_one(
        rank0["small"],
        message_type=MPI_SEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recv = require_one(
        rank1["small"],
        message_type=MPI_RECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    require_small_match(send, recv, compare_comm=True, compare_tag=True,
                        context="send_recv send/recv")

def validate_any_source(trace):
    require(trace["world_size"] == 3, "any_source: world size should be 3")

    rank0 = trace["sections"][0]
    rank1 = trace["sections"][1]
    rank2 = trace["sections"][2]

    recv = require_one(
        rank0["small"],
        message_type=MPI_RECV_TYPE,
        sender=1,
        receiver=0,
        count=1,
        bytes=4,
    )

    send1 = require_one(
        rank1["small"],
        message_type=MPI_SEND_TYPE,
        sender=1,
        receiver=0,
        count=1,
        bytes=4,
    )

    send2 = require_one(
        rank2["small"],
        message_type=MPI_SEND_TYPE,
        sender=2,
        receiver=0,
        count=1,
        bytes=4,
    )

    require_small_match(send1, recv, compare_comm=True, compare_tag=True,
                        context="any_source matched send/recv")

    # Both sends should at least be on the same communicator and tag in this test.
    require_small_match(send1, send2, compare_comm=True, compare_tag=True,
                        context="any_source sender pair")

def validate_sendrecv(trace):
    require(trace["world_size"] == 2, "sendrecv: world size should be 2")

    rank0 = trace["sections"][0]
    rank1 = trace["sections"][1]

    r0 = require_one(
        rank0["large"],
        message_type=MPI_SENDRECV_TYPE,
        sender1=0,
        receiver1=1,
        count1=1,
        bytes1=4,
        sender2=1,
        receiver2=0,
        count2=1,
        bytes2=4,
    )

    r1 = require_one(
        rank1["large"],
        message_type=MPI_SENDRECV_TYPE,
        sender1=1,
        receiver1=0,
        count1=1,
        bytes1=4,
        sender2=0,
        receiver2=1,
        count2=1,
        bytes2=4,
    )

    require_large_same_comm(r0, r1, context="sendrecv communicator")
    require_same_value(r0, "tag1", r1, "tag2", "sendrecv rank0 sendtag vs rank1 recvtag")
    require_same_value(r0, "tag2", r1, "tag1", "sendrecv rank0 recvtag vs rank1 sendtag")

def validate_subcomm_send(trace):
    require(trace["world_size"] == 4, "subcomm_send: world size should be 4")

    s02 = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_SEND_TYPE,
        sender=0,
        receiver=2,
        count=1,
        bytes=4,
    )
    r20 = require_one(
        trace["sections"][2]["small"],
        message_type=MPI_RECV_TYPE,
        sender=0,
        receiver=2,
        count=1,
        bytes=4,
    )

    s13 = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_SEND_TYPE,
        sender=1,
        receiver=3,
        count=1,
        bytes=4,
    )
    r31 = require_one(
        trace["sections"][3]["small"],
        message_type=MPI_RECV_TYPE,
        sender=1,
        receiver=3,
        count=1,
        bytes=4,
    )

    require_same_value(s02, "tag", r20, "tag", "subcomm_send pair 0->2 tag")
    require_same_value(s13, "tag", r31, "tag", "subcomm_send pair 1->3 tag")

    require(isinstance(s02["comm"], int), "subcomm_send s02 comm should be int")
    require(isinstance(r20["comm"], int), "subcomm_send r20 comm should be int")
    require(isinstance(s13["comm"], int), "subcomm_send s13 comm should be int")
    require(isinstance(r31["comm"], int), "subcomm_send r31 comm should be int")

def validate_collectives(trace):
    require(trace["world_size"] == 4, "collectives: world size should be 4")

    for r in range(4):
        small = trace["sections"][r]["small"]
        large = trace["sections"][r]["large"]

        bcast = require_one(
            small,
            message_type=MPI_BCAST_TYPE,
            sender=2,
            receiver=r,
            count=3,
            bytes=12,
        )

        reduce = require_one(
            small,
            message_type=MPI_REDUCE_TYPE,
            sender=r,
            receiver=1,
            count=5,
            bytes=20,
        )

        gather = require_one(
            large,
            message_type=MPI_GATHER_TYPE,
            sender1=r,
            receiver1=3,
            count1=2,
            bytes1=8,
            sender2=3,
            receiver2=3,
            count2=(8 if r == 3 else 0),
            bytes2=(32 if r == 3 else 0),
        )

        scatter = require_one(
            large,
            message_type=MPI_SCATTER_TYPE,
            sender1=0,
            receiver1=0,
            count1=(28 if r == 0 else 0),
            bytes1=(112 if r == 0 else 0),
            sender2=0,
            receiver2=r,
            count2=7,
            bytes2=28,
        )

        allgather = require_one(
            large,
            message_type=MPI_ALLGATHER_TYPE,
            sender1=r,
            receiver1=r,
            count1=1,
            bytes1=4,
            sender2=r,
            receiver2=r,
            count2=4,
            bytes2=16,
        )

        require_zero(bcast, "tag", "collectives bcast tag")
        require_zero(reduce, "tag", "collectives reduce tag")
        require_large_zero_tags(gather, "collectives gather tags")
        require_large_zero_tags(scatter, "collectives scatter tags")
        require_large_zero_tags(allgather, "collectives allgather tags")

        # All of these collectives run on the same communicator in this test.
        require_same_value(bcast, "comm", reduce, "comm", "collectives bcast/reduce comm")
        require_same_value(bcast, "comm", gather, "comm", "collectives bcast/gather comm")
        require_same_value(bcast, "comm", scatter, "comm", "collectives bcast/scatter comm")
        require_same_value(bcast, "comm", allgather, "comm", "collectives bcast/allgather comm")

def validate_nonblocking(trace):
    require(trace["world_size"] == 2, "nonblocking: world size should be 2")

    rank0 = trace["sections"][0]
    rank1 = trace["sections"][1]

    sends = require_n(
        rank0["small"],
        2,
        message_type=MPI_ISEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recvs = require_n(
        rank1["small"],
        2,
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    w0 = require_one(
        rank0["small"],
        message_type=MPI_WAITALL_TYPE,
        sender=0,
        receiver=0,
        count=2,
        bytes=0,
    )

    w1 = require_one(
        rank1["small"],
        message_type=MPI_WAITALL_TYPE,
        sender=1,
        receiver=1,
        count=2,
        bytes=0,
    )

    require_all_same(sends, "tag", "nonblocking rank0 isend tags")
    require_all_same(sends, "comm", "nonblocking rank0 isend comms")
    require_all_same(recvs, "tag", "nonblocking rank1 irecv tags")
    require_all_same(recvs, "comm", "nonblocking rank1 irecv comms")

    require_same_value(sends[0], "tag", recvs[0], "tag", "nonblocking send/recv tag")
    require(isinstance(sends["comm"], int), "nonblocking sends comm should be int")

    require_zero(w0, "tag", "nonblocking waitall rank0 tag")
    require_zero(w1, "tag", "nonblocking waitall rank1 tag")
    require_zero(w0, "comm", "nonblocking waitall rank0 comm")
    require_zero(w1, "comm", "nonblocking waitall rank1 comm")

def validate_nonblocking_any_source_wait(trace):
    require(trace["world_size"] == 3, "nonblocking_any_source_wait: world size should be 3")

    rank0 = trace["sections"][0]
    rank1 = trace["sections"][1]
    rank2 = trace["sections"][2]

    irecv = require_one(
        rank0["small"],
        message_type=MPI_IRECV_TYPE,
        sender=1,
        receiver=0,
        count=1,
        bytes=4,
    )

    wait = require_one(
        rank0["small"],
        message_type=MPI_WAIT_TYPE,
        sender=0,
        receiver=0,
        count=1,
        bytes=0,
    )

    send1 = require_one(
        rank1["small"],
        message_type=MPI_SEND_TYPE,
        sender=1,
        receiver=0,
        count=1,
        bytes=4,
    )

    send2 = require_one(
        rank2["small"],
        message_type=MPI_SEND_TYPE,
        sender=2,
        receiver=0,
        count=1,
        bytes=4,
    )

    require_small_match(send1, irecv, compare_comm=True, compare_tag=True,
                        context="nonblocking_any_source_wait matched send/irecv")
    require_same_value(send1, "comm", send2, "comm", "nonblocking_any_source_wait send comms")
    require_same_value(send1, "tag", send2, "tag", "nonblocking_any_source_wait send tags")

    # Single MPI_Wait wrapper records the pending request's communicator/tag.
    require_same_value(wait, "tag", irecv, "tag", "nonblocking_any_source_wait wait tag")
    require_same_value(wait, "comm", irecv, "comm", "nonblocking_any_source_wait wait comm")

def validate_waitany(trace):
    require(trace["world_size"] == 2, "waitany: world size should be 2")

    rank0 = trace["sections"][0]
    rank1 = trace["sections"][1]

    sends = require_n(
        rank0["small"],
        2,
        message_type=MPI_SEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recvs = require_n(
        rank1["small"],
        2,
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    waitany = require_one(
        rank1["small"],
        message_type=MPI_WAITANY_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    wait = require_one(
        rank1["small"],
        message_type=MPI_WAIT_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    require_all_same(sends, "tag", "waitany rank0 send tags")
    require_all_same(sends, "comm", "waitany rank0 send comms")
    require_all_same(recvs, "tag", "waitany rank1 irecv tags")
    require_all_same(recvs, "comm", "waitany rank1 irecv comms")

    require_same_value(sends[0], "tag", recvs[0], "tag", "waitany send/irecv tag")
    require_same_value(sends[0], "comm", recvs[0], "comm", "waitany send/irecv comm")

    require_zero(waitany, "tag", "waitany control tag")
    require_zero(waitany, "comm", "waitany control comm")

    require_same_value(wait, "tag", recvs[0], "tag", "waitany trailing wait tag")
    require_same_value(wait, "comm", recvs[0], "comm", "waitany trailing wait comm")

def validate_waitsome(trace):
    require(trace["world_size"] == 2, "waitsome: world size should be 2")

    rank0 = trace["sections"][0]
    rank1 = trace["sections"][1]

    sends = require_n(
        rank0["small"],
        3,
        message_type=MPI_SEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recvs = require_n(
        rank1["small"],
        3,
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    waitsome = require_one(
        rank1["small"],
        message_type=MPI_WAITSOME_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    waitall = require_one(
        rank1["small"],
        message_type=MPI_WAITALL_TYPE,
        sender=1,
        receiver=1,
        count=2,
        bytes=0,
    )

    require_all_same(sends, "tag", "waitsome rank0 send tags")
    require_all_same(sends, "comm", "waitsome rank0 send comms")
    require_all_same(recvs, "tag", "waitsome rank1 irecv tags")
    require_all_same(recvs, "comm", "waitsome rank1 irecv comms")

    require_same_value(sends[0], "tag", recvs[0], "tag", "waitsome send/irecv tag")
    require_same_value(sends[0], "comm", recvs[0], "comm", "waitsome send/irecv comm")

    require_zero(waitsome, "tag", "waitsome control tag")
    require_zero(waitsome, "comm", "waitsome control comm")
    require_zero(waitall, "tag", "waitsome trailing waitall tag")
    require_zero(waitall, "comm", "waitsome trailing waitall comm")

def validate_test_single(trace):
    require(trace["world_size"] == 2, "test_single: world size should be 2")

    rank0 = trace["sections"][0]
    rank1 = trace["sections"][1]

    send = require_one(
        rank0["small"],
        message_type=MPI_SEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    irecv = require_one(
        rank1["small"],
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    test = require_one(
        rank1["small"],
        message_type=MPI_TEST_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    require_small_match(send, irecv, compare_comm=True, compare_tag=True,
                        context="test_single send/irecv")
    require_same_value(test, "tag", irecv, "tag", "test_single test tag")
    require_same_value(test, "comm", irecv, "comm", "test_single test comm")

def validate_testall(trace):
    require(trace["world_size"] == 2, "testall: world size should be 2")

    rank0 = trace["sections"][0]
    rank1 = trace["sections"][1]

    sends = require_n(
        rank0["small"],
        2,
        message_type=MPI_SEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recvs = require_n(
        rank1["small"],
        2,
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    testall = require_one(
        rank1["small"],
        message_type=MPI_TESTALL_TYPE,
        sender=1,
        receiver=1,
        count=2,
        bytes=0,
    )

    require_all_same(sends, "tag", "testall rank0 send tags")
    require_all_same(sends, "comm", "testall rank0 send comms")
    require_all_same(recvs, "tag", "testall rank1 irecv tags")
    require_all_same(recvs, "comm", "testall rank1 irecv comms")

    require_same_value(sends[0], "tag", recvs[0], "tag", "testall send/irecv tag")
    require_same_value(sends[0], "comm", recvs[0], "comm", "testall send/irecv comm")

    require_zero(testall, "tag", "testall control tag")
    require_zero(testall, "comm", "testall control comm")

def validate_testany(trace):
    require(trace["world_size"] == 2, "testany: world size should be 2")

    rank0 = trace["sections"][0]
    rank1 = trace["sections"][1]

    sends = require_n(
        rank0["small"],
        2,
        message_type=MPI_SEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recvs = require_n(
        rank1["small"],
        2,
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    testany = require_one(
        rank1["small"],
        message_type=MPI_TESTANY_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    wait = require_one(
        rank1["small"],
        message_type=MPI_WAIT_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    require_all_same(sends, "tag", "testany rank0 send tags")
    require_all_same(sends, "comm", "testany rank0 send comms")
    require_all_same(recvs, "tag", "testany rank1 irecv tags")
    require_all_same(recvs, "comm", "testany rank1 irecv comms")

    require_same_value(sends[0], "tag", recvs[0], "tag", "testany send/irecv tag")
    require_same_value(sends[0], "comm", recvs[0], "comm", "testany send/irecv comm")

    require_zero(testany, "tag", "testany control tag")
    require_zero(testany, "comm", "testany control comm")

    require_same_value(wait, "tag", recvs[0], "tag", "testany trailing wait tag")
    require_same_value(wait, "comm", recvs[0], "comm", "testany trailing wait comm")

def validate_testsome(trace):
    require(trace["world_size"] == 2, "testsome: world size should be 2")

    rank0 = trace["sections"][0]
    rank1 = trace["sections"][1]

    sends = require_n(
        rank0["small"],
        3,
        message_type=MPI_SEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recvs = require_n(
        rank1["small"],
        3,
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    testsome = require_one(
        rank1["small"],
        message_type=MPI_TESTSOME_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    waitall = require_one(
        rank1["small"],
        message_type=MPI_WAITALL_TYPE,
        sender=1,
        receiver=1,
        count=2,
        bytes=0,
    )

    require_all_same(sends, "tag", "testsome rank0 send tags")
    require_all_same(sends, "comm", "testsome rank0 send comms")
    require_all_same(recvs, "tag", "testsome rank1 irecv tags")
    require_all_same(recvs, "comm", "testsome rank1 irecv comms")

    require_same_value(sends[0], "tag", recvs[0], "tag", "testsome send/irecv tag")
    require_same_value(sends[0], "comm", recvs[0], "comm", "testsome send/irecv comm")

    require_zero(testsome, "tag", "testsome control tag")
    require_zero(testsome, "comm", "testsome control comm")
    require_zero(waitall, "tag", "testsome trailing waitall tag")
    require_zero(waitall, "comm", "testsome trailing waitall comm")

def validate_fortran_nonblocking_wait(trace):
    require(trace["world_size"] == 2, "fortran_nonblocking_wait: world size should be 2")

    rank0 = trace["sections"][0]
    rank1 = trace["sections"][1]

    isend = require_one(
        rank0["small"],
        message_type=MPI_ISEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    w0 = require_one(
        rank0["small"],
        message_type=MPI_WAIT_TYPE,
        sender=0,
        receiver=0,
        count=1,
        bytes=0,
    )

    irecv = require_one(
        rank1["small"],
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    w1 = require_one(
        rank1["small"],
        message_type=MPI_WAIT_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    require_small_match(isend, irecv, compare_comm=True, compare_tag=True,
                        context="fortran_nonblocking_wait isend/irecv")

    require_same_value(w0, "tag", isend, "tag", "fortran_nonblocking_wait rank0 wait tag")
    require_same_value(w0, "comm", isend, "comm", "fortran_nonblocking_wait rank0 wait comm")
    require_same_value(w1, "tag", irecv, "tag", "fortran_nonblocking_wait rank1 wait tag")
    require_same_value(w1, "comm", irecv, "comm", "fortran_nonblocking_wait rank1 wait comm")

def validate_fortran_nonblocking_any_source_wait(trace):
    require(trace["world_size"] == 3, "fortran_nonblocking_any_source_wait: world size should be 3")

    rank0 = trace["sections"][0]
    rank1 = trace["sections"][1]
    rank2 = trace["sections"][2]

    irecv = require_one(
        rank0["small"],
        message_type=MPI_IRECV_TYPE,
        sender=1,
        receiver=0,
        count=1,
        bytes=4,
    )

    wait = require_one(
        rank0["small"],
        message_type=MPI_WAIT_TYPE,
        sender=0,
        receiver=0,
        count=1,
        bytes=0,
    )

    send1 = require_one(
        rank1["small"],
        message_type=MPI_SEND_TYPE,
        sender=1,
        receiver=0,
        count=1,
        bytes=4,
    )

    send2 = require_one(
        rank2["small"],
        message_type=MPI_SEND_TYPE,
        sender=2,
        receiver=0,
        count=1,
        bytes=4,
    )

    require_small_match(send1, irecv, compare_comm=True, compare_tag=True,
                        context="fortran_nonblocking_any_source_wait matched pair")
    require_same_value(send1, "comm", send2, "comm", "fortran_nonblocking_any_source_wait send comms")
    require_same_value(send1, "tag", send2, "tag", "fortran_nonblocking_any_source_wait send tags")
    require_same_value(wait, "tag", irecv, "tag", "fortran_nonblocking_any_source_wait wait tag")
    require_same_value(wait, "comm", irecv, "comm", "fortran_nonblocking_any_source_wait wait comm")

def validate_fortran_waitall(trace):
    require(trace["world_size"] == 2, "fortran_waitall: world size should be 2")

    rank0 = trace["sections"][0]
    rank1 = trace["sections"][1]

    sends = require_n(
        rank0["small"],
        2,
        message_type=MPI_ISEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    w0 = require_one(
        rank0["small"],
        message_type=MPI_WAITALL_TYPE,
        sender=0,
        receiver=0,
        count=2,
        bytes=0,
    )

    recvs = require_n(
        rank1["small"],
        2,
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    w1 = require_one(
        rank1["small"],
        message_type=MPI_WAITALL_TYPE,
        sender=1,
        receiver=1,
        count=2,
        bytes=0,
    )

    require_all_same(sends, "tag", "fortran_waitall send tags")
    require_all_same(sends, "comm", "fortran_waitall send comms")
    require_all_same(recvs, "tag", "fortran_waitall recv tags")
    require_all_same(recvs, "comm", "fortran_waitall recv comms")

    require_same_value(sends[0], "tag", recvs[0], "tag", "fortran_waitall send/recv tag")
    require_same_value(sends[0], "comm", recvs[0], "comm", "fortran_waitall send/recv comm")

    require_zero(w0, "tag", "fortran_waitall rank0 waitall tag")
    require_zero(w0, "comm", "fortran_waitall rank0 waitall comm")
    require_zero(w1, "tag", "fortran_waitall rank1 waitall tag")
    require_zero(w1, "comm", "fortran_waitall rank1 waitall comm")

def validate_fortran_waitany(trace):
    require(trace["world_size"] == 2, "fortran_waitany: world size should be 2")

    rank0 = trace["sections"][0]
    rank1 = trace["sections"][1]

    sends = require_n(
        rank0["small"],
        2,
        message_type=MPI_SEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recvs = require_n(
        rank1["small"],
        2,
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    waitany = require_one(
        rank1["small"],
        message_type=MPI_WAITANY_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    wait = require_one(
        rank1["small"],
        message_type=MPI_WAIT_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    require_all_same(sends, "tag", "fortran_waitany send tags")
    require_all_same(sends, "comm", "fortran_waitany send comms")
    require_all_same(recvs, "tag", "fortran_waitany recv tags")
    require_all_same(recvs, "comm", "fortran_waitany recv comms")

    require_same_value(sends[0], "tag", recvs[0], "tag", "fortran_waitany send/recv tag")
    require_same_value(sends[0], "comm", recvs[0], "comm", "fortran_waitany send/recv comm")

    require_zero(waitany, "tag", "fortran_waitany control tag")
    require_zero(waitany, "comm", "fortran_waitany control comm")
    require_same_value(wait, "tag", recvs[0], "tag", "fortran_waitany trailing wait tag")
    require_same_value(wait, "comm", recvs[0], "comm", "fortran_waitany trailing wait comm")

def validate_fortran_testall(trace):
    require(trace["world_size"] == 2, "fortran_testall: world size should be 2")

    rank0 = trace["sections"][0]
    rank1 = trace["sections"][1]

    sends = require_n(
        rank0["small"],
        2,
        message_type=MPI_SEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recvs = require_n(
        rank1["small"],
        2,
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    testall = require_one(
        rank1["small"],
        message_type=MPI_TESTALL_TYPE,
        sender=1,
        receiver=1,
        count=2,
        bytes=0,
    )

    require_all_same(sends, "tag", "fortran_testall send tags")
    require_all_same(sends, "comm", "fortran_testall send comms")
    require_all_same(recvs, "tag", "fortran_testall recv tags")
    require_all_same(recvs, "comm", "fortran_testall recv comms")

    require_same_value(sends[0], "tag", recvs[0], "tag", "fortran_testall send/recv tag")
    require_same_value(sends[0], "comm", recvs[0], "comm", "fortran_testall send/recv comm")

    require_zero(testall, "tag", "fortran_testall control tag")
    require_zero(testall, "comm", "fortran_testall control comm")

def validate_fortran_bsend(trace):
    require(trace["world_size"] == 2, "fortran_bsend: world size should be 2")

    rank0 = trace["sections"][0]
    rank1 = trace["sections"][1]

    bsend = require_one(
        rank0["small"],
        message_type=MPI_BSEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recv = require_one(
        rank1["small"],
        message_type=MPI_RECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    require_small_match(bsend, recv, compare_comm=True, compare_tag=True,
                        context="fortran_bsend bsend/recv")

def validate_fortran_ssend(trace):
    require(trace["world_size"] == 2, "fortran_ssend: world size should be 2")

    rank0 = trace["sections"][0]
    rank1 = trace["sections"][1]

    ssend = require_one(
        rank0["small"],
        message_type=MPI_SSEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recv = require_one(
        rank1["small"],
        message_type=MPI_RECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    require_small_match(ssend, recv, compare_comm=True, compare_tag=True,
                        context="fortran_ssend ssend/recv")

def validate_fortran_rsend(trace):
    require(trace["world_size"] == 2, "fortran_rsend: world size should be 2")

    rank0 = trace["sections"][0]
    rank1 = trace["sections"][1]

    rsend = require_one(
        rank0["small"],
        message_type=MPI_RSEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    irecv = require_one(
        rank1["small"],
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    wait = require_one(
        rank1["small"],
        message_type=MPI_WAIT_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    require_small_match(rsend, irecv, compare_comm=True, compare_tag=True,
                        context="fortran_rsend rsend/irecv")
    require_same_value(wait, "tag", irecv, "tag", "fortran_rsend wait tag")
    require_same_value(wait, "comm", irecv, "comm", "fortran_rsend wait comm")

def validate_fortran_barrier(trace):
    require(trace["world_size"] == 3, "fortran_barrier: world size should be 3")

    rank0_bar = None
    for r in range(3):
        rec = require_one(
            trace["sections"][r]["small"],
            message_type=MPI_BARRIER_TYPE,
            sender=r,
            receiver=r,
            count=0,
            bytes=0,
        )
        require_zero(rec, "tag", "fortran_barrier tag")
        if rank0_bar is None:
            rank0_bar = rec
        else:
            require_same_value(rank0_bar, "comm", rec, "comm", "fortran_barrier communicator")

def validate_fortran_bcast(trace):
    require(trace["world_size"] == 4, "fortran_bcast: world size should be 4")

    first = None
    for r in range(4):
        rec = require_one(
            trace["sections"][r]["small"],
            message_type=MPI_BCAST_TYPE,
            sender=2,
            receiver=r,
            count=3,
            bytes=12,
        )
        require_zero(rec, "tag", "fortran_bcast tag")
        if first is None:
            first = rec
        else:
            require_same_value(first, "comm", rec, "comm", "fortran_bcast communicator")

def validate_fortran_reduce(trace):
    require(trace["world_size"] == 4, "fortran_reduce: world size should be 4")

    first = None
    for r in range(4):
        rec = require_one(
            trace["sections"][r]["small"],
            message_type=MPI_REDUCE_TYPE,
            sender=r,
            receiver=1,
            count=5,
            bytes=20,
        )
        require_zero(rec, "tag", "fortran_reduce tag")
        if first is None:
            first = rec
        else:
            require_same_value(first, "comm", rec, "comm", "fortran_reduce communicator")

def validate_fortran_allreduce(trace):
    require(trace["world_size"] == 4, "fortran_allreduce: world size should be 4")

    first = None
    for r in range(4):
        rec = require_one(
            trace["sections"][r]["small"],
            message_type=MPI_ALLREDUCE_TYPE,
            sender=r,
            receiver=r,
            count=4,
            bytes=16,
        )
        require_zero(rec, "tag", "fortran_allreduce tag")
        if first is None:
            first = rec
        else:
            require_same_value(first, "comm", rec, "comm", "fortran_allreduce communicator")

def validate_fortran_gather(trace):
    require(trace["world_size"] == 4, "fortran_gather: world size should be 4")

    first = None
    for r in range(4):
        rec = require_one(
            trace["sections"][r]["large"],
            message_type=MPI_GATHER_TYPE,
            sender1=r,
            receiver1=3,
            count1=2,
            bytes1=8,
            sender2=3,
            receiver2=3,
            count2=(8 if r == 3 else 0),
            bytes2=(32 if r == 3 else 0),
        )
        require_large_zero_tags(rec, "fortran_gather tags")
        if first is None:
            first = rec
        else:
            require_same_value(first, "comm", rec, "comm", "fortran_gather communicator")

def validate_fortran_scatter(trace):
    require(trace["world_size"] == 4, "fortran_scatter: world size should be 4")

    first = None
    for r in range(4):
        rec = require_one(
            trace["sections"][r]["large"],
            message_type=MPI_SCATTER_TYPE,
            sender1=0,
            receiver1=0,
            count1=(28 if r == 0 else 0),
            bytes1=(112 if r == 0 else 0),
            sender2=0,
            receiver2=r,
            count2=7,
            bytes2=28,
        )
        require_large_zero_tags(rec, "fortran_scatter tags")
        if first is None:
            first = rec
        else:
            require_same_value(first, "comm", rec, "comm", "fortran_scatter communicator")

def validate_fortran_allgather(trace):
    require(trace["world_size"] == 4, "fortran_allgather: world size should be 4")

    first = None
    for r in range(4):
        rec = require_one(
            trace["sections"][r]["large"],
            message_type=MPI_ALLGATHER_TYPE,
            sender1=r,
            receiver1=r,
            count1=1,
            bytes1=4,
            sender2=r,
            receiver2=r,
            count2=4,
            bytes2=16,
        )
        require_large_zero_tags(rec, "fortran_allgather tags")
        if first is None:
            first = rec
        else:
            require_same_value(first, "comm", rec, "comm", "fortran_allgather communicator")

def validate_fortran_cancel(trace):
    require(trace["world_size"] == 2, "fortran_cancel: world size should be 2")

    rank0 = trace["sections"][0]
    rank1 = trace["sections"][1]

    require_none(
        rank1["small"],
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    wait = require_one(
        rank1["small"],
        message_type=MPI_WAIT_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    b0 = require_one(
        rank0["small"],
        message_type=MPI_BARRIER_TYPE,
        sender=0,
        receiver=0,
        count=0,
        bytes=0,
    )
    b1 = require_one(
        rank1["small"],
        message_type=MPI_BARRIER_TYPE,
        sender=1,
        receiver=1,
        count=0,
        bytes=0,
    )

    # Wait still exists as a local completion event; tag/comm may derive from the
    # cancelled request in some implementations.
    require(isinstance(wait["tag"], int), "fortran_cancel wait tag should be int")
    require(isinstance(wait["comm"], int), "fortran_cancel wait comm should be int")

    require_zero(b0, "tag", "fortran_cancel barrier rank0 tag")
    require_zero(b1, "tag", "fortran_cancel barrier rank1 tag")
    require_same_value(b0, "comm", b1, "comm", "fortran_cancel barrier communicator")

VALIDATORS = {
    "send_recv": validate_send_recv,
    "any_source": validate_any_source,
    "sendrecv": validate_sendrecv,
    "subcomm_send": validate_subcomm_send,
    "collectives": validate_collectives,
    "nonblocking": validate_nonblocking,
    "nonblocking_any_source_wait": validate_nonblocking_any_source_wait,
    "waitany": validate_waitany,
    "waitsome": validate_waitsome,
    "test_single": validate_test_single,
    "testall": validate_testall,
    "testany": validate_testany,
    "testsome": validate_testsome,
    "fortran_nonblocking_wait": validate_fortran_nonblocking_wait,
    "fortran_nonblocking_any_source_wait": validate_fortran_nonblocking_any_source_wait,
    "fortran_waitall": validate_fortran_waitall,
    "fortran_waitany": validate_fortran_waitany,
    "fortran_testall": validate_fortran_testall,
    "fortran_bsend": validate_fortran_bsend,
    "fortran_ssend": validate_fortran_ssend,
    "fortran_rsend": validate_fortran_rsend,
    "fortran_sendrecv": validate_sendrecv,
    "fortran_barrier": validate_fortran_barrier,
    "fortran_bcast": validate_fortran_bcast,
    "fortran_reduce": validate_fortran_reduce,
    "fortran_allreduce": validate_fortran_allreduce,
    "fortran_gather": validate_fortran_gather,
    "fortran_scatter": validate_fortran_scatter,
    "fortran_allgather": validate_fortran_allgather,
    "fortran_cancel": validate_fortran_cancel,
}

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", required=True)
    parser.add_argument("--launcher", required=True)
    parser.add_argument("--nflag", required=True)
    parser.add_argument("--launcher-extra-args", default="")
    parser.add_argument("--nprocs", type=int, required=True)
    parser.add_argument("--exe", required=True)
    parser.add_argument("--trace-lib", required=True)
    parser.add_argument("--work-dir", required=True)
    parser.add_argument(
        "--env",
        action="append",
        default=[],
        help="Extra environment setting KEY=VALUE to pass to the traced job; may be repeated",
    )
    args = parser.parse_args()

    if args.case not in VALIDATORS:
        print("Unknown case: {}".format(args.case), file=sys.stderr)
        return 2

    work_dir = Path(args.work_dir)
    if work_dir.exists():
        shutil.rmtree(str(work_dir))
    work_dir.mkdir(parents=True)

    env = os.environ.copy()

    for item in args.env:
        if "=" not in item:
            print("Invalid --env value {!r}; expected KEY=VALUE".format(item), file=sys.stderr)
            return 2
        key, value = item.split("=", 1)
        env[key] = value

    old_preload = env.get("LD_PRELOAD", "")
    if old_preload:
        env["LD_PRELOAD"] = "{}:{}".format(args.trace_lib, old_preload)
    else:
        env["LD_PRELOAD"] = args.trace_lib

    cmd = [args.launcher]
    cmd.extend(shlex.split(args.launcher_extra_args))
    cmd.extend([args.nflag, str(args.nprocs), args.exe])

    run(cmd, cwd=str(work_dir), env=env)

    traces = sorted(list(work_dir.glob("*.mpic")) + list(work_dir.glob("*.mpic.gz")))
    require(
        len(traces) == 1,
        "{}: expected one .mpic/.mpic.gz file, found {}".format(args.case, len(traces))
    )

    trace = parse_trace(traces[0])
    validate_common_trace(trace)
    VALIDATORS[args.case](trace)

    print("[PASS] {}".format(args.case))
    return 0

if __name__ == "__main__":
    sys.exit(main())

