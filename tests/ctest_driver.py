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
MPI_INIT = 39
MPI_FINALIZE = 40

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

def require_comm_int(record, key="comm", context="comm type"):
    require(key in record, "{}: missing {}".format(context, key))
    require(isinstance(record[key], int), "{}: {} should be int".format(context, key))

def require_tag_int(record, key="tag", context="tag type"):
    require(key in record, "{}: missing {}".format(context, key))
    require(isinstance(record[key], int), "{}: {} should be int".format(context, key))

def require_small_match(a, b, compare_comm=False, compare_tag=True, context="small match"):
    if compare_comm:
        require_same_value(a, "comm", b, "comm", context + " comm")
    else:
        require_comm_int(a, "comm", context + " a comm")
        require_comm_int(b, "comm", context + " b comm")
    if compare_tag:
        require_same_value(a, "tag", b, "tag", context + " tag")
    else:
        require_tag_int(a, "tag", context + " a tag")
        require_tag_int(b, "tag", context + " b tag")

def require_large_zero_tags(record, context="large zero tags"):
    require_zero(record, "tag1", context + " tag1")
    require_zero(record, "tag2", context + " tag2")

def require_ptp_pair(send_rec, recv_rec, context):
    require_valid_small_record(send_rec)
    require_valid_small_record(recv_rec)
    require_same_value(send_rec, "tag", recv_rec, "tag", context + " tag")
    require_comm_int(send_rec, "comm", context + " send comm")
    require_comm_int(recv_rec, "comm", context + " recv comm")

def require_local_same_comm(records, context):
    if not records:
        return
    for rec in records:
        require_comm_int(rec, "comm", context + " comm")
        require_tag_int(rec, "tag", context + " tag")
    require_all_same(records, "comm", context + " comms")

def require_same_tag_multiset(a_records, b_records, context):
    a_tags = sorted(r.get("tag") for r in a_records)
    b_tags = sorted(r.get("tag") for r in b_records)
    if a_tags != b_tags:
        raise AssertionError(
            "{}: tag multisets differ: {} vs {}".format(context, a_tags, b_tags)
        )

def _as_record_list(refs):
    if refs is None:
        return []
    if isinstance(refs, list):
        return refs
    return [refs]

def require_control_zero_or_from_refs(record, refs=None, context="control"):
    """
    Accept control-event tag/comm as either:
      - 0 / 0
      - any tag/comm taken from the referenced same-rank request records
    """
    require_valid_small_record(record)

    refs = _as_record_list(refs)
    acceptable_tags = {0}
    acceptable_comms = {0}

    for r in refs:
        if "tag" in r:
            acceptable_tags.add(r["tag"])
        if "comm" in r:
            acceptable_comms.add(r["comm"])

    if record["tag"] not in acceptable_tags:
        raise AssertionError(
            "{}: tag={} not in acceptable set {}".format(context, record["tag"], sorted(acceptable_tags))
        )

    if record["comm"] not in acceptable_comms:
        raise AssertionError(
            "{}: comm={} not in acceptable set {}".format(context, record["comm"], sorted(acceptable_comms))
        )

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

def verify_lifecycle_bookends(trace):
    expected_ranks = trace["world_size"]
    
    init_count = 0
    finalize_count = 0

    # Ensure chronological sanity per rank
    for rank in range(expected_ranks):
        # In the test driver, events are grouped by rank into 'sections'
        section = trace["sections"][rank]
        small_events = section["small"]
        
        if not small_events:
            print(f"FAIL: Rank {rank} has no small events.", file=sys.stderr)
            sys.exit(1)
            
        first_event = small_events[0]
        last_event = small_events[-1]

        if first_event["message_type"] != MPI_INIT:
            print(f"FAIL: Rank {rank}'s first event type was {first_event['message_type']}, expected {MPI_INIT} (MPI_INIT).", file=sys.stderr)
            sys.exit(1)
        else:
            init_count += 1
            
        if last_event["message_type"] != MPI_FINALIZE:
            print(f"FAIL: Rank {rank}'s last event type was {last_event['message_type']}, expected {MPI_FINALIZE} (MPI_FINALIZE).", file=sys.stderr)
            sys.exit(1)
        else:
            finalize_count += 1

    # Ensure every rank recorded an Init and a Finalize
    if init_count != expected_ranks:
        print(f"FAIL: Expected {expected_ranks} MPI_INIT events, found {init_count}.", file=sys.stderr)
        sys.exit(1)
        
    if finalize_count != expected_ranks:
        print(f"FAIL: Expected {expected_ranks} MPI_FINALIZE events, found {finalize_count}.", file=sys.stderr)
        sys.exit(1)
        
    print("SUCCESS: Lifecycle bookends (Init/Finalize) verified for all ranks.")

# -----------------------------------------------------------------------------
# Validators
# -----------------------------------------------------------------------------

def validate_send_recv(trace):
    require(trace["world_size"] == 2, "send_recv: world size should be 2")

    send = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_SEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recv = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_RECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    require_ptp_pair(send, recv, "send_recv")

def validate_any_source(trace):
    require(trace["world_size"] == 3, "any_source: world size should be 3")

    recv = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_RECV_TYPE,
        sender=1,
        receiver=0,
        count=1,
        bytes=4,
    )

    send1 = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_SEND_TYPE,
        sender=1,
        receiver=0,
        count=1,
        bytes=4,
    )

    send2 = require_one(
        trace["sections"][2]["small"],
        message_type=MPI_SEND_TYPE,
        sender=2,
        receiver=0,
        count=1,
        bytes=4,
    )

    require_ptp_pair(send1, recv, "any_source matched pair")
    require_same_value(send1, "tag", send2, "tag", "any_source sender tags")
    require_comm_int(send1, "comm", "any_source send1 comm")
    require_comm_int(send2, "comm", "any_source send2 comm")

def validate_sendrecv(trace):
    require(trace["world_size"] == 2, "sendrecv: world size should be 2")

    r0 = require_one(
        trace["sections"][0]["large"],
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
        trace["sections"][1]["large"],
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

    require_comm_int(r0, "comm", "sendrecv rank0 comm")
    require_comm_int(r1, "comm", "sendrecv rank1 comm")
    require_same_value(r0, "tag1", r1, "tag2", "sendrecv rank0 sendtag vs rank1 recvtag")
    require_same_value(r0, "tag2", r1, "tag1", "sendrecv rank0 recvtag vs rank1 sendtag")

def validate_init_finalize(trace):
    require(trace["world_size"] == 4, "init_finalize: world size should be 4")

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

    require_comm_int(s02, "comm", "subcomm_send s02 comm")
    require_comm_int(r20, "comm", "subcomm_send r20 comm")
    require_comm_int(s13, "comm", "subcomm_send s13 comm")
    require_comm_int(r31, "comm", "subcomm_send r31 comm")

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
        require_large_zero_tags(gather, "collectives gather")
        require_large_zero_tags(scatter, "collectives scatter")
        require_large_zero_tags(allgather, "collectives allgather")

        require_comm_int(bcast, "comm", "collectives bcast comm")
        require_comm_int(reduce, "comm", "collectives reduce comm")
        require_comm_int(gather, "comm", "collectives gather comm")
        require_comm_int(scatter, "comm", "collectives scatter comm")
        require_comm_int(allgather, "comm", "collectives allgather comm")

        # Same-rank communicator consistency is still reasonable here.
        require_same_value(bcast, "comm", reduce, "comm", "collectives same-rank bcast/reduce comm")
        require_same_value(bcast, "comm", gather, "comm", "collectives same-rank bcast/gather comm")
        require_same_value(bcast, "comm", scatter, "comm", "collectives same-rank bcast/scatter comm")
        require_same_value(bcast, "comm", allgather, "comm", "collectives same-rank bcast/allgather comm")

def validate_nonblocking(trace):
    require(trace["world_size"] == 2, "nonblocking: world size should be 2")

    sends = require_n(
        trace["sections"][0]["small"],
        2,
        message_type=MPI_ISEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recvs = require_n(
        trace["sections"][1]["small"],
        2,
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    w0 = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_WAITALL_TYPE,
        sender=0,
        receiver=0,
        count=2,
        bytes=0,
    )

    w1 = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_WAITALL_TYPE,
        sender=1,
        receiver=1,
        count=2,
        bytes=0,
    )

    require_local_same_comm(sends, "nonblocking rank0 isend")
    require_local_same_comm(recvs, "nonblocking rank1 irecv")
    require_same_tag_multiset(sends, recvs, "nonblocking send/recv tags")

    require_control_zero_or_from_refs(w0, sends, "nonblocking rank0 waitall")
    require_control_zero_or_from_refs(w1, recvs, "nonblocking rank1 waitall")

def validate_nonblocking_any_source_wait(trace):
    require(trace["world_size"] == 3, "nonblocking_any_source_wait: world size should be 3")

    irecv = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_IRECV_TYPE,
        sender=1,
        receiver=0,
        count=1,
        bytes=4,
    )

    wait = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_WAIT_TYPE,
        sender=0,
        receiver=0,
        count=1,
        bytes=0,
    )

    send1 = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_SEND_TYPE,
        sender=1,
        receiver=0,
        count=1,
        bytes=4,
    )

    send2 = require_one(
        trace["sections"][2]["small"],
        message_type=MPI_SEND_TYPE,
        sender=2,
        receiver=0,
        count=1,
        bytes=4,
    )

    require_ptp_pair(send1, irecv, "nonblocking_any_source_wait matched send/irecv")
    require_same_value(send1, "tag", send2, "tag", "nonblocking_any_source_wait send tags")

    require_control_zero_or_from_refs(wait, irecv, "nonblocking_any_source_wait wait")

def validate_waitany(trace):
    require(trace["world_size"] == 2, "waitany: world size should be 2")

    sends = require_n(
        trace["sections"][0]["small"],
        2,
        message_type=MPI_SEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recvs = require_n(
        trace["sections"][1]["small"],
        2,
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    waitany = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_WAITANY_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    wait = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_WAIT_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    require_local_same_comm(sends, "waitany sends")
    require_local_same_comm(recvs, "waitany recvs")
    require_same_tag_multiset(sends, recvs, "waitany send/recv tags")

    require_control_zero_or_from_refs(waitany, recvs, "waitany control")
    require_control_zero_or_from_refs(wait, recvs, "waitany trailing wait")

def validate_waitsome(trace):
    require(trace["world_size"] == 2, "waitsome: world size should be 2")

    sends = require_n(
        trace["sections"][0]["small"],
        3,
        message_type=MPI_SEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recvs = require_n(
        trace["sections"][1]["small"],
        3,
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    waitsome = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_WAITSOME_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    waitall = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_WAITALL_TYPE,
        sender=1,
        receiver=1,
        count=2,
        bytes=0,
    )

    require_local_same_comm(sends, "waitsome sends")
    require_local_same_comm(recvs, "waitsome recvs")
    require_same_tag_multiset(sends, recvs, "waitsome send/recv tags")

    require_control_zero_or_from_refs(waitsome, recvs, "waitsome control")
    require_control_zero_or_from_refs(waitall, recvs, "waitsome trailing waitall")

def validate_test_single(trace):
    require(trace["world_size"] == 2, "test_single: world size should be 2")

    send = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_SEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    irecv = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    test = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_TEST_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    require_ptp_pair(send, irecv, "test_single")
    require_control_zero_or_from_refs(test, irecv, "test_single test")

def validate_testall(trace):
    require(trace["world_size"] == 2, "testall: world size should be 2")

    sends = require_n(
        trace["sections"][0]["small"],
        2,
        message_type=MPI_SEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recvs = require_n(
        trace["sections"][1]["small"],
        2,
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    testall = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_TESTALL_TYPE,
        sender=1,
        receiver=1,
        count=2,
        bytes=0,
    )

    require_local_same_comm(sends, "testall sends")
    require_local_same_comm(recvs, "testall recvs")
    require_same_tag_multiset(sends, recvs, "testall send/recv tags")

    require_control_zero_or_from_refs(testall, recvs, "testall control")

def validate_testany(trace):
    require(trace["world_size"] == 2, "testany: world size should be 2")

    sends = require_n(
        trace["sections"][0]["small"],
        2,
        message_type=MPI_SEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recvs = require_n(
        trace["sections"][1]["small"],
        2,
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    testany = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_TESTANY_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    wait = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_WAIT_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    require_local_same_comm(sends, "testany sends")
    require_local_same_comm(recvs, "testany recvs")
    require_same_tag_multiset(sends, recvs, "testany send/recv tags")

    require_control_zero_or_from_refs(testany, recvs, "testany control")
    require_control_zero_or_from_refs(wait, recvs, "testany trailing wait")

def validate_testsome(trace):
    require(trace["world_size"] == 2, "testsome: world size should be 2")

    sends = require_n(
        trace["sections"][0]["small"],
        3,
        message_type=MPI_SEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recvs = require_n(
        trace["sections"][1]["small"],
        3,
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    testsome = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_TESTSOME_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    waitall = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_WAITALL_TYPE,
        sender=1,
        receiver=1,
        count=2,
        bytes=0,
    )

    require_local_same_comm(sends, "testsome sends")
    require_local_same_comm(recvs, "testsome recvs")
    require_same_tag_multiset(sends, recvs, "testsome send/recv tags")

    require_control_zero_or_from_refs(testsome, recvs, "testsome control")
    require_control_zero_or_from_refs(waitall, recvs, "testsome trailing waitall")

def validate_rsend(trace):
    require(trace["world_size"] == 2, "rsend: world size should be 2")

    rsend = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_RSEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    irecv = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    wait = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_WAIT_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    require_ptp_pair(rsend, irecv, "rsend")
    require_control_zero_or_from_refs(wait, irecv, "rsend wait")

def validate_bsend(trace):
    require(trace["world_size"] == 2, "bsend: world size should be 2")

    bsend = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_BSEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recv = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_RECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    require_ptp_pair(bsend, recv, "bsend")

def validate_ssend(trace):
    require(trace["world_size"] == 2, "ssend: world size should be 2")

    ssend = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_SSEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recv = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_RECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    require_ptp_pair(ssend, recv, "ssend")

def validate_cancel(trace):
    require(trace["world_size"] == 2, "cancel: world size should be 2")

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

    require(isinstance(wait["tag"], int), "cancel wait tag should be int")
    require(isinstance(wait["comm"], int), "cancel wait comm should be int")

    require_zero(b0, "tag", "cancel barrier rank0 tag")
    require_zero(b1, "tag", "cancel barrier rank1 tag")
    require_comm_int(b0, "comm", "cancel barrier rank0 comm")
    require_comm_int(b1, "comm", "cancel barrier rank1 comm")

def validate_allreduce(trace):
    require(trace["world_size"] == 4, "allreduce: world size should be 4")

    for r in range(4):
        rec = require_one(
            trace["sections"][r]["small"],
            message_type=MPI_ALLREDUCE_TYPE,
            sender=r,
            receiver=r,
            count=4,
            bytes=16,
        )
        require_zero(rec, "tag", "allreduce tag")
        require_comm_int(rec, "comm", "allreduce comm")

def validate_barrier(trace):
    require(trace["world_size"] == 3, "barrier: world size should be 3")

    for r in range(3):
        rec = require_one(
            trace["sections"][r]["small"],
            message_type=MPI_BARRIER_TYPE,
            sender=r,
            receiver=r,
            count=0,
            bytes=0,
        )
        require_zero(rec, "tag", "barrier tag")
        require_comm_int(rec, "comm", "barrier comm")

def validate_fortran_init_finalize(trace):
    require(trace["world_size"] == 4, "init_fortran_finalize: world size should be 4")

def validate_fortran_init_finalize_f08(trace):
    require(trace["world_size"] == 4, "init_fortran_finalize_f08: world size should be 4")

def validate_fortran_nonblocking_wait(trace):
    require(trace["world_size"] == 2, "fortran_nonblocking_wait: world size should be 2")

    isend = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_ISEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    w0 = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_WAIT_TYPE,
        sender=0,
        receiver=0,
        count=1,
        bytes=0,
    )

    irecv = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    w1 = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_WAIT_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    require_ptp_pair(isend, irecv, "fortran_nonblocking_wait")
    require_control_zero_or_from_refs(w0, isend, "fortran_nonblocking_wait rank0 wait")
    require_control_zero_or_from_refs(w1, irecv, "fortran_nonblocking_wait rank1 wait")

def validate_fortran_nonblocking_wait_f08(trace):
    require(trace["world_size"] == 2, "fortran_nonblocking_wait_f08: world size should be 2")

    isend = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_ISEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    w0 = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_WAIT_TYPE,
        sender=0,
        receiver=0,
        count=1,
        bytes=0,
    )

    irecv = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    w1 = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_WAIT_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    require_ptp_pair(isend, irecv, "fortran_nonblocking_wait_f08")
    require_control_zero_or_from_refs(w0, isend, "fortran_nonblocking_wait_f08 rank0 wait")
    require_control_zero_or_from_refs(w1, irecv, "fortran_nonblocking_wait_f08 rank1 wait")

def validate_fortran_nonblocking_any_source_wait(trace):
    require(trace["world_size"] == 3, "fortran_nonblocking_any_source_wait: world size should be 3")

    irecv = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_IRECV_TYPE,
        sender=1,
        receiver=0,
        count=1,
        bytes=4,
    )

    wait = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_WAIT_TYPE,
        sender=0,
        receiver=0,
        count=1,
        bytes=0,
    )

    send1 = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_SEND_TYPE,
        sender=1,
        receiver=0,
        count=1,
        bytes=4,
    )

    send2 = require_one(
        trace["sections"][2]["small"],
        message_type=MPI_SEND_TYPE,
        sender=2,
        receiver=0,
        count=1,
        bytes=4,
    )

    require_ptp_pair(send1, irecv, "fortran_nonblocking_any_source_wait matched pair")
    require_same_value(send1, "tag", send2, "tag", "fortran_nonblocking_any_source_wait send tags")
    require_control_zero_or_from_refs(wait, irecv, "fortran_nonblocking_any_source_wait wait")

def validate_fortran_nonblocking_any_source_wait_f08(trace):
    require(trace["world_size"] == 3, "fortran_nonblocking_any_source_wait_f08: world size should be 3")

    irecv = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_IRECV_TYPE,
        sender=1,
        receiver=0,
        count=1,
        bytes=4,
    )

    wait = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_WAIT_TYPE,
        sender=0,
        receiver=0,
        count=1,
        bytes=0,
    )

    send1 = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_SEND_TYPE,
        sender=1,
        receiver=0,
        count=1,
        bytes=4,
    )

    send2 = require_one(
        trace["sections"][2]["small"],
        message_type=MPI_SEND_TYPE,
        sender=2,
        receiver=0,
        count=1,
        bytes=4,
    )

    require_ptp_pair(send1, irecv, "fortran_nonblocking_any_source_wait_f08 matched pair")
    require_same_value(send1, "tag", send2, "tag", "fortran_nonblocking_any_source_wait_f08 send tags")
    require_control_zero_or_from_refs(wait, irecv, "fortran_nonblocking_any_source_wait_f08 wait")

def validate_fortran_waitall(trace):
    require(trace["world_size"] == 2, "fortran_waitall: world size should be 2")

    sends = require_n(
        trace["sections"][0]["small"],
        2,
        message_type=MPI_ISEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    w0 = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_WAITALL_TYPE,
        sender=0,
        receiver=0,
        count=2,
        bytes=0,
    )

    recvs = require_n(
        trace["sections"][1]["small"],
        2,
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    w1 = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_WAITALL_TYPE,
        sender=1,
        receiver=1,
        count=2,
        bytes=0,
    )

    require_local_same_comm(sends, "fortran_waitall sends")
    require_local_same_comm(recvs, "fortran_waitall recvs")
    require_same_tag_multiset(sends, recvs, "fortran_waitall send/recv tags")

    require_control_zero_or_from_refs(w0, sends, "fortran_waitall rank0 waitall")
    require_control_zero_or_from_refs(w1, recvs, "fortran_waitall rank1 waitall")

def validate_fortran_waitall_f08(trace):
    require(trace["world_size"] == 2, "fortran_waitall_f08: world size should be 2")

    sends = require_n(
        trace["sections"][0]["small"],
        2,
        message_type=MPI_ISEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    w0 = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_WAITALL_TYPE,
        sender=0,
        receiver=0,
        count=2,
        bytes=0,
    )

    recvs = require_n(
        trace["sections"][1]["small"],
        2,
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    w1 = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_WAITALL_TYPE,
        sender=1,
        receiver=1,
        count=2,
        bytes=0,
    )

    require_local_same_comm(sends, "fortran_waitall_f08 sends")
    require_local_same_comm(recvs, "fortran_waitall_f08 recvs")
    require_same_tag_multiset(sends, recvs, "fortran_waitall_f08 send/recv tags")

    require_control_zero_or_from_refs(w0, sends, "fortran_waitall rank0 waitall_f08")
    require_control_zero_or_from_refs(w1, recvs, "fortran_waitall rank1 waitall_f08")

def validate_fortran_waitany(trace):
    require(trace["world_size"] == 2, "fortran_waitany: world size should be 2")

    sends = require_n(
        trace["sections"][0]["small"],
        2,
        message_type=MPI_SEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recvs = require_n(
        trace["sections"][1]["small"],
        2,
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    waitany = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_WAITANY_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    wait = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_WAIT_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    require_local_same_comm(sends, "fortran_waitany sends")
    require_local_same_comm(recvs, "fortran_waitany recvs")
    require_same_tag_multiset(sends, recvs, "fortran_waitany send/recv tags")

    require_control_zero_or_from_refs(waitany, recvs, "fortran_waitany control")
    require_control_zero_or_from_refs(wait, recvs, "fortran_waitany trailing wait")

def validate_fortran_waitany_f08(trace):
    require(trace["world_size"] == 2, "fortran_waitany_f08: world size should be 2")

    sends = require_n(
        trace["sections"][0]["small"],
        2,
        message_type=MPI_SEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recvs = require_n(
        trace["sections"][1]["small"],
        2,
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    waitany = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_WAITANY_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    wait = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_WAIT_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    require_local_same_comm(sends, "fortran_waitany_f08 sends")
    require_local_same_comm(recvs, "fortran_waitany_f08 recvs")
    require_same_tag_multiset(sends, recvs, "fortran_waitany_f08 send/recv tags")

    require_control_zero_or_from_refs(waitany, recvs, "fortran_waitany_f08 control")
    require_control_zero_or_from_refs(wait, recvs, "fortran_waitany_f08 trailing wait")


def validate_fortran_testall(trace):
    require(trace["world_size"] == 2, "fortran_testall: world size should be 2")

    sends = require_n(
        trace["sections"][0]["small"],
        2,
        message_type=MPI_SEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recvs = require_n(
        trace["sections"][1]["small"],
        2,
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    testall = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_TESTALL_TYPE,
        sender=1,
        receiver=1,
        count=2,
        bytes=0,
    )

    require_local_same_comm(sends, "fortran_testall sends")
    require_local_same_comm(recvs, "fortran_testall recvs")
    require_same_tag_multiset(sends, recvs, "fortran_testall send/recv tags")

    require_control_zero_or_from_refs(testall, recvs, "fortran_testall control")

def validate_fortran_testall_f08(trace):
    require(trace["world_size"] == 2, "fortran_testall_f08: world size should be 2")

    sends = require_n(
        trace["sections"][0]["small"],
        2,
        message_type=MPI_SEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recvs = require_n(
        trace["sections"][1]["small"],
        2,
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    testall = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_TESTALL_TYPE,
        sender=1,
        receiver=1,
        count=2,
        bytes=0,
    )

    require_local_same_comm(sends, "fortran_testall_f08 sends")
    require_local_same_comm(recvs, "fortran_testall_f08 recvs")
    require_same_tag_multiset(sends, recvs, "fortran_testall_f08 send/recv tags")

    require_control_zero_or_from_refs(testall, recvs, "fortran_testall_f08 control")

def validate_fortran_sendrecv(trace):
    require(trace["world_size"] == 2, "fortran_sendrecv: world size should be 2")

    r0 = require_one(
        trace["sections"][0]["large"],
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
        trace["sections"][1]["large"],
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

    require_comm_int(r0, "comm", "fortran_sendrecv rank0 comm")
    require_comm_int(r1, "comm", "fortran_ssendrecv rank1 comm")
    require_same_value(r0, "tag1", r1, "tag2", "fortran_ssendrecv rank0 sendtag vs rank1 recvtag")
    require_same_value(r0, "tag2", r1, "tag1", "fortran_ssendrecv rank0 recvtag vs rank1 sendtag")

def validate_fortran_sendrecv_f08(trace):
    require(trace["world_size"] == 2, "fortran_sendrecv_f08: world size should be 2")

    r0 = require_one(
        trace["sections"][0]["large"],
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
        trace["sections"][1]["large"],
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

    require_comm_int(r0, "comm", "fortran_sendrecv_f08 rank0 comm")
    require_comm_int(r1, "comm", "fortran_ssendrecv_f08 rank1 comm")
    require_same_value(r0, "tag1", r1, "tag2", "fortran_ssendrecv_f08 rank0 sendtag vs rank1 recvtag")
    require_same_value(r0, "tag2", r1, "tag1", "fortran_ssendrecv_f08 rank0 recvtag vs rank1 sendtag")

def validate_fortran_bsend(trace):
    require(trace["world_size"] == 2, "fortran_bsend: world size should be 2")

    bsend = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_BSEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recv = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_RECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    require_ptp_pair(bsend, recv, "fortran_bsend")

def validate_fortran_bsend_f08(trace):
    require(trace["world_size"] == 2, "fortran_bsend_f08: world size should be 2")

    bsend = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_BSEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recv = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_RECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    require_ptp_pair(bsend, recv, "fortran_bsend_f08")

def validate_fortran_ssend(trace):
    require(trace["world_size"] == 2, "fortran_ssend: world size should be 2")

    ssend = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_SSEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recv = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_RECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    require_ptp_pair(ssend, recv, "fortran_ssend")

def validate_fortran_ssend_f08(trace):
    require(trace["world_size"] == 2, "fortran_ssend_f08: world size should be 2")

    ssend = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_SSEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    recv = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_RECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    require_ptp_pair(ssend, recv, "fortran_ssend_f08")

def validate_fortran_rsend(trace):
    require(trace["world_size"] == 2, "fortran_rsend: world size should be 2")

    rsend = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_RSEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    irecv = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    wait = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_WAIT_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    require_ptp_pair(rsend, irecv, "fortran_rsend")
    require_control_zero_or_from_refs(wait, irecv, "fortran_rsend wait")

def validate_fortran_rsend_f08(trace):
    require(trace["world_size"] == 2, "fortran_rsend_f08: world size should be 2")

    rsend = require_one(
        trace["sections"][0]["small"],
        message_type=MPI_RSEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    irecv = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    wait = require_one(
        trace["sections"][1]["small"],
        message_type=MPI_WAIT_TYPE,
        sender=1,
        receiver=1,
        count=1,
        bytes=0,
    )

    require_ptp_pair(rsend, irecv, "fortran_rsend_f08")
    require_control_zero_or_from_refs(wait, irecv, "fortran_rsend_f08 wait")

def validate_fortran_barrier(trace):
    require(trace["world_size"] == 3, "fortran_barrier: world size should be 3")

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
        require_comm_int(rec, "comm", "fortran_barrier comm")

def validate_fortran_barrier_f08(trace):
    require(trace["world_size"] == 3, "fortran_barrier_f08: world size should be 3")

    for r in range(3):
        rec = require_one(
            trace["sections"][r]["small"],
            message_type=MPI_BARRIER_TYPE,
            sender=r,
            receiver=r,
            count=0,
            bytes=0,
        )
        require_zero(rec, "tag", "fortran_barrier_f08 tag")
        require_comm_int(rec, "comm", "fortran_barrier_f08 comm")

def validate_fortran_bcast(trace):
    require(trace["world_size"] == 4, "fortran_bcast: world size should be 4")

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
        require_comm_int(rec, "comm", "fortran_bcast comm")

def validate_fortran_bcast_f08(trace):
    require(trace["world_size"] == 4, "fortran_bcast_f08: world size should be 4")

    for r in range(4):
        rec = require_one(
            trace["sections"][r]["small"],
            message_type=MPI_BCAST_TYPE,
            sender=2,
            receiver=r,
            count=3,
            bytes=12,
        )
        require_zero(rec, "tag", "fortran_bcast_f08 tag")
        require_comm_int(rec, "comm", "fortran_bcast_f08 comm")

def validate_fortran_reduce(trace):
    require(trace["world_size"] == 4, "fortran_reduce: world size should be 4")

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
        require_comm_int(rec, "comm", "fortran_reduce comm")

def validate_fortran_reduce_f08(trace):
    require(trace["world_size"] == 4, "fortran_reduce_f08: world size should be 4")

    for r in range(4):
        rec = require_one(
            trace["sections"][r]["small"],
            message_type=MPI_REDUCE_TYPE,
            sender=r,
            receiver=1,
            count=5,
            bytes=20,
        )
        require_zero(rec, "tag", "fortran_reduce_f08 tag")
        require_comm_int(rec, "comm", "fortran_reduce_f08 comm")

def validate_fortran_allreduce(trace):
    require(trace["world_size"] == 4, "fortran_allreduce: world size should be 4")

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
        require_comm_int(rec, "comm", "fortran_allreduce comm")

def validate_fortran_allreduce_f08(trace):
    require(trace["world_size"] == 4, "fortran_allreduce_f08: world size should be 4")

    for r in range(4):
        rec = require_one(
            trace["sections"][r]["small"],
            message_type=MPI_ALLREDUCE_TYPE,
            sender=r,
            receiver=r,
            count=4,
            bytes=16,
        )
        require_zero(rec, "tag", "fortran_allreduce_f08 tag")
        require_comm_int(rec, "comm", "fortran_allreduce_f08 comm")

def validate_fortran_gather(trace):
    require(trace["world_size"] == 4, "fortran_gather: world size should be 4")

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
        require_large_zero_tags(rec, "fortran_gather")
        require_comm_int(rec, "comm", "fortran_gather comm")

def validate_fortran_gather_f08(trace):
    require(trace["world_size"] == 4, "fortran_gather_f08: world size should be 4")

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
        require_large_zero_tags(rec, "fortran_gather_f08")
        require_comm_int(rec, "comm", "fortran_gather_f08 comm")

def validate_fortran_scatter(trace):
    require(trace["world_size"] == 4, "fortran_scatter: world size should be 4")

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
        require_large_zero_tags(rec, "fortran_scatter")
        require_comm_int(rec, "comm", "fortran_scatter comm")

def validate_fortran_scatter_f08(trace):
    require(trace["world_size"] == 4, "fortran_scatter_f08: world size should be 4")

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
        require_large_zero_tags(rec, "fortran_scatter_f08")
        require_comm_int(rec, "comm", "fortran_scatter_f08 comm")

def validate_fortran_allgather(trace):
    require(trace["world_size"] == 4, "fortran_allgather: world size should be 4")

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
        require_large_zero_tags(rec, "fortran_allgather")
        require_comm_int(rec, "comm", "fortran_allgather comm")

def validate_fortran_allgather_f08(trace):
    require(trace["world_size"] == 4, "fortran_allgather_f08: world size should be 4")

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
        require_large_zero_tags(rec, "fortran_allgather_f08")
        require_comm_int(rec, "comm", "fortran_allgather_f08 comm")

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

    require(isinstance(wait["tag"], int), "fortran_cancel wait tag should be int")
    require(isinstance(wait["comm"], int), "fortran_cancel wait comm should be int")

    require_zero(b0, "tag", "fortran_cancel barrier rank0 tag")
    require_zero(b1, "tag", "fortran_cancel barrier rank1 tag")
    require_comm_int(b0, "comm", "fortran_cancel barrier rank0 comm")
    require_comm_int(b1, "comm", "fortran_cancel barrier rank1 comm")

def validate_fortran_cancel_f08(trace):
    require(trace["world_size"] == 2, "fortran_cancel_f08: world size should be 2")

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

    require(isinstance(wait["tag"], int), "fortran_cancel_f08 wait tag should be int")
    require(isinstance(wait["comm"], int), "fortran_cancel_f08 wait comm should be int")

    require_zero(b0, "tag", "fortran_cancel_f08 barrier rank0 tag")
    require_zero(b1, "tag", "fortran_cancel_f08 barrier rank1 tag")
    require_comm_int(b0, "comm", "fortran_cancel_f08 barrier rank0 comm")
    require_comm_int(b1, "comm", "fortran_cancel_f08 barrier rank1 comm")

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
    "init_finalize": validate_init_finalize,
    "bsend": validate_bsend,
    "rsend": validate_rsend,
    "ssend": validate_ssend,
    "barrier": validate_barrier,
    "allreduce": validate_allreduce,
    "cancel": validate_cancel,
    "fortran_init_finalize": validate_fortran_init_finalize,
    "fortran_init_finalize_f08": validate_fortran_init_finalize_f08,
    "fortran_nonblocking_wait": validate_fortran_nonblocking_wait,
    "fortran_nonblocking_wait_f08": validate_fortran_nonblocking_wait_f08,
    "fortran_nonblocking_any_source_wait": validate_fortran_nonblocking_any_source_wait,
    "fortran_nonblocking_any_source_wait_f08": validate_fortran_nonblocking_any_source_wait_f08,
    "fortran_waitall": validate_fortran_waitall,
    "fortran_waitall_f08": validate_fortran_waitall_f08,
    "fortran_waitany": validate_fortran_waitany,
    "fortran_waitany_f08": validate_fortran_waitany_f08,
    "fortran_testall": validate_fortran_testall,
    "fortran_testall_f08": validate_fortran_testall_f08,
    "fortran_bsend": validate_fortran_bsend,
    "fortran_bsend_f08": validate_fortran_bsend_f08,
    "fortran_ssend": validate_fortran_ssend,
    "fortran_ssend_f08": validate_fortran_ssend_f08,
    "fortran_rsend": validate_fortran_rsend,
    "fortran_rsend_f08": validate_fortran_rsend_f08,
    "fortran_sendrecv": validate_fortran_sendrecv,
    "fortran_sendrecv_f08": validate_fortran_sendrecv_f08,
    "fortran_barrier": validate_fortran_barrier,
    "fortran_barrier_f08": validate_fortran_barrier_f08,
    "fortran_bcast": validate_fortran_bcast,
    "fortran_bcast_f08": validate_fortran_bcast_f08,
    "fortran_reduce": validate_fortran_reduce,
    "fortran_reduce_f08": validate_fortran_reduce_f08,
    "fortran_allreduce": validate_fortran_allreduce,
    "fortran_allreduce_f08": validate_fortran_allreduce_f08,
    "fortran_gather": validate_fortran_gather,
    "fortran_gather_f08": validate_fortran_gather_f08,
    "fortran_scatter": validate_fortran_scatter,
    "fortran_scatter_f08": validate_fortran_scatter_f08,
    "fortran_allgather": validate_fortran_allgather,
    "fortran_allgather_f08": validate_fortran_allgather_f08,
    "fortran_cancel": validate_fortran_cancel,
    "fortran_cancel_f08": validate_fortran_cancel_f08,
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
    verify_lifecycle_bookends(trace)
    validate_common_trace(trace)
    VALIDATORS[args.case](trace)

    print("[PASS] {}".format(args.case))
    return 0

if __name__ == "__main__":
    sys.exit(main())

