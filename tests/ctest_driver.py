#!/usr/bin/env python3

import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

from trace_parser import parse_trace

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


def validate_send_recv(trace):
    require(trace["world_size"] == 2, "send_recv: world size should be 2")

    rank0 = trace["sections"][0]
    rank1 = trace["sections"][1]

    require_one(
        rank0["small"],
        message_type=MPI_SEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    require_one(
        rank1["small"],
        message_type=MPI_RECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )


def validate_any_source(trace):
    require(trace["world_size"] == 3, "any_source: world size should be 3")

    rank0 = trace["sections"][0]
    rank1 = trace["sections"][1]
    rank2 = trace["sections"][2]

    require_one(
        rank0["small"],
        message_type=MPI_RECV_TYPE,
        sender=1,
        receiver=0,
        count=1,
        bytes=4,
    )

    require_one(
        rank1["small"],
        message_type=MPI_SEND_TYPE,
        sender=1,
        receiver=0,
        count=1,
        bytes=4,
    )

    require_one(
        rank2["small"],
        message_type=MPI_SEND_TYPE,
        sender=2,
        receiver=0,
        count=1,
        bytes=4,
    )


def validate_sendrecv(trace):
    require(trace["world_size"] == 2, "sendrecv: world size should be 2")

    rank0 = trace["sections"][0]
    rank1 = trace["sections"][1]

    require_one(
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

    require_one(
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


def validate_subcomm_send(trace):
    require(trace["world_size"] == 4, "subcomm_send: world size should be 4")

    require_one(
        trace["sections"][0]["small"],
        message_type=MPI_SEND_TYPE,
        sender=0,
        receiver=2,
        count=1,
        bytes=4,
    )
    require_one(
        trace["sections"][2]["small"],
        message_type=MPI_RECV_TYPE,
        sender=0,
        receiver=2,
        count=1,
        bytes=4,
    )

    require_one(
        trace["sections"][1]["small"],
        message_type=MPI_SEND_TYPE,
        sender=1,
        receiver=3,
        count=1,
        bytes=4,
    )
    require_one(
        trace["sections"][3]["small"],
        message_type=MPI_RECV_TYPE,
        sender=1,
        receiver=3,
        count=1,
        bytes=4,
    )


def validate_collectives(trace):
    require(trace["world_size"] == 4, "collectives: world size should be 4")

    for r in range(4):
        small = trace["sections"][r]["small"]
        large = trace["sections"][r]["large"]

        require_one(
            small,
            message_type=MPI_BCAST_TYPE,
            sender=2,
            receiver=r,
            count=3,
            bytes=12,
        )

        require_one(
            small,
            message_type=MPI_REDUCE_TYPE,
            sender=r,
            receiver=1,
            count=5,
            bytes=20,
        )

        require_one(
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

        require_one(
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

        require_one(
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


def validate_nonblocking(trace):
    require(trace["world_size"] == 2, "nonblocking: world size should be 2")

    rank0 = trace["sections"][0]
    rank1 = trace["sections"][1]

    require_n(
        rank0["small"],
        2,
        message_type=MPI_ISEND_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    require_n(
        rank1["small"],
        2,
        message_type=MPI_IRECV_TYPE,
        sender=0,
        receiver=1,
        count=1,
        bytes=4,
    )

    require_one(
        rank0["small"],
        message_type=MPI_WAITALL_TYPE,
        sender=0,
        receiver=0,
        count=2,
        bytes=0,
    )

    require_one(
        rank1["small"],
        message_type=MPI_WAITALL_TYPE,
        sender=1,
        receiver=1,
        count=2,
        bytes=0,
    )


VALIDATORS = {
    "send_recv": validate_send_recv,
    "any_source": validate_any_source,
    "sendrecv": validate_sendrecv,
    "subcomm_send": validate_subcomm_send,
    "collectives": validate_collectives,
    "nonblocking": validate_nonblocking,
}


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
    args = parser.parse_args()

    if args.case not in VALIDATORS:
        print("Unknown case: {}".format(args.case), file=sys.stderr)
        return 2

    work_dir = Path(args.work_dir)
    if work_dir.exists():
        shutil.rmtree(str(work_dir))
    work_dir.mkdir(parents=True)

    env = os.environ.copy()
    old_preload = env.get("LD_PRELOAD", "")
    if old_preload:
        env["LD_PRELOAD"] = "{}:{}".format(args.trace_lib, old_preload)
    else:
        env["LD_PRELOAD"] = args.trace_lib

    cmd = [args.launcher]
    cmd.extend(shlex.split(args.launcher_extra_args))
    cmd.extend([args.nflag, str(args.nprocs), args.exe])

    run(cmd, cwd=str(work_dir), env=env)

    traces = sorted(work_dir.glob("*.mpic"))
    require(
        len(traces) == 1,
        "{}: expected one .mpic file, found {}".format(args.case, len(traces))
    )

    trace = parse_trace(traces[0])
    VALIDATORS[args.case](trace)

    print("[PASS] {}".format(args.case))
    return 0


if __name__ == "__main__":
    sys.exit(main())

