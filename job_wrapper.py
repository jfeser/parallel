#!/usr/bin/env python

import argparse
import json
import logging
from multiprocessing.connection import Client
import os
import shutil
import socket
import subprocess

logging.basicConfig(level=logging.INFO)

SLICE_CGROUP = f"/sys/fs/cgroup/user.slice/user-{os.getuid()}.slice/user@{os.getuid()}.service/bench.slice"


def pre(name, out_dir):
    with open(f"{out_dir}/{name}.start", "w") as f:
        subprocess.run(["date", "+%s%N"], stdout=f)


def post(name, out_dir, temp_dir):
    with open(f"{out_dir}/{name}.stop", "w") as f:
        subprocess.run(["date", "+%s%N"], stdout=f)

    mem_stat = f"{SLICE_CGROUP}/{name}.service/memory.stat"
    try:
        shutil.copyfile(
            mem_stat,
            f"{out_dir}/{name}.mem",
        )
    except FileNotFoundError:
        logging.warning(f"{mem_stat} not found")

    try:
        shutil.copyfile(
            f"{SLICE_CGROUP}/{name}.service/cpu.stat",
            f"{out_dir}/{name}.cpu",
        )
    except FileNotFoundError:
        logging.warning("cpu.stat not found")

    with open(f"{out_dir}/{name}.env", "w") as f:
        json.dump(dict(os.environ), f)

    with Client(f"{temp_dir}/control.sock") as conn:
        conn.send(name)


def main():
    parser = argparse.ArgumentParser(
        description="Wrapper script that runs before and after a benchmark job."
    )
    parser.add_argument("--mode")
    parser.add_argument(
        "-o",
        "--output",
        default=os.getcwd(),
        help="final directory for job output (default: cwd)",
    )
    parser.add_argument(
        "-t",
        "--temp",
        help="temporary directory for job output",
    )
    parser.add_argument(
        "-n",
        "--name",
        help="job name",
    )
    args = parser.parse_args()

    if args.mode == "pre":
        pre(args.name, args.output)
    elif args.mode == "post":
        post(args.name, args.output, args.temp)
    else:
        logging.error(f"Unknown mode: {mode}")
        exit(1)


if __name__ == "__main__":
    main()
