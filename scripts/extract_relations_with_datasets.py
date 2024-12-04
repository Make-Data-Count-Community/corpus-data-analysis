# -*- coding: utf-8 -*-

DESCRIPTION = """go through the openaire relations data files and extract the relations involving datasets, limiting to certain relationship types. output to gzipped JSON files"""

import sys, os, time
from pathlib import Path
from datetime import datetime
from timeit import default_timer as timer
from typing import Union, List, Optional, Dict, Set
import tarfile
import gzip
import json
import pickle
from tqdm import tqdm
from joblib import Parallel, delayed

try:
    from humanfriendly import format_timespan
except ImportError:

    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)


import pandas as pd
import numpy as np

import logging

root_logger = logging.getLogger()
logger = root_logger.getChild(__name__)

MAX_FILE_SIZE = 5 * 1024**3  # 5GB uncompressed


def get_dataset_ids(datadir: Union[str, Path]) -> Set[str]:
    files = list(datadir.glob("df_openaire_types_all*.parquet"))
    df = pd.read_parquet(files, columns=["openaire_id", "openaire_type"])
    ids_set = set(df[df["openaire_type"] == "dataset"]["openaire_id"].values)
    return ids_set


def extract_from_one_file(
    file: Union[str, Path],  # .tar file containing gzipped json-lines files
    outdir: Union[str, Path],
    datadir: Union[str, Path],
    ids_set: Set[str],
    max_file_size: int = MAX_FILE_SIZE,
) -> None:
    fp = Path(file)
    outdir = Path(outdir)
    datadir = Path(datadir)
    part_file_number = 0
    part_file = None  # Initialize as None
    total_size = 0
    # ids_set = get_dataset_ids(datadir)

    logger.debug(f"starting processing for file: {fp}")

    if part_file is None:
        part_file_path = outdir.joinpath(
            f"{fp.stem}_datasets_part_{part_file_number:03}.gz"
        )
        logger.debug(f"opening file for write: {part_file_path}")
        part_file = gzip.open(part_file_path, "wt")

    with tarfile.open(fp, "r") as tar:
        members = list(tar.getmembers())
        for member in members:
            with tar.extractfile(member) as f:
                with gzip.GzipFile(fileobj=f) as gf:
                    for line in gf:
                        if line:
                            record = json.loads(line)

                            if (
                                record["relType"]["name"]
                                in [
                                    "Cites",
                                    "IsCitedBy",
                                    "IsReferencedBy",
                                    "References",
                                    "IsSupplementedBy",
                                    "IsSupplementTo",
                                ]
                            ) and (
                                record["source"] in ids_set
                                or record["target"] in ids_set
                            ):
                                out_line = json.dumps(record) + "\n"
                                line_size = len(out_line.encode("utf-8"))
                                # If this line will make the file exceed the max size, close the current file and open a new one
                                if total_size + line_size > max_file_size:
                                    part_file.close()
                                    part_file_number += 1
                                    part_file_path = outdir.joinpath(
                                        f"{fp.stem}_datasets_part_{part_file_number:03}.gz"
                                    )
                                    logger.debug(
                                        f"opening file for write: {part_file_path}"
                                    )
                                    part_file = gzip.open(part_file_path, "wt")
                                    total_size = 0

                                part_file.write(out_line)
                                total_size += line_size

    if part_file is not None:
        part_file.close()
    logger.debug(f"finished processing file: {fp}")


def main(args):
    datadir = Path(args.datadir)
    raw_data_files = list(datadir.rglob("relation_*.tar"))
    logger.info(f"found {len(raw_data_files)} raw data files")
    outdir = Path(args.outdir)
    if not outdir.exists():
        logger.debug(f"creating directory: {outdir}")
        outdir.mkdir()
    n_jobs = args.n_jobs
    logger.info(f"loading file: {args.ids_set}")
    ids_set = pickle.loads(Path(args.ids_set).read_bytes())
    logger.debug(f"ids_set contains {len(ids_set)} ids")

    parallel_args = [
        [(fp,), {"outdir": outdir, "datadir": datadir, "ids_set": ids_set}]
        for fp in raw_data_files
    ]
    logger.info(
        f"running {len(parallel_args)} jobs in total -- number of parallel jobs at a time: {n_jobs}"
    )
    Parallel(n_jobs=n_jobs, verbose=1000)(
        delayed(extract_from_one_file)(*args, **kwargs)
        for args, kwargs in parallel_args
    )


if __name__ == "__main__":
    total_start = timer()
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(name)s.%(lineno)d %(levelname)s : %(message)s",
            datefmt="%H:%M:%S",
        )
    )
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)
    logger.info(" ".join(sys.argv))
    logger.info("{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
    logger.info("pid: {}".format(os.getpid()))
    import argparse

    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument("datadir", help="input data directory")
    parser.add_argument("outdir", help="output directory")
    parser.add_argument("ids_set", help="file (.pickle) with ids to match")
    parser.add_argument(
        "--n-jobs", default=1, help="number of parallel jobs to run (default: 1)"
    )
    parser.add_argument("--debug", action="store_true", help="output debugging info")
    global args
    args = parser.parse_args()
    if args.debug:
        root_logger.setLevel(logging.DEBUG)
        logger.debug("debug mode is on")
    main(args)
    total_end = timer()
    logger.info(
        "all finished. total time: {}".format(format_timespan(total_end - total_start))
    )
