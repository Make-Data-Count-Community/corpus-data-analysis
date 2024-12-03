# -*- coding: utf-8 -*-

DESCRIPTION = """process OpenAIRE graph data files, collecting the type and doi"""

# process OpenAIRE graph data files, collecting the type and doi
# save output in chunks, deleting as we go to free up memory

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


def write_types_df_part_file(
    rows_types: Dict,
    outfp_types_idx: int,
    path_to_tarfile: Union[str, Path],
    output_basedir: Path,
) -> None:
    # write types df part file
    path_to_tarfile = Path(path_to_tarfile)
    outfp = output_basedir.joinpath(
        f"df_openaire_types_fromfile_{path_to_tarfile.stem}_part{outfp_types_idx:02}.parquet"
    )
    out_df = pd.DataFrame(rows_types)
    logger.debug(f"writing {len(out_df)} rows to {outfp}")
    out_df.to_parquet(outfp)
    del out_df


def write_doi_df_part_file(
    rows_dois: Dict,
    outfp_dois_idx: int,
    path_to_tarfile: Union[str, Path],
    output_basedir: Path,
) -> None:
    # write doi df part file
    path_to_tarfile = Path(path_to_tarfile)
    outfp = output_basedir.joinpath(
        f"df_openaire_dois_fromfile_{path_to_tarfile.stem}_part{outfp_dois_idx:02}.parquet"
    )
    out_df = pd.DataFrame(rows_dois)
    logger.debug(f"writing {len(out_df)} rows to {outfp}")
    out_df.to_parquet(outfp)
    del out_df


def process_one_tarfile(
    path_to_tarfile: Union[str, Path],
    outdir: Union[str, Path],
    chunksize: int = 10000000,
) -> None:
    rows_types = []
    outfp_types_idx = 0
    rows_dois = []
    outfp_dois_idx = 0
    with tarfile.open(path_to_tarfile, "r") as tar:
        members = list(tar.getmembers())
        logger.debug(f"file {path_to_tarfile} has {len(members)} members")
        for member in members:
            with tar.extractfile(member) as f:
                with gzip.GzipFile(fileobj=f) as gf:
                    for line in gf:
                        if line:
                            record = json.loads(line)
                            rows_types.append(
                                {
                                    "openaire_id": record["id"],
                                    "openaire_type": record["type"],
                                    "openaire_filename": path_to_tarfile.name,
                                    "tarfile_member": member.name,
                                }
                            )
                            pid = record.get("pid", [])
                            for item in pid:
                                if item.get("scheme") == "doi":
                                    rows_dois.append(
                                        {
                                            "openaire_id": record["id"],
                                            "doi": item.get("value"),
                                        }
                                    )
            if len(rows_types) > chunksize:
                write_types_df_part_file(
                    rows_types,
                    outfp_types_idx,
                    path_to_tarfile=path_to_tarfile,
                    output_basedir=outdir,
                )
                rows_types = []
                outfp_types_idx += 1

                write_doi_df_part_file(
                    rows_dois,
                    outfp_dois_idx,
                    path_to_tarfile=path_to_tarfile,
                    output_basedir=outdir,
                )
                rows_dois = []
                outfp_dois_idx += 1

    # finished with this tarfile, write the final rows
    write_types_df_part_file(
        rows_types,
        outfp_types_idx,
        path_to_tarfile=path_to_tarfile,
        output_basedir=outdir,
    )
    write_doi_df_part_file(
        rows_dois,
        outfp_dois_idx,
        path_to_tarfile=path_to_tarfile,
        output_basedir=outdir,
    )


def main(args):
    datadir = Path(args.datadir)
    raw_data_files = list(datadir.glob("*.tar"))
    for ignore in [
        "communities_infrastructures",
        "datasource",
        "organization",
        "project",
        "relation",
    ]:
        raw_data_files = [x for x in raw_data_files if not x.name.startswith(ignore)]
    logger.info(f"found {len(raw_data_files)} raw data files")
    outdir = Path(args.outdir)
    chunksize = args.chunksize
    n_jobs = args.n_jobs

    # def process_one_tarfile(
    #     path_to_tarfile: Union[str, Path],
    #     outdir: Union[str, Path],
    #     chunksize: int = 10000000,
    # ) -> None:
    parallel_args = [
        [(fp,), {"outdir": outdir, "chunksize": chunksize}] for fp in raw_data_files
    ]
    logger.info(
        f"running {len(parallel_args)} jobs in parallel -- number of parallel jobs: {n_jobs}"
    )
    Parallel(n_jobs=n_jobs, verbose=100)(
        delayed(process_one_tarfile)(*args, **kwargs) for args, kwargs in parallel_args
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
    parser.add_argument(
        "--chunksize",
        type=int,
        default=10000000,
        help="save part files when we hit this number of rows",
    )
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
