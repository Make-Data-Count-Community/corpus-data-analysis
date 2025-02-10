# -*- coding: utf-8 -*-

DESCRIPTION = (
    """collect doi data from openalex api (using filters instead of list of dois)"""
)

import sys, os, time
import gzip
import json
from pathlib import Path
from datetime import datetime
from timeit import default_timer as timer

try:
    from humanfriendly import format_timespan
except ImportError:

    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)


from openalex_utils import paginate_openalex
from clean_doi import clean_doi

import logging

root_logger = logging.getLogger()
logger = root_logger.getChild(__name__)


def doi_clean_for_api(doi: str) -> str:
    doi = doi.replace("&", "")
    doi = doi.replace(",", "")
    return doi


def main(args):
    outdir = Path(args.outdir)
    if not outdir.exists():
        logger.debug(f"creating output directory: {outdir}")
        outdir.mkdir()
    select = [
        "id",
        "doi",
        "ids",
        "title",
        "publication_date",
        "type",
        "type_crossref",
        "indexed_in",
        "open_access",
        "authorships",
        "institutions_distinct_count",
        "fwci",
        "cited_by_count",
        "is_retracted",
        "topics",
        "concepts",
        "best_oa_location",
        "grants",
        "datasets",
        "referenced_works",
        "updated_date",
        "created_date",
    ]
    params = {
        "filter": args.filter,
        "select": ",".join(select),
    }
    if args.mailto:
        params["mailto"] = args.mailto

    file_idx = 0
    while True:
        outfp = outdir.joinpath(f"openalex_works_{file_idx:02}.gz")
        if outfp.exists():
            file_idx += 1
        else:
            break
    logger.info(f"Writing to file: {outfp}...")
    outfile = gzip.open(outfp, mode="wt")

    try:
        num_written = 0
        num_written_this_file = 0
        logger.info(f"Starting API queries, using filter: {args.filter})")
        url = "https://api.openalex.org/works"
        for r in paginate_openalex(url, params=params):
            r.raise_for_status()
            for work in r.json()["results"]:
                outfile.write(f"{json.dumps(work)}\n")
                num_written += 1
                num_written_this_file += 1
                if (
                    num_written in [5, 25, 100, 1000, 10000, 20000, 30000, 40000]
                    or num_written % 50000 == 0
                ):
                    logger.info(f"Collected {num_written} works so far")
            if num_written_this_file >= 200000:
                logger.info(
                    f"Collected {num_written} works so far. closing file {outfp}"
                )
                outfile.close()
                file_idx += 1
                outfp = outdir.joinpath(f"openalex_works_{file_idx:02}.gz")
                logger.info(f"writing to file: {outfp}...")
                outfile = gzip.open(outfp, mode="wt")
                num_written_this_file = 0

    finally:
        logger.info(f"closing file: {outfp}")
        outfile.close()
        logger.info(f"Collection finished. Collected {num_written} works.")


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
    parser.add_argument(
        "filter",
        help="filters to use (e.g. 'authorships.institutions.lineage:I111979921|I188538660,publication_year:2013-2024')",
    )
    parser.add_argument("--outdir", default=".", help="path to output directory")
    parser.add_argument(
        "--mailto",
        help="email to include as an identifier in the calls to the OpenAlex API",
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
