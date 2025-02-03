# -*- coding: utf-8 -*-

DESCRIPTION = """collect all institutions data from openalex api"""

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

import logging

root_logger = logging.getLogger()
logger = root_logger.getChild(__name__)


def doi_clean_for_api(doi: str) -> str:
    doi = doi.replace("&", "")
    doi = doi.replace(",", "")
    return doi


def main(args):
    outfp = Path(args.output)
    select = [
        "id",
        "ror",
        "display_name",
        "country_code",
        "type",
        "lineage",
        "image_url",
        "display_name_acronyms",
        "display_name_alternatives",
        "works_count",
        "geo",
        "is_super_system",
        "updated_date",
        "created_date",
    ]
    params = {
        "select": ",".join(select),
    }
    if args.mailto:
        params["mailto"] = args.mailto

    logger.info(f"Writing to file: {outfp}...")
    outfile = gzip.open(outfp, mode="wt")

    try:
        url = "https://api.openalex.org/institutions"
        logger.info(f"Starting API queries for all institutions (url: {url})")
        num_written = 0
        for r in paginate_openalex(url, params=params):
            r.raise_for_status()
            for institution in r.json()["results"]:
                outfile.write(f"{json.dumps(institution)}\n")
                num_written += 1

    finally:
        logger.info(f"finished collecting data for {num_written} institutions")
        logger.info(f"closing file: {outfp}")
        outfile.close()


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
        "output",
        default="./openalex_institutions.gz",
        help="path to output file (.gz). default is openalex_institutions.gz",
    )
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
