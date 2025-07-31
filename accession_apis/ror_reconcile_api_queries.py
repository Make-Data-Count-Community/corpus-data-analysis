# -*- coding: utf-8 -*-

DESCRIPTION = """query the ROR api for affiliation matches"""

import sys, os, time
from pathlib import Path
from datetime import datetime
from timeit import default_timer as timer
import json
import requests
import backoff

try:
    from humanfriendly import format_timespan
except ImportError:

    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)


import logging

root_logger = logging.getLogger()
logger = root_logger.getChild(__name__)

import pandas as pd
import numpy as np


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_time=300)
def make_request(url: str, method="GET", **kwargs) -> requests.Request:
    r = requests.request(method=method, url=url, **kwargs)
    r.raise_for_status()
    return r


def concatenate_row(row: pd.Series) -> str:
    row = row.fillna(value="")
    q = row["contact_institute"].strip()
    if row["contact_city"].strip():
        q += f""", {row["contact_city"].strip()}"""
    if row["contact_state"].strip():
        q += f""", {row["contact_state"].strip()}"""
    if row["contact_country"].strip():
        q += f""", {row["contact_country"].strip()}"""
    return q


def get_result(idx, q, url) -> dict:
    params = {
        "affiliation": q,
    }
    item = {
        "idx": idx,
        "q": q,
    }
    r = make_request("https://api.ror.org/v2/organizations", params=params)
    if r.json()["items"]:
        first_result = r.json()["items"][0]
        item["first_result_ror_id"] = first_result["organization"]["id"]
        item["first_result_name"] = first_result["organization"]["names"][0]["value"]
        item["first_result_chosen"] = first_result["chosen"]
        item["first_result_score"] = first_result["score"]
        item["first_result_substring"] = first_result["substring"]
    return item


def main(args):
    filename = args.input
    outfp = Path(args.output)
    logger.info(f"loading input data from {filename}")
    df_geo_affil_dedup = pd.read_csv(filename)
    logger.info(f"there are {len(df_geo_affil_dedup)} affiliation rows")
    logger.info(f"opening file for write: {outfp}")
    with outfp.open("w") as outf:
        i = 0
        url = "https://api.ror.org/v2/organizations"
        logger.info(f"starting queries to url: {url}")
        for idx, row in df_geo_affil_dedup.iterrows():
            q = concatenate_row(row)
            item = get_result(idx, q, url)
            outf.write(f"{json.dumps(item)}\n")
            i += 1
            if i in [5, 10, 20, 50, 100] or i % 500 == 0:
                logger.info(
                    f"processed {i} / {len(df_geo_affil_dedup)} entries so far."
                )
        logger.info(f"finished processing {i} / {len(df_geo_affil_dedup)} entries.")


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
    parser.add_argument("input", help="path to csv file")
    parser.add_argument("output", help="path to the output file (JSON-lines)")
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
