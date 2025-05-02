# -*- coding: utf-8 -*-

DESCRIPTION = """download the data from the GEO database for all of the accession numbers present in the Data Citation Corpus"""

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


def main(args):
    corpus_dir = Path(args.corpus_dir)
    outfp = Path(args.output)
    logger.info(f"loading corpus data from {corpus_dir}")
    files = list(corpus_dir.glob("*.json"))
    df = pd.concat(pd.read_json(fp) for fp in files).set_index(
        "id", verify_integrity=True
    )
    rep = df["repository"].apply(lambda x: x.get("title", None))
    df_geo = df[rep == "Gene Expression Omnibus (GEO)"]
    accession_numbers = df_geo["dataset"].str.upper().drop_duplicates().values
    logger.info(
        f"found {len(df_geo)} citations to GEO data in the Corpus, with {len(accession_numbers)} unique accession numbers"
    )

    logger.info(f"opening file for write: {outfp}")
    with outfp.open("w") as outf:
        i = 0
        for acc in accession_numbers:
            api_url = f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={acc.lower()}&targ=self&view=brief&form=text"
            r = make_request(api_url)
            line = {
                "acc_no": acc,
                "api_response": r.text,
            }
            outf.write(f"{json.dumps(line)}\n")
            i += 1
            if i in [5, 10, 20, 50, 100] or i % 500 == 0:
                logger.info(f"downloaded {i} entries so far. The last accession number downloaded was {acc}")
        logger.info(f"finished downloading {i} entries. The last accession number downloaded was {acc}")


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
        "corpus_dir", help="path to the corpus data (directory with json files)"
    )
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
