# -*- coding: utf-8 -*-

DESCRIPTION = """convert the Data Citation Corpus from JSON-lines to parquet"""

import sys, os, time
from pathlib import Path
import json
from datetime import datetime
from timeit import default_timer as timer
from typing import Tuple

try:
    from humanfriendly import format_timespan
except ImportError:

    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)


import pandas as pd
import numpy as np

import logging

from clean_doi import clean_doi, NoDoiException

root_logger = logging.getLogger()
logger = root_logger.getChild(__name__)


def try_clean_doi(doi: str) -> Tuple[str, bool]:
    # if clean_doi is unsuccessful, return False indicating that the input was not actually a doi
    try:
        return clean_doi(doi), True
    except NoDoiException:
        return doi, False


def load_corpus_data(path_to_corpus: Path, glob_pattern="*.json") -> pd.DataFrame:
    files = list(path_to_corpus.glob(glob_pattern))
    data = []
    dtype_dict = {
        "id": "string",
        "title": "string",
        "publisher": "category",
        "journal": "category",
        "repository": "category",
        "publication": "string",
        "dataset": "string",
        "source": "category",
    }
    for fp in files:
        txt = fp.read_text()
        records = json.loads(txt)
        for r in records:
            publication, publication_is_doi = try_clean_doi(r["publication"])
            dataset, dataset_is_doi = try_clean_doi(r["dataset"])
            data.append(
                {
                    "id": r["id"],
                    "title": r["title"],
                    "publisher": r.get("publisher", {}).get("title"),
                    "journal": r.get("journal", {}).get("title"),
                    "repository": r.get("repository", {}).get("title"),
                    "publication": publication,
                    "dataset": dataset,
                    "publication_is_doi": publication_is_doi,
                    "dataset_is_doi": dataset_is_doi,
                    "publishedDate": r.get("publishedDate"),
                    "source": r["source"],
                    "affiliations": r.get("affiliations"),
                    "funders": r.get("funders"),
                    "subjects": r.get("subjects"),
                }
            )
    df_corpus = pd.DataFrame(data)
    for col, dtype in dtype_dict.items():
        df_corpus[col] = df_corpus[col].astype(dtype)
    df_corpus["publishedDate"] = pd.to_datetime(df_corpus["publishedDate"])
    df_corpus = df_corpus.set_index("id", verify_integrity=True)
    return df_corpus


def main(args):
    path_to_corpus = Path(args.path_to_corpus)
    outfp = Path(args.output)
    logger.info(f"loading corpus data from {path_to_corpus}")
    df_corpus = load_corpus_data(path_to_corpus)
    logger.info(f"writing dataframe with shape {df_corpus.shape} to file: {outfp}")
    df_corpus.to_parquet(outfp)


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
        "path_to_corpus", help="directory with Data Citation Corpus data"
    )
    parser.add_argument("output", help="path to output file")
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
