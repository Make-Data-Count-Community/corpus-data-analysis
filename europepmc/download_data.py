# -*- coding: utf-8 -*-

from typing import Union, Set, Generator, Iterable
from pathlib import Path

import requests
import backoff
from bs4 import BeautifulSoup
import io

import pandas as pd
import numpy as np

import logging

logger = logging.getLogger().getChild(__name__)

URL_ACCESSION_DATA = "https://europepmc.org/ftp/TextMinedTerms"


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_time=60)
def make_request(url: str, method="GET", **kwargs) -> requests.Request:
    r = requests.request(method=method, url=url, **kwargs)
    r.raise_for_status()
    return r


def accession_csv_to_dataframe(
    filepath_or_buffer, filename_or_repository_name: str
) -> pd.DataFrame:
    df = pd.read_csv(filepath_or_buffer, dtype="string")
    colname = filename_or_repository_name.replace(".csv", "")
    df["repository_europepmc"] = colname
    df = df.rename(columns={colname: "accession_number"})
    return df


def yield_accession_data(url=URL_ACCESSION_DATA) -> Generator[pd.DataFrame, None, None]:
    r = make_request(url)

    # Parse the HTML
    soup = BeautifulSoup(r.text, "html.parser")

    # Extract all '.csv' filenames
    csv_files = [
        a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".csv")
    ]
    logger.debug(f"{len(csv_files)} csv files found")

    for filename in csv_files:
        csv_url = f"{url}/{filename}"
        r = make_request(csv_url)
        if "text/csv" not in r.headers.get("content-type", default="").lower():
            logger.error(f"skipping {csv_url} because invalid content-type header")
            continue
        this_df = accession_csv_to_dataframe(io.StringIO(r.text), filename)
        yield this_df


def prepare_concatenated_dataframe(dfs: Iterable[pd.DataFrame]) -> pd.DataFrame:
    df = pd.concat(dfs).reset_index(drop=True)
    df["repository_europepmc"] = df["repository_europepmc"].astype("category")
    return df


def postprocess_europepmc_accession_data(df_acc: pd.DataFrame) -> pd.DataFrame:
    logger.debug("dropping duplicate rows...")
    df_acc = df_acc.drop_duplicates(subset=["PMCID", "accession_number"])
    logger.debug(
        f"EuropePMC data has {len(df_acc)} rows. {df_acc['PMCID'].isna().sum()} are missing PMCID. Dropping these..."
    )
    df_acc = df_acc.dropna(subset=["PMCID"])
    logger.debug(f"New row count: {len(df_acc)}")
    return df_acc


def load_accession_data(url=URL_ACCESSION_DATA, postprocess: bool = True) -> pd.DataFrame:
    _dfs = []
    for _df in yield_accession_data(url=url):
        _dfs.append(_df)
    df_acc = prepare_concatenated_dataframe(_dfs)
    if postprocess is True:
        df_acc = postprocess_europepmc_accession_data(df_acc)
    return df_acc
