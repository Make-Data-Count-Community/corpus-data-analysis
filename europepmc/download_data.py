# -*- coding: utf-8 -*-

from typing import Union, Set, Generator, Iterable
from pathlib import Path

import requests
import backoff
from bs4 import BeautifulSoup
import io

import pandas as pd
import numpy as np

URL_ACCESSION_DATA = "https://europepmc.org/ftp/TextMinedTerms"


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_time=60)
def make_request(url: str, method="GET", **kwargs) -> requests.Request:
    return requests.request(method=method, url=url, **kwargs)


def yield_accession_data(
    url=URL_ACCESSION_DATA, debug=False
) -> Generator[pd.DataFrame, None, None]:
    r = make_request(url)

    # Parse the HTML
    soup = BeautifulSoup(r.text, "html.parser")

    # Extract all '.csv' filenames
    csv_files = [
        a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".csv")
    ]
    # print(f"{len(csv_files)} csv files found")

    for filename in csv_files:
        csv_url = f"{url}/{filename}"
        print(csv_url)
        r = make_request(csv_url)
        # r.raise_for_status()
        if "text/csv" not in r.headers.get("content-type", default="").lower():
            if debug is True:
                yield csv_url, r
            else:
                print(f"skipping {csv_url} because invalid content-type header")
            continue
        this_df = pd.read_csv(io.StringIO(r.text), dtype="string")
        colname = filename.replace(".csv", "")
        this_df["repository_europepmc"] = colname
        this_df = this_df.rename(columns={colname: "accession_number"})
        yield this_df


def load_accession_data(url=URL_ACCESSION_DATA) -> pd.DataFrame:
    _dfs = []
    for _df in yield_accession_data(url=url):
        _dfs.append(_df)
    df_acc = pd.concat(_dfs).reset_index(drop=True)
    df_acc["repository_europepmc"] = df_acc["repository_europepmc"].astype("category")
    return df_acc
