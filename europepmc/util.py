# -*- coding: utf-8 -*-

from typing import Union, Set, Generator, Iterable
from pathlib import Path

import io

import pandas as pd
import numpy as np


def load_accession_data(url="https://europepmc.org/ftp/TextMinedTerms") -> pd.DataFrame:
    import requests
    from bs4 import BeautifulSoup

    r = requests.get(url)

    # Parse the HTML
    soup = BeautifulSoup(r.text, "html.parser")

    # Extract all '.csv' filenames
    csv_files = [
        a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".csv")
    ]
    # print(f"{len(csv_files)} csv files found")

    _dfs = []
    for filename in csv_files:
        csv_url = f"{url}/{filename}"
        print(csv_url)
        r = requests.get(csv_url)
        if r.status_code != 200:
            print("ERROR. returning response instead")
            return r
        this_df = pd.read_csv(io.StringIO(r.text))
        colname = filename.replace(".csv", "")
        this_df["repository_europepmc"] = colname
        this_df = this_df.rename(columns={colname: "accession_number"})
        _dfs.append(this_df)
    df_acc = pd.concat(_dfs)
    df_acc["repository_europepmc"] = df_acc["repository_europepmc"].astype("category")
    return df_acc
