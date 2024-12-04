# -*- coding: utf-8 -*-

from typing import Union, Set, Generator, Iterable
from pathlib import Path


def yield_dataset_ids(files: Iterable[Path], openaire_type="dataset") -> Generator[Set, None, None]:
    import pandas as pd

    for fp in files:
        _df = pd.read_parquet(fp, columns=["openaire_id", "openaire_type"])
        yield set(_df[_df["openaire_type"] == openaire_type]["openaire_id"].values)


def get_dataset_ids(
    datadir: Union[str, Path],
    glob_pattern: str = "df_openaire_types*.parquet",
    openaire_type="dataset",
) -> Set:
    datadir = Path(datadir)
    openaire_type_files = list(datadir.glob(glob_pattern))
    ids_set = set()
    for ids_set_from_one_file in yield_dataset_ids(openaire_type_files, openaire_type=openaire_type):
        ids_set.update(ids_set_from_one_file)
    return ids_set
