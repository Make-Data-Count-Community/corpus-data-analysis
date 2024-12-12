# -*- coding: utf-8 -*-

DESCRIPTION = """gather data for dataset relations."""
# At the end, we should output 3 dataframes:
# - df_relation.parquet
# - df_relation_doi.parquet
# - df_provenance.parquet

import sys, os, time
from pathlib import Path
from datetime import datetime
from timeit import default_timer as timer
from typing import Union, List, Optional, Dict, Set, Iterable
from joblib import Parallel, delayed
import json
import gc

try:
    from humanfriendly import format_timespan
except ImportError:

    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)


import pandas as pd
import numpy as np

from clean_doi import clean_doi, NoDoiException

import logging

root_logger = logging.getLogger()
logger = root_logger.getChild(__name__)


def load_relations_with_datasets(
    dirpath: Path, glob_pattern: str = "relation_*part_*.gz"
) -> pd.DataFrame:
    files = list(dirpath.glob(glob_pattern))
    _dfs = []
    for fp in files:
        _df = pd.read_json(fp, lines=True, engine="pyarrow")
        _df["relType_name"] = _df["relType"].apply(lambda x: x["name"])
        _df = _df[["source", "target", "relType_name", "provenance", "validated"]]
        prov_objs = _df["provenance"].dropna()
        _df["trust"] = prov_objs.apply(lambda x: x["trust"])
        _df["provenance"] = prov_objs.apply(lambda x: x["provenance"])
        _df = _df[_df["relType_name"].isin(["Cites", "References", "IsSupplementedBy"])]
        _dfs.append(_df)
    df_relations = pd.concat(_dfs, ignore_index=True)
    df_relations = df_relations.sort_values(["source", "target"]).reset_index(drop=True)
    return df_relations


def get_openaire_type_map(
    path_to_types: Path,
    ids_to_include: Optional[Iterable] = None,
    glob_pattern: str = "df_openaire_types*.parquet",
) -> pd.Series:
    files = list(path_to_types.glob(glob_pattern))
    _dfs = []
    for fp in files:
        _df = pd.read_parquet(fp, columns=["openaire_id", "openaire_type"])
        if ids_to_include is not None:
            _df = _df[_df["openaire_id"].isin(ids_to_include)]
        _dfs.append(_df)
    df_openaire_type = pd.concat(_dfs)
    openaire_type_map = df_openaire_type.set_index(
        "openaire_id", verify_integrity=True
    )["openaire_type"]
    return openaire_type_map


def get_openaire_dois(
    path_to_dois: Path,
    ids_to_include: Optional[Iterable] = None,
    glob_pattern: str = "df_openaire_dois*.parquet",
) -> pd.DataFrame:
    files = list(path_to_dois.glob(glob_pattern))
    _dfs = []
    for fp in files:
        _df = pd.read_parquet(fp)
        _dfs.append(_df)
    df_openaire_doi = pd.concat(_dfs)
    if ids_to_include is not None:
        df_openaire_doi = df_openaire_doi[
            df_openaire_doi["openaire_id"].isin(ids_to_include)
        ]
    return df_openaire_doi


def get_crosstab(df: pd.DataFrame, type_map: pd.Series) -> pd.DataFrame:
    df_crosstab = pd.crosstab(
        [df["source"], df["target"]], df["relType_name"]
    ).reset_index(drop=False)
    df_crosstab["source_type"] = df_crosstab["source"].map(type_map)
    df_crosstab["target_type"] = df_crosstab["target"].map(type_map)
    return df_crosstab


def load_corpus_doi_data(path_to_corpus: Path, glob_pattern="*.json") -> pd.DataFrame:
    # get citations from corpus, only doi-doi citations
    files = list(path_to_corpus.glob(glob_pattern))
    corpus_citations_doi = []
    for fp in files:
        txt = fp.read_text()
        records = json.loads(txt)
        for r in records:
            try:
                source_doi = clean_doi(r["publication"])
                target_doi = clean_doi(r["dataset"])
                corpus_citations_doi.append((source_doi, target_doi))
            except NoDoiException:
                pass
    return pd.DataFrame(
        corpus_citations_doi, columns=["doi_source", "doi_target"]
    ).drop_duplicates()


def main(args):
    path_to_relations = Path(args.path_to_relations)
    path_to_types = Path(args.path_to_types)
    path_to_dois = Path(args.path_to_dois)
    path_to_corpus = Path(args.path_to_corpus)
    outdir = Path(args.outdir)
    if not outdir.exists():
        logger.debug(f"creating directory: {outdir}")
        outdir.mkdir()

    logger.debug(
        f"Step 1: load relations with datasets from directory: {path_to_relations}..."
    )
    this_start = timer()
    df_relations = load_relations_with_datasets(path_to_relations)
    logger.debug(
        f"loaded {len(df_relations)} relations. took {format_timespan(timer()-this_start)}"
    )

    outfp = outdir.joinpath("df_provenance.parquet")
    logger.debug(f"saving provenance data to {outfp}")
    df_relations[
        ["source", "target", "provenance", "trust", "validated"]
    ].drop_duplicates(subset=["source", "target"]).to_parquet(outfp)
    logger.debug("dropping provenance columns")
    df_relations.drop(columns=['provenance', 'trust', 'validated'], inplace=True)

    pairs_dedup = df_relations[["source", "target"]].drop_duplicates()
    logger.debug(f"{len(pairs_dedup)} unique openaire id pairs")
    all_ids = (
        pd.concat([pairs_dedup["source"], pairs_dedup["target"]])
        .sort_values()
        .drop_duplicates()
        .reset_index()
    )
    logger.debug(f"{len(all_ids)} unique openaire ids (either source or target)")
    # convert dataframe to series
    all_ids = all_ids[0]

    logger.debug(f"Step 2: load openaire types data and doi data")
    logger.debug(f"Loading openaire types data from directory: {path_to_types}...")
    this_start = timer()
    openaire_type_map = get_openaire_type_map(path_to_types, ids_to_include=all_ids)
    logger.debug(
        f"loaded openaire type map ({len(openaire_type_map)} items). took {format_timespan(timer()-this_start)}"
    )
    logger.debug(f"Loading openaire doi data from directory: {path_to_dois}...")
    this_start = timer()
    df_openaire_doi = get_openaire_dois(path_to_dois, ids_to_include=all_ids)
    logger.debug(
        f"loaded openaire dois ({len(df_openaire_doi)} items). took {format_timespan(timer()-this_start)}"
    )
    logger.debug(f"num unique openaire ids: {df_openaire_doi['openaire_id'].nunique()}")
    logger.debug(f"num unique dois: {df_openaire_doi['doi'].nunique()}")

    logger.debug("Step 3: get crosstab...")
    this_start = timer()
    df_crosstab = get_crosstab(df_relations, type_map=openaire_type_map)
    logger.debug(
        f"done getting crosstab (dataframe shape: {df_crosstab.shape}). took {format_timespan(timer()-this_start)}"
    )

    # # checkpoint
    # outfp = outdir.joinpath("df_relations_crosstab_checkpoint.parquet")
    # logger.debug(f"writing to file: {outfp}")
    # df_crosstab.to_parquet(outfp)

    logger.debug("deleting df_relations and running garbage collection")
    del df_relations
    gc.collect()
    logger.debug("gc.get_stats():")
    logger.debug(gc.get_stats())
    logger.debug("gc.garbage:")
    logger.debug(gc.garbage)

    logger.debug(f"Step 4: merge relations and dois")
    this_start = timer()
    drop_cols = ["Cites", "IsSupplementedBy", "References"]
    df_relation_doi = df_crosstab.drop(columns=drop_cols)
    df_relation_doi = df_relation_doi.merge(
        df_openaire_doi.rename(columns={"openaire_id": "source", "doi": "doi_source"}),
        how="inner",
        on="source",
    )
    logger.debug("done merging on source column. merging on target column...")
    df_relation_doi = df_relation_doi.merge(
        df_openaire_doi.rename(columns={"openaire_id": "target", "doi": "doi_target"}),
        how="inner",
        on="target",
    )
    logger.debug(
        f"done merging relations and dois. df_relation_doi has shape {df_relation_doi.shape}. took {format_timespan(timer()-this_start)}"
    )

    logger.debug("Step 5: load corpus data")
    this_start = timer()
    df_corpus_citations_doi = load_corpus_doi_data(path_to_corpus)
    logger.debug(
        f"loaded {len(df_corpus_citations_doi)} rows. took {format_timespan(timer()-this_start)}"
    )

    logger.debug("Step 6: merge corpus data with openaire data and save files")
    this_start = timer()
    df_corpus_citations_doi["in_corpus"] = True
    df_relation_doi = df_relation_doi.merge(
        df_corpus_citations_doi, how="left", on=["doi_source", "doi_target"]
    )
    df_relation_doi["in_corpus"] = df_relation_doi["in_corpus"].fillna(value=False)
    outfp = outdir.joinpath("df_relation_doi.parquet")
    logger.debug(f"writing dataframe with shape {df_relation_doi.shape} to {outfp}")
    df_relation_doi.to_parquet(outfp)
    logger.debug("continuing to merge...")
    to_merge = df_relation_doi[df_relation_doi["in_corpus"] == True]
    to_merge = to_merge[["source", "target", "in_corpus"]].drop_duplicates()
    df_relation = df_crosstab.merge(to_merge, how="left", on=["source", "target"])
    df_relation["in_corpus"] = df_relation["in_corpus"].fillna(value=False)

    outfp = outdir.joinpath("df_relation.parquet")
    logger.debug(f"writing dataframe with shape {df_relation.shape} to {outfp}")
    df_relation.to_parquet(outfp)

    logger.debug(f"step 6 (merging corpus with openaire and saving files) took {format_timespan(timer()-this_start)}")


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
        "path_to_relations", help="directory with relations data in JSON format"
    )
    parser.add_argument(
        "path_to_types", help="directory with types data in parquet format"
    )
    parser.add_argument(
        "path_to_dois", help="directory with openaire doi data in parquet format"
    )
    parser.add_argument(
        "path_to_corpus", help="directory with Data Citation Corpus data"
    )
    parser.add_argument("outdir", help="output directory")
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
