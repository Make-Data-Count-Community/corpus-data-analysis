# -*- coding: utf-8 -*-

# from https://github.com/ourresearch/openalex-guts/blob/05f27d3c0d760b9175c06e1f55e12cbe71ed9d7d/cleanup/util.py

DESCRIPTION = """utils for OpenAlex API"""

import numpy
import sys, os, time
import re
import json
from pathlib import Path
from datetime import datetime
from typing import Iterable, Mapping, Generator
from timeit import default_timer as timer

try:
    from humanfriendly import format_timespan
except ImportError:

    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)


import requests
import backoff

import pandas as pd
import numpy as np


# ENTITY_TYPES = {
#     "W": "work",
#     "A": "author",
#     "S": "source",
#     "P": "publisher",
#     "F": "funder",
#     "I": "institution",
#     "C": "concept",
#     "V": "venue",  # deprecated
# }


# class OpenAlexID:
#     def __init__(
#         self, id_in_unknown_form: Union[str, int], entity_type: Optional[str] = None
#     ) -> None:
#         if hasattr(id_in_unknown_form, "openalex_id"):
#             # pass through if OpenAlexID is initialized with an instance of OpenAlexID already
#             return id_in_unknown_form
#         self.ENTITY_TYPES_PREFIX_TO_NAME = ENTITY_TYPES
#         self.ENTITY_TYPES_NAME_TO_PREFIX = {
#             v: k for k, v in self.ENTITY_TYPES_PREFIX_TO_NAME.items()
#         }
#         id_in_unknown_form = str(id_in_unknown_form)
#         if id_in_unknown_form.isnumeric():
#             if entity_type is None:
#                 raise ValueError("Numeric IDs must specify an entity_type")
#             self.validate_entity_type()
#             self.id_int = int(id_in_unknown_form)
#             self.entity_type = self.normalize_entity_type(entity_type)
#         else:
#             if entity_type is not None:
#                 logger.warning(f"ignoring specified entity_type: {entity_type}")
#             self.entity_type, self.id_int = self.normalize_openalex_id(
#                 id_in_unknown_form
#             )

#     def __str__(self) -> str:
#         return self.id

#     def __repr__(self) -> str:
#         return f'OpenAlexID("{self.id})"'

#     @property
#     def entity_prefix(self) -> str:
#         return self.ENTITY_TYPES_NAME_TO_PREFIX[self.entity_type]

#     @property
#     def openalex_id(self) -> str:
#         return f"https://openalex.org/{self.id_short}"

#     @property
#     def id_short(self) -> str:
#         return f"{self.entity_prefix}{self.id_int}"

#     @property
#     def id(self) -> str:
#         return self.openalex_id

#     # Inspired by openalex-elastic-api/core/utils.py
#     def normalize_openalex_id(self, openalex_id):
#         if not openalex_id:
#             raise ValueError
#         openalex_id = openalex_id.strip().upper()
#         valid_prefixes = "".join(self.ENTITY_TYPES_PREFIX_TO_NAME.keys())
#         p = re.compile(f"([{valid_prefixes}]\d{{2,}})")
#         matches = re.findall(p, openalex_id)
#         if len(matches) == 0:
#             raise ValueError
#         clean_openalex_id = matches[0]
#         clean_openalex_id = clean_openalex_id.replace("\0", "")
#         prefix = clean_openalex_id[0]
#         id_int = int(clean_openalex_id[1:])
#         return self.normalize_entity_type(prefix), id_int

#     def validate_entity_type(self, entity_type: str):
#         entity_type_prefixes = [
#             e.upper() for e in self.ENTITY_TYPES_PREFIX_TO_NAME.keys()
#         ]
#         entity_type_names = [
#             e.upper() for e in self.ENTITY_TYPES_PREFIX_TO_NAME.values()
#         ]
#         valid_entity_types = entity_type_prefixes + entity_type_names
#         if not entity_type or entity_type.upper() not in valid_entity_types:
#             raise ValueError(f"{entity_type} is not a valid entity type")

#     def normalize_entity_type(self, entity_type: str):
#         self.validate_entity_type(entity_type)
#         try:
#             return self.ENTITY_TYPES_PREFIX_TO_NAME[entity_type.upper()]
#         except KeyError:
#             return entity_type.lower()


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_time=60)
@backoff.on_predicate(backoff.expo, lambda x: x.status_code >= 429, max_time=60)
def make_request(url, params=None, debug=False):
    if debug:
        print(url, params)
    if params is None:
        return requests.get(url)
    else:
        return requests.get(url, params=params)


def paginate_openalex(url, params=None, per_page=200, debug=False):
    if params is None:
        params = {}
    if "per-page" not in params and per_page:
        params["per-page"] = per_page
    cursor = "*"
    while cursor:
        params["cursor"] = cursor
        r = make_request(url, params, debug=debug)
        yield r

        page_with_results = r.json()
        # update cursor to meta.next_cursor
        cursor = page_with_results["meta"]["next_cursor"]


def entities_by_ids(
    id_list,
    api_endpoint="works",
    filterkey="openalex",
    chunksize=100,
    params=None,
    debug=False,
):
    if params is None:
        params = {}
    params["per-page"] = chunksize
    existing_filter = params.get("filter")
    for i in range(0, len(id_list), chunksize):
        chunk = id_list[i : i + chunksize]
        url = f"https://api.openalex.org/{api_endpoint}"
        chunk_str = "|".join(chunk)
        if existing_filter:
            params["filter"] = existing_filter + f",{filterkey}:{chunk_str}"
        else:
            params["filter"] = f"{filterkey}:{chunk_str}"
        yield make_request(url, params=params, debug=debug)


# def openalex_entities_by_ids(id_list, chunksize=100, params=None):
#     id_list = [OpenAlexID(oid) for oid in id_list]
#     if params is None:
#         params = {}
#     params["per-page"] = chunksize
#     entity_type = set([item.entity_type for item in id_list])
#     if not len(entity_type) == 1:
#         raise RuntimeError("all ids in in id_list must be the same entity type")
#     entity_type = list(entity_type)[0]
#     api_endpoint = f"{entity_type}s"
#     ids_short = [item.id_short for item in id_list]
#     for r in entities_by_ids(
#         ids_short,
#         api_endpoint,
#         filterkey="openalex",
#         chunksize=chunksize,
#         params=params,
#     ):
#         yield r


def get_publisher_id(original_publisher):
    if not original_publisher:
        return None
    original_publisher = original_publisher.replace("&", "").replace(",", "")
    r = make_request(f"https://api.openalex.org/publishers?search={original_publisher}")
    results = r.json()["results"]
    if results:
        return int(results[0]["id"].replace("https://openalex.org/P", ""))
    else:
        return None


def process_row(work: dict) -> dict:
    # process an OpenAlex work from the API, returning a flattened
    # dict with some of the data

    def get_institutions(
        authorships: list[dict],
    ) -> tuple[list[str], list[str], list[str]]:
        institutions = set()  # OpenAlex IDs
        institutions_ror = set()  # ROR IDs
        lineage = set()  # OpenAlex IDs
        for authorship in authorships:
            for institution in authorship.get("institutions", []):
                institutions.add(institution["id"].split("/")[-1])
                if institution.get("ror"):
                    institutions_ror.add(institution["ror"].split("/")[-1])
                if "lineage" in institution:
                    for item in institution["lineage"]:
                        lineage.add(item.split("/")[-1])
        return list(institutions), list(institutions_ror), list(lineage)

    def get_funders(grants: list[dict]) -> list[str]:
        funders = set()
        for grant in grants:
            funders.add(grant.get("funder_display_name"))
        return list(funders)

    def get_primary_topic(topics: list[dict]) -> tuple[str, str, str, str]:
        if not topics:
            return (None, None, None, None)
        primary_topic_dict = topics[0]
        primary_topic_name = primary_topic_dict.get("display_name")
        subfield_name = primary_topic_dict.get("subfield", {}).get("display_name")
        field_name = primary_topic_dict.get("field", {}).get("display_name")
        domain_name = primary_topic_dict.get("domain", {}).get("display_name")
        return primary_topic_name, subfield_name, field_name, domain_name

    openalex_id = work["id"].split("/")[-1]
    doi = work["doi"].replace("https://doi.org/", "")
    if "pmid" in work["ids"]:
        pmid = work["ids"]["pmid"].split("/")[-1]
    else:
        pmid = None
    if "pmcid" in work["ids"]:
        pmcid = work["ids"]["pmcid"].split("/")[-1]
    else:
        pmcid = None
    institutions, institutions_ror, lineage = get_institutions(
        work.get("authorships", [])
    )
    funders = get_funders(work.get("grants", []))
    primary_topic, subfield, field, domain = get_primary_topic(work.get("topics"))
    row = {
        "openalex_id": openalex_id,
        "doi": doi,
        "pmid": pmid,
        "pmcid": pmcid,
        "publication_date": work["publication_date"],
        "is_oa": work.get("open_access", {}).get("is_oa", None),
        "oa_url": work.get("open_access", {}).get("oa_url", None),
        "type": work["type"],
        "type_crossref": work["type_crossref"],
        "institutions": institutions,
        "institutions_ror": institutions_ror,
        "lineage": lineage,
        "funders": funders,
        "datasets": work.get("datasets"),
        "cited_by_count": work.get("cited_by_count"),
        "primary_topic": primary_topic,
        "topic_subfield": subfield,
        "topic_field": field,
        "topic_domain": domain,
    }
    return row


def get_openalex_dataframe_from_works(
    works: Iterable[str | dict], ror_map: Mapping | None = None
) -> pd.DataFrame:
    rows = []
    seen_ids = set()
    for work in works:
        if not isinstance(work, dict):
            work = json.loads(work)
        openalex_id = work["id"].split("/")[-1]
        if openalex_id not in seen_ids:
            rows.append(process_row(work))
            seen_ids.add(openalex_id)
    df = pd.DataFrame(rows).set_index("openalex_id")
    if ror_map is not None:
        # create a new column, which is the "lineage" list mapped to ror ids
        df["lineage_ror"] = df["lineage"].apply(
            lambda id_list: [
                ror_map.get(openalex_institution_id, None)
                for openalex_institution_id in id_list
            ]
        )
    return df


def open_file(path_to_file: str | Path):
    path_to_file = Path(path_to_file)
    if path_to_file.suffix == ".gz":
        import gzip

        f = gzip.open(path_to_file, "rt")

    else:
        f = path_to_file.open("r")

    return f


def get_openalex_dataframe_from_multiple_works_files(
    datadir: str | Path,
    glob_pattern: str = "openalex_works*.gz",
    ror_map: Mapping | None = None,
) -> pd.DataFrame:
    # Example usage:
    # ror_map = get_ror_map_from_institutions_file("openalex_institutions.gz")
    # df_openalex = get_openalex_dataframe_from_multiple_works_files(
    #     "works_data/", ror_map=ror_map
    # )

    def yield_lines_from_files(files: Iterable[Path]) -> Generator[str, None, None]:
        for fp in files:
            f = open_file(fp)
            try:
                for line in f:
                    if line:
                        yield line
            finally:
                f.close()

    datadir = Path(datadir)
    files = list(datadir.glob(glob_pattern))
    files.sort()
    return get_openalex_dataframe_from_works(
        yield_lines_from_files(files), ror_map=ror_map
    )


def get_ror_map_from_institutions_file(path_to_file: str | Path) -> dict[str, str]:
    f = open_file(path_to_file)
    ror_map = {}
    try:
        for line in f:
            institution = json.loads(line)
            openalex_id = institution["id"].split("/")[-1]
            if institution.get("ror"):
                ror_id = institution["ror"].split("/")[-1]
            else:
                ror_id = None
            ror_map[openalex_id] = ror_id
    finally:
        f.close()

    return ror_map
