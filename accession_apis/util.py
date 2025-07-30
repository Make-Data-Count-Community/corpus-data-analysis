import json
import re
import warnings

GEO_PATTERN = re.compile(r"!(.*?) = (.*)")


def parse_geo_downloaded_jsonlines_file(filename: str) -> list[dict]:
    # parse the JSON-lines file written by the download_all_geo_api_data_from_acc_list script
    # into a list of dictionaries which can be made into a dataframe
    affil_data = []
    with open(filename, "r") as f:
        for i, line in enumerate(f):
            try:
                affil_data.append(parse_geo_downloaded_single_line)
            except json.JSONDecodeError:
                warnings.warn(
                    f"problem parsing line {i} in file {filename}. skipping..."
                )
    return affil_data


def parse_geo_downloaded_single_line(line: str) -> dict:
    rec = json.loads(line)
    acc = rec["acc_no"]
    item = rec["api_response"]
    row = {"acc": acc}
    for m in GEO_PATTERN.findall(item):
        for field_str in [
            "contact_institute",
            "contact_department",
            "contact_laboratory",
            "contact_city",
            "contact_state",
            "contact_country",
        ]:
            if field_str in m[0]:
                row[field_str] = m[1]
    return row
