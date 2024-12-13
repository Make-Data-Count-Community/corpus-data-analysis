# from: https://github.com/ourresearch/openalex-guts/blob/9a83890bcc715d979ca0fa7d1ddbb1c2e9656d99/util.py#L417

import re
import unicodedata
import math


# from https://stackoverflow.com/a/70274772
def is_nan(value):
    try:
        return math.isnan(float(value))
    except ValueError:
        return False


class NoDoiException(Exception):
    pass


# from http://farmdev.com/talks/unicode/
def to_unicode_or_bust(obj, encoding="utf-8"):
    if isinstance(obj, str):
        if not isinstance(obj, str):
            obj = str(obj, encoding)
    return obj


def remove_nonprinting_characters(input, encoding="utf-8"):
    input_was_unicode = True
    if isinstance(input, str):
        if not isinstance(input, str):
            input_was_unicode = False

    unicode_input = to_unicode_or_bust(input)

    # see http://www.fileformat.info/info/unicode/category/index.htm
    char_classes_to_remove = ["C", "M", "Z"]

    response = "".join(
        c
        for c in unicode_input
        if unicodedata.category(c)[0] not in char_classes_to_remove
    )

    if not input_was_unicode:
        response = response.encode(encoding)

    return response


def replace_doi_bad_chars(doi):
    replace_chars = {"‐": "-"}
    for char, replacement in replace_chars.items():
        doi = doi.replace(char, replacement)
    return doi


def clean_doi(dirty_doi, return_none_if_error=False):
    if not dirty_doi or is_nan(dirty_doi):
        if return_none_if_error:
            return None
        else:
            raise NoDoiException("There's no DOI at all.")

    dirty_doi = dirty_doi.strip()
    dirty_doi = dirty_doi.lower()
    dirty_doi = replace_doi_bad_chars(dirty_doi)

    # test cases for this regex are at https://regex101.com/r/zS4hA0/1
    p = re.compile(r"(10\.\d+\/[^\s]+)")

    matches = re.findall(p, dirty_doi)
    if len(matches) == 0:
        if return_none_if_error:
            return None
        else:
            raise NoDoiException("There's no valid DOI.")

    match = matches[0]
    match = remove_nonprinting_characters(match)

    try:
        resp = str(match, "utf-8")  # unicode is valid in dois
    except (TypeError, UnicodeDecodeError):
        resp = match

    # remove any url fragments
    if "#" in resp:
        resp = resp.split("#")[0]

    # remove double quotes, they shouldn't be there as per http://www.doi.org/syntax.html
    resp = resp.replace('"', "")

    # remove trailing period, comma -- it is likely from a sentence or citation
    if resp.endswith(",") or resp.endswith("."):
        resp = resp[:-1]

    return resp
