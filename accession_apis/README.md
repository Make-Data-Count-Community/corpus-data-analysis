# Getting affiliation data from GEO records

Use a list of GEO accession numbers to get affiliation data for those records. The list can come from EuropePMC or DataCite data citations, for example.

1. Use [`download_all_geo_api_data_from_acc_list.py`](./download_all_geo_api_data_from_acc_list.py) to download all the data from the GEO database and save to a JSON-lines file.
2. Use the `parse_geo_downloaded_jsonlines_file` function in [`util.py`](./util.py) to collection the affiliations. See [`geo_affiliation_europepmc.ipynb`](./geo_affiliation_europepmc.ipynb) for reference.