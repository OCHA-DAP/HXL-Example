import ckanapi, json, sys, time
from urllib2 import Request, urlopen, URLError, HTTPError


DELAY = 1
"""Time delay in seconds between datasets, to give HDX a break."""

CKAN_URL = "https://data.humdata.org"
"""Base URL for the CKAN instance."""


def find_hxl_datasets(start, rows):
    """Return a page of HXL datasets."""
    return ckan.action.package_search(start=start, rows=rows, fq="tags:hxl")

# Open a connection to HDX
ckan = ckanapi.RemoteCKAN(CKAN_URL)
result_start_pos = 0
result_page_size = 10000

result = find_hxl_datasets(result_start_pos, result_page_size)
result_total_count = result["count"]
print(result["count"])

packages = result["results"]

# Iterate through all the datasets ("packages") and resources on HDX
i=0
for package in packages:
    # package = ckan.action.package_show(id=package_id)
    print("Package: " + format(package["title"]))