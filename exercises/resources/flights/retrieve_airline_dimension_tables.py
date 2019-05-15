"""Download the dimension tables related to the airlines dataset.
"""
from pathlib import Path

import requests

url1 = "https://www.transtats.bts.gov/Download_Lookup.asp?Lookup=L_CARRIER_HISTORY"
url2 = "https://www.transtats.bts.gov/Download_Lookup.asp?Lookup=L_AIRPORT"

for fname, link in (("carriers.csv", url1), ("airports.csv", url2)):
    response = requests.get(link)
    Path(__file__).with_name(fname).write_bytes(response.content)