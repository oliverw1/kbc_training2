"""Download all N csv.bz2 files from dataexpo on the flights
"""
import shutil
from pathlib import Path
from urllib.parse import urljoin

import requests

url = "http://stat-computing.org/dataexpo/2009/"

# session = requests.Session()
# for year in range(1987, 2009):
#     urn = f"{year}.csv.bz2"
#     with session.get(urljoin(url, urn), stream=True) as response:
#         with (Path(__file__).with_name(urn).open(mode="wb") as fh:
#             shutil.copyfileobj(response.raw, fh)

# find ./ -type f -name '*.bz2' -print0 | xargs -0  --max-procs=3 --max-args=3 bunzip2
