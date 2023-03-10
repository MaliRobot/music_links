#!/usr/bin/python3

"""
    opensearch.py
    MediaWiki API Demos
    Demo of `Opensearch` module: Search the wiki and obtain
	results in an OpenSearch (http://www.opensearch.org) format
    MIT License
"""

import requests

S = requests.Session()

URL = "https://en.wikipedia.org/w/api.php?action=query&prop=info&titles=Earth"
R = S.get(url=URL)

print(R.json())

# URL = "https://en.wikipedia.org/w/api.php"
#
# PARAMS = {
#     "action": "opensearch",
#     "namespace": "0",
#     "search": "Adam Green",
#     # "limit": "5",
#     "format": "json"
# }
#
# R = S.get(url=URL, params=PARAMS)
# DATA = R.json()
#
# print(DATA)