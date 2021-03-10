import pytest
import json
import requests

# FIXME:
# the host should be loaded from an environment variable so that it is guaranteed to match the docker-compose settings
host = 'localhost:5000'

# routes that are in the webapp and should be tested must be manually defined here;
# each route to test is a tuple, where the first entry is the path and the second is a list of available query parameter strings
# FIXME:
# this should be generated automatically from the webapp
routes = [
    ('/',       []),
    ('/ngrams', ['query']),
    ('/search', ['query']),
    ]

# load the naughty_strings;
# the origin of this dataset is https://github.com/minimaxir/big-list-of-naughty-strings
with open('tests/blns.json') as f:
    naughty_strings = json.load(f)

# generate the test urls
test_urls = []
for route in routes:
    path, params = route
    test_urls.append(f'http://{host}{path}')
    for param in params:
        for naughty_string in naughty_strings:
            test_urls.append(f'http://{host}{path}?{param}={naughty_string}')


@pytest.mark.parametrize('url', test_urls, ids=id)
def test_url(url):
    r = requests.get(url)
    assert r.status_code < 500
