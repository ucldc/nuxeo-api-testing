import os
import json
import requests
from requests.adapters import HTTPAdapter, Retry

def configure_http_session() -> requests.Session:
    http = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[413, 429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    return http
http_session = configure_http_session()

nuxeo_api_url = os.environ['NUXEO_API_ENDPOINT']
nuxeo_api_request_headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "X-NXDocumentProperties": "*",
    "X-NXRepository": "default",
    "X-Authentication-Token": os.environ['NUXEO_API_TOKEN']
    }

def get_pages_of_documents(path):
    documents = []
    page_index = 0
    next_page_available = True
    while next_page_available:
        response = get_ancestors_for_path(path, page_index=page_index)
        next_page_available = response.json().get('isNextPageAvailable')

        documents.extend([doc for doc in response.json().get('entries', [])])
        page_index += 1

    return documents

def get_ancestors_for_path(path, page_index):
    query = (
        "SELECT * FROM SampleCustomPicture, CustomFile, CustomVideo, CustomAudio, CustomThreeD "
        f"WHERE ecm:path STARTSWITH '{path}' AND "
        "ecm:isVersion = 0 AND "
        "ecm:isTrashed = 0 "
        "ORDER BY ecm:path, ecm:pos, ecm:uid"
    )

    request = {
        'url': f"{nuxeo_api_url.rstrip('/')}/search/lang/NXQL/execute",
        'headers': nuxeo_api_request_headers,
        'params': {
            'pageSize': '100',
            'currentPageIndex': page_index,
            'query': query
        }
    }

    try:
        response = http_session.get(**request)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"Unable to fetch page {request}")
        raise(e)
    
    return response

path = '/asset-library/UCR/SCUA/Archival/Klein/Publish/2017_pilot' # 26fa054a-f967-49e7-a773-36694f671a1d 
# bc8cb1da-1876-49b9-ac8c-567643552321 /asset-library/UCR/SCUA/Archival/Klein/Publish/2017_pilot/stlouiscon_1969
# ffa33489-32b8-465b-94e2-e02fb1e02f2c /asset-library/UCOP/barbaratest
documents = get_pages_of_documents(path)
uids = [doc['uid'] for doc in documents]
paths = [doc['path']for doc in documents]

with open('output/klein-pathquery.json', 'w') as f:
    f.write(json.dumps(paths))

print(f"{len(uids)=}")
print(f"{len(set(uids))=}")