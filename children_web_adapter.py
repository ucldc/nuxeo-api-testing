import os

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
        response = get_children(path, page_index=page_index)
        next_page_available = response.json().get('isNextPageAvailable')

        documents.extend([doc for doc in response.json().get('entries', [])])
        page_index += 1

    return documents

def get_children(path, page_index):
    path = path.strip('/')
    request = {
        'url': f"{nuxeo_api_url.rstrip('/')}/path/{path}/@children",
        'headers': nuxeo_api_request_headers,
        'params': {'currentPageIndex': page_index}
    }

    try:
        response = http_session.get(**request)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"Unable to fetch page {request}")
        raise(e)
    
    return response

documents = get_pages_of_documents('/asset-library/UCR/SCUA/Archival/Klein/Publish/2017_pilot/stlouiscon_1969')
uids = [doc['uid'] for doc in documents]

print(f"{len(uids)=}")
print(f"{len(set(uids))=}")
