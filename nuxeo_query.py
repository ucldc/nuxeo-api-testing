import argparse
from collections import namedtuple
import datetime
import json
import os
import sys
from urllib.parse import quote, urlparse

import requests
from requests.adapters import HTTPAdapter, Retry

import boto3

metadata_store = os.environ['METADATA_STORE']

nuxeo_api_url = os.environ['NUXEO_API_ENDPOINT']
nuxeo_api_request_headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "X-NXDocumentProperties": "*",
    "X-NXRepository": "default",
    "X-Authentication-Token": os.environ['NUXEO_API_TOKEN']
    }

dbquery_url = os.environ['DBQUERY_URL']
dbquery_request_headers = {'Content-Type': 'application/json'}
dbquery_request_cookies = {'dbquerytoken': os.environ['DBQUERY_TOKEN']}

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

DataStorage = namedtuple(
    "DateStorage", "uri, store, bucket, path"
)

def parse_data_uri(data_uri: str):
    data_loc = urlparse(data_uri)
    return DataStorage(
        data_uri, data_loc.scheme, data_loc.netloc, data_loc.path)

def load_object_to_s3(bucket, key, content):
    s3_client = boto3.client('s3')
    #print(f"Writing s3://{bucket}/{key}")
    try:
        s3_client.put_object(
            ACL='bucket-owner-full-control',
            Bucket=bucket,
            Key=key,
            Body=content)
    except Exception as e:
        print(f"ERROR loading to S3: {e}")

    return f"s3://{bucket}/{key}"

def write_object_to_local(dir, filename, content):
    if not os.path.exists(dir):
        os.makedirs(dir)

    fullpath = os.path.join(dir, filename)
    #print(f"Writing file://{fullpath}")
    with open(fullpath, "w") as f:
        f.write(content)

    return f"file://{fullpath}"

def store_parent_metadata_page(collection_id, version, query_method, page_prefix, page_index, records):
    filename = f"{'-'.join(page_prefix)}-p{page_index}.jsonl"
    jsonl = "\n".join([json.dumps(record) for record in records])
    jsonl = f"{jsonl}\n"
  
    storage = parse_data_uri(metadata_store)
    metadata_path = os.path.join(storage.path, collection_id, version, query_method)
    if storage.store == 'file':
        write_object_to_local(metadata_path, filename, jsonl)
    elif storage.store == 's3':
        s3_key = f"{metadata_path.lstrip('/')}/{filename}"
        load_object_to_s3(storage.bucket, s3_key, jsonl)
    else:
        raise Exception(f"Unknown data scheme: {storage.store}")

def store_component_metadata_page(collection_id, version, query_method, parent_uid, page_index, records):
    filename = f"{parent_uid}-p{page_index}.jsonl"
    jsonl = "\n".join([json.dumps(record) for record in records])
    jsonl = f"{jsonl}\n"

    storage = parse_data_uri(metadata_store)
    metadata_path = os.path.join(storage.path, collection_id, version, query_method, "children")
    if storage.store == 'file':
        write_object_to_local(metadata_path, filename, jsonl)
    elif storage.store == 's3':
        s3_key = f"{metadata_path.lstrip('/')}/{filename}"
        load_object_to_s3(storage.bucket, s3_key, jsonl)
    else:
        raise Exception(f"Unknown data scheme: {storage.store}")

class NuxeoApiFetcher(object):
    def __init__(self, params):
        self.query_method = 'nuxeoapi'
        self.collection_id = params['collection_id']
        self.current_folder = {
            'path': params['path'],
            'uid': params['uid']
        }
        self.version = params['version']
        self.page_size = 100
        self.fetch_children = True
        self.http_session = configure_http_session()

    def fetch(self):
        page_prefix = ['r']

        # get documents in root folder
        self.get_pages_of_documents(self.current_folder, page_prefix)
        
        # get documents in all folders under root
        self.folder_traversal(self.current_folder, page_prefix)

    def folder_traversal(self, root_folder, page_prefix):
        page_index = 0
        next_page_available = True
        while next_page_available:
            response = self.get_page_of_folders(root_folder, page_index)

            next_page_available = response.json().get('isNextPageAvailable')
            if not response.json().get('entries', []):
                next_page_available = False
                continue
            page_prefix.append(f"fp{page_index}")

            for i, folder in enumerate(response.json().get('entries', [])):
                page_prefix.append(f"f{i}")
                self.get_pages_of_documents(folder, page_prefix)
                page_prefix.pop()

            page_prefix.pop()
            page_index += 1

    def get_page_of_folders(self, folder: dict, page_index: int):
        query = (
            "SELECT * FROM Organization "
            f"WHERE ecm:ancestorId = '{folder['uid']}' "
            "AND ecm:isVersion = 0 "
            "AND ecm:isTrashed = 0"
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
            response = self.http_session.get(**request)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"{self.collection_id:<6}: unable to fetch page {request}")
            raise(e)
        
        return response

    def get_pages_of_documents(self, folder, page_prefix):
        page_index = 0
        next_page_available = True
        while next_page_available:
            response = self.get_page_of_parent_documents(folder, page_index=page_index)
            next_page_available = response.json().get('isNextPageAvailable')
            if not response.json().get('entries', []):
                next_page_available = False
                continue
            documents = [doc for doc in response.json().get('entries', [])]

            # write page of parent metadata to storage
            store_parent_metadata_page(self.collection_id, self.version, self.query_method, page_prefix, page_index, documents)
            
            for record in response.json().get('entries', []):
                self.get_pages_of_component_documents(record)

            page_index += 1

    def get_page_of_parent_documents(self, folder: dict, page_index: int=0):
        query = (
            "SELECT * FROM SampleCustomPicture, CustomFile, CustomVideo, CustomAudio, CustomThreeD "
            f"WHERE ecm:parentId = '{folder['uid']}' AND "
            "ecm:isVersion = 0 AND "
            "ecm:isTrashed = 0 ORDER BY ecm:name, ecm:uuid"
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
            response = self.http_session.get(**request)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"{self.collection_id:<6}: unable to fetch page {request}")
            raise(e)
        
        return response

    def get_pages_of_component_documents(self, record:dict):
        page_index = 0
        next_page_available = True
        while next_page_available:
            response = self.get_page_of_components(record, page_index=page_index)
            next_page_available = response.json().get('isNextPageAvailable')
            if not response.json().get('entries', []):
                next_page_available = False
                continue
            documents = [doc for doc in response.json().get('entries', [])]

            store_component_metadata_page(self.collection_id, self.version, self.query_method, record['uid'], page_index, documents)

            page_index += 1

    def get_page_of_components(self, record: dict, page_index: int=0):
        query = (
            "SELECT * FROM SampleCustomPicture, CustomFile, CustomVideo, CustomAudio, CustomThreeD "
            f"WHERE ecm:ancestorId = '{record['uid']}' AND "
            "ecm:isVersion = 0 AND "
            "ecm:isTrashed = 0 "
            "ORDER BY ecm:pos"
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
            response = self.http_session.get(**request)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"{self.collection_id:<6}: unable to fetch components: {request}")
            raise(e)
        
        return response
    
class DbQueryFetcher(object):
    def __init__(self, params):
        self.query_method = 'dbquery'
        self.collection_id = params['collection_id']
        self.current_folder = {
            'path': params['path'],
            'uid': params['uid']
        }
        self.version = params['version']
        self.page_size = 100
        self.fetch_children = True
        self.http_session = configure_http_session()

    def fetch(self):
        page_prefix = ['r']

        # get documents in root folder
        self.get_pages_of_records(self.current_folder, page_prefix)
        
        # get documents in all folders under root
        self.folder_traversal(self.current_folder, page_prefix)

    def folder_traversal(self, root_folder, page_prefix):
        pages = []

        collection_folders = self.get_descendant_folders(root_folder)

        for i, folder in enumerate(collection_folders):
            page_prefix.append(f"f{i}")
            pages.extend(self.get_pages_of_records(folder, page_prefix))
            page_prefix.pop()

        return pages
    
    def get_descendant_folders(self, root_folder):
        descendant_folders = []

        def recurse(folders):
            descendant_folders.extend(folders)
            for folder in folders:
                subfolders = self.get_child_folders(folder)
                recurse(subfolders)

        # get child folders of root
        root_folders = self.get_child_folders(root_folder)

        # recurse down the tree
        recurse(root_folders)

        return descendant_folders

    def get_child_folders(self, folder):
        subfolders = []
        more_pages_of_folders = True
        resume_after = ''

        while more_pages_of_folders:
            folder_resp = self.get_page_of_folders(folder, resume_after)
            more_pages_of_folders = folder_resp.json().get('isNextPageAvailable')
            resume_after = folder_resp.json().get('resumeAfter')

            for child_folder in folder_resp.json().get('entries', []):
                subfolders.append(child_folder)

        return subfolders

    def get_pages_of_record_components(self, root_record:dict):
        pages_of_record_components = []

        def recurse(pages):
            pages_of_record_components.extend(pages)
            for page in pages:
                records = page.get('entries', [])
                for record in records:
                    child_component_pages = self.get_pages_of_child_components(record)
                    recurse(child_component_pages)

        # get components of root record
        root_component_pages = self.get_pages_of_child_components(root_record)

        # recurse to fetch any nested components
        recurse(root_component_pages)

        component_page_count = 0
        for page in pages_of_record_components:
            store_component_metadata_page(self.collection_id, self.version, self.query_method, root_record['uid'], component_page_count, page['entries'])

            component_page_count += 1

    def get_pages_of_child_components(self, record:dict):
        pages = []
        more_component_pages = True
        resume_after = ''

        # fetch a list of components
        components = []
        while more_component_pages:
            component_resp = self.get_page_of_documents(record, resume_after, 'full')
            more_component_pages = component_resp.json().get('isNextPageAvailable')
            if not component_resp.json().get('entries', []):
                more_component_pages = False
                continue
            resume_after = component_resp.json().get('resumeAfter')

            components.extend(component_resp.json().get('entries', []))

        # sort components based on `pos`
        components = sorted(components, key=lambda x: x['pos'])
        batch_size = 100
        for i in range(0, len(components), batch_size):
            pages.append({"entries": components[i:i+batch_size]})

        return pages

    def get_page_of_documents(self, parent: dict, resume_after: str, results_type: str):
        payload = {
            'uid': parent['uid'],
            'doc_type': 'records',
            'results_type': results_type,
            'resume_after': resume_after
        }

        request = {
            'url': 'https://nuxeo.cdlib.org/cdl_dbquery',
            'headers': dbquery_request_headers,
            'cookies': dbquery_request_cookies,
            'data': json.dumps(payload)
        }

        try:
            response = self.http_session.get(**request)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"{self.collection_id:<6}: unable to fetch page {request}")
            raise(e)
        return response

    def get_document(self, uid):
        payload = {
            'uid': uid,
            'relation': 'self',
            'results_type': 'full'
        }

        request = {
            'url': 'https://nuxeo.cdlib.org/cdl_dbquery',
            'headers': dbquery_request_headers,
            'cookies': dbquery_request_cookies,
            'data': json.dumps(payload)
        }

        try:
            response = self.http_session.get(**request)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"{self.collection_id:<6}: unable to fetch page {request}")
            raise(e)
        return response

    def get_pages_of_records(self, folder: dict, page_prefix: list):
        record_pages = []
        record_page_count = 0
        more_pages_of_records = True
        resume_after = ''
        while more_pages_of_records:
            document_resp = self.get_page_of_documents(folder, resume_after, 'full')
            more_pages_of_records = document_resp.json().get('isNextPageAvailable')
            if not document_resp.json().get('entries', []):
                more_pages_of_records = False
                continue
            resume_after = document_resp.json().get('resumeAfter')

            store_parent_metadata_page(self.collection_id, self.version, self.query_method, page_prefix, record_page_count, document_resp.json().get('entries', []))

            # pages of records components is a flat list of all children of all
            # records that were found on this page of records
            pages_of_records_components = []
            for record in document_resp.json().get('entries', []):
                self.get_pages_of_record_components(record)

            record_page_count += 1
        return record_pages

    def get_page_of_folders(self, folder: dict, resume_after: str):
        payload = {
            'uid': folder['uid'],
            'doc_type': 'folders',
            'results_type': 'listing',
            'resume_after': resume_after
        }

        request = {
            'url': 'https://nuxeo.cdlib.org/cdl_dbquery',
            'headers': dbquery_request_headers,
            'cookies': dbquery_request_cookies,
            'data': json.dumps(payload)
        }
        try:
            response = self.http_session.get(**request)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"{self.collection_id:<6}: unable to fetch page {request}")
            raise(e)
        return response

def get_nuxeo_uid_for_path(path):
    escaped_path = quote(path, safe=' /')
    url = u'/'.join([nuxeo_api_url, "path", escaped_path.strip('/')])
    headers = nuxeo_api_request_headers
    request = {'url': url, 'headers': headers}
    response = http_session.get(**request)
    response.raise_for_status()
    return response.json()['uid']

def main(params):
    uid = get_nuxeo_uid_for_path(params.path)
    version = datetime.datetime.now()
    version = version.replace(tzinfo=datetime.timezone.utc)
    version = version.isoformat()
    fetcher_payload = {
        "collection_id": params.collection_id,
        "uid": uid,
        "path": params.path,
        "version": version
    }

    # Run the same query twice: using the Nuxeo API and then using dbquery lambda
    # Compare the results
    # Queries to run:
    # - Jay Kay Klein collection 26943 /asset-library/UCR/SCUA/Archival/Klein/Publish
    # - Big UCM collection with lots of nesting
    print(f"Fetching data via Nuxeo API")
    fetcher_payload.update({"query_method": "nuxeoapi"})
    nuxeo_api_fetcher = NuxeoApiFetcher(fetcher_payload)
    nuxeo_api_fetcher.fetch()

    print(f"Fetching data via dbquery lambda")
    fetcher_payload.update({"query_method": "dbquery"})
    dbquery_fetcher = DbQueryFetcher(fetcher_payload)
    dbquery_fetcher.fetch()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Verify nuxeo API results')
    parser.add_argument('--path', help='nuxeo path')
    parser.add_argument('--collection_id', help='registry collection id or other string to be used for storing data')

    args = parser.parse_args()
    sys.exit(main(args))