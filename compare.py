import argparse
from collections import namedtuple
import os
import sys
from urllib.parse import urlparse

import boto3

METADATA_STORE = os.environ.get('METADATA_STORE')

DataStorage = namedtuple(
    "DateStorage", "uri, store, bucket, path"
)

def parse_data_uri(data_uri: str):
    data_loc = urlparse(data_uri)
    return DataStorage(
        data_uri, data_loc.scheme, data_loc.netloc, data_loc.path)

def get_records(collection_id, version, query_method, children=False):
    records = []
    data = parse_data_uri(METADATA_STORE)
    metadata_path = os.path.join(data.path, collection_id, version, query_method)
    if children:
        metadata_path = os.path.join(metadata_path, "children")
    if data.store == 'file':
        store_uri = f"file://{metadata_path}"
        for file in os.listdir(metadata_path):
            fullpath = os.path.join(metadata_path, file)
            if os.path.isfile(fullpath):
                with open(fullpath, "r") as f:
                    for line in f.readlines():
                        records.append(line)
    elif data.store == 's3':
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects_v2')
        prefix = metadata_path.lstrip('/')
        store_uri = f"s3://{data.bucket}/{prefix}"
        pages = paginator.paginate(
            Bucket=data.bucket,
            Prefix=prefix
        )
        for page in pages:
            for item in page.get('Contents', []):
                if not item['Key'].startswith(f'{prefix}/children/'):
                    #print(f"getting s3 object: {item['Key']}")
                    response = s3_client.get_object(
                        Bucket=data.bucket,
                        Key=item['Key']
                    )
                    for line in response['Body'].iter_lines(): 
                        records.append(line)
    else:
        raise Exception(f"Unknown data scheme: {data.store}")

    if not records:
        raise Exception(
            f"No metadata records found for collection {collection_id} "
            f"at {store_uri}"
        )

    return records

def main(params):
    nuxeoapi_parent_data = get_records(params.collection_id, params.version, 'nuxeoapi')
    dbquery_parent_data = get_records(params.collection_id, params.version, 'nuxeoapi')

    print(f"nuxeoapi parent count: {len(nuxeoapi_parent_data)}")
    print(f"dbquery  parent count: {len(dbquery_parent_data)}")

    print(f"Parent data sets equal?: {nuxeoapi_parent_data == dbquery_parent_data}")

    nuxeoapi_child_data = get_records(params.collection_id, params.version, 'nuxeoapi', children=True)
    dbquery_child_data = get_records(params.collection_id, params.version, 'nuxeoapi', children=True)

    print(f"nuxeoapi child count: {len(nuxeoapi_child_data)}")
    print(f"dbquery  child count: {len(dbquery_child_data)}")

    print(f"Child data sets equal?: {nuxeoapi_child_data == dbquery_child_data}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Compare fetched metadata from nuxeo')
    parser.add_argument('--collection_id', help='Registry collection ID')
    parser.add_argument('--version', help='Metadata version')

    args = parser.parse_args()
    sys.exit(main(args))