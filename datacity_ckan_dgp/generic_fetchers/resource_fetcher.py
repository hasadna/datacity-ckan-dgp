import os

from .. import ckan
from ..utils import http_stream_download
from ..utils.locking import instance_package_lock
from ..operators import packages_processing


DESCRIPTION = """
Fetch from any URL

URL example: https://next.obudget.org/datapackages/facilities/tamat/tamat-facilities.csv

After create/update of data it will run the packages_processing to create xlsx / geojson related resources

Local development example:
  python3 -m datacity_ckan_dgp.operators.generic_fetcher '{
    "source_url": "https://next.obudget.org/datapackages/facilities/tamat/tamat-facilities.csv",
    "target_instance_name": "LOCAL_DEVELOPMENT",
    "target_package_id": "tamat-facilities",
    "target_organization_id": "budgetkey",
    "tmpdir": ".data/resource_fetcher_tmpdir",
    "target_package_title": "Tamat Facilities"
  }'
"""

DEVEL_SKIP_DOWNLOAD = os.getenv('DEVEL_SKIP_DOWNLOAD', 'false').lower() == 'true'


def run_packages_processing(instance_name, package_id):
    for task in ['geojson', 'xlsx']:
        assert packages_processing.operator('_', {
            'instance_name': instance_name,
            'task': task
        }, only_package_id=package_id, with_lock=False)


def fetch(
    source_url, target_instance_name, target_package_id, target_organization_id, tmpdir, source_filter, post_processing,
    target_package_title=None, target_resource_format=None,
):
    assert not post_processing and not source_filter, 'post processing and source filter are not supported yet'
    res = ckan.package_show(target_instance_name, target_package_id)
    target_package_exists = False
    existing_resource = None
    filename = source_url.split('/')[-1]
    if res:
        target_package_exists = True
        for resource in res['resources']:
            if resource.get('name') and resource['name'] == filename:
                existing_resource = resource
                break
    if DEVEL_SKIP_DOWNLOAD:
        print(f'skipping download of {filename} from {source_url}')
        source_hash = ''
    else:
        source_hash = http_stream_download(f'{tmpdir}/{filename}', {'url': source_url})
    if not existing_resource or existing_resource.get('hash') != source_hash:
        description = 'מקור המידע: ' + source_url
        with instance_package_lock(target_instance_name, target_package_id):
            print(f'updating resource')
            if not target_package_exists:
                print('creating target package')
                res = ckan.package_create(target_instance_name, {
                    'name': target_package_id,
                    'title': target_package_title or target_package_id,
                    'notes': description,
                    'owner_org': target_organization_id
                })
                assert res['success'], str(res)
            print(filename)
            if existing_resource:
                if existing_resource.get('hash') and existing_resource['hash'] == source_hash:
                    print('existing resource found and hash is the same, skipping resource data update')
                else:
                    print('existing resource found, but hash is different, updating resource data')
                    data = {
                        'id': existing_resource['id'],
                        'hash': source_hash,
                        'description': description
                    }
                    upload_filename = f'{tmpdir}/{filename}'
                    res = ckan.resource_update(target_instance_name, data, files=[('upload', open(upload_filename, 'rb'))])
                    assert res['success'], str(res)
            else:
                print('no existing resource found, creating new resource')
                data = {
                    'package_id': target_package_id,
                    'format': target_resource_format,
                    'name': filename,
                    'hash': source_hash,
                    'description': description
                }
                upload_filename = f'{tmpdir}/{filename}'
                res = ckan.resource_create(target_instance_name, data, files=[('upload', open(upload_filename, 'rb'))])
                assert res['success'], str(res)
            run_packages_processing(target_instance_name, target_package_id)
            print('done, all resources created/updated')
    else:
        print('no resources to create/update')
