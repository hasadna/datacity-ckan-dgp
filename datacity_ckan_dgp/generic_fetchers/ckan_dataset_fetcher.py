import os
import json
import shutil

import requests
import dataflows as DF

from .. import ckan
from ..utils import http_stream_download


DESCRIPTION = """
Fetch from CKAN Dataset

URL example: https://data.gov.il/dataset/automated-devices

If source filter is not provided it will copy all resources as-is

If source filter is provided it will do the following:
  1. Find the source resource for tabular data - either CSV or XLSX
  2. Create the filtered tabular data as a CSV (XLSX will be created by our other automation)
  3. If GEOJSON resource is available it will be copied and filtered separately
  4. All other resources will be ignored

Local development examples:
   without source filter:
     python3 -m datacity_ckan_dgp.operators.generic_fetcher '{"source_url": "https://data.gov.il/dataset/automated-devices", "target_instance_name": "LOCAL_DEVELOPMENT", "target_package_id": "automated-devices", "target_organization_id": "israel-gov", "tmpdir": ".data/ckan_fetcher_tmpdir"}'
  with source filter:
     python3 -m datacity_ckan_dgp.operators.generic_fetcher '{"source_url": "https://data.gov.il/dataset/automated-devices", "target_instance_name": "LOCAL_DEVELOPMENT", "target_package_id": "automated-devices", "target_organization_id": "israel-gov", "tmpdir": ".data/ckan_fetcher_tmpdir", "source_filter": {"City": "חיפה"}}'
"""

DEVEL_SKIP_DOWNLOAD = os.getenv('DEVEL_SKIP_DOWNLOAD', 'false').lower() == 'true'


def get_filtered_tabular_resources_to_update(tmpdir, source_filter, id_, name, format_, hash_, description, filename):
    print(f'filtering tabular data from {filename} with format {format_}...')
    resources_to_update = []
    DF.Flow(
        DF.load(f'{tmpdir}/{id_}', name='filtered', format=format_.lower(), infer_strategy=DF.load.INFER_STRINGS, cast_strategy=DF.load.CAST_TO_STRINGS),
        DF.filter_rows(lambda row: all(row.get(k) == v for k, v in source_filter.items())),
        DF.printer(),
        DF.dump_to_path(f'{tmpdir}/{id_}-filtered')
    ).process()
    with open(f'{tmpdir}/{id_}-filtered/datapackage.json', 'r') as f:
        dp = json.load(f)
        hash_ = dp['hash']
        count_of_rows = dp['count_of_rows']
    if count_of_rows == 0:
        print('no rows found, skipping resource')
    else:
        shutil.copyfile(f'{tmpdir}/{id_}-filtered/filtered.csv', f'{tmpdir}/{id_}')
        if not filename.lower().endswith('.csv'):
            filename = filename.lower().replace('.xlsx', '.csv').replace('.xls', '.csv')
            if not filename.endswith('.csv'):
                filename = f'{filename}.csv'
        resources_to_update.append((id_, name, 'CSV', hash_, description, filename))
    return resources_to_update


def get_filtered_geojson_resources_to_update(tmpdir, source_filter, id_, name, format_, hash_, description, filename):
    print(f'filtering geojson data from {filename} with format {format_}...')
    resources_to_update = []
    with open(f'{tmpdir}/{id_}', 'r') as f:
        data = json.load(f)
    features = data.get('features') or []
    features = [feature for feature in features if all(feature['properties'].get(k) == v for k, v in source_filter.items())]
    if not features:
        print('no features found, skipping resource')
    else:
        data['features'] = features
        with open(f'{tmpdir}/{id_}', 'w') as f:
            json.dump(data, f)
        resources_to_update.append((id_, name, 'GEOJSON', hash_, description, filename))
    return resources_to_update


def get_resources_to_update(resources, tmpdir, headers, existing_target_resources, source_filter):
    resources_to_update = []
    for resource in resources:
        id_ = resource.get('id') or ''
        url = resource.get('url') or ''
        if url and id_:
            if 'e.data.gov.il' in url:
                url = url.replace('e.data.gov.il', 'data.gov.il')
            filename = url.split('/')[-1]
            if DEVEL_SKIP_DOWNLOAD:
                print(f'skipping download of {filename} from {url}')
                source_hash = ''
            else:
                source_hash = http_stream_download(f'{tmpdir}/{id_}', {'url': url, 'headers': headers})
            source_format = resource.get('format') or ''
            source_name = resource.get('name') or ''
            description = resource.get('description') or ''
            if source_filter or existing_target_resources.get(f'{source_name}.{source_format}', {}).get('hash') != source_hash:
                resources_to_update.append((id_, source_name, source_format, source_hash, description, filename))
    if source_filter:
        prefiltered_resources = resources_to_update
        resources_to_update = []
        names = set(args[1].lower() for args in prefiltered_resources)
        for name in names:
            print(f'filtering resources for {name}')
            source_resources_by_format = {args[2].lower(): args for args in prefiltered_resources if args[1].lower() == name}
            if 'csv' in source_resources_by_format:
                resources_to_update.extend(get_filtered_tabular_resources_to_update(tmpdir, source_filter, *source_resources_by_format['csv']))
            elif 'xlsx' in source_resources_by_format:
                resources_to_update.extend(get_filtered_tabular_resources_to_update(tmpdir, source_filter, *source_resources_by_format['xlsx']))
            if 'geojson' in source_resources_by_format:
                resources_to_update.extend(get_filtered_geojson_resources_to_update(tmpdir, source_filter, *source_resources_by_format['geojson']))
    return resources_to_update


def fetch(source_url, target_instance_name, target_package_id, target_organization_id, tmpdir, source_filter):
    res = ckan.package_show(target_instance_name, target_package_id)
    target_package_exists = False
    existing_target_resources = {}
    if res:
        target_package_exists = True
        for resource in res['resources']:
            format_ = resource.get('format') or ''
            name = resource.get('name') or ''
            hash_ = resource.get('hash') or ''
            id_ = resource.get('id') or ''
            if format_ and name and id_:
                existing_target_resources[f'{name}.{format_}'] = {'hash': hash_, 'id': id_}
    source_package_id = source_url.split('/dataset/')[1].split('/')[0]
    source_instance_baseurl = source_url.split('/dataset/')[0]
    if 'data.gov.il' in source_instance_baseurl:
        headers = {'user-agent': 'datagov-external-client'}
    else:
        headers = None
    if DEVEL_SKIP_DOWNLOAD:
        print('skipping download of package metadata')
        with open(f'{tmpdir}/package.json', 'r') as f:
            res = json.load(f)
    else:
        try:
            res = requests.get(f'{source_instance_baseurl}/api/3/action/package_show?id={source_package_id}', headers=headers)
            res_json = res.json()
            assert res_json['success']
        except Exception as e:
            raise Exception(f'Failed to fetch source package\n{res.text if res else ""}') from e
        res = res_json
    with open(f'{tmpdir}/package.json', 'w') as f:
        json.dump(res, f)
    package_title = res['result']['title']
    resources_to_update = get_resources_to_update(res['result']['resources'], tmpdir, headers, existing_target_resources, source_filter)
    if resources_to_update:
        print(f'updating {len(resources_to_update)} resources')
        if not target_package_exists:
            print('creating target package')
            res = ckan.package_create(target_instance_name, {
                'name': target_package_id,
                'title': package_title,
                'owner_org': target_organization_id
            })
            assert res['success'], str(res)
        for id_, name, format_, hash_, description, filename in resources_to_update:
            print(f'{name}.{format_}')
            if os.path.exists(f'{tmpdir}/{filename}'):
                os.unlink(f'{tmpdir}/{filename}')
            os.rename(f'{tmpdir}/{id_}', f'{tmpdir}/{filename}')
            if f'{name}.{format_}' in existing_target_resources:
                if existing_target_resources[f'{name}.{format_}'].get('hash') and existing_target_resources[f'{name}.{format_}']['hash'] == hash_:
                    print('existing resource found and hash is the same, skipping resource data update')
                else:
                    print('existing resource found, but hash is different, updating resource data')
                    res = ckan.resource_update(target_instance_name, {
                        'id': existing_target_resources[f'{name}.{format_}']['id'],
                        'hash': hash_,
                        'description': description
                    }, files=[('upload', open(f'{tmpdir}/{filename}', 'rb'))])
                    assert res['success'], str(res)
            else:
                print('no existing resource found, creating new resource')
                res = ckan.resource_create(target_instance_name, {
                    'package_id': target_package_id,
                    'format': format_,
                    'name': name,
                    'hash': hash_,
                    'description': description
                }, files=[('upload', open(f'{tmpdir}/{filename}', 'rb'))])
                assert res['success'], str(res)
        print('done, all resources created/updated')
    else:
        print('no resources to create/update')
