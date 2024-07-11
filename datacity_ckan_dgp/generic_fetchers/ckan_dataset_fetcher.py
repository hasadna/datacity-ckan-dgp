import os

import requests

from .. import ckan
from ..utils import http_stream_download


def fetch(source_url, target_instance_name, target_package_id, target_organization_id, tmpdir):
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
            if format_ and name and hash_ and id_:
                existing_target_resources[f'{name}.{format_}'] = {'hash': hash_, 'id': id_}
    source_package_id = source_url.split('/dataset/')[1].split('/')[0]
    source_instance_baseurl = source_url.split('/dataset/')[0]
    if 'data.gov.il' in source_instance_baseurl:
        headers = {'user-agent': 'datagov-external-client'}
    else:
        headers = None
    res = requests.get(f'{source_instance_baseurl}/api/3/action/package_show?id={source_package_id}', headers=headers).json()
    assert res['success']
    package_title = res['result']['title']
    resources_to_update = []
    for resource in res['result']['resources']:
        id_ = resource.get('id') or ''
        url = resource.get('url') or ''
        if url and id_:
            if 'e.data.gov.il' in url:
                url = url.replace('e.data.gov.il', 'data.gov.il')
            filename = url.split('/')[-1]
            source_hash = http_stream_download(f'{tmpdir}/{id_}', {'url': url, 'headers': headers})
            source_format = resource.get('format') or ''
            source_name = resource.get('name') or ''
            description = resource.get('description') or ''
            if existing_target_resources.get(f'{source_name}.{source_format}', {}).get('hash') != source_hash:
                resources_to_update.append((id_, source_name, source_format, source_hash, description, filename))
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
