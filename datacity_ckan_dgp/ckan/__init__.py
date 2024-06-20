import os
import json
import requests
import datetime


def get_instance_api_key_url(instance_name):
    if instance_name.startswith('http'):
        return None, instance_name
    else:
        return (
            os.environ.get('CKAN_INSTANCE_' + instance_name.upper().replace(' ', '_') + '_API_KEY'),
            os.environ['CKAN_INSTANCE_' + instance_name.upper().replace(' ', '_') + '_URL']
        )


def api_request(method, instance_name, action_name, auth=True, **kwargs):
    api_key, url = get_instance_api_key_url(instance_name)
    url = os.path.join(url, 'api', '3', 'action', action_name)
    headers = {'Authorization': api_key} if auth and api_key else {}
    if "://data.gov.il" in url:
        headers['user-agent'] = 'datagov-external-client'
    res = getattr(requests, method.lower())(url, headers=headers, **kwargs)
    try:
        return res.json()
    except Exception:
        print(url)
        print(headers)
        print(kwargs)
        print(res.text)
        raise


def api_get(instance_name, action_name, auth=True, **kwargs):
    return api_request('get', instance_name, action_name, auth=auth, **kwargs)


def api_post(instance_name, action_name, auth=True, **kwargs):
    return api_request('post', instance_name, action_name, auth=auth, **kwargs)


def api_get_list(instance_name, action_name, auth=True, **params):
    limit, offset = 100, 0
    while True:
        results = api_get(instance_name, action_name, auth=auth, params={'limit': limit, 'offset': offset, **params})['result']
        for result in results:
            yield result
        if len(results) == 0:
            break
        offset += limit


def package_list_public(instance_name):
    yield from api_get_list(instance_name, 'package_list', auth=False)


def package_list(instance_name):
    yield from api_get_list(instance_name, 'package_list', auth=True)


def package_show_public(instance_name, package_name):
    return api_get(instance_name, 'package_show', auth=False, params={'id': package_name})['result']


def package_show(instance_name, package_name):
    res = api_get(instance_name, 'package_show', auth=True, params={'id': package_name})
    if res['success']:
        return res['result']
    else:
        return None


def resource_show(instance_name, resource_id):
    res = api_get(instance_name, 'resource_show', auth=True, params={'id': resource_id})
    if res['success']:
        return res['result']
    else:
        return None


def package_search(instance_name, params):
    res = api_get(instance_name, 'package_search', params=params)
    assert res['success'], res
    return res['result']


def resource_search(instance_name, params):
    res = api_get(instance_name, 'resource_search', params=params)
    assert res['success'], res
    return res['result']


def package_create(instance_name, data):
    return api_post(instance_name, 'package_create', json=data)


def parse_datetime(datetime_str):
    return datetime.datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S.%f')


def resource_create(instance_name, data, files=None):
    return api_post(instance_name, 'resource_create', data=data, files=files)


def package_update(instance_name, data):
    return api_post(instance_name, 'package_update', json=data)


def resource_update(instance_name, data, files=None):
    return api_post(instance_name, 'resource_update', data=data, files=files)


def group_list(instance_name, **params):
    yield from api_get_list(instance_name, 'group_list', auth=True, **params)


def group_show(instance_name, group_name, group_type="group", **params):
    res = api_get(instance_name, 'group_show', auth=True, params={'id': group_name, 'type': group_type, **params})
    if res['success']:
        return res['result']
    else:
        return None


def group_create(instance_name, group_name, group_type='group', **data):
    res = api_post(instance_name, 'group_create', data={"name": group_name, 'type': group_type, **data})
    assert res['success'], res


def organization_show(instance_name, name, **params):
    res = api_get(instance_name, 'organization_show', auth=True, params={'id': name, **params})
    if res['success']:
        return res['result']
    else:
        return None


def organization_create(instance_name, name, **data):
    res = api_post(instance_name, 'organization_create', data={"name": name, **data})
    assert res['success'], res


def group_update(instance_name, data):
    res = api_post(instance_name, 'group_update', json=data)
    assert res['success'], res


def automation_group_get(instance_name, group_name, key):
    group = group_show(instance_name, group_name, group_type='automation')
    return json.loads(group['json']).get(key) if group else None


def automation_group_set(instance_name, group_name, key, value):
    group = group_show(instance_name, group_name, group_type='automation')
    if group:
        group_json = json.loads(group['json'])
        group_json[key] = value
        group['json'] = json.dumps(group_json)
        group_update(instance_name, group)
    else:
        group_create(instance_name, group_name, group_type='automation', json=json.dumps({key: value}))


def datastore_info(instance_name, resource_id):
    res = api_post(instance_name, 'datastore_info', json={'id': resource_id})
    assert res['success'], res
    return res['result']
