import os
import requests
import datetime


def get_instance_api_key_url(instance_name):
    return (
        os.environ.get('CKAN_INSTANCE_' + instance_name.upper().replace(' ', '_') + '_API_KEY'),
        os.environ['CKAN_INSTANCE_' + instance_name.upper().replace(' ', '_') + '_URL']
    )


def api_request(method, instance_name, action_name, auth=True, **kwargs):
    api_key, url = get_instance_api_key_url(instance_name)
    url = os.path.join(url, 'api', '3', 'action', action_name)
    return getattr(requests, method.lower())(url, headers={'Authorization': api_key} if auth and api_key else {}, **kwargs).json()


def api_get(instance_name, action_name, auth=True, **kwargs):
    return api_request('get', instance_name, action_name, auth=auth, **kwargs)


def api_post(instance_name, action_name, auth=True, **kwargs):
    return api_request('post', instance_name, action_name, auth=auth, **kwargs)


def api_get_list(instance_name, action_name, auth=True):
    limit, offset = 100, 0
    while True:
        results = api_get(instance_name, action_name, auth=auth, params={'limit': limit, 'offset': offset})['result']
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


def package_create(instance_name, data):
    return api_post(instance_name, 'package_create', json=data)


def parse_datetime(datetime_str):
    return datetime.datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S.%f')


def resource_create(instance_name, data, files=None):
    return api_post(instance_name, 'resource_create', data=data, files=files)


def package_update(instance_name, data):
    return api_post(instance_name, 'package_update', json=data)
