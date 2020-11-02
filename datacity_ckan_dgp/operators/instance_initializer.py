import os
from ruamel import yaml

from datacity_ckan_dgp import ckan


AUTOMATION_GROUP_NAME='instance_initializer'
GROUPS_YAML=os.path.join(os.path.dirname(__file__), '..', 'instance_initializer_groups.yaml')


def init_settings_group(instance_name):
    print("Initializing settings group")
    if not ckan.group_show(instance_name, 'settings', group_type='settings'):
        ckan.group_create(instance_name, 'settings', group_type='settings', title='settings')
        print("OK")
    else:
        print("Already initialized settings group")


def init_groups(instance_name):
    print("Initializing groups")
    if not ckan.automation_group_get(instance_name, AUTOMATION_GROUP_NAME, 'initialized_groups'):
        with open(GROUPS_YAML) as f:
            groups = yaml.safe_load(f)
        for group in groups:
            ckan.group_create(instance_name, group['id'], title=group['title'], image_url=group['icon'])
        ckan.automation_group_set(instance_name, AUTOMATION_GROUP_NAME, 'initialized_groups', True)
        print("OK")
    else:
        print("Already initialized groups")


def operator(name, params):
    instance_name = params['instance_name']
    print('starting instance_initializer operator: {}'.format(name))
    print('instance_name={}'.format(instance_name))
    api_key, url = ckan.get_instance_api_key_url(instance_name)
    assert len(api_key) > 10 and len(url) > 10, 'missing instance api_key or url'
    init_settings_group(instance_name)
    init_groups(instance_name)


if __name__ == '__main__':
    import sys
    import json
    exit(0 if operator('_', json.loads(sys.argv[1])) else 1)
