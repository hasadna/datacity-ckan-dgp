import os
import traceback
from glob import glob
from ruamel.yaml import YAML
from collections import defaultdict

import pyproj
import dataflows as DF

from datacity_ckan_dgp import ckan
from datacity_ckan_dgp import utils


yaml = YAML(typ='safe', pure=True)


AUTOMATION_GROUP_NAME = 'instance_initializer'
ORGANIZATIONS_YAML = os.path.join(os.path.dirname(__file__), '..', 'instance_initializer_organizations.yaml')
GROUPS_YAML = os.path.join(os.path.dirname(__file__), '..', 'instance_initializer_groups.yaml')
PACKAGES_YAML = os.path.join(os.path.dirname(__file__), '..', 'instance_initializer_packages.yaml')
DEFAULT_ORGANIZATION_ID = 'muni'  # this must be unique across both groups and organizations


CRS = '+ellps=GRS80 +k=1.00007 +lat_0=31.73439361111111 +lon_0=35.20451694444445 +no_defs +proj=tmerc +units=m +x_0=219529.584 +y_0=626907.39'
projector = pyproj.Proj(CRS)


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
            groups = yaml.load(f)
        for group in groups:
            if not ckan.group_show(instance_name, group['id']):
                ckan.group_create(instance_name, group['id'], title=group['title'], image_url=group['icon'])
        ckan.automation_group_set(instance_name, AUTOMATION_GROUP_NAME, 'initialized_groups', True)
        print("OK")
    else:
        print("Already initialized groups")


def init_organizations(instance_name, default_organization_title):
    print("Initializing organizations")
    if not ckan.automation_group_get(instance_name, AUTOMATION_GROUP_NAME, 'initialized_organizations'):
        with open(ORGANIZATIONS_YAML) as f:
            organizations = yaml.load(f)
        for organization in organizations:
            if not ckan.organization_show(instance_name, organization['id']):
                title = default_organization_title if organization['id'] == 'muni' else organization['title']
                image_url = organization.get('icon')
                ckan.organization_create(instance_name, organization['id'], title=title, image_url=image_url)
        ckan.automation_group_set(instance_name, AUTOMATION_GROUP_NAME, 'initialized_organizations', True)
        print("OK")
    else:
        print("Already initialized organizations")


def muni_resource_filter(source_filename, target_path, muni_filter_column, muni_filter_texts, geo_wgs_parsing, muni_filter_column_in):
    assert muni_filter_column_in or muni_filter_column, "missing muni_filter_column or muni_filter_column_id properties"
    stats = defaultdict(int)

    def _muni_filter(rows):
        for row in rows:
            ok = False
            for text in muni_filter_texts:
                if muni_filter_column_in:
                    for col in muni_filter_column_in:
                        if ' {} '.format(text.strip()) in ' {} '.format(row[col].strip()):
                            ok = True
                            break
                else:
                    if row[muni_filter_column].strip() == text.strip():
                        ok = True
                        break
            if ok:
                if geo_wgs_parsing:
                    if geo_wgs_parsing.get('field_x') and geo_wgs_parsing.get('field_y'):
                        try:
                            lon, lat = projector(
                                float(row[geo_wgs_parsing['field_x']]),
                                float(row[geo_wgs_parsing['field_y']]),
                                inverse=True
                            )
                            stats['geo_wgs_parsing_num_valid_rows'] += 1
                        except:
                            lon, lat = None, None
                        row[geo_wgs_parsing['output_field_lon']], row[geo_wgs_parsing['output_field_lat']] = lon, lat
                yield row
                stats['num_rows'] += 1

    DF.Flow(
        DF.load(source_filename, infer_strategy=DF.load.INFER_STRINGS),
        *(
            [
                DF.add_field(geo_wgs_parsing['output_field_lon'], 'number'),
                DF.add_field(geo_wgs_parsing['output_field_lat'], 'number'),
            ] if geo_wgs_parsing else []
        ),
        _muni_filter,
        DF.dump_to_path(target_path)
    ).process()
    if geo_wgs_parsing:
        print("geo_wgs_parsing_num_valid_rows: {}".format(stats['geo_wgs_parsing_num_valid_rows']))
    return stats['num_rows']


def init_package(instance_name, package, muni_filter_texts):
    existing_package, created_package_res = ckan.package_show(instance_name, package['id']), None
    if not existing_package:
        res = ckan.package_search(package['ckan']['url'], {'q': 'name:{}'.format(package['ckan']['package_id'])})
        assert res['count'] == 1, res
        source_package = res['results'][0]
        source_resource = None
        for resource in source_package['resources']:
            if resource['id'] == package['ckan']['resource_id']:
                source_resource = resource
                break
        if not source_resource:
            print("WARNING! Using first resource instead of the specified resource")
            source_resource = source_package['resources'][0]
        try:
            resource_name = source_resource['name']
            resource_url = source_resource['url']
            if "://e.data.gov.il" in resource_url:
                resource_url = resource_url.replace("://e.data.gov.il", "://data.gov.il")
        except:
            print("Failed to get metadata from source resource")
            print(source_resource)
            raise
        resource_description = source_resource.get('description')
        # resource_last_modified = source_resource.get('last_modified')
        with utils.tempdir(keep=False) as tmpdir:
            resource_filename = resource_url.split("/")[-1]
            if resource_filename.startswith("."):
                resource_filename = "data{}".format(resource_filename)
            headers = {}
            if "://data.gov.il" in resource_url:
                headers['User-Agent'] = 'datagov-external-client'
            utils.http_stream_download(os.path.join(tmpdir, resource_filename), {
                "url": resource_url,
                "headers": headers
            })
            num_filtered_rows = muni_resource_filter(
                os.path.join(tmpdir, resource_filename), os.path.join(tmpdir, "muni_filtered"), package.get('muni_filter_column'),
                muni_filter_texts, package.get('geo_wgs_parsing'),
                package.get('muni_filter_column_in')
            )
            if num_filtered_rows > 0:
                dirnames = list(glob(os.path.join(tmpdir, "muni_filtered", "*.csv")))
                assert len(dirnames) == 1
                muni_filtered_csv_filename = dirnames[0]
                package_description = source_package.get('notes')
                source_package_url = os.path.join(package['ckan']['url'], 'dataset', package['ckan']['package_id'])
                source_package_note = "מקור המידע: " + source_package_url
                package_description = source_package_note if not package_description else package_description + "\n\n" + source_package_note
                res = ckan.package_create(instance_name, {
                    'name': package['id'],
                    'title': '{} | {}'.format(package['title'], package['source_title']),
                    'private': True,
                    'license_id': source_package.get('license_id'),
                    'notes': package_description,
                    'url': source_package.get('url'),
                    'version': source_package.get('version'),
                    'owner_org': DEFAULT_ORGANIZATION_ID,
                    'extras': [
                        {"key": "sync_source_package_url", "value": package['ckan']['url'].strip('/') + '/dataset/{}'.format(package['ckan']['package_id'])},
                        {'key': 'sync_source_org_description', 'value': source_package.get('organization', {}).get('description')}
                    ]
                })
                assert res['success'], 'create package failed: {}'.format(res)
                target_package_id = res['result']['id']
                with open(muni_filtered_csv_filename) as f:
                    res = ckan.resource_create(instance_name, {
                        'package_id': target_package_id,
                        'description': resource_description,
                        'format': "CSV",
                        'name': resource_name,
                        'url': os.path.basename(muni_filtered_csv_filename),
                        **({
                            "geo_lat_field": package['geo_wgs_parsing']["output_field_lat"],
                            "geo_lon_field": package['geo_wgs_parsing']["output_field_lon"],
                           } if package.get('geo_wgs_parsing') else {}),
                    }, files={
                        'upload': (os.path.basename(muni_filtered_csv_filename), f)
                    })
                assert res['success'], 'create resource failed: {}'.format(res)
                created_package = ckan.package_show(instance_name, target_package_id)
                created_package['private'] = False
                created_package_res = ckan.package_update(instance_name, created_package)
                assert created_package_res['success'], 'failed to set package to public: {}'.format(created_package_res)
            else:
                print("no rows after muni filter")
    else:
        print("Package already exists ({})".format(existing_package['id']))
    created_package = created_package_res['result'] if created_package_res else existing_package
    created_package_group_names = [g['name'] for g in created_package.get('groups', [])]
    created_package['groups'] = [{"name": name} for name in created_package_group_names]
    num_added_groups = 0
    for group_name in package.get('groups', []):
        if group_name not in created_package_group_names:
            created_package['groups'].append({'name': group_name})
            num_added_groups += 1
    res = ckan.package_update(instance_name, created_package)
    assert res['success'], 'failed to add groups to created package: {}'.format(res)
    print("Added {} groups".format(num_added_groups))

def init_packages(instance_name, muni_filter_texts, init_package_id=None):
    print("Initializing packages")
    if not ckan.automation_group_get(instance_name, AUTOMATION_GROUP_NAME, 'initialized_packages'):
        with open(PACKAGES_YAML) as f:
            packages = yaml.load(f)
        num_errors = 0
        num_success = 0
        for package in packages:
            if init_package_id and package['id'] != init_package_id:
                continue
            try:
                print("Initializing package {}: {} | {}".format(package['id'], package['title'], package['source_title']))
                init_package(instance_name, package, muni_filter_texts)
                num_success += 1
            except:
                traceback.print_exc()
                print("Failed to initialize package {}: {} | {}".format(package['id'], package['title'], package['source_title']))
                num_errors += 1
        assert num_success > 0 and num_errors == 0, "Errors initializing packages"
        ckan.automation_group_set(instance_name, AUTOMATION_GROUP_NAME, 'initialized_packages', True)
        print("OK")
    else:
        print("Already initialized packages")


def operator(name, params, init_package_id=None):
    instance_name = params['instance_name']
    default_organization_title = params['default_organization_title']
    muni_filter_texts = [t.strip() for t in params['muni_filter_texts'].split(",") if t.strip()]
    print('starting instance_initializer operator: {}'.format(name))
    print('instance_name={}'.format(instance_name))
    api_key, url = ckan.get_instance_api_key_url(instance_name)
    assert len(api_key) > 10 and len(url) > 10, 'missing instance api_key or url'
    if not init_package_id:
        init_settings_group(instance_name)
        init_groups(instance_name)
        init_organizations(instance_name, default_organization_title)
    init_packages(instance_name, muni_filter_texts, init_package_id)


# python3 -m datacity_ckan_dgp.operators.instance_initializer '{"instance_name": "local_development", "default_organization_title": "עיריית חיפה", "muni_filter_texts": "חיפה"}'


if __name__ == '__main__':
    import sys
    import json
    exit(0 if operator('_', json.loads(sys.argv[1]), *sys.argv[2:]) else 1)
