from decimal import Decimal

import dataflows as DF

import pyproj
import geojson
from geojson import Feature, Point, FeatureCollection

from datacity_ckan_dgp import ckan
from datacity_ckan_dgp import utils

def update_package(instance_name, org_id, package_name, title, resources):
    print("Creating/updating package {} {}".format(package_name, title))

    package = ckan.package_show(instance_name, package_name)
    if not package or package['state'] == 'deleted':
        res = ckan.package_create(instance_name, {
            'name': package_name,
            'title': title,
            'private': False,
            'owner_org': org_id
        })
        if res['success']:
            package = ckan.package_show(instance_name, package_name)
    if package:
        existing_resources = package.get('resources', [])
        existing_resources = dict((r['format'], r['id']) for r in existing_resources)
        print(existing_resources)
        for format, filename in resources:
            with open(filename, 'rb') as f:
                resource = {
                    'package_id': package['id'],
                    'description': '{} - {}'.format(title, format),
                    'format': format,
                    'name': format,
                }
                if format in existing_resources:
                    resource['id'] = existing_resources[format]
                    res = ckan.resource_update(instance_name, resource, files=[('upload', f)])
                    if not res['success']:
                        print('update resource failed: {}'.format(res))
                    else:
                        print('updated resource {} {}'.format(package_name, format))
                else:
                    res = ckan.resource_create(instance_name, resource, files=[('upload', f)])
                    if not res['success']:
                        print('create resource failed: {}'.format(res))
                    else:
                        print('created resource {} {}'.format(package_name, format))
  
def operator(name, params):
    connection_string = params['db_url']
    source_table = params['db_table']
    target_instance_name = params['target_instance_name']
    target_package_id = params['target_package_id']
    target_organization_id = params['target_organization_id']
    
    print('starting db_fetcher operator')
    print('source_table={} target_instance_name={} target_package_id={} target_organization_id={}'.format(
           source_table,   target_instance_name,   target_package_id,   target_organization_id))
    DF.Flow(
        DF.load(connection_string, table=source_table, name=target_package_id,
                infer_strategy=DF.load.INFER_PYTHON_TYPES),
        DF.update_resource(-1, path=target_package_id + '.csv'),
        DF.dump_to_path(target_package_id)
    ).process()
    update_package(
        target_instance_name,
        target_organization_id,
        target_package_id,
        target_package_id,
        [('CSV', '{}/{}.csv'.format(target_package_id, target_package_id))]
    )


if __name__ == '__main__':
    import sys
    import json
    exit(0 if operator('_', json.loads(sys.argv[1])) else 1)
