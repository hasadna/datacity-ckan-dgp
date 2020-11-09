import os
import tempfile
from decimal import Decimal

import dataflows as DF

from datacity_ckan_dgp import ckan

def update_package(instance_name, org_id, package_name, title, resources):
    print("Creating/updating package {}@{} {}".format(package_name, org_id, title))

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
        else:
            print('Failed to create package', res)
    print(package)
    if package:
        existing_resources = package.get('resources', [])
        existing_resources = dict((r['format'], r['id']) for r in existing_resources)
        print(existing_resources)
        for format, filename in resources:
            print(format, filename)
            with open(filename, 'rb') as f:
                resource = {
                    'package_id': package['id'],
                    'description': '{} - {}'.format(title, format),
                    'format': format,
                    'name': format,
                }
                if format in existing_resources:
                    print('Updating resource', existing_resources[format])
                    resource['id'] = existing_resources[format]
                    res = ckan.resource_update(instance_name, resource, files=[('upload', f)])
                    if not res['success']:
                        print('update resource failed: {}'.format(res))
                    else:
                        print('updated resource {} {}: {}'.format(package_name, format, res))
                else:
                    print('Creating resource', resource)
                    res = ckan.resource_create(instance_name, resource, files=[('upload', f)])
                    if not res['success']:
                        print('create resource failed: {}'.format(res))
                    else:
                        print('created resource {} {}: {}'.format(package_name, format, res))
  
def operator(name, params):
    connection_string = params['db_url']
    source_table = params['db_table']
    target_instance_name = params['target_instance_name']
    target_package_id = params['target_package_id']
    target_organization_id = params['target_organization_id']
    
    print('starting db_fetcher operator')
    print('source_table={} target_instance_name={} target_package_id={} target_organization_id={}'.format(
           source_table,   target_instance_name,   target_package_id,   target_organization_id))
    with tempfile.TemporaryDirectory() as tempdir:
        csv_filename = target_package_id + '.csv'
        DF.Flow(
            DF.load(connection_string, table=source_table, name=target_package_id,
                    infer_strategy=DF.load.INFER_PYTHON_TYPES),
            DF.update_resource(-1, path=csv_filename),
            DF.conditional(
                lambda dp: any(f.name == '_source' for f in dp.resources[0].schema.fields),
                DF.Flow(
                    DF.delete_fields(['_source'])
                )
            ),
            DF.dump_to_path(tempdir)
        ).process()
        csv_filename = os.path.join(tempdir, csv_filename)
        update_package(
            target_instance_name,
            target_organization_id,
            target_package_id,
            target_package_id,
            [('CSV', csv_filename)]
        )


if __name__ == '__main__':
    import sys
    import json
    exit(0 if operator('_', json.loads(sys.argv[1])) else 1)
