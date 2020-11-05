import os
import sys
from glob import glob

import dataflows_xlsx
import dataflows as DF

from datacity_ckan_dgp import ckan
from datacity_ckan_dgp import utils
from datacity_ckan_dgp.package_processing_tasks import common


def process_resource(instance_name, package, resource, package_extras_processed_res):
    print("Processing resource...")
    with utils.tempdir() as tmpdir:
        DF.Flow(
            DF.load(resource['url']),
            dataflows_xlsx.dump_to_path(os.path.join(tmpdir), format='xlsx')
        ).process()
        filenames = list(glob(os.path.join(tmpdir, "*.xlsx")))
        assert len(filenames) == 1
        filename = filenames[0]
        with open(filename, "rb") as f:
            ckan.resource_create(instance_name, {
                'package_id': package['id'],
                'description': resource['description'],
                'format': 'XLSX',
                'name': resource['name'].replace('.csv', '') + '.xlsx',
            }, files=[('upload', f)])
    common.update_package_extras(instance_name, package, package_extras_processed_res)


def is_resource_valid_for_processing(instance_name, package, resource):
    return resource.get('format') == 'CSV'


def process_package(instance_name, package_id):
    common.process_package(instance_name, package_id, "xlsx", is_resource_valid_for_processing, process_resource)


if __name__ == "__main__":
    process_package(*sys.argv[1:])
