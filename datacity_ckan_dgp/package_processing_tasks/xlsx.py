import os
import sys
from glob import glob

import dataflows as DF

from datacity_ckan_dgp import ckan
from datacity_ckan_dgp import utils
from datacity_ckan_dgp.package_processing_tasks import common


XLSX_PROCESSING_MAX_GB = float(os.getenv('XLSX_PROCESSING_MAX_GB', '1'))


def process_resource(instance_name, package, resource, package_extras_processed_res):
    with utils.tempdir() as tmpdir:
        with common.try_download_resource_url(resource['url'], max_bytes=XLSX_PROCESSING_MAX_GB*1024*1024*1024) as (exceeded_max_bytes, downloaded_filename):
            if not exceeded_max_bytes:
                DF.Flow(
                    DF.load(downloaded_filename or resource['url'], infer_strategy=DF.load.INFER_STRINGS),
                    DF.dump_to_path(os.path.join(tmpdir), format='xlsx')
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
