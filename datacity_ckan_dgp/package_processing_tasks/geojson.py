import os
import sys
from decimal import Decimal

import geojson
import dataflows as DF
from geojson import Feature, Point, FeatureCollection

from datacity_ckan_dgp import ckan
from datacity_ckan_dgp import utils
from datacity_ckan_dgp.package_processing_tasks import common


def process_resource(instance_name, package, resource, package_extras_processed_res):
    rows = DF.Flow(DF.load(resource['url'])).results()[0][0]
    fc = FeatureCollection([
        Feature(geometry=Point((float(r.pop('lon')), float(r.pop('lat')))),
                properties=dict((k, float(v) if isinstance(v, Decimal) else v) for k, v in r.items()))
        for r in rows
    ])
    with utils.tempdir() as tmpdir:
        with open(os.path.join(tmpdir, "data.geojson"), 'w') as f:
            geojson.dump(fc, f)
        with open(os.path.join(tmpdir, "data.geojson")) as f:
            ckan.resource_create(instance_name, {
                'package_id': package['id'],
                'description': resource['description'],
                'format': 'GeoJSON',
                'name': resource['name'].replace('.csv', '') + '.geojson',
            }, files=[('upload', f)])
    common.update_package_extras(instance_name, package, package_extras_processed_res)


def is_resource_valid_for_processing(instance_name, package, resource):
    return resource.get("geo_lat_field") and resource.get("geo_lon_field") and resource.get('format') == 'CSV'


def process_package(instance_name, package_id):
    common.process_package(instance_name, package_id, "geojson", is_resource_valid_for_processing, process_resource)


if __name__ == "__main__":
    process_package(*sys.argv[1:])
