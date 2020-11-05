import os
import sys
from decimal import Decimal

import geojson
import dataflows as DF
from geojson import Feature, Point, FeatureCollection

from datacity_ckan_dgp import ckan
from datacity_ckan_dgp import utils


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
    package = ckan.package_show(instance_name, package['id'])
    package.setdefault('extras', []).append({"key": package_extras_processed_res, "value": "yes"})
    ckan.package_update(instance_name, package)


def process_package(instance_name, package_id):
    package = ckan.package_show(instance_name, package_id)
    for resource in package['resources']:
        if resource.get("geo_lat_field") and resource.get("geo_lon_field") and resource.get('format') == 'CSV':
            package_extras_processed_res = "processed_res_geojson_{}".format(resource['id'])
            package_extras = {e['key']: e['value'] for e in package.get('extras', [])}
            if package_extras.get(package_extras_processed_res) == "yes":
                print("Already processed geojson ({} > {} > {} {})".format(instance_name, package['name'], resource['name'], resource['id']))
            else:
                print("Starting geojson processing ({} > {} > {} {})".format(instance_name, package['name'], resource['name'], resource['id']))
                process_resource(instance_name, package, resource, package_extras_processed_res)
                print("OK")


if __name__ == "__main__":
    process_package(*sys.argv[1:])
