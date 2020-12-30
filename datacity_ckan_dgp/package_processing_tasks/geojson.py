import os
import sys
from decimal import Decimal

import pyproj
import geojson
import dataflows as DF
from geojson import Feature, Point, FeatureCollection

from datacity_ckan_dgp import ckan
from datacity_ckan_dgp import utils
from datacity_ckan_dgp.package_processing_tasks import common


LAT_LON_FIELD_NAMES = (
    ('lat', 'lon'),
    ('lat', 'long'),
    ('latitude', 'longtitude'),
    ('y', 'x'),
    ('y_itm', 'x_itm'),
    ('n_ord', 'e_ord'),
)


CRS = '+ellps=GRS80 +k=1.00007 +lat_0=31.73439361111111 +lon_0=35.20451694444445 +no_defs +proj=tmerc +units=m +x_0=219529.584 +y_0=626907.39'
projector = pyproj.Proj(CRS)


def pop_case_insensitive(row, field, default=None):
    value = row.get(field)
    if value is not None:
        return row.pop(field)
    else:
        for k, v in row.items():
            if k.lower() == field.lower():
                return row.pop(k)
    return default


def get_lat_lon_values(row, lon_field, lat_field):
    lon = float(pop_case_insensitive(row, lon_field))
    lat = float(pop_case_insensitive(row, lat_field))
    if lon > 200 and lat > 200:
        lon, lat = projector(lon, lat, inverse=True)
    return lon, lat


def process_resource(instance_name, package, resource, package_extras_processed_res):
    lat_field = resource.get("geo_lat_field")
    lon_field = resource.get("geo_lon_field")
    rows = DF.Flow(DF.load(resource['url'])).results()[0][0]
    fc = FeatureCollection([
        Feature(geometry=Point(get_lat_lon_values(r, lon_field, lat_field)),
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
    if resource.get('geo_lat_field') and resource.get('geo_lon_field'):
        return True
    # disabled for now, see comments on this issue for details: https://github.com/hasadna/datacity-k8s/issues/146
    # elif resource.get('datastore_active'):
    #     datastore_info = ckan.datastore_info(instance_name, resource['id'])
    #     schema_fields = {f.lower().strip(): f for f in datastore_info.get('schema', {})}
    #     for lat_field, lon_field in LAT_LON_FIELD_NAMES:
    #         if lat_field in schema_fields and lon_field in schema_fields:
    #             resource['geo_lat_field'] = schema_fields[lat_field]
    #             resource['geo_lon_field'] = schema_fields[lon_field]
    #             return True
    #     return False
    else:
        return False


def process_package(instance_name, package_id):
    common.process_package(instance_name, package_id, "geojson", is_resource_valid_for_processing, process_resource)


if __name__ == "__main__":
    process_package(*sys.argv[1:])
