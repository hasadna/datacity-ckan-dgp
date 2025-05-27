import os
import sys
import json
import shutil
import hashlib
import tempfile
import requests
import contextlib

import geopandas
from osgeo import ogr
import dataflows as DF

from datacity_ckan_dgp.package_processing_tasks.geojson import projector
from datacity_ckan_dgp import ckan


def gis_query_geojson_iterate_all(gis_url):
    url = gis_url.rstrip('/') + '/query'
    result_offset = 0
    result_record_count = 1000
    while True:
        params = {
            'where': '1=1',
            'outFields': '*',
            'f': 'geojson',
            'resultOffset': result_offset,
            'resultRecordCount': result_record_count
        }
        res = requests.get(url, params=params)
        res.raise_for_status()
        data = res.json()
        assert data['type'] == 'FeatureCollection'
        if not data['features']:
            break
        for feature in data['features']:
            yield feature
            result_offset += 1


def iterate_gis_jsonlines(tmpdir):
    with open(os.path.join(tmpdir, 'gis.jsonlines'), 'r') as f:
        for line in f:
            if line.strip():
                try:
                    yield json.loads(line)
                except Exception as e:
                    raise Exception(f'failed to parse json line: {line}') from e


def fetch_gis_json(gis_url):
    gis_json_url = f'{gis_url}?f=pjson'
    print(f'fetching gis json from {gis_json_url}')
    res = requests.get(gis_json_url)
    res.raise_for_status()
    return res.json()


@contextlib.contextmanager
def tempdir(tmpdir):
    if tmpdir:
        os.makedirs(tmpdir, exist_ok=True)
        yield tmpdir
    else:
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir


class FailedToConvertFeature(Exception):
    pass


def geojson_feature_to_itm(feature):
    if feature['type'] != 'Feature':
        raise FailedToConvertFeature(f'feature is not a Feature: {feature}')
    if feature.get('geometry', {}).get('type') == 'Polygon':
        new_coordinates = []
        for ring in feature['geometry']['coordinates']:
            new_ring = []
            for coord in ring:
                new_ring.append([*projector(*coord)])
            new_coordinates.append(new_ring)
        feature['geometry']['coordinates'] = new_coordinates
    elif feature.get('geometry', {}).get('type') == 'MultiPolygon':
        new_coordinates = []
        for polygon in feature['geometry']['coordinates']:
            new_polygon = []
            for ring in polygon:
                new_ring = []
                for coord in ring:
                    new_ring.append([*projector(*coord)])
                new_polygon.append(new_ring)
            new_coordinates.append(new_polygon)
        feature['geometry']['coordinates'] = new_coordinates
    else:
        raise FailedToConvertFeature(f'unsupported geometry type: {feature.get('geometry', {}).get('type')}')
    return feature


def features_to_csv(features, fields=None):
    for feature in features:
        if not fields:
            yield feature['properties']
        else:
            yield {k: str(feature['properties'].get(k) or '') for k in fields}


def geojson_to_kml(geojson_path, kml_path):
    ogr.RegisterAll()
    geojson_ds = ogr.Open(geojson_path)
    kml_driver = ogr.GetDriverByName('KML')
    kml_ds = kml_driver.CreateDataSource(kml_path)
    for i in range(geojson_ds.GetLayerCount()):
        layer = geojson_ds.GetLayerByIndex(i)
        kml_layer = kml_ds.CreateLayer(layer.GetName(), geom_type=layer.GetGeomType())
        layer_defn = layer.GetLayerDefn()
        for j in range(layer_defn.GetFieldCount()):
            field_defn = layer_defn.GetFieldDefn(j)
            kml_layer.CreateField(field_defn)
        for feature in layer:
            kml_layer.CreateFeature(feature.Clone())


def get_geoxml_coordinates_item(root_item):
    res = '<item type="list">'
    for item in root_item:
        if isinstance(item, list):
            res += get_geoxml_coordinates_item(item)
        else:
            res += f'<item type="float">{item}</item>'
    res += '</item>'
    return res


def geojson_to_geoxml(features, geoxml_path, itm=False):
    with open(geoxml_path, 'w') as f:
        f.write('<?xml version="1.0" ?>\n')
        f.write('<root>\n')
        f.write('  <type type="str">FeatureCollection</type>\n')
        f.write('  <features type="list">\n')
        for feature in features:
            if itm:
                feature = geojson_feature_to_itm(feature)
            f.write('    <item type="dict">\n')
            if feature['type'] != 'Feature':
                raise FailedToConvertFeature(f'feature is not a Feature: {feature}')
            f.write('      <type type="str">Feature</type>\n')
            geometry = feature['geometry']
            f.write('      <geometry type="dict">\n')
            if feature['geometry']['type'] not in ['Polygon', 'MultiPolygon']:
                raise FailedToConvertFeature(f'unsupported geometry type: {feature["geometry"]["type"]}')
            f.write(f'        <type type="str">{geometry["type"]}</type>\n')
            f.write('        <coordinates type="list">\n')
            for item in geometry['coordinates']:
                f.write(f'          {get_geoxml_coordinates_item(item)}\n')
            f.write('        </coordinates>\n')
            f.write('      </geometry>\n')
            f.write('    </item>\n')
        f.write('</root>\n')


def create_gis_data(gis_url, tmpdir):
    with open(os.path.join(tmpdir, 'gis.json'), 'w') as f:
        json.dump(fetch_gis_json(gis_url), f, ensure_ascii=False, indent=2)
    with open(os.path.join(tmpdir, 'gis.jsonlines'), 'w') as f:
        for feature in gis_query_geojson_iterate_all(gis_url):
            f.write(json.dumps(feature, ensure_ascii=False) + '\n')
    print("Create geojson")
    with open(os.path.join(tmpdir, 'gis.geojson'), 'w') as f:
        f.write('{"type": "FeatureCollection","features": [\n')
        for i, feature in enumerate(iterate_gis_jsonlines(tmpdir)):
            if i > 0:
                f.write(',\n')
            f.write('  ' + json.dumps(feature, ensure_ascii=False))
        f.write(']}')
    print("Create gis.itm.geojson")
    try:
        with open(os.path.join(tmpdir, 'gis.itm.geojson'), 'w') as f:
            f.write('{"type": "FeatureCollection","features": [\n')
            for i, feature in enumerate(iterate_gis_jsonlines(tmpdir)):
                if i > 0:
                    f.write(',\n')
                feature = geojson_feature_to_itm(feature)
                f.write('  ' + json.dumps(feature, ensure_ascii=False))
            f.write(']}')
    except FailedToConvertFeature as e:
        print(str(e))
        print('failed to convert feature to itm')
        os.unlink(os.path.join(tmpdir, 'gis.itm.geojson'))
    print("Create shapefile.zip")
    geojson_data = geopandas.read_file(os.path.join(tmpdir, 'gis.geojson'))
    os.makedirs(os.path.join(tmpdir, 'shapefile'), exist_ok=True)
    geojson_data.to_file(os.path.join(tmpdir, 'shapefile/gis.shp'), driver='ESRI Shapefile')
    os.system(f'cd {tmpdir} && zip -r shapefile.zip shapefile')
    print("Create gis.csv, gis.xlsx")
    feature_properties = set()
    for row in features_to_csv(iterate_gis_jsonlines(tmpdir)):
        feature_properties.update(row.keys())
    DF.Flow(
        features_to_csv(iterate_gis_jsonlines(tmpdir), fields=feature_properties),
        DF.dump_to_path(os.path.join(tmpdir, 'csv')),
        DF.dump_to_path(os.path.join(tmpdir, 'xlsx'), format='xlsx')
    ).process()
    shutil.copyfile(os.path.join(tmpdir, 'csv', 'res_1.csv'), os.path.join(tmpdir, 'gis.csv'))
    shutil.copyfile(os.path.join(tmpdir, 'xlsx', 'res_1.xlsx'), os.path.join(tmpdir, 'gis.xlsx'))
    print("Create gis.xml")
    with open(os.path.join(tmpdir, 'gis.xml'), 'w') as f:
        f.write('<?xml version="1.0" encoding="UTF-8" ?>\n')
        f.write('<root>\n')
        for properties in features_to_csv(iterate_gis_jsonlines(tmpdir), fields=feature_properties):
            f.write('  <item>\n')
            for k, v in properties.items():
                v = v.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;').replace(
                    '\'', '&apos;')
                f.write(f'    <{k} type="str">{v}</{k}>\n')
            f.write('  </item>\n')
        f.write('</root>\n')
    print("Create gis.kml")
    geojson_to_kml(os.path.join(tmpdir, 'gis.geojson'), os.path.join(tmpdir, 'gis.kml'))
    print("Create gis.geoxml")
    try:
        geojson_to_geoxml(iterate_gis_jsonlines(tmpdir), os.path.join(tmpdir, 'gis.geoxml'))
    except FailedToConvertFeature as e:
        print(str(e))
        print('failed to convert feature to geoxml')
        os.unlink(os.path.join(tmpdir, 'gis.geoxml'))
    print("Create gis.itm.geoxml")
    try:
        geojson_to_geoxml(iterate_gis_jsonlines(tmpdir), os.path.join(tmpdir, 'gis.itm.geoxml'), itm=True)
    except FailedToConvertFeature as e:
        print(str(e))
        print('failed to convert feature to geoxml')
        os.unlink(os.path.join(tmpdir, 'gis.itm.geoxml'))


def update_resource(target_instance_name, package, format_, resource_name, file_path):
    print(f'updating resource {resource_name}...')
    if os.path.exists(file_path):
        existing_resource_id = None
        existing_resource_hash = None
        for resource in package['resources']:
            if resource['name'] == resource_name:
                existing_resource_id = resource['id']
                existing_resource_hash = resource.get('hash')
        new_resource_hash = hashlib.md5(open(file_path, 'rb').read()).hexdigest()
        if not existing_resource_id:
            print('no existing resource found, creating new resource')
            res = ckan.resource_create(target_instance_name, {
                'package_id': package['id'],
                'format': format_,
                'name': resource_name,
                'hash': new_resource_hash
            }, files=[('upload', open(file_path, 'rb'))])
            assert res['success'], str(res)
        elif existing_resource_hash != new_resource_hash:
            print('existing resource found, but hash is different, updating resource data')
            res = ckan.resource_update(target_instance_name, {
                'id': existing_resource_id,
                'hash': new_resource_hash
            }, files=[('upload', open(file_path, 'rb'))])
            assert res['success'], str(res)
        else:
            print('existing resource found, and hash is the same, skipping resource update')
    else:
        print(f'file {file_path} does not exist, skipping resource update')


def operator(name, params):
    gis_url = params['gis_url']
    target_instance_name = params['target_instance_name']
    target_package_id = params['target_package_id']
    target_organization_id = params['target_organization_id']
    tmpdir = params.get('tmpdir')
    with tempdir(tmpdir) as tmpdir:
        print('starting gis_fetcher operator')
        print(f'gis_url={gis_url} target_instance_name={target_instance_name} target_package_id={target_package_id} target_organization_id={target_organization_id}')
        print(f'tmpdir={tmpdir}')
        create_gis_data(gis_url, tmpdir)
        with open(os.path.join(tmpdir, 'gis.json'), 'r') as f:
            gis_json = json.load(f)
        name = gis_json['name']
        print(f'gis name={name}')
        package = ckan.package_show(target_instance_name, target_package_id)
        if not package:
            res = ckan.package_create(target_instance_name, {
                'name': target_package_id,
                'title': name,
                'owner_org': target_organization_id
            })
            assert res['success'], str(res)
            package = res['result']
        for format_, resource_name, file_name in [
            ('shapefile', 'SHP', 'shapefile.zip'),
            ('csv', 'CSV', 'gis.csv'),
            ('xlsx', 'XLSX', 'gis.xlsx'),
            ('geojson', 'GeoJSON', 'gis.geojson'),
            ('geojson', 'GeoJSON-ITM', 'gis.itm.geojson'),
            ('xml', 'XML', 'gis.xml'),
            ('kml', 'KML', 'gis.kml'),
            ('geoxml', 'GeoXML', 'gis.geoxml'),
            ('geoxml', 'GeoXML-ITM', 'gis.itm.geoxml'),
        ]:
            update_resource(target_instance_name, package, format_, resource_name, os.path.join(tmpdir, file_name))
    print('gis_fetcher operator completed successfully')


# python3 -m datacity_ckan_dgp.operators.gis_fetcher '{"gis_url": "https://gisserver.haifa.muni.il/arcgiswebadaptor/rest/services/PublicSite/Haifa_Eng_Public/MapServer/13", "target_instance_name": "LOCAL_DEVELOPMENT", "target_package_id": "yeudei_karka", "target_organization_id": "muni", "tmpdir": ".data/gis_fetcher_tmpdir"}'
if __name__ == '__main__':
    operator('_', json.loads(sys.argv[1]))
