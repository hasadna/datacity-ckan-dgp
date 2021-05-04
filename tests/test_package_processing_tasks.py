import os
import json
import tempfile

import dataflows as DF


def test_xlsx(patch_ckan, patch_package_processing_tasks_common):
    from datacity_ckan_dgp.package_processing_tasks.xlsx import process_resource
    instance_name = 'mock_instance'
    package = {
        'id': 'mock_package'
    }
    resource = {
        'url': './tests/data/tma-38.csv',
        'description': 'תמ"א 38',
        'name': 'tma-38.csv'
    }
    package_extras_processed_res = 'package_extras_processed_res'
    process_resource(instance_name, package, resource, package_extras_processed_res)
    assert len(patch_ckan.mock_calls) == 1
    assert patch_ckan.mock_calls[0][0] == 'resource_create'
    assert patch_ckan.mock_calls[0][1] == ('mock_instance', ({
        'description': 'תמ"א 38', 'format': 'XLSX', 'name': 'tma-38.xlsx', 'package_id': 'mock_package'
    }))
    assert list(patch_ckan.mock_calls[0][2].keys()) == ['files']
    assert list(patch_ckan.mock_calls[0][2]['files'].keys()) == ['upload']
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir, 'file.xlsx'), 'wb') as f:
            f.write(patch_ckan.mock_calls[0][2]['files']['upload'])
        rows = DF.Flow(DF.load(os.path.join(tmpdir, 'file.xlsx'))).results()[0][0]
        assert len(rows) == 862
        assert rows[861]['כתובת'] == 'הורקניה 10'
        assert rows[858]['יחד קימות'] == '20'
        assert rows[0]['Y'] == '220244'
    assert patch_package_processing_tasks_common.mock_calls == [('update_package_extras', (
        'mock_instance', {'id': 'mock_package'}, 'package_extras_processed_res'
    ), {})]


def test_geojson(patch_ckan, patch_package_processing_tasks_common):
    from datacity_ckan_dgp.package_processing_tasks.geojson import process_resource
    instance_name = 'mock_instance'
    package = {
        'id': 'mock_package'
    }
    resource = {
        'url': './tests/data/tma-38.csv',
        'description': 'תמ"א 38',
        'name': 'tma-38.csv',
        'geo_lat_field': 'y',
        'geo_lon_field': 'x',
    }
    package_extras_processed_res = 'package_extras_processed_res'
    process_resource(instance_name, package, resource, package_extras_processed_res)
    assert len(patch_ckan.mock_calls) == 1
    assert patch_ckan.mock_calls[0][0] == 'resource_create'
    assert patch_ckan.mock_calls[0][1] == ('mock_instance', ({
        'description': 'תמ"א 38', 'format': 'GeoJSON', 'name': 'tma-38.geojson', 'package_id': 'mock_package'
    }))
    assert list(patch_ckan.mock_calls[0][2].keys()) == ['files']
    assert list(patch_ckan.mock_calls[0][2]['files'].keys()) == ['upload']
    geojson = json.loads(patch_ckan.mock_calls[0][2]['files']['upload'])
    assert set(geojson.keys()) == {'features', 'type'}
    assert geojson['type'] == 'FeatureCollection'
    assert len(geojson['features']) == 859
    assert geojson['features'][858] == {
        'geometry': {
            'coordinates': [35.201775, 31.756339],
            'type': 'Point'
        },
        'properties': {
            'OBJECTID': 450981.0, 'Shape': None, 'X': None, 'Y': None,
            'x': 219269.82770000026, 'y': 629340.8712000009, 'הערה': None,
            'הריסה ובניה מחדש': 'לא', 'יחד קימות': 24.0, 'כתובת': 'הורקניה 10',
            'מספר תיק': 15507.0, 'סטטוס': 'תכנון - טרם כניסה לרישוי', 'סטטוס בניה': None,
            'רובע': None, 'שכונה': 'קטמון הישנה', 'תאריך סטטוס': '09/02/2021',
            'תאריך פתיחת תיק': None, 'תוספת יחד': 10.0
        }, 'type': 'Feature'
    }
    assert patch_package_processing_tasks_common.mock_calls == [('update_package_extras', (
        'mock_instance', {'id': 'mock_package'}, 'package_extras_processed_res'
    ), {})]
