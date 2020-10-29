from decimal import Decimal

import dataflows as DF

import pyproj
import geojson
from geojson import Feature, Point, FeatureCollection

from dags.operators.dgp_kind.fileloader import bucket

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

CRS = '+ellps=GRS80 +k=1.00007 +lat_0=31.73439361111111 +lon_0=35.20451694444445 +no_defs +proj=tmerc +units=m +x_0=219529.584 +y_0=626907.39'

projector = pyproj.Proj(CRS)

def fixer(row):
    for k, v in row.items():
        if isinstance(v, str):
            for w in ('_x000d_', '_x000D_'):
                if w in v:
                    row[k] = v.replace(w, ' ')
            row[k] = row[k].strip()
        
def category():
    
    CATEGORIES = {
        'אולם מופעים, היכל תרבות' : 'תרבות',
        'אולם ספורט, התעמלות' : 'ספורט',
        'איצטדיון' : 'ספורט',
        'בית כנסת' : 'דת',
        'בית ספר' : 'חינוך',
        'בריכת שחייה ציבורית או עירונית' : 'ספורט',
        'גן ילדים' : 'חינוך',
        'טיפת חלב' : 'בריאות',
        'לשכת רווחה של הרשות המקומית' : 'רווחה',
        'מבנה עירייה' : 'כלליים',
        'מגרש ספורט' : 'ספורט',
        'מגרש ציבורי פנוי' : 'כלליים',
        'מועדון נוער' : 'קהילה',
        'מועדון קהילתי כולל מרכז צעירים' : 'קהילה',
        'מועדון קשישים, מרכז לאזרחים ותיקים,מרכז יום לקשישים' : 'קהילה',
        'מעון יום' : 'חינוך',
        'מקווה טוהרה' : 'דת',
        'מקלט' : 'כלליים',
        'מרפאה' : 'בריאות',
        'ספרייה' : 'תרבות',
        'פנימייה, כפר נוער' : 'חינוך',
    }
    
    def cat(row):
        row['category'] = CATEGORIES[row['kind']]
    
    return DF.Flow(
        DF.add_field('category', 'string'),
        cat
    )

def geo():
    
    def proj(row):
        row['lon'], row['lat'] = projector(row['lon'], row['lat'], inverse=True)
    return DF.Flow(
        proj,
        DF.set_type('lon', type='number'),
        DF.set_type('lat', type='number'),
    )

def loader(name, cat):
    return DF.Flow(
        DF.load('mosadot.xlsx'),
        DF.concatenate(dict(
            municipality=['מועצה אזורית'],
            town=['שם יישוב'],
            name=['שם המוסד'],
            kind=['סוג המוסד'],
            address=['כתובת'],
            status=['סטטוס'],
            target_audience=['קהל יעד'],
            area=['שטח'],
            lat=['Y'],
            lon=['X'],
        )),
        fixer,
        category(),
        DF.filter_rows(lambda r: r['category'] == cat),
        geo(),
    #     DF.join_with_self('concat', ['kind'], dict(kind=None)),
        DF.update_resource(-1, name=name, path=name + '.csv'),
        DF.dump_to_path(name),
    ).results()[0][0]

  
def operator(name, params):
    mosadot_filename = params['mosadot_filename']
    target_instance_name = params['target_instance_name']
    target_organization_id = params['target_organization_id']
    print('starting ckan_sync operator')
    print('target_instance_name={} target_organization_id={} mosadot_filename={}'.format(
           target_instance_name,   target_organization_id,   mosadot_filename))

    try:
        obj = bucket().Object(mosadot_filename)
        obj.download_file(Filename='mosadot.xlsx')
    except:
        print('Failed to download mosadot file')

    for name, cat in [
        ('culture', 'תרבות'),
        ('sports', 'ספורט'),
        ('religion', 'דת'),
        ('education', 'חינוך'),
        ('welfare', 'רווחה'),
        ('general', 'כלליים'),
        ('community', 'קהילה'),
    ]:
        name = name + '_institutions'
        rows = loader(name, cat)
        fc = FeatureCollection([
            Feature(geometry=Point((float(r.pop('lon')), float(r.pop('lat')))),
                    properties=dict((k, float(v) if isinstance(v, Decimal) else v) for k, v in r.items()))
            for r in rows
        ])
        geojson_filename = name + '.geojson'
        csv_filename = name + '/' + name + '.csv' 
        dataset_name = name
        geojson.dump(fc, open(geojson_filename, 'w'))
        update_package(
            target_instance_name,
            target_organization_id,
            dataset_name,
            f'רשימת מוסדות {cat} ברשות',
            [('CSV', csv_filename), ('GeoJSON', geojson_filename)]
        )


if __name__ == '__main__':
    import sys
    import json
    exit(0 if operator('_', json.loads(sys.argv[1])) else 1)
