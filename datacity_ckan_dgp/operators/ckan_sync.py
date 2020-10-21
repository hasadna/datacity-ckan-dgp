import os
import traceback
from collections import defaultdict
from contextlib import contextmanager
from tempfile import TemporaryDirectory

from datacity_ckan_dgp import ckan
from datacity_ckan_dgp import utils


@contextmanager
def _download_active_resources(package):
    with TemporaryDirectory() as tmpdir:
        source_resource_hashes = {}
        for i, resource in enumerate(package['resources']):
            if resource['state'] != 'active':
                continue
            source_resource_hashes[i] = utils.http_stream_download(os.path.join(tmpdir, 'resource{}'.format(i)), requests_kwargs={'url': resource['url']})
        yield tmpdir, source_resource_hashes


def _create_resources(source_package, tmpdir, target_instance_name, target_package_id, resource_hashes):
    for i, resource in enumerate(source_package['resources']):
        if resource['state'] != 'active':
            continue
        try:
            resource_filename = resource['url'].split('/')[-1]
            with open(os.path.join(tmpdir, 'resource{}'.format(i)), 'rb') as f:
                res = ckan.resource_create(target_instance_name, {
                    'package_id': target_package_id,
                    'url': resource_filename,
                    'description': resource['description'],
                    'format': resource['format'],
                    'name': resource['name'],
                    'hash': resource_hashes[i]
                }, files={
                    'upload': (resource_filename, f)
                })
            assert res['success'], 'create resource {} failed: {}'.format(i, res)
        except Exception as e:
            print('Failed to process resource {}: {}'.format(i, resource))
            raise


def _update_existing_package(source_package, target_instance_name, target_organization_id, target_package_name,
                             target_existing_package, stats):
    try:
        stats['packages_existing'] += 1
        package = {**target_existing_package}
        has_package_changes = False
        for attr in ['title', 'license_id', 'notes', 'url', 'version']:
            if package.get(attr) != source_package.get(attr):
                has_package_changes = True
                package[attr] = source_package.get(attr) or ''
        with _download_active_resources(source_package) as (tmpdir, source_resource_hashes):
            target_resource_hashes = {}
            for i, resource in enumerate(package['resources']):
                if resource['state'] != 'active':
                    continue
                target_resource_hashes[i] = resource['hash']
            has_resource_changes = len(source_resource_hashes) != len(target_resource_hashes)
            if not has_resource_changes:
                for i in source_resource_hashes:
                    if source_resource_hashes[i] != target_resource_hashes.get(i):
                        has_resource_changes = True
                    else:
                        try:
                            source_resource = source_package['resources'][i]
                            target_resource = target_existing_package['resources'][i]
                        except AttributeError:
                            source_resource = None
                            target_resource = None
                        if not source_resource or not target_resource:
                            has_resource_changes = True
                        else:
                            for attr in ['description', 'format', 'name']:
                                if source_resource.get(attr) != target_resource.get(attr):
                                    has_resource_changes = True
            if has_package_changes and not has_resource_changes:
                print('updating package, no resource changes ({})'.format(target_package_name))
                ckan.package_update(target_instance_name, package)
                stats['packages_existing_only_package_changes'] += 1
            elif has_resource_changes:
                print('updating package, with resource changes ({})'.format(target_package_name))
                package['private'] = True
                package['resources'] = []
                ckan.package_update(target_instance_name, package)
                _create_resources(source_package, tmpdir, target_instance_name, package['id'], source_resource_hashes)
                package = ckan.package_show(target_instance_name, target_package_name)
                package['private'] = False
                ckan.package_update(target_instance_name, package)
                stats['packages_existing_has_resource_changes'] += 1
    except Exception:
        print('exception updating existing package {}: {}'.format(target_package_name, target_existing_package))
        raise


def _create_new_package(source_package, target_instance_name, target_organization_id, target_package_name,
                        stats):
    print("Creating new package: {}".format(target_package_name))
    with _download_active_resources(source_package) as (tmpdir, resource_hashes):
        res = ckan.package_create(target_instance_name, {
            'name': target_package_name,
            'title': source_package['title'],
            'private': True,
            'license_id': source_package['license_id'],
            'notes': source_package['notes'],
            'url': source_package['url'],
            'version': source_package['version'],
            'owner_org': target_organization_id
        })
        assert res['success'], 'create package failed: {}'.format(res)
        target_package_id = res['result']['id']
        _create_resources(source_package, tmpdir, target_instance_name, target_package_id, resource_hashes)
        package = ckan.package_show(target_instance_name, target_package_id)
        package['private'] = False
        res = ckan.package_update(target_instance_name, package)
        assert res['success'], 'failed to set package to public: {}'.format(res)
    stats['packages_new_created'] += 1


def operator(name, params):
    source_instance_name = params['source_instance_name']
    target_instance_name = params['target_instance_name']
    target_organization_id = params['target_organization_id']
    target_package_name_prefix = params['target_package_name_prefix']
    print('starting ckan_sync operator')
    print('source_instance_name={} target_instance_name={} target_organization_id={} target_package_name_prefix={}'.format(
        source_instance_name, target_instance_name, target_organization_id, target_package_name_prefix))
    stats = defaultdict(int)
    for source_package_name in ckan.package_list_public(source_instance_name):
        source_package = None
        try:
            if source_package_name.startswith(target_package_name_prefix):
                stats['source_packages_invalid_prefix'] += 1
                continue
            source_package = ckan.package_show_public(source_instance_name, source_package_name)
            if source_package['private'] or source_package['state'] != 'active' or source_package['type'] != 'dataset':
                stats['source_packages_invalid_attrs'] += 1
                continue
            stats['source_packages_valid'] += 1
            target_package_name = 'dgpsync_{}'.format(source_package_name)
            target_existing_package = ckan.package_show(target_instance_name, target_package_name)
            if target_existing_package and target_existing_package['state'] != 'deleted':
                _update_existing_package(source_package, target_instance_name, target_organization_id, target_package_name,
                                         target_existing_package, stats)
            else:
                _create_new_package(source_package, target_instance_name, target_organization_id, target_package_name,
                                    stats)
            if stats['source_packages_valid'] % 10 == 0:
                print(dict(stats))
        except Exception:
            traceback.print_exc()
            print('exception processing source package {}: {}'.format(source_package_name, source_package))
    print(dict(stats))


if __name__ == '__main__':
    import sys
    import json
    operator('_', json.loads(sys.argv[1]))
