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
            if resource['url']:
                source_resource_hashes[i] = utils.http_stream_download(os.path.join(tmpdir, 'resource{}'.format(i)), requests_kwargs={'url': resource['url']})
            else:
                source_resource_hashes[i] = None
        yield tmpdir, source_resource_hashes


def _create_resources(source_package, tmpdir, target_instance_name, target_package_id, resource_hashes):
    for i, resource in enumerate(source_package['resources']):
        if resource['state'] != 'active':
            continue
        try:
            resource_filename = resource['url'].split('/')[-1] if resource_hashes[i] else None
            resource_kwargs = {
                'package_id': target_package_id,
                'description': resource['description'],
                'format': resource['format'],
                'name': resource['name'],
            }
            if resource_filename:
                with open(os.path.join(tmpdir, 'resource{}'.format(i)), 'rb') as f:
                    res = ckan.resource_create(target_instance_name, {
                        **resource_kwargs,
                        'url': resource_filename,
                        'hash': resource_hashes[i]
                    }, files={
                        'upload': (resource_filename, f)
                    })
            else:
                res = ckan.resource_create(target_instance_name, resource_kwargs)
            assert res['success'], 'create resource {} failed: {}'.format(i, res)
        except Exception as e:
            print('Failed to process resource {}: {}'.format(i, resource))
            raise


def _update_existing_package(source_instance_name, source_package, target_instance_name, target_organization_id, target_package_name,
                             target_existing_package, target_package_title_prefix, stats):
    try:
        stats['packages_existing'] += 1
        package = {**target_existing_package}
        has_package_changes = False
        for attr in ['license_id', 'notes', 'url', 'version']:
            if package.get(attr) != source_package.get(attr):
                has_package_changes = True
                package[attr] = source_package.get(attr) or ''
        _, source_instance_url = ckan.get_instance_api_key_url(source_instance_name)
        source_package_url = source_instance_url.strip('/') + '/dataset/{}'.format(source_package['name'])
        source_org_description = source_package.get('organization', {}).get('description', '')
        new_target_package_title = source_package['title']
        if target_package_title_prefix:
            new_target_package_title = '{} {}'.format(target_package_title_prefix, new_target_package_title)
        if package['title'] != new_target_package_title:
            has_package_changes = True
            package['title'] = new_target_package_title
        got_extra_url, got_extra_description = False, False
        for extra in (package.get('extras') or []):
            if extra["key"] == "sync_source_package_url":
                got_extra_url = True
                if extra["value"] != source_package_url:
                    has_package_changes = True
                    extra["value"] = source_package_url
            elif extra["key"] == "sync_source_org_description":
                got_extra_description = True
                if extra["value"] != source_org_description:
                    has_package_changes = True
                    extra["value"] = source_org_description
        if not got_extra_url:
            has_package_changes = True
            package.setdefault('extras', []).append({"key": "sync_source_package_url", "value": source_package_url})
        if not got_extra_description:
            has_package_changes = True
            package.setdefault('extras', []).append({"key": "sync_source_org_description", "value": source_org_description})
        with _download_active_resources(source_package) as (tmpdir, source_resource_hashes):
            target_resource_hashes = {}
            for i, resource in enumerate(package['resources']):
                if resource['state'] != 'active':
                    continue
                target_resource_hashes[i] = resource['hash']
            has_resource_changes = len(source_resource_hashes) != len(target_resource_hashes)
            if not has_resource_changes:
                for i in source_resource_hashes:
                    if (source_resource_hashes[i] or '') != (target_resource_hashes.get(i) or ''):
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
                print('updating package, no resource changes ({} > {})'.format(source_package['name'], target_package_name))
                ckan.package_update(target_instance_name, package)
                stats['packages_existing_only_package_changes'] += 1
            elif has_resource_changes:
                print('updating package, with resource changes ({} > {})'.format(source_package['name'], target_package_name))
                package['private'] = True
                package['resources'] = []
                ckan.package_update(target_instance_name, package)
                _create_resources(source_package, tmpdir, target_instance_name, package['id'], source_resource_hashes)
                package = ckan.package_show(target_instance_name, target_package_name)
                package['private'] = False
                ckan.package_update(target_instance_name, package)
                stats['packages_existing_has_resource_changes'] += 1
            else:
                stats['packages_existing_no_changes'] += 1
    except Exception:
        print('exception updating existing package {}: {}'.format(target_package_name, target_existing_package))
        raise


def _create_new_package(source_instance_name, source_package, target_instance_name, target_organization_id, target_package_name,
                        target_package_title_prefix, stats):
    print("Creating new package ({} > {})".format(source_package['name'], target_package_name))
    _, source_instance_url = ckan.get_instance_api_key_url(source_instance_name)
    source_package_url = source_instance_url.strip('/') + '/dataset/{}'.format(source_package['name'])
    source_org_description = source_package.get('organization', {}).get('description', '')
    target_package_title = source_package['title']
    if target_package_title_prefix:
        target_package_title = '{} {}'.format(target_package_title_prefix, target_package_title)
    with _download_active_resources(source_package) as (tmpdir, resource_hashes):
        res = ckan.package_create(target_instance_name, {
            'name': target_package_name,
            'title': target_package_title,
            'private': True,
            'license_id': source_package['license_id'],
            'notes': source_package['notes'],
            'url': source_package['url'],
            'version': source_package['version'],
            'owner_org': target_organization_id,
            'extras': [{"key": "sync_source_package_url", "value": source_package_url},
                       {"key": "sync_source_org_description", "value": source_org_description}]
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
    target_package_title_prefix = params['target_package_title_prefix']
    print('starting ckan_sync operator')
    print('source_instance_name={} target_instance_name={} target_organization_id={} target_package_name_prefix={} target_package_title_prefix={}'.format(
        source_instance_name, target_instance_name, target_organization_id, target_package_name_prefix, target_package_title_prefix))
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
            target_package_name = '{}{}'.format(target_package_name_prefix, source_package_name)
            target_existing_package = ckan.package_show(target_instance_name, target_package_name)
            if target_existing_package and target_existing_package['state'] != 'deleted':
                _update_existing_package(source_instance_name, source_package, target_instance_name, target_organization_id, target_package_name,
                                         target_existing_package, target_package_title_prefix, stats)
            else:
                _create_new_package(source_instance_name, source_package, target_instance_name, target_organization_id, target_package_name,
                                    target_package_title_prefix, stats)
            if stats['source_packages_valid'] % 10 == 0:
                print(dict(stats))
        except Exception:
            traceback.print_exc()
            print('exception processing source package {}: {}'.format(source_package_name, source_package))
            stats['source_packages_exceptions'] += 1
    print(dict(stats))
    return stats['source_packages_exceptions'] == 0


if __name__ == '__main__':
    import sys
    import json
    exit(0 if operator('_', json.loads(sys.argv[1])) else 1)
