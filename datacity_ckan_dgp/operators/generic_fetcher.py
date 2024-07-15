import os
import sys
import json
import tempfile
import contextlib
from importlib import import_module


FETCHERS = [
    {
        'fetcher': 'ckan_dataset',
        'match': {
            'url_contains': '/dataset/'
        }
    }
]


@contextlib.contextmanager
def tempdir(tmpdir):
    if tmpdir:
        os.makedirs(tmpdir, exist_ok=True)
        yield tmpdir
    else:
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir


def operator(name, params):
    source_url = params['source_url']
    source_filter = params.get('source_filter')
    target_instance_name = params['target_instance_name']
    target_package_id = params['target_package_id']
    target_organization_id = params['target_organization_id']
    tmpdir = params.get('tmpdir')
    with tempdir(tmpdir) as tmpdir:
        print('starting generic_fetcher operator')
        print(json.dumps(params))
        for fetcher in FETCHERS:
            assert fetcher['match'].keys() == {'url_contains'}, 'only url_contains match is supported at the moment'
            if fetcher['match']['url_contains'] in source_url:
                import_module(f'datacity_ckan_dgp.generic_fetchers.{fetcher["fetcher"]}_fetcher').fetch(source_url, target_instance_name, target_package_id, target_organization_id, tmpdir, source_filter)
                break


if __name__ == '__main__':
    operator('_', json.loads(sys.argv[1]))
