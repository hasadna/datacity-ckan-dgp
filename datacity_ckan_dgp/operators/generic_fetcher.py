import os
import sys
import json
import tempfile
import contextlib
from importlib import import_module


# the source url will be checked against the following types in order to determine which type of source it is
FETCHERS = [
    {
        # python3 -m datacity_ckan_dgp.operators.generic_fetcher '{"source_url": "https://data.gov.il/dataset/automated-devices", "target_instance_name": "LOCAL_DEVELOPMENT", "target_package_id": "automated-devices", "target_organization_id": "israel-gov", "tmpdir": ".data/ckan_fetcher_tmpdir"}'
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
    target_instance_name = params['target_instance_name']
    target_package_id = params['target_package_id']
    target_organization_id = params['target_organization_id']
    tmpdir = params.get('tmpdir')
    with tempdir(tmpdir) as tmpdir:
        print('starting generic_fetcher operator')
        print(f'source_url={source_url} target_instance_name={target_instance_name} target_package_id={target_package_id} target_organization_id={target_organization_id}')
        print(f'tmpdir={tmpdir}')
        for fetcher in FETCHERS:
            assert fetcher['match'].keys() == {'url_contains'}, 'only url_contains match is supported at the moment'
            if fetcher['match']['url_contains'] in source_url:
                import_module(f'datacity_ckan_dgp.generic_fetchers.{fetcher["fetcher"]}_fetcher').fetch(source_url, target_instance_name, target_package_id, target_organization_id, tmpdir)
                break


# python3 -m datacity_ckan_dgp.operators.generic_fetcher '{"source_url": "https://data.gov.il/dataset/automated-devices", "target_instance_name": "LOCAL_DEVELOPMENT", "target_package_id": "automated-devices", "target_organization_id": "israel-gov", "tmpdir": ".data/ckan_fetcher_tmpdir"}'
if __name__ == '__main__':
    operator('_', json.loads(sys.argv[1]))
