import copy
import os
import sys
import json
import tempfile
import contextlib
from importlib import import_module


FETCHERS = [
    # fetchers are tried in order, the first one that matches is used
    {
        'fetcher': 'ckan_dataset',
        'match': {
            'url_contains': '/dataset/'
        }
    },
    # fallback fetcher that downloads whatever is in the source_url as-is and uploads it to the target instance
    # if it detects tabular data it will try to handle it as such
    {
        'fetcher': 'resource'
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
    print('starting generic_fetcher operator')
    print(json.dumps(params, ensure_ascii=False))
    params = copy.deepcopy(params)
    source_url = params.pop('source_url')
    source_filter = params.pop('source_filter', None)
    target_instance_name = params.pop('target_instance_name')
    target_package_id = params.pop('target_package_id')
    target_organization_id = params.pop('target_organization_id')
    tmpdir = params.pop('tmpdir', None)
    post_processing = params.pop('post_processing', None)
    with tempdir(tmpdir) as tmpdir:
        for fetcher in FETCHERS:
            if fetcher.get('match'):
                is_match = False
                if fetcher['match'].get('url_contains') and fetcher['match']['url_contains'] in source_url:
                    is_match = True
            else:
                is_match = True
            if is_match:
                import_module(f'datacity_ckan_dgp.generic_fetchers.{fetcher["fetcher"]}_fetcher').fetch(
                    source_url, target_instance_name, target_package_id, target_organization_id, tmpdir, source_filter, post_processing,
                    **params
                )
                break


if __name__ == '__main__':
    operator('_', json.loads(sys.argv[1]))
