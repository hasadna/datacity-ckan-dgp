import pytest
from unittest.mock import patch

from .mocks import ckan, package_processing_tasks_common


@pytest.fixture()
def patch_ckan():
    with patch.dict('sys.modules', {
        'datacity_ckan_dgp.ckan': ckan
    }):
        ckan.mock_calls = []
        yield ckan
        ckan.mock_calls = []


@pytest.fixture()
def patch_package_processing_tasks_common():
    with patch.dict('sys.modules', {
        'datacity_ckan_dgp.package_processing_tasks.common': package_processing_tasks_common
    }):
        package_processing_tasks_common.mock_calls = []
        yield package_processing_tasks_common
        package_processing_tasks_common.mock_calls = []
